"""Book collection module.

This module provides a small JSON-backed collection of books with safe
atomic writes, cross-platform file locking (via the filelock package when
available), schema versioning, and a convenient API.

Example CLI usage:

    from samples.book_app_project.books import BookCollection
    bc = BookCollection()
    bc.add_book("The Hobbit", "J.R.R. Tolkien", 1937)
    for book in bc:
        print(book.title, book.author)

"""

import json
import logging
import os
import tempfile
import uuid
from dataclasses import dataclass, asdict, field
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Iterator, ContextManager, Iterable
import difflib

# Try to use filelock (cross-platform). If unavailable, fall back to POSIX fcntl locking.
try:
    from filelock import FileLock as _FileLockLib  # type: ignore
    _HAS_FILELOCK = True
except Exception:
    _HAS_FILELOCK = False

try:
    import fcntl
    _HAS_FCNTL = True
except Exception:
    _HAS_FCNTL = False

# Default data file lives next to this module but can be overridden per-collection
DATA_FILE = Path(__file__).parent / "data.json"

logger = logging.getLogger(__name__)

SCHEMA_VERSION = 2


class BookError(Exception):
    pass


class DuplicateBookError(BookError):
    pass


class BookNotFoundError(BookError):
    pass


class InvalidBookError(BookError):
    pass


@dataclass
class Book:
    title: str
    author: str
    year: int
    read: bool = False
    id: str = field(default_factory=lambda: str(uuid.uuid4()))

    def __post_init__(self) -> None:
        current_year = datetime.now(timezone.utc).year
        if not self.title or not self.title.strip():
            raise InvalidBookError("title must not be empty")
        if not self.author or not self.author.strip():
            raise InvalidBookError("author must not be empty")
        if not isinstance(self.year, int) or self.year < 0 or self.year > current_year:
            raise InvalidBookError(f"year must be a non-negative integer <= {current_year}")

    def to_dict(self) -> dict:
        return {"id": self.id, "title": self.title, "author": self.author, "year": self.year, "read": self.read}

    @classmethod
    def from_dict(cls, data: dict) -> "Book":
        return cls(id=data.get("id", str(uuid.uuid4())), title=data["title"], author=data["author"], year=int(data["year"]), read=bool(data.get("read", False)))


class _FlockFallback:
    """Context manager using POSIX fcntl for locking when filelock isn't available."""

    def __init__(self, lock_path: Path, exclusive: bool = True):
        self._lock_path = lock_path
        self._fd = None
        self.exclusive = exclusive

    def __enter__(self):
        if not _HAS_FCNTL:
            return self
        # Ensure directory exists
        self._lock_path.parent.mkdir(parents=True, exist_ok=True)
        self._fd = open(self._lock_path, "a+")
        try:
            if self.exclusive:
                fcntl.flock(self._fd.fileno(), fcntl.LOCK_EX)
            else:
                fcntl.flock(self._fd.fileno(), fcntl.LOCK_SH)
        except Exception:
            self._fd.close()
            raise
        return self

    def __exit__(self, exc_type, exc, tb):
        if not _HAS_FCNTL:
            return
        try:
            fcntl.flock(self._fd.fileno(), fcntl.LOCK_UN)
        finally:
            try:
                self._fd.close()
            except Exception:
                pass


def _choose_lock(lock_path: Path, exclusive: bool = True) -> ContextManager:
    """Return a context manager implementing file locking.

    Prefers filelock if installed; falls back to POSIX flock when available; otherwise no-op.
    """
    if _HAS_FILELOCK:
        return _FileLockLib(str(lock_path), timeout=10)
    if _HAS_FCNTL:
        return _FlockFallback(lock_path, exclusive=exclusive)
    # no-op context manager
    class _NoOp:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    return _NoOp()


def _timestamped_corrupt_name(path: Path) -> Path:
    ts = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S%z")
    return path.with_suffix(path.suffix + f".corrupt.{ts}")


def migrate_payload(raw: object) -> dict:
    """Normalize or migrate various payload shapes to the current schema format.

    Returns a dict {'version': SCHEMA_VERSION, 'books': [...]}
    """
    if isinstance(raw, dict) and raw.get("version") == SCHEMA_VERSION and "books" in raw:
        return raw
    if isinstance(raw, dict) and "version" in raw and raw.get("version") < SCHEMA_VERSION:
        # Future migration steps would run here
        logger.info("Migrating data from version %s to %s", raw.get("version"), SCHEMA_VERSION)
        # Simple no-op migration for now
        return {"version": SCHEMA_VERSION, "books": raw.get("books", [])}
    if isinstance(raw, list):
        return {"version": SCHEMA_VERSION, "books": raw}
    # Unknown format
    raise ValueError("Unsupported data format for migration")


__all__ = ["Book", "BookCollection", "DuplicateBookError", "BookNotFoundError", "InvalidBookError"]


class BookCollection(Iterable[Book]):
    """Manage a collection of Book objects persisted as versioned JSON.

    Features:
    - Unique id for each book (UUID)
    - Custom exceptions for error cases
    - Atomic writes with temp-file + os.replace
    - Cross-platform file locking via filelock when available
    - Batch context manager to reduce disk writes
    - Versioned JSON schema with migration helpers
    - Search and pagination helpers
    """

    def __init__(self, data_file: Optional[str] = None):
        # Ensure data_file is always a Path regardless of how DATA_FILE is patched in tests
        self.data_file: Path = Path(data_file) if data_file is not None else Path(DATA_FILE)
        self._lock_file: Path = self.data_file.with_suffix(self.data_file.suffix + ".lock")
        self.books: List[Book] = []
        self._in_batch = False
        self._dirty = False
        self.load_books()

    def __iter__(self) -> Iterator[Book]:
        return iter(self.books)

    def _normalize(self, value: str) -> str:
        return value.strip().lower()

    def load_books(self) -> None:
        """Load books from the JSON file if it exists.

        If the file is corrupted it is renamed with a timestamped .corrupt suffix.
        """
        if not self.data_file.exists():
            self.books = []
            return

        # Acquire shared lock for reading
        try:
            with _choose_lock(self._lock_file, exclusive=False):
                with self.data_file.open("r", encoding="utf-8") as f:
                    raw = json.load(f)
        except json.JSONDecodeError:
            backup = _timestamped_corrupt_name(self.data_file)
            try:
                os.replace(self.data_file, backup)
                logger.warning("Corrupted data moved to %s", backup)
            except Exception:
                logger.exception("Failed to move corrupted data file %s", self.data_file)
            self.books = []
            return
        except FileNotFoundError:
            self.books = []
            return
        except Exception:
            logger.exception("Unexpected error loading books from %s", self.data_file)
            self.books = []
            return

        # Migrate/normalize payload
        try:
            payload = migrate_payload(raw)
        except Exception:
            logger.exception("Unknown payload format in %s", self.data_file)
            self.books = []
            return

        try:
            self.books = [Book.from_dict(b) for b in payload.get("books", [])]
        except Exception:
            logger.exception("Failed to parse books from %s", self.data_file)
            self.books = []

    def _atomic_write(self) -> None:
        """Write books to disk atomically with locking."""
        tmp_path: Optional[Path] = None
        try:
            dirpath = self.data_file.parent
            dirpath.mkdir(parents=True, exist_ok=True)

            payload = {"version": SCHEMA_VERSION, "books": [b.to_dict() for b in self.books]}

            # Acquire exclusive lock while writing
            with _choose_lock(self._lock_file, exclusive=True):
                with tempfile.NamedTemporaryFile("w", dir=str(dirpath), delete=False, encoding="utf-8") as tf:
                    json.dump(payload, tf, indent=2, ensure_ascii=False)
                    tf.flush()
                    os.fsync(tf.fileno())
                    tmp_path = Path(tf.name)
                os.replace(tmp_path, self.data_file)
        except Exception:
            logger.exception("Failed to write books to %s", self.data_file)
            if tmp_path and tmp_path.exists():
                try:
                    tmp_path.unlink()
                except Exception:
                    logger.debug("Failed to remove temp file %s", tmp_path)
            raise

    def save_books(self) -> None:
        """Persist current collection to the configured JSON file."""
        if self._in_batch:
            self._dirty = True
            return
        self._atomic_write()
        self._dirty = False

    def add_book(self, title: str, author: str, year: int) -> Book:
        """Add a new book. Raises InvalidBookError or DuplicateBookError on error."""
        title_clean = title.strip()
        author_clean = author.strip()

        # Validation is also performed by Book, so constructing will raise InvalidBookError if invalid
        candidate = Book(title=title_clean, author=author_clean, year=year)

        # Prevent exact-duplicate (title+author) entries
        for b in self.books:
            if self._normalize(b.title) == self._normalize(candidate.title) and self._normalize(b.author) == self._normalize(candidate.author):
                raise DuplicateBookError("book already exists in the collection")

        self.books.append(candidate)
        self.save_books()
        return candidate

    def list_books(self) -> List[Book]:
        """Return a shallow copy of the in-memory list of books."""
        return list(self.books)

    def get_by_id(self, book_id: str) -> Optional[Book]:
        """Return a book by its unique id."""
        for b in self.books:
            if b.id == book_id:
                return b
        return None

    def update_book(self, book_id: str, title: Optional[str] = None, author: Optional[str] = None, year: Optional[int] = None, read: Optional[bool] = None) -> Book:
        """Update fields of a book by id. Raises BookNotFoundError or DuplicateBookError/InvalidBookError."""
        book = self.get_by_id(book_id)
        if not book:
            raise BookNotFoundError(book_id)

        new_title = title.strip() if title is not None else book.title
        new_author = author.strip() if author is not None else book.author
        new_year = year if year is not None else book.year
        new_read = read if read is not None else book.read

        # Validate new values by creating a temporary Book (will raise InvalidBookError if invalid)
        temp = Book(title=new_title, author=new_author, year=new_year, read=new_read, id=book.id)

        # Check duplicates (excluding the book being updated)
        for b in self.books:
            if b.id == book.id:
                continue
            if self._normalize(b.title) == self._normalize(temp.title) and self._normalize(b.author) == self._normalize(temp.author):
                raise DuplicateBookError("update would duplicate another book")

        # Apply updates
        book.title = temp.title
        book.author = temp.author
        book.year = temp.year
        book.read = temp.read
        self.save_books()
        return book

    def find_book_by_title(self, title: str) -> Optional[Book]:
        """Find the first book with a matching title (case- and surrounding-space-insensitive)."""
        target = self._normalize(title)
        for book in self.books:
            if self._normalize(book.title) == target:
                return book
        return None

    def mark_as_read(self, title: str) -> bool:
        """Mark a book as read by title. Returns True if found and updated."""
        book = self.find_book_by_title(title)
        if not book:
            return False
        if not book.read:
            book.read = True
            self.save_books()
        return True

    def remove_book(self, title: str) -> bool:
        """Remove a book by title. Returns True when a book was removed."""
        book = self.find_book_by_title(title)
        if not book:
            return False
        self.books.remove(book)
        self.save_books()
        return True

    def find_by_author(self, author: str) -> List[Book]:
        """Find all books by a given author (case- and surrounding-space-insensitive)."""
        target = self._normalize(author)
        return [b for b in self.books if self._normalize(b.author) == target]

    def search(self, query: str, fuzzy: bool = False, page: int = 1, per_page: int = 20) -> List[Book]:
        """Search books by title or author. Supports fuzzy matching and pagination.

        - fuzzy=False: substring (case-insensitive)
        - fuzzy=True: uses difflib.SequenceMatcher on combined title+author
        """
        q = query.strip().lower()
        if not q:
            return []

        results: List[Book] = []
        if not fuzzy:
            for b in self.books:
                if q in b.title.lower() or q in b.author.lower():
                    results.append(b)
        else:
            # Score each book by best match ratio against title and author
            scored: List[tuple[float, Book]] = []
            for b in self.books:
                title_score = difflib.SequenceMatcher(None, q, b.title.lower()).ratio()
                author_score = difflib.SequenceMatcher(None, q, b.author.lower()).ratio()
                score = max(title_score, author_score)
                if score > 0.4:
                    scored.append((score, b))
            scored.sort(key=lambda t: t[0], reverse=True)
            results = [t[1] for t in scored]

        # Pagination
        start = (page - 1) * per_page
        end = start + per_page
        return results[start:end]

    # Batch context manager
    def batch(self) -> ContextManager["BookCollection"]:
        """Context manager that defers writes until exit. Usage: with bc.batch(): bc.add_book(...);"""
        class _BatchCtx:
            def __init__(self, outer: "BookCollection"):
                self._outer = outer

            def __enter__(self) -> "BookCollection":
                self._outer._in_batch = True
                self._outer._dirty = False
                return self._outer

            def __exit__(self, exc_type, exc, tb) -> None:
                try:
                    if exc_type is None and self._outer._dirty:
                        self._outer._atomic_write()
                finally:
                    self._outer._in_batch = False
                    self._outer._dirty = False

        return _BatchCtx(self)
