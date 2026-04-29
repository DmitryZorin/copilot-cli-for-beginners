import json
import logging
import os
import tempfile
import uuid
import time
from dataclasses import dataclass, asdict, field
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Iterator

# Use POSIX flock when available for simple file locking
try:
    import fcntl
    _HAS_FCNTL = True
except Exception:
    _HAS_FCNTL = False

# Default data file lives next to this module but can be overridden per-collection
DATA_FILE = Path(__file__).parent / "data.json"

logger = logging.getLogger(__name__)

SCHEMA_VERSION = 1


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

    def __post_init__(self):
        if not self.title or not self.title.strip():
            raise InvalidBookError("title must not be empty")
        if not self.author or not self.author.strip():
            raise InvalidBookError("author must not be empty")
        if not isinstance(self.year, int) or self.year < 0:
            raise InvalidBookError("year must be a non-negative integer")

    def to_dict(self) -> dict:
        return {"id": self.id, "title": self.title, "author": self.author, "year": self.year, "read": self.read}

    @classmethod
    def from_dict(cls, data: dict) -> "Book":
        return cls(id=data.get("id", str(uuid.uuid4())), title=data["title"], author=data["author"], year=int(data["year"]), read=bool(data.get("read", False)))


class _FileLock:
    """Context manager for simple POSIX file locking. No-op on platforms without fcntl."""

    def __init__(self, path: Path, exclusive: bool = True):
        self.path = path
        self.exclusive = exclusive
        self._fd = None

    def __enter__(self):
        if not _HAS_FCNTL:
            return self
        # Ensure directory exists
        self.path.parent.mkdir(parents=True, exist_ok=True)
        # Open (or create) the lock file
        self._fd = open(self.path, "a+")
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


class BookCollection:
    """Manage a collection of Book objects persisted as versioned JSON.

    Features:
    - Unique id for each book (UUID)
    - Custom exceptions for error cases
    - Atomic writes with temp-file + os.replace
    - POSIX file locking when available
    - Batch context manager to reduce disk writes
    - Versioned JSON schema for future migrations
    """

    def __init__(self, data_file: Optional[str] = None):
        # Ensure data_file is always a Path regardless of how DATA_FILE is patched in tests
        self.data_file: Path = Path(data_file) if data_file is not None else Path(DATA_FILE)
        self._lock_file: Path = self.data_file.with_suffix(self.data_file.suffix + ".lock")
        self.books: List[Book] = []
        self._in_batch = False
        self._dirty = False
        self.load_books()

    def _normalize(self, value: str) -> str:
        return value.strip().lower()

    def load_books(self):
        """Load books from the JSON file if it exists.

        File format (v1): {"version": 1, "books": [ {book}, ... ]}
        Legacy format (list of book dicts) is supported for backwards compatibility.
        If the file is corrupted it is renamed with a timestamped .corrupt suffix.
        """
        if not self.data_file.exists():
            self.books = []
            return

        # Acquire shared lock for reading
        try:
            with _FileLock(self._lock_file, exclusive=False):
                with self.data_file.open("r", encoding="utf-8") as f:
                    raw = json.load(f)
        except json.JSONDecodeError:
            ts = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
            backup = self.data_file.with_suffix(self.data_file.suffix + f".corrupt.{ts}")
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

        # Parse different schema shapes
        if isinstance(raw, dict) and raw.get("version") == SCHEMA_VERSION and "books" in raw:
            items = raw["books"]
        elif isinstance(raw, list):
            items = raw
        else:
            logger.warning("Unknown data format in %s, starting empty", self.data_file)
            self.books = []
            return

        try:
            self.books = [Book.from_dict(b) for b in items]
        except Exception:
            logger.exception("Failed to parse books from %s", self.data_file)
            self.books = []

    def _atomic_write(self):
        """Write books to disk atomically with locking."""
        tmp_path = None
        try:
            dirpath = self.data_file.parent
            dirpath.mkdir(parents=True, exist_ok=True)

            payload = {"version": SCHEMA_VERSION, "books": [b.to_dict() for b in self.books]}

            # Acquire exclusive lock while writing
            with _FileLock(self._lock_file, exclusive=True):
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

    def save_books(self):
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

    # Batch context manager
    def batch(self) -> Iterator["BookCollection"]:
        """Context manager that defers writes until exit. Usage: with bc.batch(): bc.add_book(...);"""
        class _BatchCtx:
            def __init__(self, outer: "BookCollection"):
                self._outer = outer

            def __enter__(self):
                self._outer._in_batch = True
                self._outer._dirty = False
                return self._outer

            def __exit__(self, exc_type, exc, tb):
                try:
                    if exc_type is None and self._outer._dirty:
                        self._outer._atomic_write()
                finally:
                    self._outer._in_batch = False
                    self._outer._dirty = False

        return _BatchCtx(self)
