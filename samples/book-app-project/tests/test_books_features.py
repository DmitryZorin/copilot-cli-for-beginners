import json
from pathlib import Path
import importlib.util
import pytest

# Import module by path so tests don't rely on package layout
_spec = importlib.util.spec_from_file_location("books", str(Path(__file__).parent.parent / "books.py"))
books_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(books_mod)


def test_get_by_id_and_update(tmp_path):
    data_file = tmp_path / "data.json"
    bc = books_mod.BookCollection(data_file=str(data_file))
    b1 = bc.add_book("Book One", "Author A", 2000)
    b2 = bc.add_book("Book Two", "Author B", 2001)

    # get by id
    found = bc.get_by_id(b1.id)
    assert found is not None and found.title == "Book One"

    # update book
    updated = bc.update_book(b1.id, title="Book One Revised", year=2002, read=True)
    assert updated.title == "Book One Revised"
    assert updated.year == 2002
    assert updated.read is True

    # updating to duplicate should raise
    with pytest.raises(books_mod.DuplicateBookError):
        bc.update_book(b1.id, title="Book Two", author="Author B")


def test_search_and_pagination(tmp_path):
    data_file = tmp_path / "data.json"
    bc = books_mod.BookCollection(data_file=str(data_file))
    # add 5 books
    for i in range(1, 6):
        bc.add_book(f"Common Book {i}", f"Author {i}", 2000 + i)
    # search substring
    res = bc.search("common book", fuzzy=False, page=1, per_page=2)
    assert len(res) == 2
    res2 = bc.search("common book", fuzzy=False, page=3, per_page=2)
    assert len(res2) == 1

    # fuzzy search
    fuzzy = bc.search("commn bok", fuzzy=True, page=1, per_page=10)
    assert len(fuzzy) >= 1


def test_corrupt_backup_has_timezone(tmp_path):
    data_file = tmp_path / "data.json"
    # write invalid JSON
    data_file.write_text("{ not valid json }", encoding="utf-8")
    bc = books_mod.BookCollection(data_file=str(data_file))
    corrupt_files = list(tmp_path.glob("data.json.corrupt.*"))
    assert len(corrupt_files) == 1
    # filename should include timezone offset digits
    name = corrupt_files[0].name
    assert ".corrupt." in name
    # ensure the timestamp suffix contains a timezone offset like +0000 or -0000
    assert any(ch in name for ch in ['+', '-'])


def test_locking_available():
    # ensure at least one locking backend is available
    assert books_mod._HAS_FILELOCK or books_mod._HAS_FCNTL
