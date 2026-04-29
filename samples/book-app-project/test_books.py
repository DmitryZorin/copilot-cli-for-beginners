import json
from pathlib import Path
import importlib.util
import pytest

# Import module by path so tests don't rely on package layout
_spec = importlib.util.spec_from_file_location("books", str(Path(__file__).parent / "books.py"))
books_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(books_mod)


def test_add_and_list(tmp_path):
    data_file = tmp_path / "data.json"
    bc = books_mod.BookCollection(data_file=str(data_file))
    b = bc.add_book("The Hobbit", "J.R.R. Tolkien", 1937)
    assert b.id
    assert b.title == "The Hobbit"
    assert len(bc.list_books()) == 1


def test_duplicate_raises(tmp_path):
    data_file = tmp_path / "data.json"
    bc = books_mod.BookCollection(data_file=str(data_file))
    bc.add_book("1984", "George Orwell", 1949)
    with pytest.raises(books_mod.DuplicateBookError):
        bc.add_book("1984", "George Orwell", 1949)


def test_mark_and_remove(tmp_path):
    data_file = tmp_path / "data.json"
    bc = books_mod.BookCollection(data_file=str(data_file))
    bc.add_book("Dune", "Frank Herbert", 1965)
    assert bc.mark_as_read("Dune") is True
    assert bc.find_book_by_title("Dune").read is True
    assert bc.remove_book("Dune") is True
    assert bc.find_book_by_title("Dune") is None


def test_corrupted_backup(tmp_path):
    data_file = tmp_path / "data.json"
    # write invalid JSON
    data_file.write_text("{ this is not json }", encoding="utf-8")
    bc = books_mod.BookCollection(data_file=str(data_file))
    # corrupted file should be moved to .corrupt.TIMESTAMP
    corrupt_files = list(tmp_path.glob("data.json.corrupt.*"))
    assert len(corrupt_files) == 1
    # collection should be empty
    assert bc.list_books() == []


def test_batch_writes_once(tmp_path):
    data_file = tmp_path / "data.json"
    bc = books_mod.BookCollection(data_file=str(data_file))
    with bc.batch():
        bc.add_book("Book A", "Author", 2000)
        bc.add_book("Book B", "Author", 2001)
    # after exiting batch, file should exist and contain both books
    raw = json.loads(data_file.read_text(encoding="utf-8"))
    assert raw["version"] == books_mod.SCHEMA_VERSION
    assert len(raw["books"]) == 2
