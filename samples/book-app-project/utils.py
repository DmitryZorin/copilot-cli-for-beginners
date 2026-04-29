def print_menu():
    print("\n📚 Book Collection App")
    print("1. Add a book")
    print("2. List books")
    print("3. Mark book as read")
    print("4. Remove a book")
    print("5. Exit")


def get_user_choice() -> str:
    """Prompt for a menu choice and validate it.

    Keeps asking until the user enters a number 1-5 (non-empty, numeric).
    Returns the validated choice as a string.
    """
    valid_choices = {"1", "2", "3", "4", "5"}
    while True:
        choice = input("Choose an option (1-5): ").strip()
        if not choice:
            print("Please enter a choice (1-5).")
            continue
        if not choice.isdigit():
            print("Invalid input; please enter a number between 1 and 5.")
            continue
        if choice not in valid_choices:
            print("Choice must be between 1 and 5.")
            continue
        return choice


def get_book_details():
    """Prompt interactively for book details and validate inputs.

    The function repeatedly prompts the user for book information until valid
    values are provided for required fields. It performs simple parsing and
    fallback handling for the publication year.

    Side effects:
    - Reads from standard input using input().
    - Prints validation prompts or error messages to standard output.

    Parameters:
    - None. This is an interactive helper — it reads user input directly.

    Returns:
    - tuple[str, str, int]: A tuple containing (title, author, year), where
      title and author are non-empty trimmed strings, and year is an integer
      (defaults to 0 when the user omits or provides an invalid year).

    Example:
        title, author, year = get_book_details()

    Notes:
    - Title and author are required and the prompt repeats until non-empty
      values are provided.
    - Year is optional; if the user enters an empty value or an invalid
      integer, the function returns 0 for the year.
    """
    while True:
        title = input("Enter book title: ").strip()
        if not title:
            print("Title cannot be empty. Please enter a title.")
            continue

        author = input("Enter author: ").strip()
        if not author:
            print("Author cannot be empty. Please enter an author.")
            continue

        year_input = input("Enter publication year: ").strip()
        try:
            year = int(year_input) if year_input else 0
        except ValueError:
            print("Invalid year. Defaulting to 0.")
            year = 0

        return title, author, year


def print_books(books):
    if not books:
        print("No books in your collection.")
        return

    print("\nYour Books:")
    for index, book in enumerate(books, start=1):
        status = "✅ Read" if book.read else "📖 Unread"
        print(f"{index}. {book.title} by {book.author} ({book.year}) - {status}")
