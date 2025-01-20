import os


def get(path):
    # Replace dots in the path with the OS directory separator
    file_path = os.path.normpath(
        os.path.join(os.path.dirname(__file__), "sql/get", *path.split(".")) + ".sql"
    )

    # Check if the file exists
    if not os.path.isfile(file_path):
        raise FileNotFoundError(f"SQL file not found for path: {path}")

    # Read and return the file contents
    with open(file_path, "r") as sql_file:
        return sql_file.read()
