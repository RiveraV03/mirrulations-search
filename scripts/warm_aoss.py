"""Periodic tiny query against AOSS to keep OCUs from scaling to the floor."""
from mirrsearch.db import get_opensearch_connection


def main():
    client = get_opensearch_connection()
    body = {"size": 0, "query": {"match_all": {}}}
    for index in ("documents_text", "comments", "comments_extracted_text"):
        try:
            client.search(index=index, body=body)
        except Exception as exc:  # pylint: disable=broad-exception-caught
            print(f"warm_aoss: {index} failed: {exc}")


if __name__ == "__main__":
    main()
