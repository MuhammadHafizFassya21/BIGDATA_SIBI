#!/usr/bin/env python3
"""
Hadoop Streaming Mapper for Project BIGDATA_SIBI.
Reads 'book_id\ttoken' from stdin.
Emits 'book_id\ttoken\t1' to stdout.
Validates input format and logs invalid records to stderr.
"""

import sys


def process_mapper():
    """Read lines from standard input and emit key-value pairs."""
    for line_num, line in enumerate(sys.stdin, start=1):
        line = line.rstrip("\r\n")

        if not line:
            continue

        parts = line.split("\t")

        if len(parts) != 2:
            sys.stderr.write(
                "[MAPPER ERROR] Line {}: Expected 2 tab-separated fields, "
                "got {} -> '{}'\n".format(
                    line_num, len(parts), line[:50]
                )
            )
            continue

        book_id = parts[0].strip()
        token = parts[1].strip()

        if not book_id:
            sys.stderr.write(
                "[MAPPER ERROR] Line {}: Empty book_id\n".format(line_num)
            )
            continue

        if not token:
            sys.stderr.write(
                "[MAPPER ERROR] Line {}: Empty token for book {}\n".format(
                    line_num, book_id
                )
            )
            continue

        sys.stdout.write(
            "{}\t{}\t1\n".format(book_id, token)
        )


if __name__ == "__main__":
    process_mapper()