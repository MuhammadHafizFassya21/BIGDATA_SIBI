#!/usr/bin/env python3
"""
Hadoop Streaming Reducer for Project BIGDATA_SIBI.
Reads sorted 'book_id\ttoken\tcount' tuples from stdin.
Aggregates occurrence frequencies for each composite key.
Emits 'book_id\ttoken\tfrequency' to stdout.
"""

import sys


def process_reducer():
    """Aggregate sorted key-value pairs from standard input."""

    current_key = None
    current_count = 0

    for line_num, line in enumerate(sys.stdin, start=1):
        line = line.rstrip("\r\n")

        if not line:
            continue

        parts = line.split("\t")

        if len(parts) != 3:
            sys.stderr.write(
                "[REDUCER ERROR] Line {}: Expected 3 tab-separated fields, "
                "got {} -> '{}'\n".format(
                    line_num, len(parts), line[:50]
                )
            )
            continue

        book_id = parts[0].strip()
        token = parts[1].strip()
        count_str = parts[2].strip()

        if not book_id or not token:
            sys.stderr.write(
                "[REDUCER ERROR] Line {}: Missing book_id or token\n".format(
                    line_num
                )
            )
            continue

        try:
            count = int(count_str)

            if count < 0:
                sys.stderr.write(
                    "[REDUCER ERROR] Line {}: Negative count {}\n".format(
                        line_num, count
                    )
                )
                continue

        except ValueError:
            sys.stderr.write(
                "[REDUCER ERROR] Line {}: Invalid count integer '{}'\n".format(
                    line_num, count_str
                )
            )
            continue

        composite_key = (book_id, token)

        if current_key == composite_key:
            current_count += count

        else:
            if current_key is not None:
                sys.stdout.write(
                    "{}\t{}\t{}\n".format(
                        current_key[0],
                        current_key[1],
                        current_count
                    )
                )

            current_key = composite_key
            current_count = count

    if current_key is not None:
        sys.stdout.write(
            "{}\t{}\t{}\n".format(
                current_key[0],
                current_key[1],
                current_count
            )
        )


if __name__ == "__main__":
    process_reducer()