#!/usr/bin/env python3
"""
Hadoop Streaming Reducer for Project BIGDATA_SIBI.
Reads sorted 'book_id\\ttoken\\tcount' tuples from stdin.
Aggregates occurrence frequencies for each composite key (book_id, token).
Emits 'book_id\\ttoken\\tfrequency' to stdout.
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
                f"[REDUCER ERROR] Line {line_num}: Expected 3 tab-separated fields, "
                f"got {len(parts)} -> '{line[:50]}'\n"
            )
            continue

        book_id, token, count_str = parts[0].strip(), parts[1].strip(), parts[2].strip()

        if not book_id or not token:
            sys.stderr.write(f"[REDUCER ERROR] Line {line_num}: Missing book_id or token\n")
            continue

        try:
            count = int(count_str)
            if count < 0:
                sys.stderr.write(f"[REDUCER ERROR] Line {line_num}: Negative count {count}\n")
                continue
        except ValueError:
            sys.stderr.write(
                f"[REDUCER ERROR] Line {line_num}: Invalid count integer '{count_str}'\n"
            )
            continue

        composite_key = (book_id, token)

        if current_key == composite_key:
            current_count += count
        else:
            if current_key is not None:
                # Emit aggregated frequency for previous key
                sys.stdout.write(f"{current_key[0]}\t{current_key[1]}\t{current_count}\n")
            current_key = composite_key
            current_count = count

    # Flush the last key
    if current_key is not None:
        sys.stdout.write(f"{current_key[0]}\t{current_key[1]}\t{current_count}\n")


if __name__ == "__main__":
    process_reducer()

