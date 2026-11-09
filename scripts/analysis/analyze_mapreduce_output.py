#!/usr/bin/env python3
"""
BIGDATA_SIBI - MapReduce Result Analysis
Analyzes validated Hadoop word-frequency output.
"""

import argparse
import csv
import json
import os
import sys
from collections import Counter, defaultdict
from pathlib import Path


def read_input(path):
    counts = Counter()
    book_totals = Counter()

    with open(path, "r", encoding="utf-8-sig") as f:
        for line_no, line in enumerate(f, 1):
            line = line.rstrip("\r\n")
            if not line:
                continue

            parts = line.split("\t")
            if len(parts) != 2:
                raise ValueError(
                    "Invalid input at line {}".format(line_no)
                )

            book_id, token = parts[0].strip(), parts[1].strip()

            if not book_id or not token:
                raise ValueError(
                    "Empty field at input line {}".format(line_no)
                )

            counts[(book_id, token)] += 1
            book_totals[book_id] += 1

    return counts, book_totals


def read_output(path):
    counts = {}
    total = 0
    sorted_ok = True
    previous = None

    with open(path, "r", encoding="utf-8-sig") as f:
        for line_no, line in enumerate(f, 1):
            line = line.rstrip("\r\n")
            if not line:
                continue

            parts = line.split("\t")
            if len(parts) != 3:
                raise ValueError(
                    "Invalid output at line {}".format(line_no)
                )

            book_id, token, value = (
                parts[0].strip(),
                parts[1].strip(),
                parts[2].strip()
            )

            frequency = int(value)

            if not book_id or not token or frequency <= 0:
                raise ValueError(
                    "Invalid record at output line {}".format(line_no)
                )

            key = (book_id, token)

            if key in counts:
                raise ValueError(
                    "Duplicate key: {}".format(key)
                )

            if previous is not None and key < previous:
                sorted_ok = False

            counts[key] = frequency
            total += frequency
            previous = key

    return counts, total, sorted_ok


def read_metadata(path):
    titles = {}

    if not os.path.isfile(path):
        return titles

    with open(path, "r", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            titles[row["book_id"].strip()] = row["title"].strip()

    return titles


def write_csv(path, headers, rows):
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(rows)


def main():
    parser = argparse.ArgumentParser(
        description="Analyze validated BIGDATA_SIBI MapReduce output."
    )

    parser.add_argument("--input", default="dataset/processed/bow_input_primary.tsv")
    parser.add_argument("--mapreduce-output", required=True)
    parser.add_argument("--metadata", default="metadata/books.csv")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--top-n", type=int, default=20)

    args = parser.parse_args()

    if args.top_n < 1:
        parser.error("--top-n must be at least 1")

    print("=" * 65)
    print(" BIGDATA_SIBI - MAPREDUCE DATA ANALYSIS")
    print("=" * 65)

    # 1. Read independent ground truth
    print("\n[1/5] Reading primary BoW dataset...")

    ground_truth, input_totals = read_input(args.input)

    print("Total input tokens :", sum(input_totals.values()))
    print("Books detected     :", len(input_totals))
    print("Distinct word pairs:", len(ground_truth))

    # 2. Read Hadoop output
    print("\n[2/5] Reading Hadoop MapReduce output...")

    mr_counts, mr_total, sorted_ok = read_output(
        args.mapreduce_output
    )

    print("Output frequencies:", mr_total)
    print("Distinct word pairs:", len(mr_counts))
    print("Output sorted      :", sorted_ok)

    # 3. Exact validation before analysis
    print("\n[3/5] Performing independent consistency check...")

    missing = set(ground_truth) - set(mr_counts)
    extra = set(mr_counts) - set(ground_truth)

    mismatches = [
        key for key in ground_truth.keys() & mr_counts.keys()
        if ground_truth[key] != mr_counts[key]
    ]

    passed = (
        not missing
        and not extra
        and not mismatches
        and sum(input_totals.values()) == mr_total
    )

    print("Missing keys      :", len(missing))
    print("Unexpected keys   :", len(extra))
    print("Frequency mismatch:", len(mismatches))

    if not passed:
        print("\n[FAILED] Analysis stopped. Output does not match input.")
        sys.exit(2)

    print("\n[PASS] Exact match confirmed.")

    # 4. Prepare analysis
    print("\n[4/5] Calculating statistics...")

    titles = read_metadata(args.metadata)
    book_counts = defaultdict(Counter)

    for (book_id, token), frequency in mr_counts.items():
        book_counts[book_id][token] = frequency

    book_ids = sorted(
        input_totals.keys(),
        key=lambda x: (0, int(x)) if x.isdigit() else (1, x)
    )

    summary_rows = []
    top_rows = []

    for book_id in book_ids:
        words = book_counts[book_id]
        total = input_totals[book_id]
        title = titles.get(book_id, "Book " + book_id)

        ranking = sorted(
            words.items(),
            key=lambda item: (-item[1], item[0])
        )

        top_token, top_frequency = ranking[0]

        summary_rows.append([
            book_id,
            title,
            total,
            len(words),
            top_token,
            top_frequency,
            round(top_frequency / total * 1000, 4)
        ])

        for rank, (token, frequency) in enumerate(
            ranking[:args.top_n], 1
        ):
            top_rows.append([
                rank,
                book_id,
                title,
                token,
                frequency,
                round(frequency / total * 1000, 4)
            ])

    # Overall word frequency
    overall = Counter()

    for book_id in book_ids:
        overall.update(book_counts[book_id])

    total_all = sum(input_totals.values())

    overall_rows = []

    for rank, (token, frequency) in enumerate(
        sorted(
            overall.items(),
            key=lambda item: (-item[1], item[0])
        )[:args.top_n],
        1
    ):
        overall_rows.append([
            rank,
            token,
            frequency,
            round(frequency / total_all * 1000, 4)
        ])

    # Cross-book comparison using relative frequencies
    all_tokens = sorted(overall.keys())

    comparison_headers = ["token"]

    for book_id in book_ids:
        comparison_headers.extend([
            "book_{}_frequency".format(book_id),
            "book_{}_per_1000".format(book_id)
        ])

    comparison_headers.extend([
        "total_frequency",
        "books_present",
        "highest_relative_frequency_book"
    ])

    comparison_rows = []

    for token in all_tokens:
        row = [token]
        relative_values = []

        for book_id in book_ids:
            frequency = book_counts[book_id].get(token, 0)
            relative = (
                frequency / input_totals[book_id] * 1000
            )

            row.extend([frequency, round(relative, 4)])

            if frequency > 0:
                relative_values.append((relative, book_id))

        highest_book = max(
            relative_values,
            key=lambda item: (item[0], item[1])
        )[1]

        present = len(relative_values)

        row.extend([
            overall[token],
            present,
            highest_book
        ])

        comparison_rows.append(row)

    # 5. Save results
    print("\n[5/5] Saving analysis outputs...")

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    write_csv(
        output_dir / "book_summary.csv",
        [
            "book_id", "title", "total_tokens",
            "unique_tokens", "top_word",
            "top_word_frequency", "top_word_per_1000"
        ],
        summary_rows
    )

    write_csv(
        output_dir / "top_words_per_book.csv",
        [
            "rank", "book_id", "title", "token",
            "frequency", "frequency_per_1000"
        ],
        top_rows
    )

    write_csv(
        output_dir / "top_words_overall.csv",
        [
            "rank", "token", "frequency",
            "frequency_per_1000"
        ],
        overall_rows
    )

    write_csv(
        output_dir / "cross_book_word_comparison.csv",
        comparison_headers,
        comparison_rows
    )

    report = {
        "project": "BIGDATA_SIBI",
        "status": "PASS",
        "input_tokens": total_all,
        "distinct_book_token_pairs": len(mr_counts),
        "total_frequency": mr_total,
        "sorting_verified": sorted_ok,
        "missing_keys": len(missing),
        "extra_keys": len(extra),
        "frequency_mismatches": len(mismatches),
        "books": summary_rows,
        "top_n": args.top_n
    }

    with open(
        output_dir / "analysis_summary.json",
        "w",
        encoding="utf-8"
    ) as f:
        json.dump(report, f, indent=4, ensure_ascii=False)

    print("\n==============================================")
    print(" ANALYSIS COMPLETED SUCCESSFULLY")
    print("==============================================")
    print("Total tokens       :", total_all)
    print("Distinct word pairs:", len(mr_counts))
    print("Books analyzed     :", len(book_ids))
    print("Top N per book     :", args.top_n)
    print("Output directory   :", output_dir.resolve())

    print("\nGenerated files:")

    for name in [
        "book_summary.csv",
        "top_words_per_book.csv",
        "top_words_overall.csv",
        "cross_book_word_comparison.csv",
        "analysis_summary.json"
    ]:
        print(" -", name)


if __name__ == "__main__":
    main()