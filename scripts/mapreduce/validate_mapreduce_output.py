#!/usr/bin/env python3
"""
Independent MapReduce Output Validation Program for Project BIGDATA_SIBI.
Validates Hadoop MapReduce output (part-00000) against ground-truth TSV input
using Python collections.Counter.

Verifies:
1. Exact match of total token frequencies (sum of counts == total input lines).
2. Exact match of distinct composite keys (book_id, token).
3. Zero missing tokens.
4. Zero unexpected/hallucinated tokens.
5. Zero frequency count discrepancies.
6. Lexicographical ordering compliance.
"""

import argparse
from collections import Counter
import json
import os
import sys
import time


def load_ground_truth(input_path: str):
    """Read input TSV (book_id \\t token) and compute exact word frequencies."""
    print(f">> [1/4] Reading ground truth input from: {input_path}")
    counter = Counter()
    total_lines = 0

    with open(input_path, "r", encoding="utf-8") as f:
        for line_num, line in enumerate(f, start=1):
            line = line.rstrip("\r\n")
            if not line:
                continue
            parts = line.split("\t")
            if len(parts) != 2:
                raise ValueError(f"Malformed input TSV at line {line_num}: '{line}'")
            book_id, token = parts[0].strip(), parts[1].strip()
            counter[(book_id, token)] += 1
            total_lines += 1

    print(
        f"   Loaded {total_lines:,} tokens spanning {len(counter):,} distinct (book_id, token) pairs."
    )
    return counter, total_lines


def load_mapreduce_output(output_path: str):
    """Read Hadoop MapReduce part file (book_id \\t token \\t count)."""
    print(f">> [2/4] Reading Hadoop MapReduce output from: {output_path}")
    mr_counts = {}
    mr_total_count = 0
    is_sorted = True
    prev_key = None

    with open(output_path, "r", encoding="utf-8") as f:
        for line_num, line in enumerate(f, start=1):
            line = line.rstrip("\r\n")
            if not line:
                continue
            parts = line.split("\t")
            if len(parts) != 3:
                raise ValueError(
                    f"Malformed MapReduce output at line {line_num}: expected 3 fields, got {len(parts)} -> '{line}'"
                )
            book_id, token, freq_str = parts[0].strip(), parts[1].strip(), parts[2].strip()
            try:
                freq = int(freq_str)
            except ValueError:
                raise ValueError(
                    f"Non-integer frequency at line {line_num}: '{freq_str}'"
                )

            key = (book_id, token)
            if key in mr_counts:
                raise ValueError(f"Duplicate key found in MapReduce output: {key}")

            mr_counts[key] = freq
            mr_total_count += freq

            # Check sort ordering
            if prev_key is not None and key < prev_key:
                is_sorted = False
            prev_key = key

    print(
        f"   Loaded {len(mr_counts):,} distinct keys with aggregated sum = {mr_total_count:,}."
    )
    return mr_counts, mr_total_count, is_sorted


def validate_results(ground_truth: Counter, gt_total: int, mr_counts: dict, mr_total: int, is_sorted: bool):
    """Compare ground-truth Counter against MapReduce output."""
    print(">> [3/4] Performing comprehensive consistency audit...")
    discrepancies = []
    missing_in_mr = []
    extra_in_mr = []
    count_mismatches = []

    # Check missing in MR or count mismatch
    for key, gt_count in ground_truth.items():
        if key not in mr_counts:
            missing_in_mr.append(key)
        else:
            mr_count = mr_counts[key]
            if mr_count != gt_count:
                count_mismatches.append(
                    {"key": key, "expected": gt_count, "actual": mr_count}
                )

    # Check extra in MR
    for key in mr_counts:
        if key not in ground_truth:
            extra_in_mr.append(key)

    total_keys_match = len(ground_truth) == len(mr_counts)
    total_freq_match = gt_total == mr_total
    all_passed = (
        total_keys_match
        and total_freq_match
        and len(missing_in_mr) == 0
        and len(extra_in_mr) == 0
        and len(count_mismatches) == 0
    )

    result_summary = {
        "status": "PASS" if all_passed else "FAIL",
        "ground_truth_total_tokens": gt_total,
        "mapreduce_total_tokens": mr_total,
        "ground_truth_distinct_keys": len(ground_truth),
        "mapreduce_distinct_keys": len(mr_counts),
        "keys_match": total_keys_match,
        "frequencies_sum_match": total_freq_match,
        "output_is_sorted": is_sorted,
        "missing_in_mr_count": len(missing_in_mr),
        "extra_in_mr_count": len(extra_in_mr),
        "count_mismatches_count": len(count_mismatches),
        "sample_missing": missing_in_mr[:5],
        "sample_extra": extra_in_mr[:5],
        "sample_mismatches": count_mismatches[:5],
    }

    print(">> [4/4] Validation Summary:")
    print(f"   Status                     : {result_summary['status']}")
    print(f"   Total Token Count Match    : {total_freq_match} ({gt_total:,} vs {mr_total:,})")
    print(f"   Distinct Keys Match        : {total_keys_match} ({len(ground_truth):,} vs {len(mr_counts):,})")
    print(f"   Sorting Compliance         : {'YES' if is_sorted else 'NO'}")
    print(f"   Missing Keys Count         : {len(missing_in_mr)}")
    print(f"   Unexpected Extra Keys      : {len(extra_in_mr)}")
    print(f"   Frequency Mismatch Count   : {len(count_mismatches)}")

    return all_passed, result_summary


def main():
    parser = argparse.ArgumentParser(
        description="Validate Hadoop MapReduce output against ground-truth TSV input."
    )
    parser.add_argument(
        "--input",
        required=True,
        help="Path to input TSV file (e.g. dataset/processed/bow_input_primary.tsv)",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Path to MapReduce output file (e.g. output/mapreduce/run_.../part-00000)",
    )
    parser.add_argument(
        "--report",
        default=None,
        help="Optional path to output validation JSON report.",
    )

    args = parser.parse_args()

    if not os.path.isfile(args.input):
        print(f"[ERROR] Input file does not exist: {args.input}", file=sys.stderr)
        sys.exit(1)

    if not os.path.isfile(args.output):
        print(f"[ERROR] MapReduce output file does not exist: {args.output}", file=sys.stderr)
        sys.exit(1)

    gt_counter, gt_total = load_ground_truth(args.input)
    mr_counts, mr_total, is_sorted = load_mapreduce_output(args.output)
    passed, summary = validate_results(
        gt_counter, gt_total, mr_counts, mr_total, is_sorted
    )

    if args.report:
        os.makedirs(os.path.dirname(os.path.abspath(args.report)), exist_ok=True)
        with open(args.report, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2)
        print(f">> Report saved to: {args.report}")

    if not passed:
        print(">> [VALIDATION FAILED] Hadoop output does not match ground truth!", file=sys.stderr)
        sys.exit(2)
    else:
        print(">> [VALIDATION SUCCESS] 100% Exact match between MapReduce output and ground truth.")


if __name__ == "__main__":
    main()

