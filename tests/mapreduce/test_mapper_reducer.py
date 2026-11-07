#!/usr/bin/env python3
"""
Unit and Integration Tests for Hadoop Streaming Mapper and Reducer (BIGDATA_SIBI).
Covers all test scenarios specified in Section P:
1. Normal tokens
2. Repeated tokens
3. Multi-book identical words (isolation verification)
4. Indonesian reduplication words (anak-anak, buku-buku)
5. Indonesian domain vocabulary
6. Malformed input handling (missing columns, extra columns)
7. Empty tokens and empty book IDs
8. Delimiter and numerical anomalies (spaces vs tabs, negative counts, non-int counts)
9. End-to-end Map -> Sort -> Reduce pipeline test matching specification example
"""

import io
import sys
import unittest

# Import mapper and reducer logic
from scripts.mapreduce.mapper import process_mapper
from scripts.mapreduce.reducer import process_reducer


class TestHadoopMapper(unittest.TestCase):
    """Test suite for Hadoop Streaming Mapper."""

    def run_mapper(self, input_text: str):
        """Helper to execute process_mapper with simulated stdin and captured stdout/stderr."""
        old_stdin = sys.stdin
        old_stdout = sys.stdout
        old_stderr = sys.stderr
        try:
            sys.stdin = io.StringIO(input_text)
            sys.stdout = io.StringIO()
            sys.stderr = io.StringIO()
            process_mapper()
            return sys.stdout.getvalue(), sys.stderr.getvalue()
        finally:
            sys.stdin = old_stdin
            sys.stdout = old_stdout
            sys.stderr = old_stderr

    def test_01_normal_tokens(self):
        """Test mapper emits correct 3-field tuples for normal tokens."""
        input_data = "1\tbuku\n2\tsekolah\n3\tguru\n"
        stdout, stderr = self.run_mapper(input_data)
        expected = "1\tbuku\t1\n2\tsekolah\t1\n3\tguru\t1\n"
        self.assertEqual(stdout, expected)
        self.assertEqual(stderr, "")

    def test_02_repeated_tokens(self):
        """Test mapper emits an individual tuple for every occurrence without dropping duplicates."""
        input_data = "1\tmembaca\n1\tmembaca\n1\tmembaca\n"
        stdout, stderr = self.run_mapper(input_data)
        expected = "1\tmembaca\t1\n1\tmembaca\t1\n1\tmembaca\t1\n"
        self.assertEqual(stdout, expected)
        self.assertEqual(stderr, "")

    def test_03_multi_book_same_token(self):
        """Test mapper retains correct book_id when multiple books share the same word."""
        input_data = "1\tbelajar\n2\tbelajar\n3\tbelajar\n4\tbelajar\n"
        stdout, stderr = self.run_mapper(input_data)
        expected = "1\tbelajar\t1\n2\tbelajar\t1\n3\tbelajar\t1\n4\tbelajar\t1\n"
        self.assertEqual(stdout, expected)
        self.assertEqual(stderr, "")

    def test_04_indonesian_kata_ulang(self):
        """Test mapper correctly processes hyphenated Indonesian reduplicated words."""
        input_data = "1\tanak-anak\n2\tkupu-kupu\n3\tbersama-sama\n"
        stdout, stderr = self.run_mapper(input_data)
        expected = "1\tanak-anak\t1\n2\tkupu-kupu\t1\n3\tbersama-sama\t1\n"
        self.assertEqual(stdout, expected)
        self.assertEqual(stderr, "")

    def test_05_indonesian_domain_vocab(self):
        """Test mapper handles typical Indonesian educational and domain vocabulary."""
        input_data = "1\tpembelajaran\n2\tkurikulum\n3\tpancasila\n"
        stdout, stderr = self.run_mapper(input_data)
        expected = "1\tpembelajaran\t1\n2\tkurikulum\t1\n3\tpancasila\t1\n"
        self.assertEqual(stdout, expected)
        self.assertEqual(stderr, "")

    def test_06_malformed_input_columns(self):
        """Test mapper catches and logs lines with too few or too many columns."""
        input_data = "1\tvalid\nmalformed_single_column\n1\ttoo\tmany\tcolumns\n2\tok\n"
        stdout, stderr = self.run_mapper(input_data)
        expected = "1\tvalid\t1\n2\tok\t1\n"
        self.assertEqual(stdout, expected)
        self.assertIn("Expected 2 tab-separated fields, got 1", stderr)
        self.assertIn("Expected 2 tab-separated fields, got 4", stderr)

    def test_07_empty_token_or_book_id(self):
        """Test mapper rejects empty book_id or empty token."""
        input_data = "1\t\n\ttoken\n \t \n1\tvalid\n"
        stdout, stderr = self.run_mapper(input_data)
        expected = "1\tvalid\t1\n"
        self.assertEqual(stdout, expected)
        self.assertIn("Empty token for book 1", stderr)
        self.assertIn("Empty book_id", stderr)

    def test_08_delimiter_anomalies(self):
        """Test mapper catches lines using spaces instead of tabs."""
        input_data = "1 membaca buku\n1\tbuku\n"
        stdout, stderr = self.run_mapper(input_data)
        expected = "1\tbuku\t1\n"
        self.assertEqual(stdout, expected)
        self.assertIn("Expected 2 tab-separated fields, got 1", stderr)


class TestHadoopReducer(unittest.TestCase):
    """Test suite for Hadoop Streaming Reducer."""

    def run_reducer(self, input_text: str):
        """Helper to execute process_reducer with simulated stdin and captured stdout/stderr."""
        old_stdin = sys.stdin
        old_stdout = sys.stdout
        old_stderr = sys.stderr
        try:
            sys.stdin = io.StringIO(input_text)
            sys.stdout = io.StringIO()
            sys.stderr = io.StringIO()
            process_reducer()
            return sys.stdout.getvalue(), sys.stderr.getvalue()
        finally:
            sys.stdin = old_stdin
            sys.stdout = old_stdout
            sys.stderr = old_stderr

    def test_01_single_key_aggregation(self):
        """Test reducer correctly sums counts for identical key."""
        input_data = "1\tbuku\t1\n1\tbuku\t1\n1\tbuku\t1\n"
        stdout, stderr = self.run_reducer(input_data)
        expected = "1\tbuku\t3\n"
        self.assertEqual(stdout, expected)
        self.assertEqual(stderr, "")

    def test_02_multi_key_aggregation(self):
        """Test reducer outputs distinct aggregate for different keys."""
        input_data = "1\tbuku\t1\n1\tbuku\t1\n1\tmembaca\t1\n"
        stdout, stderr = self.run_reducer(input_data)
        expected = "1\tbuku\t2\n1\tmembaca\t1\n"
        self.assertEqual(stdout, expected)
        self.assertEqual(stderr, "")

    def test_03_multi_book_isolation(self):
        """Test reducer DOES NOT merge counts across different books for same word."""
        input_data = "1\tmembaca\t1\n1\tmembaca\t1\n2\tmembaca\t1\n"
        stdout, stderr = self.run_reducer(input_data)
        expected = "1\tmembaca\t2\n2\tmembaca\t1\n"
        self.assertEqual(stdout, expected)
        self.assertEqual(stderr, "")

    def test_04_kata_ulang_aggregation(self):
        """Test reducer aggregates Indonesian reduplicated words accurately."""
        input_data = "1\tanak-anak\t1\n1\tanak-anak\t1\n2\tanak-anak\t1\n"
        stdout, stderr = self.run_reducer(input_data)
        expected = "1\tanak-anak\t2\n2\tanak-anak\t1\n"
        self.assertEqual(stdout, expected)
        self.assertEqual(stderr, "")

    def test_05_malformed_input_records(self):
        """Test reducer handles malformed records with invalid count or field counts."""
        input_data = (
            "1\tbuku\t1\n"
            "invalid_record\n"
            "1\tbuku\tnot_a_number\n"
            "1\tbuku\t-5\n"
            "1\tbuku\t1\n"
        )
        stdout, stderr = self.run_reducer(input_data)
        expected = "1\tbuku\t2\n"
        self.assertEqual(stdout, expected)
        self.assertIn("Expected 3 tab-separated fields", stderr)
        self.assertIn("Invalid count integer", stderr)
        self.assertIn("Negative count", stderr)


class TestMapReduceIntegration(unittest.TestCase):
    """End-to-end integration test simulating Hadoop Map -> Sort -> Reduce pipeline."""

    def test_prompt_specification_example(self):
        """
        Verify exact example from prompt specification:
        Input:
        1    membaca
        1    membaca
        1    buku
        2    membaca

        Expected output:
        1    buku       1
        1    membaca    2
        2    membaca    1
        """
        raw_input = "1\tmembaca\n1\tmembaca\n1\tbuku\n2\tmembaca\n"

        # 1. Map Phase
        old_stdin, old_stdout, old_stderr = sys.stdin, sys.stdout, sys.stderr
        try:
            sys.stdin = io.StringIO(raw_input)
            sys.stdout = io.StringIO()
            sys.stderr = io.StringIO()
            process_mapper()
            map_output = sys.stdout.getvalue()
        finally:
            sys.stdin, sys.stdout, sys.stderr = old_stdin, old_stdout, old_stderr

        # 2. Hadoop Shuffle & Sort Phase (2 composite key fields: book_id, token)
        map_lines = [l for l in map_output.strip().split("\n") if l]
        # Sort lexicographically by (book_id, token)
        sorted_lines = sorted(map_lines, key=lambda x: (x.split("\t")[0], x.split("\t")[1]))
        shuffle_output = "\n".join(sorted_lines) + "\n"

        # 3. Reduce Phase
        try:
            sys.stdin = io.StringIO(shuffle_output)
            sys.stdout = io.StringIO()
            sys.stderr = io.StringIO()
            process_reducer()
            final_output = sys.stdout.getvalue()
        finally:
            sys.stdin, sys.stdout, sys.stderr = old_stdin, old_stdout, old_stderr

        expected_final = "1\tbuku\t1\n1\tmembaca\t2\n2\tmembaca\t1\n"
        self.assertEqual(final_output, expected_final)


if __name__ == "__main__":
    unittest.main()

