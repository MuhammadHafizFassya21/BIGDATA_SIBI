#!/usr/bin/env python
"""
BIGDATA_SIBI - Safe MinIO dataset uploader (Boto3).

Default behavior is non-destructive:
  --dry-run : validate local data, authenticate, check buckets, show upload plan.
  --upload  : upload files and independently verify each remote object by
              downloading it and comparing SHA-256 and byte length.

Credentials are read from configs/minio/minio.env. Never prints credentials.
Does not create/delete buckets or delete/modify local source files.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import mimetypes
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError


PROJECT_ROOT = Path(__file__).resolve().parents[2]
ENV_FILE = PROJECT_ROOT / "configs" / "minio" / "minio.env"
ENDPOINT_URL = "http://127.0.0.1:19000"
REGION_NAME = "us-east-1"

EXPECTED_BUCKETS = ("sibi-raw", "sibi-processed", "sibi-output")

# Source folder -> (target bucket, object prefix, expected file count)
SOURCE_GROUPS = (
    ("dataset/raw", "sibi-raw", "dataset_sibi/raw", 3),
    ("dataset/optional", "sibi-raw", "dataset_sibi/optional", 1),
    ("metadata", "sibi-raw", "dataset_sibi/metadata", 1),
    (
        "extracted_text/raw",
        "sibi-processed",
        "dataset_sibi/extracted_text/raw",
        4,
    ),
    (
        "extracted_text/cleaned",
        "sibi-processed",
        "dataset_sibi/extracted_text/cleaned",
        4,
    ),
    (
        "dataset/processed",
        "sibi-processed",
        "dataset_sibi/processed",
        4,
    ),
)

EXPECTED_BOW = {
    "bow_input.tsv": {
        "rows": 90894,
        "book_counts": {"1": 9865, "2": 13937, "3": 30079, "4": 37013},
    },
    "bow_input_primary.tsv": {
        "rows": 53881,
        "book_counts": {"1": 9865, "2": 13937, "3": 30079},
    },
}

EXPECTED_CSV_HEADER_COUNT = 12
EXPECTED_CSV_DATA_ROWS = 4


def read_env_file(path: Path) -> dict[str, str]:
    if not path.is_file():
        raise FileNotFoundError(f"Configuration file not found: {path}")

    values: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8-sig").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key, value = key.strip(), value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
            value = value[1:-1]
        values[key] = value
    return values


def create_s3_client():
    env = read_env_file(ENV_FILE)
    access_key = env.get("MINIO_ROOT_USER", "")
    secret_key = env.get("MINIO_ROOT_PASSWORD", "")

    if not access_key or not secret_key:
        raise ValueError(
            "MINIO_ROOT_USER or MINIO_ROOT_PASSWORD is missing in "
            "configs/minio/minio.env."
        )
    if secret_key == "REPLACE_WITH_UNIQUE_PASSWORD_MIN_8_CHARS":
        raise ValueError("MinIO password is still the placeholder.")

    return boto3.client(
        "s3",
        endpoint_url=ENDPOINT_URL,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=REGION_NAME,
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
            connect_timeout=8,
            read_timeout=60,
            retries={"max_attempts": 3, "mode": "standard"},
        ),
    )


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def validate_csv_metadata(path: Path) -> None:
    if not path.is_file():
        raise FileNotFoundError(f"Metadata CSV not found: {path}")

    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.reader(handle, strict=True))

    if not rows:
        raise ValueError("metadata/books.csv is empty.")

    header = rows[0]
    if len(header) != EXPECTED_CSV_HEADER_COUNT:
        raise ValueError(
            f"books.csv header has {len(header)} columns; "
            f"expected {EXPECTED_CSV_HEADER_COUNT}."
        )
    if len(rows) - 1 != EXPECTED_CSV_DATA_ROWS:
        raise ValueError(
            f"books.csv has {len(rows) - 1} data rows; "
            f"expected {EXPECTED_CSV_DATA_ROWS}."
        )

    for line_number, row in enumerate(rows[1:], start=2):
        if len(row) != len(header):
            raise ValueError(
                f"books.csv line {line_number} has {len(row)} fields; "
                f"expected {len(header)}."
            )

    if "book_id" not in header:
        raise ValueError("books.csv is missing the book_id column.")

    id_index = header.index("book_id")
    book_ids = [row[id_index].strip() for row in rows[1:]]
    if len(set(book_ids)) != len(book_ids):
        raise ValueError("books.csv contains duplicate book_id values.")
    if set(book_ids) != {"1", "2", "3", "4"}:
        raise ValueError(
            f"books.csv book_id values are {book_ids}; expected 1, 2, 3, 4."
        )


def validate_bow_file(path: Path, expected: dict[str, Any]) -> None:
    if not path.is_file():
        raise FileNotFoundError(f"Required BoW file not found: {path}")

    row_count = 0
    counts: dict[str, int] = {}

    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        for line_number, line in enumerate(handle, start=1):
            row_count += 1
            line = line.rstrip("\r\n")
            parts = line.split("\t")

            if len(parts) != 2:
                raise ValueError(
                    f"{path.name}, line {line_number}: expected 2 TSV fields, "
                    f"found {len(parts)}."
                )

            book_id, token = parts
            if book_id not in {"1", "2", "3", "4"} or not token.strip():
                raise ValueError(
                    f"{path.name}, line {line_number}: invalid book_id/token."
                )

            counts[book_id] = counts.get(book_id, 0) + 1

    if row_count != expected["rows"]:
        raise ValueError(
            f"{path.name}: found {row_count} rows; "
            f"expected {expected['rows']}."
        )

    if counts != expected["book_counts"]:
        raise ValueError(
            f"{path.name}: book distribution {counts}; "
            f"expected {expected['book_counts']}."
        )


def collect_upload_files() -> list[dict[str, Any]]:
    planned: list[dict[str, Any]] = []

    for relative_dir, bucket, key_prefix, expected_count in SOURCE_GROUPS:
        source_dir = PROJECT_ROOT / relative_dir
        if not source_dir.is_dir():
            raise FileNotFoundError(f"Required source directory missing: {source_dir}")

        files = sorted(path for path in source_dir.rglob("*") if path.is_file())
        if len(files) != expected_count:
            raise ValueError(
                f"{relative_dir}: found {len(files)} files; "
                f"expected {expected_count}. Upload stopped for review."
            )

        for path in files:
            relative_to_group = path.relative_to(source_dir).as_posix()
            object_key = f"{key_prefix}/{relative_to_group}"
            project_relative = path.relative_to(PROJECT_ROOT).as_posix()
            planned.append(
                {
                    "path": path,
                    "source": project_relative,
                    "bucket": bucket,
                    "key": object_key,
                    "size": path.stat().st_size,
                }
            )

    if len(planned) != 17:
        raise ValueError(
            f"Expected 17 source files based on the audited inventory; "
            f"found {len(planned)}."
        )

    return planned


def verify_required_buckets(s3) -> None:
    print("\nChecking required MinIO buckets...")
    for bucket in EXPECTED_BUCKETS:
        s3.head_bucket(Bucket=bucket)
        print(f"  [FOUND] {bucket}")


def remote_sha256(s3, bucket: str, key: str) -> tuple[str, int]:
    response = s3.get_object(Bucket=bucket, Key=key)
    body = response["Body"]
    digest = hashlib.sha256()
    total = 0
    try:
        for chunk in iter(lambda: body.read(1024 * 1024), b""):
            digest.update(chunk)
            total += len(chunk)
    finally:
        body.close()
    return digest.hexdigest(), total


def save_report(report: dict[str, Any]) -> Path:
    report_dir = PROJECT_ROOT / "logs" / "minio"
    report_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = report_dir / f"upload_report_{stamp}.json"
    report_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return report_path


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Validate or upload BIGDATA_SIBI data to MinIO."
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate local files and MinIO access without uploading.",
    )
    mode.add_argument(
        "--upload",
        action="store_true",
        help="Upload files and verify each remote object with SHA-256.",
    )
    args = parser.parse_args()

    print("=" * 68)
    print("BIGDATA_SIBI - MINIO DATASET UPLOAD")
    print("=" * 68)
    print(f"Project root : {PROJECT_ROOT}")
    print(f"Endpoint     : {ENDPOINT_URL}")
    print(f"Mode         : {'DRY RUN' if args.dry_run else 'UPLOAD'}")
    print("Credentials  : loaded locally; values are never displayed")

    try:
        print("\n[1/5] Validating metadata/books.csv...")
        validate_csv_metadata(PROJECT_ROOT / "metadata" / "books.csv")
        print("  [PASS] 12 columns, 4 data rows, book_id 1-4.")

        print("\n[2/5] Validating Bag of Words files...")
        processed_dir = PROJECT_ROOT / "dataset" / "processed"
        for filename, expected in EXPECTED_BOW.items():
            validate_bow_file(processed_dir / filename, expected)
            print(
                f"  [PASS] {filename}: {expected['rows']:,} rows; "
                "book distribution matches audit."
            )

        print("\n[3/5] Building upload inventory...")
        planned = collect_upload_files()
        total_bytes = sum(item["size"] for item in planned)
        print(f"  [PASS] {len(planned)} files found.")
        print(f"  Total size: {total_bytes / (1024 * 1024):.2f} MiB")

        for item in planned:
            print(
                f"  {item['source']} -> "
                f"s3://{item['bucket']}/{item['key']} "
                f"({item['size']:,} bytes)"
            )

        print("\n[4/5] Authenticating and checking buckets...")
        s3 = create_s3_client()
        verify_required_buckets(s3)
        print("  [PASS] MinIO authentication and bucket checks succeeded.")

        if args.dry_run:
            print("\n[DRY RUN SUCCESS]")
            print("No files were uploaded or modified.")
            print("If this inventory is correct, run the same script with --upload.")
            return 0

        print("\n[5/5] Uploading and independently verifying objects...")
        report: dict[str, Any] = {
            "project": "BIGDATA_SIBI",
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "endpoint": ENDPOINT_URL,
            "status": "in_progress",
            "file_count_expected": len(planned),
            "files": [],
        }

        try:
            for index, item in enumerate(planned, start=1):
                path: Path = item["path"]
                bucket = item["bucket"]
                key = item["key"]
                local_size = item["size"]
                local_digest = sha256_file(path)
                content_type = (
                    mimetypes.guess_type(path.name)[0]
                    or "application/octet-stream"
                )

                print(f"\n[{index}/{len(planned)}] Uploading {item['source']}")
                s3.upload_file(
                    str(path),
                    bucket,
                    key,
                    ExtraArgs={
                        "ContentType": content_type,
                        "Metadata": {
                            "sha256": local_digest,
                            "source-path": item["source"],
                        },
                    },
                )

                head = s3.head_object(Bucket=bucket, Key=key)
                metadata_digest = head.get("Metadata", {}).get("sha256", "")
                remote_digest, remote_size = remote_sha256(s3, bucket, key)

                if head["ContentLength"] != local_size or remote_size != local_size:
                    raise RuntimeError(
                        f"Size mismatch after upload for {item['source']}."
                    )
                if metadata_digest != local_digest:
                    raise RuntimeError(
                        f"SHA-256 metadata mismatch for {item['source']}."
                    )
                if remote_digest != local_digest:
                    raise RuntimeError(
                        f"Remote SHA-256 mismatch for {item['source']}."
                    )

                print(
                    f"  [VERIFIED] {local_size:,} bytes | "
                    f"SHA-256 {local_digest}"
                )

                report["files"].append(
                    {
                        "source": item["source"],
                        "bucket": bucket,
                        "key": key,
                        "size_bytes": local_size,
                        "sha256": local_digest,
                        "status": "verified",
                    }
                )

            report["status"] = "success"
            report["verified_file_count"] = len(report["files"])
            report_path = save_report(report)

            print("\n" + "=" * 68)
            print("[SUCCESS] All dataset objects uploaded and verified.")
            print(f"Verified files : {len(report['files'])}")
            print(f"Report         : {report_path.relative_to(PROJECT_ROOT)}")
            print("=" * 68)
            return 0

        except Exception as exc:
            report["status"] = "failed"
            report["error_type"] = type(exc).__name__
            report["error_message"] = str(exc)
            report["verified_file_count"] = len(report["files"])
            report_path = save_report(report)
            print(f"\n[FAILED] {type(exc).__name__}: {exc}")
            print(f"Partial report: {report_path.relative_to(PROJECT_ROOT)}")
            print("Do not delete uploaded objects; rerun only after reviewing the error.")
            return 1

    except (FileNotFoundError, ValueError) as exc:
        print(f"\n[VALIDATION FAILED] {exc}")
        print("No files were uploaded.")
        return 2
    except (ClientError, BotoCoreError) as exc:
        code = "Unknown"
        message = str(exc)
        if isinstance(exc, ClientError):
            error = exc.response.get("Error", {})
            code = error.get("Code", "Unknown")
            message = error.get("Message", str(exc))
        print("\n[MINIO ERROR]")
        print(f"Error code : {code}")
        print(f"Message    : {message}")
        print("No upload should be considered successful.")
        return 3
    except Exception as exc:
        print(f"\n[ERROR] {type(exc).__name__}: {exc}")
        return 4


if __name__ == "__main__":
    sys.exit(main())
