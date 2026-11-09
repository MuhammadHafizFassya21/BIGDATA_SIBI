"""
BIGDATA_SIBI - MinIO S3 client and connection diagnostic.

Reads MINIO_ROOT_USER and MINIO_ROOT_PASSWORD from:
    configs/minio/minio.env

Uses the local MinIO API published by Docker Compose:
    http://127.0.0.1:19000

This diagnostic only lists buckets. It does not create, delete, or upload objects.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Dict

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError


PROJECT_ROOT = Path(__file__).resolve().parents[2]
ENV_FILE = PROJECT_ROOT / "configs" / "minio" / "minio.env"
MINIO_ENDPOINT = "http://127.0.0.1:19000"
REQUIRED_BUCKETS = ("sibi-raw", "sibi-processed", "sibi-output")


def read_env_file(env_path: Path) -> Dict[str, str]:
    """Read simple KEY=VALUE entries without printing secret values."""
    if not env_path.is_file():
        raise FileNotFoundError(f"MinIO environment file not found: {env_path}")

    values: Dict[str, str] = {}
    for raw_line in env_path.read_text(encoding="utf-8-sig").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()

        # Support values wrapped in matching single or double quotes.
        if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
            value = value[1:-1]

        values[key] = value

    return values


def create_s3_client():
    """Create a Boto3 S3 client configured for MinIO."""
    env = read_env_file(ENV_FILE)
    access_key = env.get("MINIO_ROOT_USER", "")
    secret_key = env.get("MINIO_ROOT_PASSWORD", "")

    if not access_key or not secret_key:
        raise ValueError(
            "MINIO_ROOT_USER or MINIO_ROOT_PASSWORD is missing in "
            "configs/minio/minio.env."
        )

    if secret_key == "REPLACE_WITH_UNIQUE_PASSWORD_MIN_8_CHARS":
        raise ValueError("MinIO password is still set to the placeholder.")

    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-1",
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
            connect_timeout=5,
            read_timeout=20,
            retries={"max_attempts": 2, "mode": "standard"},
        ),
    )


def test_connection() -> int:
    """Authenticate to MinIO and report existing required buckets."""
    print("=" * 64)
    print("BIGDATA_SIBI - MinIO Boto3 Connection Test")
    print("=" * 64)
    print(f"Endpoint : {MINIO_ENDPOINT}")
    print(f"Env file : {ENV_FILE}")
    print("Action   : List buckets only (no data will be modified)")
    print()

    try:
        s3 = create_s3_client()
        response = s3.list_buckets()
        existing = sorted(
            bucket["Name"] for bucket in response.get("Buckets", [])
        )

        print("[SUCCESS] Authentication to MinIO succeeded.")
        print("\nBuckets currently found:")
        if existing:
            for name in existing:
                print(f"  - {name}")
        else:
            print("  (No buckets returned)")

        print("\nRequired bucket checks:")
        all_present = True
        for name in REQUIRED_BUCKETS:
            present = name in existing
            status = "FOUND" if present else "MISSING"
            print(f"  {name}: {status}")
            all_present = all_present and present

        if all_present:
            print("\n[SUCCESS] All three required buckets are present.")
            return 0

        print(
            "\n[WARNING] Authentication works, but one or more required "
            "buckets are missing. No buckets were created."
        )
        return 2

    except (ClientError, BotoCoreError) as exc:
        error_code = "Unknown"
        message = str(exc)
        if isinstance(exc, ClientError):
            error_code = exc.response.get("Error", {}).get("Code", "Unknown")
            message = exc.response.get("Error", {}).get("Message", str(exc))

        print("[FAILED] Could not authenticate or communicate with MinIO.")
        print(f"Error code: {error_code}")
        print(f"Message   : {message}")
        print("Check that the MinIO container is running and that the")
        print("credentials in configs/minio/minio.env match the container.")
        return 1

    except Exception as exc:
        print(f"[FAILED] {type(exc).__name__}: {exc}")
        return 1


if __name__ == "__main__":
    sys.exit(test_connection())
