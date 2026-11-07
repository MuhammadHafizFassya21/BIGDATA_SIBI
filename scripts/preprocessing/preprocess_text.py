#!/usr/bin/env python3
"""
==============================================================================
TAHAP 3: TEXT PREPROCESSING BAHASA INDONESIA — PROYEK BIGDATA_SIBI
==============================================================================
Program preprocessing teks Bahasa Indonesia untuk mempersiapkan dataset buku
digital SIBI ke dalam format Bag of Words (BoW) yang siap diproses oleh
Hadoop MapReduce.

Alur Pemrosesan per Buku:
1. Pembacaan teks mentah dari extracted_text/raw/
2. Text Cleaning:
   - Penghapusan penanda halaman (===== PAGE X =====)
   - Penghapusan nomor halaman tunggal yang berdiri sendiri
   - Pembersihan karakter kontrol non-cetak
   - Normalisasi Unicode NFC
3. Penanganan Pemenggalan Suku Kata (Syllable Breakdown & Spelled Letters):
   - Deteksi dan eliminasi pola seperti 'bi - ru', 'bi – ru', 'b - i - r - u'
   - Perlindungan terhadap kata ulang sah (misal: 'anak-anak', 'buku-buku')
   - Perlindungan terhadap imbuhan/afiks (misal: 'ber- dan', 'meN- yang')
4. Case Folding: pengubahan teks menjadi huruf kecil (lowercase)
5. Tokenization: ekstraksi token kata Unicode termasuk kata berhubung internal
6. Stopword Removal: eliminasi kata fungsi berdasarkan configs/stopwords_id.txt
   (kata substantif seperti buku, bahasa, indonesia, membaca tetap terjaga)
7. Filter Token 1 Karakter: dicatat dan dikeluarkan dari hasil utama (default)
8. Penyimpanan Output:
   - Output per buku di extracted_text/cleaned/<nama_buku>_cleaned.txt
   - Input gabungan TSV di dataset/processed/bow_input.tsv (book_id<TAB>token)
   - Manifest di dataset/processed/bow_manifest.json
   - Laporan pemrosesan di logs/preprocessing_report.json dan .csv
==============================================================================
"""

import argparse
import csv
from datetime import datetime
import json
import logging
from pathlib import Path
import re
import sys
import unicodedata
from typing import Dict, List, Tuple, Any, Set


# ==============================================================================
# KONFIGURASI LOGGING
# ==============================================================================
def setup_logging(log_file: Path) -> logging.Logger:
    """Mengatur logging ganda ke file dan konsol."""
    log_file.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("preprocess_text")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    # File handler
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setLevel(logging.INFO)
    fh_formatter = logging.Formatter(
        "[%(asctime)s] [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )
    fh.setFormatter(fh_formatter)
    logger.addHandler(fh)

    # Stream (console) handler
    sh = logging.StreamHandler(sys.stdout)
    sh.setLevel(logging.INFO)
    sh_formatter = logging.Formatter("[%(levelname)s] %(message)s")
    sh.setFormatter(sh_formatter)
    logger.addHandler(sh)

    return logger


# ==============================================================================
# KELAS PREPROCESSOR TEKS
# ==============================================================================
class TextPreprocessor:
    """Melakukan tahapan preprocessing teks Bahasa Indonesia sesuai standar Big Data SIBI."""

    def __init__(
        self,
        base_dir: Path,
        stopwords_path: Path,
        exclude_single_char: bool = True,
        logger: logging.Logger = None,
    ):
        self.base_dir = base_dir.resolve()
        self.stopwords_path = stopwords_path.resolve()
        self.exclude_single_char = exclude_single_char
        self.logger = logger or logging.getLogger("preprocess_text")
        self.stopwords = self._load_stopwords()

        # Regular Expressions Kompilasi
        self.dash_chars = r"[-–—]"
        
        # Pola penanda halaman
        self.page_marker_re = re.compile(
            r"^\s*={3,}\s*PAGE\s+\d+\s*={3,}\s*$", re.MULTILINE
        )
        # Pola nomor halaman tunggal pada satu baris
        self.standalone_num_re = re.compile(r"^\s*\d+\s*$", re.MULTILINE)
        # Pola angka romawi kecil tunggal pada satu baris (misal halaman depan: i, ii, iii, iv)
        self.standalone_roman_re = re.compile(
            r"^\s*[ivxlcdm]+\s*$", re.MULTILINE | re.IGNORECASE
        )
        
        # Karakter kontrol (selain \t, \n, \r)
        self.control_chars_re = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")
        
        # Pola pemenggalan suku kata (1-3 huruf dengan spasi horizontal di sekitar tanda hubung/dash)
        # Contoh: bi - ru, bi – ru, ba - tu, po - li - si, sat - pam
        self.syllable_pattern_re = re.compile(
            r"\b[A-Za-z]{1,3}[ \t]+"
            + self.dash_chars
            + r"[ \t]+[A-Za-z]{1,3}(?:[ \t]+"
            + self.dash_chars
            + r"[ \t]+[A-Za-z]{1,3})*\b"
        )
        
        # Pola ejaan huruf tunggal yang dipisah tanda hubung (dengan atau tanpa spasi)
        # Contoh: b - i - r - u, b-i-r-u, s-u-s-u, h - a - t - i
        self.spelled_letters_re = re.compile(
            r"\b[A-Za-z](?:[ \t]*" + self.dash_chars + r"[ \t]*[A-Za-z]){1,}\b"
        )
        
        # Tokenizer regex yang mendukung karakter huruf Unicode dan tanda hubung internal (kata ulang)
        # Contoh: anak-anak, buku-buku, 2023
        self.token_re = re.compile(
            r"(?u)\b[a-z0-9\u00e0-\u024f]+(?:-[a-z0-9\u00e0-\u024f]+)*\b"
        )

    def _load_stopwords(self) -> Set[str]:
        """Memuat daftar stopwords dari file teks lokal."""
        if not self.stopwords_path.is_file():
            self.logger.error(f"File stopwords tidak ditemukan: {self.stopwords_path}")
            raise FileNotFoundError(f"Stopwords file missing: {self.stopwords_path}")

        stopwords = set()
        with open(self.stopwords_path, mode="r", encoding="utf-8") as f:
            for line in f:
                line = line.strip().lower()
                if line and not line.startswith("#"):
                    stopwords.add(line)

        self.logger.info(
            f"Berhasil memuat {len(stopwords)} stopwords dari {self.stopwords_path.name}"
        )
        return stopwords

    def clean_text(self, raw_text: str) -> Tuple[str, int, int]:
        """
        Tahap 1 & 2: Pembersihan teks, penanganan penanda halaman, kontrol karakter,
        dan pemenggalan suku kata.
        Mengembalikan: (cleaned_text, pages_detected, hyphenated_syllables_removed)
        """
        # 1. Normalisasi Unicode (NFC)
        text = unicodedata.normalize("NFC", raw_text)

        # Hitung jumlah halaman dari penanda halaman
        pages_detected = len(self.page_marker_re.findall(text))

        # 2. Hapus penanda halaman
        text = self.page_marker_re.sub("", text)

        # 3. Hapus nomor halaman tunggal yang berdiri sendiri
        text = self.standalone_num_re.sub("", text)
        text = self.standalone_roman_re.sub("", text)

        # 4. Hapus karakter kontrol yang tidak diperlukan
        text = self.control_chars_re.sub("", text)

        # 5. Normalisasi kata ulang yang terpisah spasi di sekitar strip jika bagian identik
        # Misalnya: hati - hati -> hati-hati, kata – kata -> kata-kata
        def repl_reduplication(match: re.Match) -> str:
            val = match.group(0)
            parts = re.split(self.dash_chars, val)
            parts_clean = [p.strip().lower() for p in parts if p.strip()]
            if len(set(parts_clean)) == 1 and len(parts_clean) > 1:
                return "-".join(parts_clean)
            return val

        redup_pattern = re.compile(
            r"\b[A-Za-z]+[ \t]+" + self.dash_chars + r"[ \t]+[A-Za-z]+\b"
        )
        text = redup_pattern.sub(repl_reduplication, text)

        # 6. Hapus pola pemenggalan suku kata dan ejaan huruf per huruf
        syllables_removed = 0

        def repl_syllable(match: re.Match) -> str:
            nonlocal syllables_removed
            syllables_removed += 1
            return " "

        # Sub pola suku kata (max 3 huruf per segmen dengan spasi)
        text = self.syllable_pattern_re.sub(repl_syllable, text)

        # Sub pola huruf dieja satu per satu (b - i - r - u, s-u-s-u, dll.)
        text = self.spelled_letters_re.sub(repl_syllable, text)

        return text, pages_detected, syllables_removed

    def process_book(
        self, book_meta: Dict[str, str], raw_file_path: Path
    ) -> Dict[str, Any]:
        """
        Memproses satu buku secara terisolasi dan mengembalikan statistik serta daftar token.
        """
        book_id = book_meta["book_id"]
        title = book_meta.get("title", "")
        status = book_meta.get("dataset_status", "primary")

        self.logger.info(f"--- Memproses Book ID {book_id}: {title} ({raw_file_path.name}) ---")

        if not raw_file_path.is_file():
            err_msg = f"File input raw tidak ditemukan: {raw_file_path}"
            self.logger.error(err_msg)
            return {
                "book_id": book_id,
                "title": title,
                "dataset_status": status,
                "source_file": raw_file_path.name,
                "output_file": "",
                "raw_characters": 0,
                "tokens_before_cleaning": 0,
                "tokens_after_cleaning": 0,
                "stopwords_removed": 0,
                "single_character_tokens_excluded": 0,
                "hyphenated_syllable_patterns_removed": 0,
                "tokens_after_preprocessing": 0,
                "unique_tokens": 0,
                "pages_detected": 0,
                "processing_status": "FAILED",
                "error_message": err_msg,
                "tokens": [],
            }

        try:
            with open(raw_file_path, mode="r", encoding="utf-8") as f:
                raw_text = f.read()

            raw_chars = len(raw_text)

            # Hitung total token sebelum cleaning menggunakan regex standar
            tokens_before_cleaning = len(self.token_re.findall(raw_text.lower()))

            # Tahap Cleaning & Pemenggalan Suku Kata
            cleaned_text, pages_detected, syl_removed = self.clean_text(raw_text)

            # Case Folding
            cleaned_text_lower = cleaned_text.lower()

            # Tokenization
            all_cleaned_tokens = self.token_re.findall(cleaned_text_lower)
            tokens_after_cleaning = len(all_cleaned_tokens)

            # Stopword Removal & Single Character Exclusion
            stopwords_removed_count = 0
            single_char_excluded_count = 0
            final_tokens = []

            for token in all_cleaned_tokens:
                if len(token) <= 1:
                    single_char_excluded_count += 1
                elif token in self.stopwords:
                    stopwords_removed_count += 1
                else:
                    final_tokens.append(token)

            # Verifikasi integritas token
            assert (
                tokens_after_cleaning
                == single_char_excluded_count + stopwords_removed_count + len(final_tokens)
            ), "Inkonsistensi jumlah token setelah cleaning!"

            unique_tokens_count = len(set(final_tokens))
            tokens_after_preprocessing = len(final_tokens)

            self.logger.info(
                f"Selesai: {raw_chars:,} char, "
                f"Cleaning: {tokens_before_cleaning:,} -> {tokens_after_cleaning:,} token, "
                f"Syllables removed: {syl_removed}, Single-char excluded: {single_char_excluded_count:,}, "
                f"Stopwords removed: {stopwords_removed_count:,}, "
                f"Final tokens: {tokens_after_preprocessing:,} (Unique: {unique_tokens_count:,})"
            )

            # Tentukan nama output cleaned file
            raw_stem = raw_file_path.stem
            cleaned_filename = f"{raw_stem}_cleaned.txt"

            return {
                "book_id": book_id,
                "title": title,
                "dataset_status": status,
                "source_file": raw_file_path.name,
                "output_file": cleaned_filename,
                "raw_characters": raw_chars,
                "tokens_before_cleaning": tokens_before_cleaning,
                "tokens_after_cleaning": tokens_after_cleaning,
                "stopwords_removed": stopwords_removed_count,
                "single_character_tokens_excluded": single_char_excluded_count,
                "hyphenated_syllable_patterns_removed": syl_removed,
                "tokens_after_preprocessing": tokens_after_preprocessing,
                "unique_tokens": unique_tokens_count,
                "pages_detected": pages_detected,
                "processing_status": "SUCCESS",
                "error_message": "",
                "tokens": final_tokens,
            }

        except Exception as e:
            err_msg = str(e)
            self.logger.exception(f"Error memproses {raw_file_path.name}: {err_msg}")
            return {
                "book_id": book_id,
                "title": title,
                "dataset_status": status,
                "source_file": raw_file_path.name,
                "output_file": "",
                "raw_characters": 0,
                "tokens_before_cleaning": 0,
                "tokens_after_cleaning": 0,
                "stopwords_removed": 0,
                "single_character_tokens_excluded": 0,
                "hyphenated_syllable_patterns_removed": 0,
                "tokens_after_preprocessing": 0,
                "unique_tokens": 0,
                "pages_detected": 0,
                "processing_status": "FAILED",
                "error_message": err_msg,
                "tokens": [],
            }


# ==============================================================================
# PIPELINE ORKESTRASI LENGKAP
# ==============================================================================
def run_pipeline(
    base_dir: Path,
    stopwords_file: Path,
    dry_run: bool = False,
    exclude_single_char: bool = True,
) -> Dict[str, Any]:
    """Menjalankan seluruh tahapan pipeline preprocessing untuk 4 buku SIBI."""
    base_dir = base_dir.resolve()
    logs_dir = base_dir / "logs"
    extracted_cleaned_dir = base_dir / "extracted_text" / "cleaned"
    processed_dataset_dir = base_dir / "dataset" / "processed"
    metadata_file = base_dir / "metadata" / "books.csv"

    # Inisialisasi Logger
    log_file = logs_dir / "preprocessing.log"
    logger = setup_logging(log_file)
    logger.info("==================================================================")
    logger.info("MEMULAI TAHAP 3: TEXT PREPROCESSING BAHASA INDONESIA SIBI")
    logger.info("==================================================================")
    logger.info(f"Base Directory: {base_dir}")
    logger.info(f"Stopwords File: {stopwords_file}")
    logger.info(f"Exclude Single Character Tokens: {exclude_single_char}")
    logger.info(f"Dry Run Mode: {dry_run}")

    # 1. Validasi Keberadaan Input & Stopwords
    if not metadata_file.is_file():
        logger.error(f"File metadata tidak ditemukan: {metadata_file}")
        sys.exit(1)

    if not stopwords_file.is_file():
        logger.error(f"File stopwords tidak ditemukan: {stopwords_file}")
        sys.exit(1)

    # 2. Baca metadata buku
    books_meta = []
    with open(metadata_file, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            books_meta.append(row)

    logger.info(f"Metadata berhasil dibaca: {len(books_meta)} entri buku ditemukan.")

    # Pemetaan file raw untuk masing-masing book_id
    raw_files_map = {
        "1": base_dir / "extracted_text" / "raw" / "book_001_SD_sd.txt",
        "2": base_dir / "extracted_text" / "raw" / "book_002_SD_sd.txt",
        "3": base_dir / "extracted_text" / "raw" / "book_003_SMP_mp.txt",
        "4": base_dir / "extracted_text" / "raw" / "book_004_SMP_optional.txt",
    }

    # Inisialisasi Preprocessor
    preprocessor = TextPreprocessor(
        base_dir=base_dir,
        stopwords_path=stopwords_file,
        exclude_single_char=exclude_single_char,
        logger=logger,
    )

    results = []
    combined_bow_entries: List[Tuple[str, str]] = []
    primary_bow_entries: List[Tuple[str, str]] = []

    # 3. Proses setiap buku secara terpisah
    for meta in books_meta:
        b_id = str(meta["book_id"])
        raw_path = raw_files_map.get(b_id)
        if not raw_path or not raw_path.is_file():
            logger.error(f"File raw untuk book_id {b_id} tidak ditemukan: {raw_path}")
            sys.exit(1)

        res = preprocessor.process_book(meta, raw_path)
        if res["processing_status"] != "SUCCESS":
            logger.error(f"Pemrosesan gagal untuk book_id {b_id}: {res['error_message']}")
            sys.exit(1)

        results.append(res)

        # Kumpulkan entri Bag of Words
        tokens = res["tokens"]
        for t in tokens:
            combined_bow_entries.append((b_id, t))
            if meta.get("dataset_status") == "primary":
                primary_bow_entries.append((b_id, t))

    # 4. Simpan Output jika bukan dry-run
    if not dry_run:
        extracted_cleaned_dir.mkdir(parents=True, exist_ok=True)
        processed_dataset_dir.mkdir(parents=True, exist_ok=True)

        # 4a. Simpan file per buku ke extracted_text/cleaned/
        for res in results:
            out_path = extracted_cleaned_dir / res["output_file"]
            logger.info(f"Menyimpan output bersih per buku: {out_path.name}")
            with open(out_path, mode="w", encoding="utf-8") as f:
                for token in res["tokens"]:
                    f.write(f"{token}\n")

        # 4b. Simpan gabungan input Bag of Words ke dataset/processed/bow_input.tsv
        bow_tsv_path = processed_dataset_dir / "bow_input.tsv"
        logger.info(
            f"Menyimpan gabungan Bag of Words TSV: {bow_tsv_path.name} "
            f"({len(combined_bow_entries):,} baris)"
        )
        with open(bow_tsv_path, mode="w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f, delimiter="\t", lineterminator="\n")
            for b_id, token in combined_bow_entries:
                writer.writerow([b_id, token])

        # 4c. Simpan file TSV primary-only sebagai opsi pendamping yang aman
        bow_primary_path = processed_dataset_dir / "bow_input_primary.tsv"
        logger.info(
            f"Menyimpan pendamping Bag of Words TSV (Primary only): {bow_primary_path.name} "
            f"({len(primary_bow_entries):,} baris)"
        )
        with open(bow_primary_path, mode="w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f, delimiter="\t", lineterminator="\n")
            for b_id, token in primary_bow_entries:
                writer.writerow([b_id, token])

        # 4d. Buat Manifest Dataset Processed
        manifest_data = {
            "created_at": datetime.now().isoformat(),
            "description": "Manifest pemetaan dataset Bag of Words SIBI hasil Text Preprocessing",
            "files": {
                "combined_tsv": "bow_input.tsv",
                "primary_only_tsv": "bow_input_primary.tsv",
            },
            "total_combined_tokens": len(combined_bow_entries),
            "total_primary_tokens": len(primary_bow_entries),
            "books": [
                {
                    "book_id": r["book_id"],
                    "title": r["title"],
                    "dataset_status": r["dataset_status"],
                    "source_raw_file": r["source_file"],
                    "cleaned_file": r["output_file"],
                    "total_tokens": r["tokens_after_preprocessing"],
                    "unique_tokens": r["unique_tokens"],
                }
                for r in results
            ],
        }

        manifest_json_path = processed_dataset_dir / "bow_manifest.json"
        with open(manifest_json_path, mode="w", encoding="utf-8") as f:
            json.dump(manifest_data, f, indent=2, ensure_ascii=False)
        logger.info(f"Manifest disimpan: {manifest_json_path.name}")

        manifest_csv_path = processed_dataset_dir / "bow_manifest.csv"
        with open(manifest_csv_path, mode="w", encoding="utf-8", newline="") as f:
            fieldnames = [
                "book_id",
                "title",
                "dataset_status",
                "source_raw_file",
                "cleaned_file",
                "total_tokens",
                "unique_tokens",
            ]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for r in results:
                writer.writerow(
                    {
                        "book_id": r["book_id"],
                        "title": r["title"],
                        "dataset_status": r["dataset_status"],
                        "source_raw_file": r["source_file"],
                        "cleaned_file": r["output_file"],
                        "total_tokens": r["tokens_after_preprocessing"],
                        "unique_tokens": r["unique_tokens"],
                    }
                )

        # 5. Buat Laporan Preprocessing
        timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_json_path = logs_dir / "preprocessing_report.json"
        report_csv_path = logs_dir / "preprocessing_report.csv"

        # Jika sudah ada, jangan menimpa, gunakan nama timestamp
        if report_json_path.exists():
            report_json_path = logs_dir / f"preprocessing_report_{timestamp_str}.json"
        if report_csv_path.exists():
            report_csv_path = logs_dir / f"preprocessing_report_{timestamp_str}.csv"

        # Hitung ringkasan total
        total_summary = {
            "total_books_processed": len(results),
            "total_raw_characters": sum(r["raw_characters"] for r in results),
            "total_tokens_before_cleaning": sum(
                r["tokens_before_cleaning"] for r in results
            ),
            "total_tokens_after_cleaning": sum(
                r["tokens_after_cleaning"] for r in results
            ),
            "total_stopwords_removed": sum(r["stopwords_removed"] for r in results),
            "total_single_character_tokens_excluded": sum(
                r["single_character_tokens_excluded"] for r in results
            ),
            "total_hyphenated_syllable_patterns_removed": sum(
                r["hyphenated_syllable_patterns_removed"] for r in results
            ),
            "total_tokens_after_preprocessing": sum(
                r["tokens_after_preprocessing"] for r in results
            ),
            "total_unique_tokens_across_corpus": len(
                set(t for r in results for t in r["tokens"])
            ),
            "combined_bow_tsv_file": str(bow_tsv_path.relative_to(base_dir)),
        }

        # Simpan Laporan JSON
        report_data = {
            "generated_at": datetime.now().isoformat(),
            "pipeline_stage": "TAHAP_3_TEXT_PREPROCESSING",
            "summary": total_summary,
            "book_reports": [
                {k: v for k, v in r.items() if k != "tokens"} for r in results
            ],
        }

        with open(report_json_path, mode="w", encoding="utf-8") as f:
            json.dump(report_data, f, indent=2, ensure_ascii=False)
        logger.info(f"Laporan JSON disimpan: {report_json_path.name}")

        # Simpan Laporan CSV
        report_fields = [
            "book_id",
            "title",
            "dataset_status",
            "source_file",
            "output_file",
            "raw_characters",
            "tokens_before_cleaning",
            "tokens_after_cleaning",
            "stopwords_removed",
            "single_character_tokens_excluded",
            "hyphenated_syllable_patterns_removed",
            "tokens_after_preprocessing",
            "unique_tokens",
            "processing_status",
            "error_message",
        ]
        with open(report_csv_path, mode="w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=report_fields)
            writer.writeheader()
            for r in results:
                row_dict = {k: r.get(k, "") for k in report_fields}
                writer.writerow(row_dict)
        logger.info(f"Laporan CSV disimpan: {report_csv_path.name}")

    logger.info("==================================================================")
    logger.info("PREPROCESSING BERHASIL DISELESAIKAN SECARA LENGKAP & VALID")
    logger.info("==================================================================")

    return {
        "results": results,
        "combined_tokens_count": len(combined_bow_entries),
        "primary_tokens_count": len(primary_bow_entries),
    }


# ==============================================================================
# ENTRY POINT CLI
# ==============================================================================
def main():
    parser = argparse.ArgumentParser(
        description="Text Preprocessing Bahasa Indonesia untuk Buku Pelajaran SIBI."
    )
    parser.add_argument(
        "--base-dir",
        type=Path,
        default=Path(__file__).resolve().parent.parent.parent,
        help="Path ke direktori utama proyek BIGDATA_SIBI",
    )
    parser.add_argument(
        "--stopwords",
        type=Path,
        default=None,
        help="Path ke file stopwords_id.txt (default: configs/stopwords_id.txt)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Jalankan simulasi tanpa menyimpan file output ke disk",
    )
    parser.add_argument(
        "--include-single-char",
        action="store_true",
        help="Sertakan token satu karakter (secara default dikeluarkan)",
    )

    args = parser.parse_args()

    base_dir = args.base_dir.resolve()
    stopwords_path = (
        args.stopwords.resolve()
        if args.stopwords
        else base_dir / "configs" / "stopwords_id.txt"
    )

    exclude_single_char = not args.include_single_char

    run_pipeline(
        base_dir=base_dir,
        stopwords_file=stopwords_path,
        dry_run=args.dry_run,
        exclude_single_char=exclude_single_char,
    )


if __name__ == "__main__":
    main()

