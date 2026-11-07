"""
PDF Text Extraction Script for BIGDATA_SIBI Project
Men-ekstrak teks dari buku PDF Bahasa Indonesia SIBI menggunakan PyMuPDF.

Aturan:
- Hanya menambahkan file baru, tidak menghapus/memindahkan file lama
- File PDF tidak diubah nama atau dipindahkan
- books.csv dan README.md tidak diubah
- Folder hadoop-wordcount tidak disentuh
"""

import csv
import json
import logging
from datetime import datetime
from pathlib import Path

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/extraction.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


def check_dependencies():
    """Check if PyMuPDF is available."""
    try:
        import pymupdf
        logger.info("PyMuPDF (pymupdf) tersedia")
        return True
    except ImportError:
        logger.warning("PyMuPDF (pymupdf) tidak tersedia")
        return False


def read_metadata(csv_path):
    """Read metadata from books.csv."""
    books = []
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            books.append(row)
    logger.info(f"Baca {len(books)} buku dari metadata")
    return books


def determine_pdf_location(book, dataset_root):
    """Determine PDF file location based on dataset_status."""
    status = book["dataset_status"]
    filename = book["filename"]

    if status == "primary":
        edu_level = book["education_level"]
        # Path: dataset/raw/{education_level}/{filename}
        pdf_path = Path(dataset_root) / "raw" / edu_level / filename
    elif status == "optional_comparison":
        # Path: dataset/optional/SMP_2017/{filename}
        pdf_path = Path(dataset_root) / "optional" / "SMP_2017" / filename
    else:
        logger.error(f"Unknown dataset_status: {status} for book {book['book_id']}")
        return None

    return pdf_path


def extract_text_from_pdf(pdf_path):
    """Extract text from PDF using PyMuPDF."""
    import pymupdf

    text_parts = []
    actual_pages = 0
    empty_pages = 0

    try:
        doc = pymupdf.open(pdf_path)
        actual_pages = len(doc)

        for page_num in range(doc.page_count):
            page = doc[page_num]
            page_text = page.get_text("text")
            text_parts.append(f"===== PAGE {page_num + 1} =====\n{page_text}")
            if not page_text.strip():
                empty_pages += 1

        doc.close()
    except Exception as e:
        logger.error(f"Error membuka PDF {pdf_path}: {e}")
        return None, 0, 0

    extracted_pages = len(text_parts)
    total_chars = sum(len(part.split("===== PAGE")[0]) if "===== PAGE" in part else 0 for part in text_parts)
    # Count total characters from all text
    total_chars = sum(len(part.split("\n", 1)[1]) if "===== PAGE" in part else 0 for part in text_parts)

    return text_parts, actual_pages, extracted_pages, empty_pages, total_chars


def save_extracted_text(output_path, page_texts, book_id):
    """Save extracted text to file."""
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Check if file already exists
    if output_path.exists():
        logger.warning(f"File tujuan sudah ada: {output_path}")
        return False

    try:
        with open(output_path, "w", encoding="utf-8") as f:
            for part in page_texts:
                f.write(part)
                f.write("\n")
        logger.info(f"Teks berhasil disimpan: {output_path}")
        return True
    except Exception as e:
        logger.error(f"Error menyimpan file {output_path}: {e}")
        return False


def main():
    """Main extraction process."""
    logger.info("=== MEMULAI PDF TEXT EXTRACTION ===")
    logger.info("BIGDATA_SIBI - Analisis Frekuensi Kemunculan Kata pada Buku Digital")

    # Check dependencies
    has_fitz = check_dependencies()

    # Paths
    project_root = Path(__file__).parent.parent.parent
    metadata_path = project_root / "metadata" / "books.csv"
    dataset_root = project_root / "dataset"
    extracted_text_root = project_root / "extracted_text" / "raw"

    # Read metadata
    books = read_metadata(metadata_path)

    # Validation
    logger.info("=== VALIDASI DATASET ===")
    pdf_status = {}

    for book in books:
        book_id = book["book_id"]
        title = book["title"]
        filename = book["filename"]
        status = book["dataset_status"]

        # Verify PDF exists
        pdf_path = determine_pdf_location(book, dataset_root)

        if pdf_path is None:
            pdf_status[book_id] = {
                "status": "error",
                "message": "Cannot determine PDF location",
            }
            continue

        if pdf_path.exists():
            pdf_status[book_id] = {
                "status": "found",
                "path": str(pdf_path),
                "filename": filename,
            }
            logger.info(f"✓ PDF ditemukan: {pdf_path}")
        else:
            pdf_status[book_id] = {
                "status": "missing",
                "path": str(pdf_path),
                "filename": filename,
                "error": f"PDF not found at: {pdf_path}",
            }
            logger.error(f"✗ PDF TIDAK DITEMUKAN: {pdf_path}")
            # Peraturan: "Jika ada PDF yang tidak ditemukan, hentikan proses ekstraksi
            # dan laporkan. Jangan mencari atau mengunduh penggantinya secara otomatis."
            logger.error(
                f"Menghentikan ekstraksi karena PDF tidak ditemukan: {filename}"
            )
            return

    # Check CSV row count and columns
    logger.info("Validasi struktur CSV...")
    with open(metadata_path, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        headers = next(reader)
        data_rows = list(reader)

    logger.info(f"Jumlah baris data: {len(data_rows)} (harus: 4)")
    logger.info(f"Jumlah kolom header: {len(headers)} (harus: 12)")

    if len(data_rows) != 4:
        logger.error(f"Jumlah baris data salah: {len(data_rows)}")
    if len(headers) != 12:
        logger.error(f"Jumlah kolom salah: {len(headers)}")

    # Process each book
    logger.info("=== MEMULAI EKSTRAKSI TEKS ===")
    summary = {
        "total_books": len(books),
        "successful": 0,
        "failed": 0,
        "total_pages_expected": 0,
        "total_pages_actual": 0,
        "total_empty_pages": 0,
        "total_characters": 0,
        "details": [],
    }

    for book in books:
        book_id_raw = book["book_id"].strip()
        title = book["title"]
        filename = book["filename"]
        status = book["dataset_status"]

        # Validate book_id - harus berupa angka
        try:
            book_id_num = int(book_id_raw)
        except ValueError:
            logger.error(f"Invalid book_id: '{book_id_raw}' for book {book.get('title', 'unknown')}")
            summary["failed"] += 1
            summary["details"].append(
                {
                    "book_id": book_id_raw,
                    "title": title,
                    "filename": filename,
                    "status": "error",
                    "error_message": f"Invalid book_id: '{book_id_raw}' must be a number",
                }
            )
            continue

        pdf_path = determine_pdf_location(book, dataset_root)
        if pdf_path is None or pdf_status[book_id_raw]["status"] != "found":
            summary["failed"] += 1
            summary["details"].append(
                {
                    "book_id": book_id_raw,
                    "title": title,
                    "filename": filename,
                    "status": "skipped",
                    "error": "PDF not found or location error",
                }
            )
            continue

        # Extract text
        result = extract_text_from_pdf(pdf_path)

        if result is None:
            summary["failed"] += 1
            summary["details"].append(
                {
                    "book_id": book_id_raw,
                    "title": title,
                    "filename": filename,
                    "status": "failed",
                    "error": "Extraction error",
                }
            )
            continue

        page_texts, actual_pages, extracted_pages, empty_pages, total_chars = result

        # Determine output filename
        # book_001_sd_kelas_i.txt, etc.
        # Extract level and create appropriate naming
        edu_level = book["education_level"]
        if status == "primary":
            # primary books: book_001, book_002, book_003
            # Map: SD -> sd, SMP -> mp
            level_short = "sd" if edu_level == "SD" else "mp"
            output_name = f"book_{book_id_num:03d}_{edu_level}_{level_short}.txt"
        else:
            # optional book
            output_name = f"book_{book_id_num:03d}_{edu_level}_optional.txt"

        output_path = extracted_text_root / output_name

        # Save extracted text
        success = save_extracted_text(output_path, page_texts, book_id_raw)

        if success:
            summary["successful"] += 1
            summary["total_pages_expected"] += actual_pages
            summary["total_pages_actual"] += extracted_pages
            summary["total_empty_pages"] += empty_pages
            summary["total_characters"] += total_chars
            summary["details"].append(
                {
                    "book_id": book_id_num,
                    "title": title,
                    "filename": filename,
                    "status": "success",
                    "output_file": str(output_path),
                    "expected_pages": actual_pages,
                    "extracted_pages": extracted_pages,
                    "empty_pages": empty_pages,
                    "total_characters": total_chars,
                }
            )
            logger.info(
                f"✓ {title}: {extracted_pages}/{actual_pages} halaman diekstrak, "
                f"{total_chars} karakter -> {output_name}"
            )
        else:
            summary["failed"] += 1
            summary["details"].append(
                {
                    "book_id": book_id_raw,
                    "title": title,
                    "filename": filename,
                    "status": "save_failed",
                    "error": "Gagal menyimpan file output",
                }
            )
            logger.error(f"✗ Gagal menyimpan: {title}")

    # Generate report JSON
    report_time = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_json_path = Path(f"logs/pdf_extraction_report_{report_time}.json")
    report_json_path.parent.mkdir(parents=True, exist_ok=True)

    # Avoid overwriting existing report
    if report_json_path.exists():
        report_json_path = Path(f"logs/pdf_extraction_report_{report_time}_v2.json")

    summary_report = {
        "timestamp": datetime.now().isoformat(),
        "total_books": summary["total_books"],
        "successful": summary["successful"],
        "failed": summary["failed"],
        "total_pages_expected": summary["total_pages_expected"],
        "total_pages_actual": summary["total_pages_actual"],
        "total_empty_pages": summary["total_empty_pages"],
        "total_characters": summary["total_characters"],
        "details": summary["details"],
    }

    try:
        with open(report_json_path, "w", encoding="utf-8") as f:
            json.dump(summary_report, f, indent=2, ensure_ascii=False)
        logger.info(f"Laporan JSON disimpan: {report_json_path}")
    except Exception as e:
        logger.error(f"Error menyimpan laporan JSON: {e}")

    # Generate report CSV
    report_csv_path = Path(f"logs/pdf_extraction_report_{report_time}.csv")
    if report_csv_path.exists():
        report_csv_path = Path(f"logs/pdf_extraction_report_{report_time}_v2.csv")

    try:
        with open(report_csv_path, "w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(
                [
                    "book_id",
                    "title",
                    "filename",
                    "dataset_status",
                    "expected_pages",
                    "extracted_pages",
                    "empty_pages",
                    "total_characters",
                    "output_file",
                    "extraction_status",
                    "error_message",
                ]
            )
            for detail in summary["details"]:
                writer.writerow(
                    [
                        detail.get("book_id", ""),
                        detail.get("title", ""),
                        detail.get("filename", ""),
                        detail.get("status", ""),
                        detail.get("expected_pages", ""),
                        detail.get("extracted_pages", ""),
                        detail.get("empty_pages", ""),
                        detail.get("total_characters", ""),
                        detail.get("output_file", ""),
                        detail.get("status", ""),
                        detail.get("error", ""),
                    ]
                )
        logger.info(f"Laporan CSV disimpan: {report_csv_path}")
    except Exception as e:
        logger.error(f"Error menyimpan laporan CSV: {e}")

    # Print summary
    logger.info("=== RINGKASAN EKSTRAKSI ===")
    logger.info(f"Total buku: {summary['total_books']}")
    logger.info(f"Berhasil: {summary['successful']}")
    logger.info(f"Gagal: {summary['failed']}")
    logger.info(f"Total halaman expected: {summary['total_pages_expected']}")
    logger.info(f"Total halaman actual: {summary['total_pages_actual']}")
    logger.info(f"Total halaman kosong: {summary['total_empty_pages']}")
    logger.info(f"Total karakter: {summary['total_characters']}")

    if has_fitz:
        logger.info("PyMuPDF tersedia - ekstraksi selesai")
    else:
        logger.warning(
            "PyMuPDF TIDAK TERSedia - file TXT belum terbentuk. "
            "Install PyMuPDF manually dengan: pip install PyMuPDF"
        )

    logger.info("=== SELESAI ===")


if __name__ == "__main__":
    main()