# DOKUMENTASI KONFIGURASI TAHAP 4B: INFRASTRUKTUR MINIO & HADOOP MAPREDUCE (LOCAL MODE)

## 1. Identitas Proyek
- **Nama Proyek**: `BIGDATA_SIBI`
- **Judul**: Analisis Frekuensi Kemunculan Kata pada Buku Digital Bahasa Indonesia SIBI Menggunakan Hadoop MapReduce.
- **Tujuan**: Membangun pipeline Big Data untuk memproses dan menghitung frekuensi kemunculan kata pada empat buku digital PDF Bahasa Indonesia dari Sistem Informasi Perbukuan Indonesia (SIBI) secara efisien dan terstruktur.

---

## 2. Arsitektur Komputasi: LocalJobRunner
Pada Tahap 4B, arsitektur komputasi MapReduce dikonfigurasi menggunakan **Hadoop LocalJobRunner** (`mapreduce.framework.name=local`).

### Penjelasan Mekanisme:
1. **Eksekusi Komputasi Single-JVM**:
   - Seluruh siklus eksekusi MapReduce (fase Map, Partition/Sort/Spill, Shuffle, dan Reduce) dijalankan secara lokal di dalam satu JVM proses pada kontainer `sibi_namenode`.
   - LocalJobRunner tidak memerlukan daemon penjadwalan klaster terdistribusi (YARN) maupun peluncur kontainer dinamis (Container Executor).
2. **Peran HDFS sebagai Storage Terdistribusi Logis**:
   - Meskipun komputasi berjalan lokal (`LocalJobRunner`), input dan output data disimpan dan dialirkan langsung melalui **HDFS** (`hdfs://namenode:9000`), didukung oleh 1 daemon `sibi_datanode` dengan `dfs.replication=1`.
   - Hal ini memastikan integritas pipeline Big Data: dataset dibaca dari sistem berkas terdistribusi HDFS dan output dituliskan kembali ke HDFS sebelum ditarik ke host atau diarsipkan ke MinIO.
3. **Pernyataan Batasan (Non-Distributed Disclaimer)**:
   > **PERNYATAAN RESMI**: Arsitektur komputasi ini **BUKAN** distributed execution antarnode fisik/virtual. Komputasi dijalankan secara lokal pada single-node LocalJobRunner. Arsitektur ini dipilih secara sadar dan optimal guna menyesuaikan spesifikasi perangkat keras host (RAM 8 GB dengan sisa bebas ~1,9 GB) tanpa mengorbankan fungsionalitas algoritma Hadoop MapReduce.

---

## 3. Perbandingan Mendalam: LocalJobRunner vs YARN (Distributed Mode)

| Aspek Arsitektur | Hadoop LocalJobRunner (Diimplementasikan) | Hadoop YARN (Distributed Mode Penuh) |
| :--- | :--- | :--- |
| **Framework Pengatur** | `mapreduce.framework.name=local` | `mapreduce.framework.name=yarn` |
| **Daemon Cluster Manager** | **Tidak ada** (tanpa ResourceManager) | `resourcemanager` aktif |
| **Daemon Node Worker** | **Tidak ada** (tanpa NodeManager) | `nodemanager` aktif pada setiap node pekerja |
| **Alokasi Resource** | Dikelola langsung oleh proses lokal JVM | Dikelola dinamis melalui YARN ApplicationMaster & Containers |
| **Konsumsi Memori Idle** | Sangat rendah (~512 MB – 768 MB) | Tinggi (~2.5 GB – 4.5 GB hanya untuk daemon YARN) |
| **Kebutuhan RAM Host** | Sangat ramah untuk host 8 GB (bebas ~1.9 GB) | Rentang risiko Out-Of-Memory (OOM) tinggi pada RAM < 16 GB |
| **Replication HDFS** | `dfs.replication=1` | `dfs.replication=2` atau `3` |
| **Skalabilitas Komputasi** | Terbatas pada thread CPU single node | Horizontal scaling melintasi banyak node fisik/virtual |
| **Kesesuaian Proyek** | **Sangat Sesuai** untuk dataset SIBI (90.894 kata) | Berlebihan (overhead klaster jauh lebih besar dari data) |

---

## 4. Peran dan Fungsi Komponen Infrastruktur

### A. MinIO Object Storage (`sibi_minio`)
- **Fungsi**: Berfungsi sebagai penyimpanan objek berbasis S3 API untuk menyimpan snapshot data mentah (PDF buku SIBI), data preprocessing (TXT cleaned dan TSV), serta arsip jangka panjang hasil MapReduce.
- **Daftar Bucket**:
  1. `sibi-raw`: Menyimpan file PDF asli buku pelajaran SIBI dan file metadata (`books.csv`).
  2. `sibi-processed`: Menyimpan token bersih (`*_cleaned.txt`), file gabungan TSV (`bow_input.tsv`, `bow_input_primary.tsv`), dan manifest Bag-of-Words.
  3. `sibi-output`: Menyimpan hasil perhitungan frekuensi kata part-00000 dan laporan audit tiap run.
- **Port**:
  - API: `127.0.0.1:9000` (terikat lokal)
  - Web Console: `127.0.0.1:9001` (antarmuka grafis browser)

### B. Hadoop NameNode (`sibi_namenode`)
- **Fungsi**:
  1. Mengelola namespace dan metadata sistem berkas HDFS (blok, pohon direktori, izin).
  2. Menjadi master host pengeksekusi Hadoop Streaming via LocalJobRunner.
  3. Menyediakan antarmuka pemantauan Web UI HDFS.
- **Port**:
  - Web UI: `127.0.0.1:9870` (terikat lokal)
  - HDFS RPC: Port `9000` **hanya internal Docker bridge** (`hdfs://namenode:9000`), tidak diekspos ke host agar tidak bertabrakan dengan MinIO S3 API!

### C. Hadoop DataNode (`sibi_datanode`)
- **Fungsi**:
  1. Menyimpan blok fisik berkas HDFS pada persistent named volume `sibi_hadoop_datanode_data`.
  2. Melayani permintaan read/write blok data dari NameNode dan Hadoop Streaming runner.
- **Karakteristik**:
  - Berkomunikasi melalui jaringan internal `sibi_network`.
  - Beroperasi dengan `dfs.replication=1`.

---

## 5. Konfigurasi Port & Pencegahan Port Collision

Salah satu titik kritis konfigurasi Hadoop dan MinIO adalah perebutan port default `9000` (HDFS NameNode RPC default = 9000, MinIO S3 API default = 9000).

### Solusi Pemetaan Port:
1. **MinIO S3 API**: Dipetakan ke host Windows pada `127.0.0.1:9000:9000`.
2. **MinIO Web Console**: Dipetakan ke host Windows pada `127.0.0.1:9001:9001`.
3. **Hadoop NameNode Web UI**: Dipetakan ke host Windows pada `127.0.0.1:9870:9870`.
4. **Hadoop HDFS RPC**: **TIDAK DIPETAKAN KE HOST**. HDFS RPC tetap berjalan pada port internal 9000 di dalam kontainer `namenode` dan diakses oleh DataNode serta skrip MapReduce melalui DNS kontainer: `hdfs://namenode:9000`.

Dengan skema ini, tidak terjadi benturan port (*zero port collision*) pada host Windows.

---

## 6. Alokasi Resource & Batas Beban Memori

Berdasarkan hasil audit perangkat keras host (Intel Core i5-7300U, 2 core / 4 thread, RAM 8 GB dengan sisa bebas ~1,9 GB), ditetapkan batas alokasi konservatif:

| Layanan / Kontainer | CPU Limit | CPU Reservation | Memory Limit | Memory Reservation | Catatan |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `sibi_minio` | 0.25 core | 0.10 core | 256 MB | 128 MB | Object Storage daemon |
| `sibi_namenode` | 0.75 core | 0.25 core | 768 MB | 384 MB | NameNode daemon + LocalJobRunner JVM |
| `sibi_datanode` | 0.50 core | 0.20 core | 512 MB | 256 MB | HDFS Data Storage block daemon |
| **Total Maksimal** | **1.50 core** | **0.55 core** | **1.536 MB (~1.5 GB)** | **768 MB** | **Aman di bawah sisa RAM host (1.9 GB)** |

---

## 7. Desain Komponen MapReduce (Python 3)

### A. Mapper (`scripts/mapreduce/mapper.py`)
- **Input**: Berkas TSV dari HDFS dibaca baris per baris melalui `sys.stdin`.
  - Format: `book_id<TAB>token`
- **Validasi**:
  1. Memastikan terdapat tepat 2 kolom terpisah tab (`\t`).
  2. Memvalidasi bahwa `book_id` tidak kosong dan merupakan digit yang valid.
  3. Memvalidasi bahwa `token` tidak kosong.
  4. Baris tidak valid dicatat ke `sys.stderr` dan dilewati tanpa menghentikan pemrosesan.
- **Output**: Dituliskan ke `sys.stdout`.
  - Format: `book_id<TAB>token<TAB>1`
- **Karakteristik**:
  - Tidak melakukan deduplikasi (seluruh kemunculan diteruskan).
  - Tidak mengubah casing (token telah dibersihkan menjadi lowercase pada Tahap 3).

### B. Hadoop Shuffle & Sort
- Hadoop Streaming dikonfigurasi dengan opsi:
  `-D stream.num.map.output.key.fields=2`
- Menjadikan pasangan `(book_id, token)` sebagai **composite key**.
- Data secara otomatis diurutkan secara leksikografis sebelum masuk ke Reducer:
  ```
  1    buku       1
  1    buku       1
  1    membaca    1
  2    membaca    1
  ```

### C. Reducer (`scripts/mapreduce/reducer.py`)
- **Input**: Tuple terurut dari `sys.stdin`.
  - Format: `book_id<TAB>token<TAB>1`
- **Pengelompokan & Agregasi**:
  1. Menggunakan pola agregasi streaming: membandingkan `composite_key = (book_id, token)`.
  2. Selama key sama, akumulator `current_count += count`.
  3. Saat key berganti, emit nilai terakumulasi ke `sys.stdout` dan inisialisasi key baru.
  4. Melakukan flush untuk key terakhir saat loop selesai.
- **Isolasi Antar-Buku**:
  - Token yang sama pada buku yang berbeda (misal `1\tmembaca` dan `2\tmembaca`) memiliki composite key yang berbeda, sehingga frekuensi dihitung secara terpisah dan tidak tercampur.
- **Output**: Dituliskan ke `sys.stdout` dan disimpan Hadoop ke berkas `part-00000` di HDFS.
  - Format: `book_id<TAB>token<TAB>frequency`

---

## 8. Panduan Eksekusi & Validasi Melalui PowerShell

Semua operasi diatur secara otomatis melalui skrip PowerShell terstruktur:

### 1. Memulai Lingkungan Docker (Saat Siap Dijalankan)
```powershell
docker compose up -d
```

### 2. Inisialisasi Bucket MinIO
```powershell
.\scripts\minio\init_minio.ps1
```
*Skrip ini akan membuat alias MinIO dan bucket `sibi-raw`, `sibi-processed`, dan `sibi-output` secara idempoten.*

### 3. Mengunggah Dataset ke MinIO (Opsional / Arsip)
```powershell
.\scripts\minio\upload_dataset.ps1
```

### 4. Menjalankan Job MapReduce
- **Dataset Utama (Buku 1, 2, 3)**:
  ```powershell
  .\scripts\mapreduce\run_mapreduce.ps1 -DatasetType primary
  ```
- **Seluruh Dataset (Buku 1, 2, 3, 4)**:
  ```powershell
  .\scripts\mapreduce\run_mapreduce.ps1 -DatasetType all
  ```

*Skrip controller akan secara otomatis:*
1. Mengunggah file TSV yang dipilih ke HDFS (`/sibi/input/`).
2. Menghasilkan ID unik berbasis timestamp (misal: `run_primary_20261104_120000`).
3. Menjalankan skrip `run_mapreduce.sh` di dalam kontainer NameNode.
4. Mengunduh hasil dari HDFS ke direktori lokal: `output/mapreduce/<run_id>/part-00000`.

### 5. Memvalidasi Hasil MapReduce
Jalankan skrip validator independen:
```powershell
python scripts\mapreduce\validate_mapreduce_output.py `
    --input dataset\processed\bow_input_primary.tsv `
    --output output\mapreduce\<run_id>\part-00000 `
    --report logs\mapreduce_validation_<run_id>.json
```

*Validator memverifikasi:*
- Kesesuaian jumlah total token (sum of frequencies == total input lines).
- Kesesuaian jumlah entri unik `(book_id, token)`.
- Ketiadaan token yang hilang (*missing tokens*) atau token siluman (*extra/hallucinated tokens*).
- Ketiadaan selisih frekuensi (*count mismatches*).
- Kepatuhan urutan leksikografis (*sort order*).

---

## 9. Analisis Potensi Kendala & Mitigasi

1. **Port Collision 9000**:
   - *Kendala*: MinIO API dan HDFS RPC sama-sama menggunakan port 9000.
   - *Mitigasi*: HDFS RPC port 9000 diisolasi hanya di dalam bridge network Docker. Host Windows hanya memetakan MinIO API ke port 9000 dan NameNode Web UI ke port 9870.
2. **Keterbatasan RAM Host (8 GB)**:
   - *Kendala*: Risiko OOM jika kontainer meminta memori berlebih.
   - *Mitigasi*: Ditetapkan limit ketat: MinIO (256 MB), NameNode (768 MB), DataNode (512 MB). YARN dinonaktifkan sepenuhnya.
3. **Ketersediaan Python 3 pada Image Hadoop BDE**:
   - *Kendala*: Image Hadoop BDE 2.0.0 berbasis Debian lama dan mungkin belum memiliki Python 3 sebagai default command `python3`.
   - *Mitigasi*: Disediakan `docker/hadoop/Dockerfile` yang secara eksplisit menginstal `python3` dan symlink `/usr/bin/python3`, dengan tetap mempertahankan entrypoint resmi `/entrypoint.sh`.
4. **Pencegahan Penimpaan Data (Data Loss)**:
   - *Kendala*: Eksekusi ulang berpotensi menimpa hasil run sebelumnya.
   - *Mitigasi*: Setiap run Hadoop dan MinIO menggunakan prefix berbasis timestamp unik (`run_<dataset>_<timestamp>`). Skrip Hadoop akan membatalkan job jika direktori output target sudah ada di HDFS.

---

## 10. Konfirmasi Keamanan & Integritas Data
Seluruh dataset tahap sebelumnya dipelihara secara utuh:
- Berkas PDF mentah (`dataset/raw/`, `dataset/optional/`) tidak berubah.
- Berkas TXT mentah (`extracted_text/raw/`) tidak berubah.
- Berkas TXT bersih (`extracted_text/cleaned/`) tidak berubah.
- Berkas TSV Bag of Words (`dataset/processed/bow_input.tsv`, `bow_input_primary.tsv`) tidak berubah.
- Berkas manifest (`bow_manifest.json`, `bow_manifest.csv`) tidak berubah.
- Daftar stopwords (`configs/stopwords_id.txt`) tidak berubah.
- Folder referensi legacy (`references/legacy_hadoop_wordcount/`) tidak berubah.

