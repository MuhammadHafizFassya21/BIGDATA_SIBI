# Analisis Frekuensi Kemunculan Kata pada Buku Digital Bahasa Indonesia SIBI Menggunakan Hadoop MapReduce

## Identitas Proyek

**Judul:** Analisis Frekuensi Kemunculan Kata pada Buku Digital Bahasa Indonesia SIBI Menggunakan Hadoop MapReduce.

## Tujuan Proyek

Menganalisis frekuensi kemunculan kata pada buku-buku digital Bahasa Indonesia dari Sistem Informasi Perbukuan Indonesia (SIBI) menggunakan framework Hadoop MapReduce dan konsep Bag of Words. Tujuan utama adalah mengidentifikasi kata-kata yang paling sering muncul pada kumpulan buku digital SIBI ini untuk mendukung analisis teks dan pengambilan keputusan berbasis data dari koleksi buku tersebut.

## Sumber Dataset

Dataset berasal dari buku PDF Bahasa Indonesia yang dipublikasikan melalui Sistem Informasi Perbukuan Indonesia (SIBI). Empat buku dipilih merepresentasikan jenjang pendidikan SD dan SMP, dengan satu buku edisi 2017 sebagai dataset pembanding untuk analisis perbandingan edisi.

## Inventaris Dataset

Empat buku dalam dataset ini meliputi:

1. **Buku 1:** Bahasa Indonesia: Aku Bisa! (Edisi Revisi) - Jenjang SD, Edisi 2023 - Status: Primary
2. **Buku 2:** Bahasa Indonesia: Kawan Seiring (Edisi Revisi) - Jenjang SD, Edisi 2025 - Status: Primary
3. **Buku 3:** Bahasa Indonesia untuk SMP/MTs Kelas VII (Edisi Revisi) - Jenjang SMP, Edisi 2023 - Status: Primary
4. **Buku 4:** Bahasa Indonesia untuk SMP/MTs Kelas VII - Jenjang SMP, Edisi 2017 - Status: Optional Comparison (Pembanding)

## Struktur Folder

`
BIGDATA_SIBI/
├── dataset/
│   ├── raw/
│   │   ├── SD/
│   │   ├── SMP/
│   │   └── SMA_SMK/
│   ├── optional/
│   │   └── SMP_2017/
│   └── processed/
├── extracted_text/
│   ├── raw/
│   └── cleaned/
├── scripts/
│   ├── extraction/
│   ├── preprocessing/
│   ├── mapreduce/
│   └── analytics/
├── configs/
│   ├── hadoop/
│   └── minio/
├── output/
│   ├── mapreduce/
│   └── analytics/
├── visualization/
├── logs/
├── docs/
├── references/
│   └── legacy_hadoop_wordcount/
└── README.md
`

## Arsitektur Pipeline

Rencana alur kerja proyek adalah sebagai berikut:

**PDF → MinIO → PDF Text Extraction → Text Preprocessing → HDFS → Hadoop MapReduce → Bag of Words → Descriptive Analytics → Visualization**

Setiap tahapan memiliki tujuan spesifik:
1. **PDF:** Sumber data asli dalam format buku digital
2. **MinIO:** Layanan storage objek untuk menyimpan file PDF
3. **PDF Text Extraction:** Ekstraksi teks dari file PDF (belum diimplementasikan)
4. **Text Preprocessing:** Pembersih teks (stopword removal, stemming, lowercase) (belum diimplementasikan)
5. **HDFS:** Distribusi data teks ke cluster Hadoop
6. **Hadoop MapReduce:** Pemrosesan paralel frekuensi kemunculan kata
7. **Bag of Words:** Konversi teks menjadi representasi matriks frekuensi kata
8. **Descriptive Analytics:** Analisis deskriptif statistik hasil frekuensi
9. **Visualization:** Pemvisualisasian hasil analisis ke dalam grafik dan diagram

## Penjelasan Setiap Folder

- **dataset:** Menyimpan file PDF asli dan data mentah yang telah diolah. berisi subfolder 
aw/ (PDF asli), processed/ (teks sudah diproses), dan optional/ (dataset pembanding tahap 2017).
- **metadata:** Berisi file books.csv yang merupakan katalog metadata lengkap setiap buku dalam dataset.
- **extracted_text:** Menyimpan hasil ekstraksi teks dari PDF. berisi 
aw/ (teks hasil ekstraksi mentah) dan cleaned/ (teks setelah pra-processing).
- **scripts:** Berisi kode program pengolahan data dalam berbagai tahap ekstraksi, preprocessing, dan analisis MapReduce.
- ** configs:** Konfigurasi sistem terkait Hadoop dan MinIO untuk pengelolaan cluster dan storage.
- **output:** Menyimpan hasil akhir dari proses MapReduce dan analitik deskriptif.
- **visualization:** Berisi visualisasi hasil analisis frekuensi kata dalam bentuk grafik, chart, atau dashboard.
- **logs:** Catatan proses operasi, error log, dan progres pekerjaan selama ekstraksi dan analisis berjalan.
- **docs:** Dokumentasi lengkap mengenai konfigurasi, alur kerja, dan petunjuk penggunaan proyek.
- **references:** Referensi tambahan, termasuk proyek legacy Hadoop WordCount sebagai pembelajaran/reference.
- **logs:** Log-operasi selama proses berjalan.

## Status Implementasi

- **Struktur folder:** Selesai.
- **Dataset PDF:** Empat file sudah tersedia di folder dataset/raw/.
- **Metadata CSV:** Sedang divalidasi (sudah diperbaiki header dan isi data).
- **PDF Text Extraction:** Belum dikerjakan.
- **Text Preprocessing:** Belum dikerjakan.
- **MinIO:** Belum diimplementasikan.
- **Hadoop HDFS:** Belum diintegrasikan ke proyek SIBI.
- **Hadoop MapReduce:** Belum dijalankan untuk dataset SIBI.
- **Analytics:** Belum dikerjakan.
- **Visualisasi:** Belum dikerjakan.

## Tahapan Pengerjaan Selanjutnya

Urutan implementasi yang akan diikuti mulai dari tahap pertama:

1. **Ekstraksi PDF** - Mengambil teks dari file PDF menggunakan library Python (misalnya PyPDF2 atau pdfminer.six)
2. **Text Preprocessing** - Membersihkan teks dengan menghapus stopword, melakukan stemming, dan normalisasi lowercase
3. **Setup MinIO** - Menyiapkan storage objek untuk menyimpan file PDF dan teks hasil ekstraksi
4. **Integrasi HDFS** - Mengonfigurasi Hadoop Distributed File System untuk menyimpan data teks
5. **Eksekusi Hadoop MapReduce** - Menjalankan mapper dan reducer untuk menghitung frekuensi kemunculan kata
6. **Pembuatan Bag of Words** - Mengonversi teks menjadi representasi matriks frekuensi
7. **Descriptive Analytics** - Melakukan analisis statistik deskriptif pada hasil frekuensi
8. **Visualisasi** - Membuat grafik dan dashboard untuk menampilkan hasil analisis

## Catatan Keamanan

Proyek memiliki referensi legacy berupa proyek **hadoop-wordcount** yang berada di folder 
eferences/legacy_hadoop_wordcount/. Proyek lama ini merupakan referensi pembelajaran dan **diharuskan tetap utuh** tanpa dihapus, dipindahkan, atau diubah kode/configurasi apa pun. Semua pekerjaan baru harus dikembangkan secara terpisah dan tidak boleh mengganggu struktur atau kode dari proyek hadoop-wordcount tersebut. Penjagaan integritas data dan struktur folder proyek lama adalah prioritas absolut.

---

*Dokumen ini dibuat dengan encoding UTF-8 dengan BOM untuk kompatibilitas karakter pada Windows PowerShell.*
