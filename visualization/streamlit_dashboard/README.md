# BIGDATA_SIBI – Dashboard Analisis Frekuensi Kata pada Buku Digital Bahasa Indonesia

## Deskripsi
Dashboard Streamlit untuk mengeksplorasi hasil analisis frekuensi kata dari tiga buku primary dalam proyek BIGDATA_SIBI. Dashboard membaca keluaran CSV/JSON aktual, bukan data simulasi.

## Struktur folder
```text
visualization/
└── streamlit_dashboard/
    ├── app.py
    ├── requirements.txt
    ├── README.md
    └── data/
        ├── analysis_summary.json
        ├── books.csv
        ├── book_summary.csv
        ├── top_words_overall.csv
        ├── top_words_per_book.csv
        └── cross_book_word_comparison.csv
```

## Cara menjalankan di Windows PowerShell
Jalankan perintah dari direktori utama proyek BIGDATA_SIBI:

```powershell
# 1. Aktifkan virtual environment proyek
.\.venv\Scripts\Activate.ps1

# 2. Masuk ke folder dashboard
cd .\visualization\streamlit_dashboard

# 3. Instal dependensi
python -m pip install -r requirements.txt

# 4. Jalankan dashboard
python -m streamlit run app.py
```

Streamlit akan menampilkan alamat lokal, biasanya:
`http://localhost:8501`

Untuk menghentikan dashboard, tekan `Ctrl + C` pada terminal.

## Halaman dashboard
1. Overview
2. Frekuensi Kata
3. Analisis Per Buku
4. Perbandingan Kosakata
5. Word Cloud
6. Data Explorer
7. Metodologi & Catatan

## Catatan metodologis
- Data utama berstatus primary.
- Frekuensi relatif dihitung per 1.000 token.
- Word Cloud hanya menampilkan token alfabetis minimal tiga karakter untuk keterbacaan.
- Stopword belum dihapus; hasil frekuensi mentah tidak boleh ditafsirkan langsung sebagai tema semantik.
- Data CSV/JSON sumber disertakan agar dashboard dapat direproduksi.

## Troubleshooting
- Jika `streamlit` tidak ditemukan, jalankan `python -m pip install -r requirements.txt` dari folder dashboard.
- Jika data gagal dimuat, pastikan folder `data` berada satu tingkat dengan `app.py`.
- Word Cloud menggunakan gambar PNG yang disertakan dalam folder assets/wordcloud sehingga tidak memerlukan instalasi paket tambahan.
