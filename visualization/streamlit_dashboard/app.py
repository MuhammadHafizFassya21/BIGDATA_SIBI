"""
BIGDATA_SIBI – Dashboard Analisis Frekuensi Kata pada Buku Digital Bahasa Indonesia
Dashboard berbasis Streamlit untuk mengeksplorasi hasil analisis frekuensi kata
dari tiga buku primary yang diproses menggunakan Hadoop Streaming MapReduce.
"""

from pathlib import Path
import json
import re

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"

BOOK_NAMES = {
    1: "Bahasa Indonesia – Kelas I",
    2: "Bahasa Indonesia – Kelas III",
    3: "Bahasa Indonesia – Kelas VII",
}

st.set_page_config(
    page_title="BIGDATA_SIBI | Dashboard Analisis Frekuensi Kata",
    page_icon="📚",
    layout="wide",
    initial_sidebar_state="expanded",
)

@st.cache_data
def load_csv(filename):
    path = DATA_DIR / filename
    if not path.exists():
        raise FileNotFoundError(f"File tidak ditemukan: {path}")
    try:
        return pd.read_csv(path, encoding="utf-8-sig")
    except UnicodeDecodeError:
        return pd.read_csv(path, encoding="latin-1")

@st.cache_data
def load_summary():
    path = DATA_DIR / "analysis_summary.json"
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)

def get_book_name(book_id):
    try:
        return BOOK_NAMES.get(int(book_id), f"Buku {book_id}")
    except (TypeError, ValueError):
        return str(book_id)

def numeric(df, columns):
    for col in columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    return df

def metric_value(df, column):
    if column not in df.columns or df.empty:
        return 0
    return int(pd.to_numeric(df[column], errors="coerce").fillna(0).sum())

def show_data_warning(message):
    st.warning(message)

try:
    books = load_csv("books.csv")
    book_summary = numeric(load_csv("book_summary.csv"), ["total_tokens", "unique_tokens"])
    overall = numeric(load_csv("top_words_overall.csv"), ["frequency"])
    per_book = numeric(load_csv("top_words_per_book.csv"), ["frequency", "frequency_per_1000"])
    comparison = load_csv("cross_book_word_comparison.csv")
    summary_json = load_summary()
except Exception as exc:
    st.error(f"Data dashboard gagal dimuat: {exc}")
    st.info("Pastikan enam file CSV/JSON tersedia di folder data/ yang sejajar dengan app.py.")
    st.stop()

if "book_id" in book_summary.columns:
    book_summary["book_id"] = pd.to_numeric(book_summary["book_id"], errors="coerce")
    book_summary["book_name"] = book_summary["book_id"].map(get_book_name)
if "book_id" in per_book.columns:
    per_book["book_id"] = pd.to_numeric(per_book["book_id"], errors="coerce")
    per_book["book_name"] = per_book["book_id"].map(get_book_name)

primary_books = books.copy()
if "dataset_status" in primary_books.columns:
    primary_books = primary_books[
        primary_books["dataset_status"].astype(str).str.lower().eq("primary")
    ]
primary_ids = sorted(book_summary["book_id"].dropna().astype(int).unique().tolist())
available_names = [get_book_name(i) for i in primary_ids]

# Header
st.title("BIGDATA_SIBI")
st.subheader("Dashboard Analisis Frekuensi Kata pada Buku Digital Bahasa Indonesia")
st.caption("Analisis tiga buku primary menggunakan Hadoop Streaming MapReduce dan visualisasi berbasis Streamlit.")

with st.sidebar:
    st.header("Navigasi Dashboard")
    page = st.radio(
        "Pilih halaman",
        [
            "Overview",
            "Frekuensi Kata",
            "Analisis Per Buku",
            "Perbandingan Kosakata",
            "Word Cloud",
            "Data Explorer",
            "Metodologi & Catatan",
        ],
    )
    st.divider()
    st.markdown("**Dataset aktif**")
    st.write(f"{len(primary_books)} buku primary")
    st.write(f"{len(book_summary):,} ringkasan buku")
    st.caption("Sumber: file hasil analisis proyek BIGDATA_SIBI.")

total_tokens = metric_value(book_summary, "total_tokens")
# Jumlah pasangan buku-kata unik adalah penjumlahan kosakata unik tiap buku.
total_book_word_pairs = metric_value(book_summary, "unique_tokens")
# Kosakata unik gabungan menghitung token berbeda setelah ketiga buku digabungkan.
combined_vocab = int(comparison["token"].nunique()) if "token" in comparison.columns else 0

if page == "Overview":
    st.markdown("### Ringkasan hasil pengolahan")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total token", f"{total_tokens:,}".replace(",", "."))
    c2.metric("Kosakata unik gabungan", f"{combined_vocab:,}".replace(",", "."))
    c3.metric("Buku primary", f"{len(primary_books)}")
    c4.metric("Pasangan buku-kata unik", f"{total_book_word_pairs:,}".replace(",", "."))

    if summary_json.get("status") == "PASS":
        st.success(
            "Validasi MapReduce: PASS | "
            f"Input {summary_json.get('input_tokens', 0):,} token | "
            f"Pasangan unik {summary_json.get('distinct_book_token_pairs', 0):,} | "
            f"Ketidaksesuaian frekuensi {summary_json.get('frequency_mismatches', 0)}"
        )
    else:
        st.info("Status validasi MapReduce tidak tercantum sebagai PASS pada ringkasan JSON.")

    st.markdown("### Profil dataset")
    if {"book_name", "total_tokens"}.issubset(book_summary.columns):
        left, right = st.columns(2)
        with left:
            fig = px.bar(
                book_summary, x="book_name", y="total_tokens",
                labels={"book_name": "Buku", "total_tokens": "Jumlah token"},
                title="Jumlah token pada setiap buku",
                text="total_tokens",
            )
            fig.update_traces(texttemplate="%{text:,.0f}", textposition="outside")
            fig.update_layout(xaxis_title="", yaxis_title="Jumlah token")
            st.plotly_chart(fig, use_container_width=True)
        with right:
            if "unique_tokens" in book_summary.columns:
                fig = px.bar(
                    book_summary, x="book_name", y="unique_tokens",
                    labels={"book_name": "Buku", "unique_tokens": "Kosakata unik"},
                    title="Kosakata unik pada setiap buku",
                    text="unique_tokens",
                )
                fig.update_traces(texttemplate="%{text:,.0f}", textposition="outside")
                fig.update_layout(xaxis_title="", yaxis_title="Jumlah kosakata unik")
                st.plotly_chart(fig, use_container_width=True)

    st.markdown("### Ringkasan per buku")
    display_cols = [c for c in ["book_id", "book_name", "total_tokens", "unique_tokens"] if c in book_summary.columns]
    st.dataframe(book_summary[display_cols], use_container_width=True, hide_index=True)

    st.markdown("### 20 kata dengan frekuensi tertinggi")
    if {"token", "frequency"}.issubset(overall.columns):
        top20 = overall.nlargest(20, "frequency").sort_values("frequency", ascending=True)
        fig = px.bar(
            top20, x="frequency", y="token", orientation="h",
            labels={"frequency": "Frekuensi", "token": "Kata"},
            title="Top 20 kata pada gabungan buku",
        )
        st.plotly_chart(fig, use_container_width=True)

elif page == "Frekuensi Kata":
    st.markdown("### Analisis frekuensi kata keseluruhan")
    # cross_book_word_comparison memuat seluruh vocabulary, sehingga Top 50
    # dihitung dari total_frequency, bukan dibatasi pada CSV Top 20.
    if {"token", "total_frequency"}.issubset(comparison.columns):
        frequency_data = comparison[["token", "total_frequency"]].rename(
            columns={"total_frequency": "frequency"}
        ).copy()
    elif {"token", "frequency"}.issubset(overall.columns):
        frequency_data = overall[["token", "frequency"]].copy()
    else:
        show_data_warning("Kolom token dan frekuensi tidak ditemukan pada file analisis.")
        frequency_data = pd.DataFrame()

    if not frequency_data.empty:
        frequency_data["frequency"] = pd.to_numeric(
            frequency_data["frequency"], errors="coerce"
        ).fillna(0)
        max_n = min(50, len(frequency_data))
        n = st.slider(
            "Jumlah kata yang ditampilkan",
            min_value=10, max_value=max_n, value=min(20, max_n), step=5
        )
        top = frequency_data.nlargest(n, "frequency").sort_values(
            "frequency", ascending=True
        )
        fig = px.bar(
            top, x="frequency", y="token", orientation="h",
            labels={"frequency": "Frekuensi kemunculan", "token": "Token"},
            title=f"Top {n} kata dengan frekuensi tertinggi",
        )
        fig.update_layout(yaxis={"categoryorder": "total ascending"})
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(
            frequency_data.nlargest(n, "frequency"),
            use_container_width=True, hide_index=True
        )
        st.download_button(
            "Unduh tabel frekuensi keseluruhan (CSV)",
            frequency_data.nlargest(n, "frequency").to_csv(index=False).encode("utf-8-sig"),
            file_name=f"top_{n}_kata_keseluruhan.csv",
            mime="text/csv",
        )

elif page == "Analisis Per Buku":
    st.markdown("### Analisis frekuensi kata berdasarkan buku")
    if "book_id" not in per_book.columns:
        show_data_warning("Kolom book_id tidak ditemukan pada top_words_per_book.csv.")
    else:
        book_choice = st.selectbox("Pilih buku", available_names)
        selected_id = next((i for i in primary_ids if get_book_name(i) == book_choice), primary_ids[0])
        selected = per_book[per_book["book_id"] == selected_id].copy()
        if selected.empty:
            show_data_warning("Tidak ada data frekuensi untuk buku yang dipilih.")
        else:
            sort_col = "frequency_per_1000" if "frequency_per_1000" in selected.columns else "frequency"
            selected = selected.sort_values(sort_col, ascending=False).head(20)
            x_col = sort_col
            x_label = "Frekuensi per 1.000 token" if x_col == "frequency_per_1000" else "Frekuensi"
            fig = px.bar(
                selected.sort_values(x_col, ascending=True),
                x=x_col, y="token", orientation="h",
                labels={x_col: x_label, "token": "Token"},
                title=f"20 kata dominan – {book_choice}",
            )
            st.plotly_chart(fig, use_container_width=True)
            st.dataframe(selected, use_container_width=True, hide_index=True)
            st.download_button(
                "Unduh data buku terpilih (CSV)",
                selected.to_csv(index=False).encode("utf-8-sig"),
                file_name=f"frekuensi_buku_{selected_id}.csv",
                mime="text/csv",
            )

elif page == "Perbandingan Kosakata":
    st.markdown("### Perbandingan kosakata antarbuku")
    if "token" not in comparison.columns:
        show_data_warning("Kolom token tidak ditemukan pada cross_book_word_comparison.csv.")
    else:
        mode = st.radio(
            "Jenis perbandingan",
            ["Frekuensi relatif per 1.000 token", "Frekuensi absolut"],
            horizontal=True,
        )
        top_n = st.slider("Jumlah token yang dibandingkan", 5, 30, 20, 5)
        metric_suffix = "per_1000" if mode.startswith("Frekuensi relatif") else "frequency"
        selected_cols = [f"book_{i}_{metric_suffix}" for i in primary_ids]
        missing = [c for c in selected_cols if c not in comparison.columns]
        if missing:
            show_data_warning(f"Kolom pembanding tidak tersedia: {', '.join(missing)}")
        else:
            ranking_col = "total_frequency" if "total_frequency" in comparison.columns else None
            if ranking_col:
                chosen = comparison.nlargest(top_n, ranking_col).copy()
            else:
                chosen = comparison.head(top_n).copy()
            long = chosen[["token"] + selected_cols].melt(
                id_vars="token", var_name="book_metric", value_name="value"
            )
            long["Buku"] = long["book_metric"].str.extract(r"book_(\d+)_")[0].astype(int).map(get_book_name)
            y_label = "Kemunculan per 1.000 token" if metric_suffix == "per_1000" else "Frekuensi"
            fig = px.bar(
                long, x="token", y="value", color="Buku", barmode="group",
                labels={"token": "Token", "value": y_label},
                title=f"Perbandingan {top_n} kosakata pada ketiga buku",
            )
            fig.update_layout(xaxis_tickangle=-35)
            st.plotly_chart(fig, use_container_width=True)

            heat = chosen.set_index("token")[selected_cols].copy()
            heat.columns = [get_book_name(i) for i in primary_ids]
            fig_heat = px.imshow(
                heat, aspect="auto", text_auto=".2f",
                labels={"x": "Buku", "y": "Token", "color": y_label},
                title="Heatmap perbandingan kosakata",
            )
            st.plotly_chart(fig_heat, use_container_width=True)
            st.caption("Frekuensi relatif lebih tepat untuk membandingkan buku dengan jumlah token yang berbeda.")

elif page == "Word Cloud":
    st.markdown("### Word Cloud kosakata per buku")
    st.write(
        "Word Cloud ditampilkan dari visualisasi yang telah dihasilkan berdasarkan "
        "frekuensi token aktual pada masing-masing buku."
    )
    book_choice = st.selectbox("Pilih buku untuk Word Cloud", available_names, key="wc_book")
    selected_id = next((i for i in primary_ids if get_book_name(i) == book_choice), primary_ids[0])
    image_path = BASE_DIR / "assets" / "wordcloud" / f"07_wordcloud_kelas_{selected_id}.png"
    if image_path.exists():
        st.image(str(image_path), caption=f"Word Cloud – {book_choice}", use_container_width=True)
    else:
        show_data_warning(f"File Word Cloud tidak ditemukan: {image_path.name}")
    st.caption(
        "Catatan: Word Cloud hanya menampilkan token alfabetis minimal tiga karakter "
        "untuk keterbacaan visual. Stopword belum dihapus. Filter ini tidak mengubah "
        "hasil sumber maupun keluaran MapReduce."
    )

elif page == "Data Explorer":
    st.markdown("### Eksplorasi tabel hasil analisis")
    dataset_choice = st.selectbox(
        "Pilih tabel",
        [
            "Ringkasan buku",
            "Top kata keseluruhan",
            "Top kata per buku",
            "Perbandingan antarbuku",
            "Metadata buku",
        ],
    )
    mapping = {
        "Ringkasan buku": ("book_summary.csv", book_summary),
        "Top kata keseluruhan": ("top_words_overall.csv", overall),
        "Top kata per buku": ("top_words_per_book.csv", per_book),
        "Perbandingan antarbuku": ("cross_book_word_comparison.csv", comparison),
        "Metadata buku": ("books.csv", books),
    }
    filename, frame = mapping[dataset_choice]
    st.write(f"Jumlah baris: {len(frame):,} | Jumlah kolom: {len(frame.columns)}")
    st.dataframe(frame, use_container_width=True, hide_index=True)
    st.download_button(
        f"Unduh {filename}",
        frame.to_csv(index=False).encode("utf-8-sig"),
        file_name=filename,
        mime="text/csv",
    )

elif page == "Metodologi & Catatan":
    st.markdown("### Metodologi pembacaan dashboard")
    st.markdown(
        """
        1. Dataset yang ditampilkan pada dashboard adalah tiga buku berstatus **primary**.
        2. Input analisis berupa pasangan `book_id` dan `token`.
        3. Hadoop Streaming MapReduce menghitung frekuensi setiap pasangan buku-kata.
        4. Frekuensi absolut menunjukkan jumlah kemunculan token.
        5. Frekuensi relatif dihitung per 1.000 token untuk membantu membandingkan buku dengan panjang berbeda.
        """
    )
    st.markdown("### Catatan interpretasi")
    st.warning(
        "Stopword dan sejumlah kata struktural masih terdapat pada data. "
        "Karena itu, kata yang paling sering muncul belum tentu merepresentasikan tema semantik utama buku."
    )
    st.info(
        "Word Cloud menerapkan filter tampilan token alfabetis minimal tiga karakter. "
        "Filter tersebut hanya untuk keterbacaan visual dan tidak mengubah file hasil MapReduce."
    )
    st.markdown("### File sumber")
    for filename in [
        "analysis_summary.json", "books.csv", "book_summary.csv",
        "top_words_overall.csv", "top_words_per_book.csv",
        "cross_book_word_comparison.csv",
    ]:
        st.write(f"- `{filename}`")

st.divider()
st.caption("BIGDATA_SIBI | Dashboard Analisis Frekuensi Kata pada Buku Digital Bahasa Indonesia")
