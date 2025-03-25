import streamlit as st
import pandas as pd
import plotly.express as px
from snowflake.snowpark import Session

# Inisialisasi sesi Snowflake
try:
    session = get_active_session()
except:
    session = Session.builder.configs({
        "user": 'USERBERCA',
        "password": 'P@ssvv0rd032025',
        "account": 'jf57268.ap-southeast-3.aws',
        "host": 'jf57268.ap-southeast-3.aws.snowflakecomputing.com',
        "port": 443,
        "warehouse": 'COMPUTE_WH',
        "role": 'ACCOUNTADMIN',
    }).create()

# Konfigurasi layout aplikasi
st.set_page_config(layout="wide")
st.title("📊 Dynamic BI Dashboard - Snowflake & Streamlit")

# Simpan daftar tile dalam session_state
if "tiles" not in st.session_state:
    st.session_state.tiles = []
if "tile_titles" not in st.session_state:
    st.session_state.tile_titles = {}

# Fungsi untuk mendapatkan daftar database
def get_databases():
    result = session.sql("SHOW DATABASES").collect()
    return [row["name"] for row in result]

# Fungsi untuk mendapatkan daftar schema
def get_schemas(database):
    result = session.sql(f"SHOW SCHEMAS IN {database}").collect()
    return [row["name"] for row in result]

# Fungsi untuk mendapatkan daftar tabel
def get_tables(database, schema):
    result = session.sql(f"SHOW TABLES IN {database}.{schema}").collect()
    return [row["name"] for row in result]

# Fungsi untuk mendapatkan data dari tabel yang dipilih
def get_table_data(database, schema, table):
    query = f"SELECT * FROM {database}.{schema}.{table}"
    return session.sql(query).to_pandas()

# Sidebar untuk filter data
st.sidebar.header("🔍 Filter Data")
db_list = get_databases()
selected_db = st.sidebar.selectbox("🔹 Pilih Database", db_list)

if selected_db:
    schema_list = get_schemas(selected_db)
    selected_schema = st.sidebar.selectbox("🔹 Pilih Schema", schema_list)

    if selected_schema:
        table_list = get_tables(selected_db, selected_schema)
        selected_table = st.sidebar.selectbox("🔹 Pilih Tabel", table_list)

        if selected_table:
            try:
                df = get_table_data(selected_db, selected_schema, selected_table)
                df['TANGGAL'] = pd.to_datetime(df['TANGGAL'])  # Konversi ke datetime

                # Ambil daftar unik tahun & bulan, lalu urutkan
                tahun_options = sorted(df['TANGGAL'].dt.year.unique().tolist())
                bulan_options = sorted(df['TANGGAL'].dt.month.unique().tolist())

                # Filter Tahun & Bulan
                # Filter Tahun & Bulan tanpa default nilai
                selected_tahun = st.sidebar.multiselect("📅 Pilih Tahun", tahun_options, default=[])
                selected_bulan = st.sidebar.multiselect("📅 Pilih Bulan", bulan_options, default=[])

                # Filter data hanya jika tahun dan bulan dipilih    
                if selected_tahun and selected_bulan:
                    df = df[df['TANGGAL'].dt.year.isin(selected_tahun) & df['TANGGAL'].dt.month.isin(selected_bulan)]


                # Filter Range Tanggal
                st.sidebar.subheader("📅 Pilih Rentang Tanggal")
                min_date = df['TANGGAL'].min().date()
                max_date = df['TANGGAL'].max().date()
                start_date = st.sidebar.date_input("Dari Tanggal", value=min_date, min_value=min_date, max_value=max_date)
                end_date = st.sidebar.date_input("Sampai Tanggal", value=max_date, min_value=start_date, max_value=max_date)

                # Terapkan filter jika ada pilihan
                if selected_tahun and selected_bulan:
                    df = df[df['TANGGAL'].dt.year.isin(selected_tahun) & df['TANGGAL'].dt.month.isin(selected_bulan)]
                
                # Terapkan filter berdasarkan rentang tanggal
                df = df[(df['TANGGAL'].dt.date >= start_date) & (df['TANGGAL'].dt.date <= end_date)]

            except Exception as e:
                st.sidebar.error(f"❌ Error mengambil data: {str(e)}")

# Tambah Tile Baru
if st.sidebar.button("➕ Tambah Tile Baru"):
    tile_id = len(st.session_state.tiles) + 1
    st.session_state.tiles.append(tile_id)
    st.session_state.tile_titles[tile_id] = f"Tile {tile_id}"  # Set nama default

# Layout untuk visualisasi
st.header("📊 Visualisasi Data")
for tile in st.session_state.tiles:
    with st.container():
        # Input teks untuk mengubah nama tile
        tile_title = st.text_input(f"Ubah Nama Tile {tile}", st.session_state.tile_titles[tile], key=f"title_{tile}")
        st.session_state.tile_titles[tile] = tile_title  # Simpan perubahan nama

        st.subheader(f"📌 {tile_title}")  # Tampilkan nama yang bisa diubah pengguna

        if selected_table and not df.empty:
            # Pilihan jenis visualisasi
            chart_type = st.selectbox(
                f"📊 Pilih Jenis Visualisasi (Tile {tile})",
                ["Table", "Bar Chart", "Line Chart", "Scatter", "Pie Chart"],
                key=f"chart_{tile}"
            )

            if chart_type == "Table":
                st.dataframe(df)
            else:
                # Ambil kolom kategori dan numerik
                all_cols = df.columns.tolist()
                cat_cols = df.select_dtypes(include=['object']).columns.tolist()
                num_cols = df.select_dtypes(include=['number']).columns.tolist()

                if chart_type == "Pie Chart":
                    # Pie Chart butuh kategori (label) dan nilai (value)
                    pie_labels = st.selectbox("Pilih Kategori (Label)", cat_cols, key=f"pie_label_{tile}")
                    pie_values = st.selectbox("Pilih Nilai (Value)", num_cols, key=f"pie_value_{tile}")

                    # Buat pie chart dengan Plotly
                    fig = px.pie(df, names=pie_labels, values=pie_values, title=f"Pie Chart - {pie_labels} vs {pie_values}")
                else:
                    # Pilihan untuk X dan Y Axis pada grafik lainnya
                    x_axis = st.selectbox("Pilih X-Axis", all_cols, key=f"x_{tile}")
                    y_axis = st.selectbox("Pilih Y-Axis", num_cols, key=f"y_{tile}")

                    if chart_type == "Bar Chart":
                        fig = px.bar(df, x=x_axis, y=y_axis)
                    elif chart_type == "Line Chart":
                        fig = px.line(df, x=x_axis, y=y_axis)
                    elif chart_type == "Scatter":
                        fig = px.scatter(df, x=x_axis, y=y_axis)

                # Tampilkan plot dengan key unik untuk menghindari error duplicate
                st.plotly_chart(fig, key=f"plot_{tile}")

# Hapus Semua Tile
if st.sidebar.button("🗑 Hapus Semua Tile"):
    st.session_state.tiles = []
    st.session_state.tile_titles = {}