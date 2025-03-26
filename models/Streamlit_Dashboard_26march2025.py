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
if "filters" not in st.session_state:
    st.session_state.filters = []
if "tile_date_cols" not in st.session_state:
    st.session_state.tile_date_cols = {}
if "tile_date_filters" not in st.session_state:
    st.session_state.tile_date_filters = {}

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
                
                # Tambah Filter Baru
                if st.sidebar.button("➕ Tambah Filter Baru"):
                    st.session_state.filters.append({"column": None, "values": []})

                # Terapkan Filter
                for i in range(len(st.session_state.filters)):
                    with st.sidebar.expander(f"Filter {i+1}"):
                        filter_data = st.session_state.filters[i]
                        col_selected = st.selectbox(f"Pilih Kolom {i+1}", df.columns, key=f"filter_col_{i}", index=None)
                        
                        if col_selected:
                            unique_values = df[col_selected].dropna().unique().tolist()
                            selected_values = st.multiselect(f"Pilih Nilai Filter {i+1}", unique_values, default=[], key=f"filter_value_{i}")
                            
                            # Simpan hasil filter
                            st.session_state.filters[i]["column"] = col_selected
                            st.session_state.filters[i]["values"] = selected_values
                            
                            # Terapkan filter jika ada pilihan
                            if selected_values:
                                df = df[df[col_selected].astype(str).isin(selected_values)]

            except Exception as e:
                st.sidebar.error(f"❌ Error mengambil data: {str(e)}")

# Tambah Tile Baru
if st.sidebar.button("➕ Tambah Tile Baru"):
    tile_id = len(st.session_state.tiles) + 1
    st.session_state.tiles.append(tile_id)
    st.session_state.tile_titles[tile_id] = f"Tile {tile_id}"  # Set nama default
    st.session_state.tile_date_cols[tile_id] = None
    st.session_state.tile_date_filters[tile_id] = {}

# Layout untuk visualisasi
st.header("📊 Visualisasi Data")
for tile in st.session_state.tiles:
    with st.container():
        # Input teks untuk mengubah nama tile
        tile_title = st.text_input(f"Ubah Nama Tile {tile}", st.session_state.tile_titles[tile], key=f"title_{tile}")
        st.session_state.tile_titles[tile] = tile_title  # Simpan perubahan nama

        st.subheader(f"📌 {tile_title}")  # Tampilkan nama yang bisa diubah pengguna

        if selected_table and not df.empty:
            # Pilih kolom tanggal untuk tile ini
            date_cols = [col for col in df.columns if 'date' in col.lower() or 'tanggal' in col.lower()]
            selected_date_col = st.selectbox(
                "📅 Pilih Kolom Tanggal", 
                date_cols, 
                index=None, 
                placeholder="Pilih Kolom Tanggal (Opsional)",
                key=f"date_col_{tile}"
            )
            
            # Simpan pilihan kolom tanggal untuk tile ini
            st.session_state.tile_date_cols[tile] = selected_date_col
            
            # Buat salinan dataframe untuk tile ini
            tile_df = df.copy()
            
            # Jika ada kolom tanggal yang dipilih
            if selected_date_col:
                tile_df[selected_date_col] = pd.to_datetime(tile_df[selected_date_col])
                
                # Filter Tahun & Bulan
                with st.expander("📅 Filter Tanggal"):
                    tahun_options = sorted(tile_df[selected_date_col].dt.year.unique().tolist())
                    bulan_options = sorted(tile_df[selected_date_col].dt.month.unique().tolist())

                    selected_tahun = st.multiselect(
                        "Pilih Tahun", 
                        tahun_options,
                        key=f"tahun_{tile}"
                    )
                    selected_bulan = st.multiselect(
                        "Pilih Bulan", 
                        bulan_options,
                        key=f"bulan_{tile}"
                    )

                    # Filter Range Tanggal
                    min_date = tile_df[selected_date_col].min().date()
                    max_date = tile_df[selected_date_col].max().date()

                    start_date = st.date_input(
                        "Dari Tanggal", 
                        value=min_date, 
                        min_value=min_date, 
                        max_value=max_date, 
                        key=f"start_date_{tile}"
                    )
                    end_date = st.date_input(
                        "Sampai Tanggal", 
                        value=max_date, 
                        min_value=min_date, 
                        max_value=max_date, 
                        key=f"end_date_{tile}"
                    )

                    # Terapkan filter
                    if selected_tahun:
                        tile_df = tile_df[tile_df[selected_date_col].dt.year.isin(selected_tahun)]
                    if selected_bulan:
                        tile_df = tile_df[tile_df[selected_date_col].dt.month.isin(selected_bulan)]
                    if start_date and end_date:
                        tile_df = tile_df[
                            (tile_df[selected_date_col].dt.date >= start_date) & 
                            (tile_df[selected_date_col].dt.date <= end_date)
                        ]

            # Pilihan jenis visualisasi
            chart_type = st.selectbox(
                f"📊 Pilih Jenis Visualisasi",
                ["Table", "Bar Chart", "Line Chart", "Scatter", "Pie Chart"],
                key=f"chart_{tile}"
            )
            
            if chart_type == "Table":
                st.dataframe(tile_df)
            else:
                # Ambil kolom kategori dan numerik
                all_cols = tile_df.columns.tolist()
                cat_cols = tile_df.select_dtypes(include=['object']).columns.tolist()
                num_cols = tile_df.select_dtypes(include=['number']).columns.tolist()

                if chart_type == "Pie Chart":
                    pie_labels = st.selectbox("Pilih Kategori (Label)", cat_cols, key=f"pie_label_{tile}")
                    pie_values = st.selectbox("Pilih Nilai (Value)", num_cols, key=f"pie_value_{tile}")
                    fig = px.pie(tile_df, names=pie_labels, values=pie_values)
                else:
                    x_axis = st.selectbox("Pilih X-Axis", all_cols, key=f"x_{tile}")
                    y_axis = st.selectbox("Pilih Y-Axis", num_cols, key=f"y_{tile}")
                    if chart_type == "Bar Chart":
                        fig = px.bar(tile_df, x=x_axis, y=y_axis)
                    elif chart_type == "Line Chart":
                        fig = px.line(tile_df, x=x_axis, y=y_axis)
                    else:  # Scatter
                        fig = px.scatter(tile_df, x=x_axis, y=y_axis)
                st.plotly_chart(fig)

# Hapus Semua Tile
if st.sidebar.button("🗑 Hapus Semua Tile"):
    st.session_state.tiles = []
    st.session_state.tile_titles = {}
    st.session_state.tile_date_cols = {}
    st.session_state.tile_date_filters = {}