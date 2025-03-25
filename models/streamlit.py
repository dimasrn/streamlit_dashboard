import streamlit as st
import pandas as pd
import plotly.express as px
from snowflake.snowpark import Session
import os


try:
    session = get_active_session()
except:
    session = Session.builder.configs({
            "user": 'USERBERCA',
            "password":'P@ssvv0rd032025',
            "account":'jf57268.ap-southeast-3.aws',
            "host":'jf57268.ap-southeast-3.aws.snowflakecomputing.com',
            "port": 443,
            "warehouse":'COMPUTE_WH',
            "role": 'ACCOUNTADMIN',
        }).create()

# # Membuat session ke Snowflake
# session = Session.builder.getOrCreate()

st.title("📊 Dynamic BI Dashboard - Snowflake & Streamlit")

# Simpan daftar tile dalam session_state
if "tiles" not in st.session_state:
    st.session_state.tiles = []

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

# Tambah Tile Baru
if st.button("➕ Tambah Tile Baru"):
    tile_id = len(st.session_state.tiles) + 1
    st.session_state.tiles.append(tile_id)

# Loop untuk setiap tile
for tile in st.session_state.tiles:
    with st.container():
        st.subheader(f"📌 Tile {tile}")

        db_list = get_databases()
        selected_db = st.selectbox(f"🔹 Pilih Database (Tile {tile})", db_list, key=f"db_{tile}")
        
        if selected_db:
            schema_list = get_schemas(selected_db)
            selected_schema = st.selectbox(f"🔹 Pilih Schema (Tile {tile})", schema_list, key=f"schema_{tile}")
            
            if selected_schema:
                table_list = get_tables(selected_db, selected_schema)
                selected_table = st.selectbox(f"🔹 Pilih Tabel (Tile {tile})", table_list, key=f"table_{tile}")
                
                if selected_table:
                    try:
                        df = get_table_data(selected_db, selected_schema, selected_table)
                        st.success(f"✅ Data berhasil diambil dari {selected_table}")
                        
                        # Menambahkan Filter Berdasarkan Data yang Dipilih
                        filter_cols = df.select_dtypes(include=['object', 'category']).columns.tolist()
                        date_cols = df.select_dtypes(include=['datetime']).columns.tolist()
                        numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
                        
                        filters = {}
                        for col in filter_cols:
                            unique_values = df[col].unique().tolist()
                            filters[col] = st.multiselect(f"Filter {col}", unique_values, default=unique_values, key=f"filter_{col}_{tile}")
                        
                        for col in date_cols:
                            df[col] = pd.to_datetime(df[col])
                            filters[f"Tahun {col}"] = st.multiselect(f"Filter Tahun {col}", df[col].dt.year.unique().tolist(), key=f"year_{col}_{tile}")
                            filters[f"Bulan {col}"] = st.multiselect(f"Filter Bulan {col}", df[col].dt.month.unique().tolist(), key=f"month_{col}_{tile}")
                            filters[f"Tanggal {col}"] = st.multiselect(f"Filter Tanggal {col}", df[col].dt.day.unique().tolist(), key=f"day_{col}_{tile}")
                        
                        # Terapkan Filter
                        for col, values in filters.items():
                            if "Tahun" in col or "Bulan" in col or "Tanggal" in col:
                                date_col = col.split(" ")[1]
                                if "Tahun" in col:
                                    df = df[df[date_col].dt.year.isin(values)]
                                elif "Bulan" in col:
                                    df = df[df[date_col].dt.month.isin(values)]
                                elif "Tanggal" in col:
                                    df = df[df[date_col].dt.day.isin(values)]
                            elif values:
                                df = df[df[col].isin(values)]
                        
                        # Pilih Jenis Visualisasi
                        chart_type = st.selectbox(
                            f"📊 Pilih Jenis Visualisasi (Tile {tile})", 
                            ["Table", "Bar Chart", "Line Chart", "Scatter"],
                            key=f"chart_{tile}")
                        
                        if chart_type == "Table":
                            st.dataframe(df)
                        else:
                            x_axis = st.selectbox("Pilih X-Axis", filter_cols + numeric_cols, key=f"x_{tile}")
                            y_axis = st.selectbox("Pilih Y-Axis", numeric_cols, key=f"y_{tile}")
                            
                            # Buat Visualisasi
                            if chart_type == "Bar Chart":
                                fig = px.bar(df, x=x_axis, y=y_axis)
                            elif chart_type == "Line Chart":
                                fig = px.line(df, x=x_axis, y=y_axis)
                            elif chart_type == "Scatter":
                                fig = px.scatter(df, x=x_axis, y=y_axis)
                            st.plotly_chart(fig)
                    
                    except Exception as e:
                        st.error(f"❌ Error mengambil data: {str(e)}")

# Hapus Semua Tile
if st.button("🗑 Hapus Semua Tile"):
    st.session_state.tiles = []

