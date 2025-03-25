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

st.set_page_config(layout="wide")
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

# Layout dua kolom
col1, col2 = st.columns([1, 3])

with col1:
    st.header("🔍 Filter Data")
    db_list = get_databases()
    selected_db = st.selectbox("🔹 Pilih Database", db_list)

    if selected_db:
        schema_list = get_schemas(selected_db)
        selected_schema = st.selectbox("🔹 Pilih Schema", schema_list)

        if selected_schema:
            table_list = get_tables(selected_db, selected_schema)
            selected_table = st.selectbox("🔹 Pilih Tabel", table_list)

            if selected_table:
                try:
                    df = get_table_data(selected_db, selected_schema, selected_table)
                    df['TANGGAL'] = pd.to_datetime(df['TANGGAL'])
                    
                    tahun_options = df['TANGGAL'].dt.year.unique().tolist()
                    bulan_options = df['TANGGAL'].dt.month.unique().tolist()
                    
                    selected_tahun = st.multiselect("📅 Pilih Tahun", tahun_options, default=tahun_options)
                    selected_bulan = st.multiselect("📅 Pilih Bulan", bulan_options, default=bulan_options)
                    
                    df = df[df['TANGGAL'].dt.year.isin(selected_tahun) & df['TANGGAL'].dt.month.isin(selected_bulan)]
                except Exception as e:
                    st.error(f"❌ Error mengambil data: {str(e)}")

# Tambah Tile Baru
if st.button("➕ Tambah Tile Baru"):
    tile_id = len(st.session_state.tiles) + 1
    st.session_state.tiles.append(tile_id)

# Loop untuk setiap tile
for tile in st.session_state.tiles:
    with st.container():
        st.subheader(f"📌 Tile {tile}")
        if selected_table and not df.empty:
            chart_type = st.selectbox(
                f"📊 Pilih Jenis Visualisasi (Tile {tile})",
                ["Table", "Bar Chart", "Line Chart", "Scatter"],
                key=f"chart_{tile}"
            )
            
            if chart_type == "Table":
                st.dataframe(df)
            else:
                numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
                x_axis = st.selectbox("Pilih X-Axis", numeric_cols, key=f"x_{tile}")
                y_axis = st.selectbox("Pilih Y-Axis", numeric_cols, key=f"y_{tile}")
                
                if chart_type == "Bar Chart":
                    fig = px.bar(df, x=x_axis, y=y_axis)
                elif chart_type == "Line Chart":
                    fig = px.line(df, x=x_axis, y=y_axis)
                elif chart_type == "Scatter":
                    fig = px.scatter(df, x=x_axis, y=y_axis)
                st.plotly_chart(fig)

# Hapus Semua Tile
if st.button("🗑 Hapus Semua Tile"):
    st.session_state.tiles = []