import streamlit as st
import pandas as pd
import plotly.express as px
from snowflake.snowpark import Session
from datetime import datetime
import pandas.api.types

# Membuat session ke Snowflake
session = Session.builder.getOrCreate()

# Konfigurasi layout aplikasi
st.set_page_config(layout="wide")
st.title("📊 Dynamic BI Dashboard")

# Fungsi untuk mendeteksi kolom tanggal
def detect_date_columns(df):
    date_cols = []
    date_formats = ['%Y-%m-%d', '%d-%m-%Y', '%m/%d/%Y', '%Y%m%d']  # Format tanggal yang mungkin
    
    for col in df.columns:
        # Coba konversi ke datetime dengan berbagai format
        for fmt in date_formats:
            try:
                pd.to_datetime(df[col], format=fmt, errors='raise')
                date_cols.append(col)
                break
            except:
                continue
    return list(set(date_cols))  # Hapus duplikat jika ada

# Inisialisasi session_state
state_keys = [
    "tiles", "tile_titles", "filters", "tile_date_cols", "tile_date_filters",
    "join_tables", "tile_viz_count", "tile_dataframes", "tile_viz_data",
    "tile_filters", "global_date_filter"
]

for key in state_keys:
    if key not in st.session_state:
        if key in ["tiles", "filters", "join_tables"]:
            st.session_state[key] = []
        elif key == "global_date_filter":
            st.session_state[key] = {
                "column": None,
                "start_date": None,
                "end_date": None
            }
        else:
            st.session_state[key] = {}

# Flag untuk rerun manual
if "run_rerun" not in st.session_state:
    st.session_state.run_rerun = False

# Fungsi utilitas ke Snowflake
def get_databases():
    result = session.sql("SHOW DATABASES").collect()
    return [row["name"] for row in result]

def get_schemas(database):
    result = session.sql(f"SHOW SCHEMAS IN {database}").collect()
    return [row["name"] for row in result]

def get_tables(database, schema):
    result = session.sql(f"SHOW TABLES IN {database}.{schema}").collect()
    return [row["name"] for row in result]

def get_table_data(database, schema, table):
    query = f"SELECT * FROM {database}.{schema}.{table}"
    df = session.sql(query).to_pandas()
    
    # Deteksi dan konversi kolom tanggal
    date_cols = detect_date_columns(df)
    for col in date_cols:
        try:
            df[col] = pd.to_datetime(df[col], errors='coerce')
        except:
            continue
    
    return df

# Fungsi untuk mengedit judul tile langsung
def editable_title(tile_id):
    edit_key = f"edit_title_{tile_id}"
    title_key = f"title_{tile_id}"

    if title_key not in st.session_state:
        st.session_state[title_key] = st.session_state.tile_titles[tile_id]

    if edit_key not in st.session_state:
        st.session_state[edit_key] = False

    # Jika sedang dalam mode edit
    if st.session_state[edit_key]:
        new_title = st.text_input("Ubah Judul", st.session_state[title_key], key=f"title_input_{tile_id}")
        col1, col2 = st.columns([1, 8])
        with col1:
            if st.button("✅", key=f"save_{tile_id}"):
                st.session_state[title_key] = new_title
                st.session_state.tile_titles[tile_id] = new_title
                st.session_state[edit_key] = False
        with col2:
            st.write("")  # Kosongkan kolom
    else:
        col1, col2 = st.columns([8, 1])
        with col1:
            st.subheader(f"📌 {st.session_state[title_key]}")
        with col2:
            if st.button("✏️", key=f"edit_btn_{tile_id}"):
                st.session_state[edit_key] = True

# Fungsi untuk menerapkan filter global tanggal
def apply_global_date_filter(df):
    if st.session_state.global_date_filter["column"]:
        date_col = st.session_state.global_date_filter["column"]
        start_date = st.session_state.global_date_filter["start_date"]
        end_date = st.session_state.global_date_filter["end_date"]
        
        if date_col in df.columns and start_date and end_date:
            # Pastikan kolom bertipe datetime
            if not pd.api.types.is_datetime64_any_dtype(df[date_col]):
                df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
                
            mask = (
                (df[date_col] >= pd.to_datetime(start_date)) & 
                (df[date_col] <= pd.to_datetime(end_date))
            )
            return df[mask]
    return df

# Sidebar - Kategori 1: Data
with st.sidebar.expander("📁 Data", expanded=True):
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
                except Exception as e:
                    st.sidebar.error(f"❌ Error mengambil data: {str(e)}")

    if selected_table:
        if st.button("➕ Tambah Tabel Join"):
            st.session_state.join_tables.append({
                "database": selected_db, "schema": selected_schema,
                "table": None, "join_type": "INNER",
                "left_on": None, "right_on": None
            })
            st.session_state.run_rerun = True

        for i, join_info in enumerate(st.session_state.join_tables):
            st.subheader(f"Tabel Join {i+1}")
            join_db = st.selectbox("Pilih Database Join", db_list, key=f"join_db_{i}")
            st.session_state.join_tables[i]["database"] = join_db

            join_schema_list = get_schemas(join_db)
            join_schema = st.selectbox("Pilih Schema Join", join_schema_list, key=f"join_schema_{i}")
            st.session_state.join_tables[i]["schema"] = join_schema

            join_table_list = get_tables(join_db, join_schema)
            join_table = st.selectbox("Pilih Tabel untuk Join", join_table_list, key=f"join_table_{i}")
            st.session_state.join_tables[i]["table"] = join_table

            join_type = st.selectbox("Tipe Join", ["INNER", "LEFT", "RIGHT", "OUTER"], key=f"join_type_{i}")
            st.session_state.join_tables[i]["join_type"] = join_type

            if join_table:
                join_df = get_table_data(join_db, join_schema, join_table)
                join_cols = join_df.columns.tolist()

                left_col = st.selectbox("Kolom di Tabel Utama", df.columns, key=f"left_col_{i}")
                right_col = st.selectbox("Kolom di Tabel Join", join_cols, key=f"right_col_{i}")
                st.session_state.join_tables[i]["left_on"] = left_col
                st.session_state.join_tables[i]["right_on"] = right_col

        # Tombol Hapus Semua Join hanya muncul jika ada join yang ditambahkan
        if st.session_state.join_tables:
            if st.button("🗑 Hapus Semua Join"):
                st.session_state.join_tables = []
                st.session_state.run_rerun = True

        for join_info in st.session_state.join_tables:
            if all([join_info["table"], join_info["left_on"], join_info["right_on"]]):
                try:
                    join_df = get_table_data(join_info["database"], join_info["schema"], join_info["table"])
                    df = df.merge(
                        join_df,
                        how=join_info["join_type"].lower(),
                        left_on=join_info["left_on"],
                        right_on=join_info["right_on"]
                    )
                except Exception as e:
                    st.sidebar.error(f"Gagal join dengan tabel {join_info['table']}: {e}")

# Sidebar - Filter Global Tanggal
with st.sidebar.expander("📅 Filter Global Tanggal", expanded=True):
    if selected_table and not df.empty:
        # Deteksi kolom tanggal
        date_cols = detect_date_columns(df)
        
        if date_cols:
            selected_date_col = st.selectbox(
                "Pilih Kolom Tanggal untuk Filter Global",
                date_cols,
                key="global_date_filter_col",
                index=0
            )
            
            if selected_date_col:
                # Pastikan kolom bertipe datetime
                if not pd.api.types.is_datetime64_any_dtype(df[selected_date_col]):
                    df[selected_date_col] = pd.to_datetime(df[selected_date_col], errors='coerce')
                
                # Hapus nilai NA/NAT setelah konversi
                df = df.dropna(subset=[selected_date_col])
                
                min_date = df[selected_date_col].min().to_pydatetime().date()
                max_date = df[selected_date_col].max().to_pydatetime().date()
                
                date_range = st.date_input(
                    "Rentang Tanggal",
                    value=(min_date, max_date),
                    min_value=min_date,
                    max_value=max_date,
                    key="global_date_range"
                )
                
                if len(date_range) == 2:
                    start_date, end_date = date_range
                    st.session_state.global_date_filter = {
                        "column": selected_date_col,
                        "start_date": start_date,
                        "end_date": end_date
                    }
                    
                    # Terapkan filter ke semua tile
                    for tile_id in st.session_state.tiles:
                        if tile_id in st.session_state.tile_dataframes:
                            original_df = st.session_state.tile_dataframes[tile_id]
                            filtered_df = apply_global_date_filter(original_df)
                            st.session_state.tile_dataframes[tile_id] = filtered_df
                            
                            # Update juga data visualisasi jika ada
                            for viz_key in list(st.session_state.tile_viz_data.keys()):
                                if f"tile_{tile_id}_viz" in viz_key:
                                    viz_df = st.session_state.tile_viz_data[viz_key]
                                    filtered_viz = apply_global_date_filter(viz_df)
                                    st.session_state.tile_viz_data[viz_key] = filtered_viz
        else:
            st.info("Tidak ditemukan kolom bertipe tanggal dalam data.")

# Sidebar - Tile
with st.sidebar.expander("🧩 Tile", expanded=True):
    if selected_table:
        if st.button("➕ Tambah Tile Baru"):
            tile_id = len(st.session_state.tiles) + 1
            st.session_state.tiles.append(tile_id)
            st.session_state.tile_titles[tile_id] = f"Tile {tile_id}"
            st.session_state.tile_date_cols[tile_id] = None
            st.session_state.tile_date_filters[tile_id] = {}
            st.session_state.tile_viz_count[tile_id] = 1
            
            # Pastikan kolom tanggal sudah dikonversi sebelum disimpan
            df_copy = df.copy()
            date_cols = detect_date_columns(df_copy)
            for col in date_cols:
                df_copy[col] = pd.to_datetime(df_copy[col], errors='coerce')
            
            st.session_state.tile_dataframes[tile_id] = df_copy
            st.session_state.tile_viz_data.update({f"tile_{tile_id}_viz_0": df_copy.copy()})
            st.session_state.tile_filters[tile_id] = []
            st.session_state.run_rerun = True

        if len(st.session_state.tiles) > 0:
            if st.button("🗑 Hapus Semua Tile"):
                st.session_state.tiles = []
                st.session_state.tile_titles.clear()
                st.session_state.tile_date_cols.clear()
                st.session_state.tile_date_filters.clear()
                st.session_state.tile_viz_count.clear()
                st.session_state.tile_dataframes.clear()
                st.session_state.tile_viz_data.clear()
                st.session_state.tile_filters.clear()
                st.session_state.run_rerun = True

# Fungsi untuk render tile visualisasi
def render_tile(tile, tile_index):
    editable_title(tile)
    df_tile = st.session_state.tile_dataframes.get(tile, pd.DataFrame()).copy()

    if tile not in st.session_state.tile_filters:
        st.session_state.tile_filters[tile] = []

    if st.button("🔄 Refresh Data", key=f"refresh_{tile_index}"):
        # Ambil data asli (sebelum filter global)
        original_df = df.copy()
        # Terapkan filter global ke data asli
        filtered_df = apply_global_date_filter(original_df)
        st.session_state.tile_dataframes[tile] = filtered_df
        st.session_state.tile_viz_data[f"tile_{tile}_viz_0"] = filtered_df.copy()
        st.session_state.run_rerun = True

    chart_type = st.selectbox("Jenis Visualisasi", ["Table", "Bar Chart", "Line Chart", "Pie Chart"], key=f"chart_{tile_index}")

    # Filter (Kategori)
    cat_cols = df_tile.columns.tolist()
    if chart_type:
        new_filters = []
        for i, f in enumerate(st.session_state.tile_filters[tile]):
            cols = st.columns([4, 5, 1])
            with cols[0]:
                col = st.selectbox(f"Kolom Filter #{i+1}", cat_cols, key=f"filter_col_{tile}_{i}")
            with cols[1]:
                options = df_tile[col].dropna().unique().tolist() if col else []
                vals = st.multiselect(f"Nilai untuk '{col}'", options, key=f"filter_val_{tile}_{i}")
            with cols[2]:
                if st.button("🗑", key=f"remove_filter_btn_{tile}_{i}"):
                    continue
            if vals:
                df_tile = df_tile[df_tile[col].isin(vals)]
            new_filters.append({"column": col, "values": vals})
        st.session_state.tile_filters[tile] = new_filters

    if st.button("➕ Tambah Filter", key=f"add_filter_button_{tile}"):
        st.session_state.tile_filters[tile].append({"column": None, "values": []})
        st.session_state.run_rerun = True

    if chart_type == "Table":
        st.dataframe(df_tile)
    else:
        all_cols = df_tile.columns.tolist()
        num_cols = df_tile.select_dtypes(include=['number']).columns.tolist()

        if chart_type == "Pie Chart":
            if cat_cols and num_cols:
                label = st.selectbox("Label", cat_cols, key=f"pie_label_{tile_index}")
                value = st.selectbox("Value", num_cols, key=f"pie_val_{tile_index}")
                fig = px.pie(df_tile, names=label, values=value)
                st.plotly_chart(fig)
            else:
                st.warning("Kolom untuk Pie Chart tidak tersedia.")
        else:
            if all_cols and num_cols:
                x = st.selectbox("X Axis", all_cols, key=f"x_{tile_index}")
                y = st.selectbox("Y Axis", num_cols, key=f"y_{tile_index}")
                if chart_type == "Bar Chart":
                    fig = px.bar(df_tile, x=x, y=y)
                else:
                    fig = px.line(df_tile, x=x, y=y)
                st.plotly_chart(fig)
            else:
                st.warning("Kolom untuk visualisasi tidak tersedia.")

# Layout untuk visualisasi
st.header("📊 Visualisasi Data")
tiles = st.session_state.tiles

index = 0
while index < len(tiles):
    remaining = len(tiles) - index
    if remaining >= 2:
        cols = st.columns(2)
        for j in range(2):
            if index + j < len(tiles):
                with cols[j]:
                    render_tile(tiles[index + j], index + j)
        index += 2
    else:
        render_tile(tiles[index], index)
        index += 1

# Trigger rerun jika diperlukan
if st.session_state.get("run_rerun"):
    st.session_state.run_rerun = False
    st.rerun()