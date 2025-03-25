import streamlit as st
import pandas as pd
from pygwalker.api.streamlit import StreamlitRenderer

uploaded_file = st.file_uploader("your csv")

if uploaded_file is not None:
    df = pd.read_csv(uploaded_file)
    pyg_app= StreamlitRenderer(df)
    pyg_app.explorer()
    
