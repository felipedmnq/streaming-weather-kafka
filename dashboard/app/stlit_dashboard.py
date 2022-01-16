import os
import streamlit as st
from PIL import Image
from datetime import datetime

NOW = datetime.now()

# DEFINING CONTAINERS
header = st.container()
sidebar = st.container()
dashboard = st.container()
get_data = st.container()

with sidebar:
    with header:
        logo = Image.open(os.path.join("..", "static", "logo_white_cropped.png"))
        st.sidebar.image(logo)

    st.sidebar.header("Get Data")

    with get_data:
        last_update = f"LAST UPDATE: {NOW.date()} - {NOW.time().strftime('%H:%M:%S')}"
        st.sidebar.write(last_update)
        update_help = "Get up to date data."
        update_button = st.sidebar.button("Update", help=update_help)
        if update_button:
            last_update = last_update

