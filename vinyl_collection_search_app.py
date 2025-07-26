
import streamlit as st
import pandas as pd

# Load the data
@st.cache_data
def load_data():
    return pd.read_csv("merged_12inch_records_only.csv")

df = load_data()

st.title("🎧 12\" Vinyl Collection Search")

# Sidebar filters
st.sidebar.header("🔍 Filter Collection")

artist = st.sidebar.text_input("Artist")
album = st.sidebar.text_input("Album Title")
track = st.sidebar.text_input("Track Title")
producer = st.sidebar.text_input("Producer")
remixer = st.sidebar.text_input("Remixer")
label = st.sidebar.text_input("Label")
catalog = st.sidebar.text_input("Catalog Number")
release_date = st.sidebar.text_input("Release Date")

# Apply filters
filtered_df = df.copy()

if artist:
    filtered_df = filtered_df[filtered_df["Artist"].str.contains(artist, case=False, na=False)]
if album:
    filtered_df = filtered_df[filtered_df["Album Title"].str.contains(album, case=False, na=False)]
if track:
    filtered_df = filtered_df[filtered_df["Track Title"].str.contains(track, case=False, na=False)]
if producer:
    filtered_df = filtered_df[filtered_df["Producer"].str.contains(producer, case=False, na=False)]
if remixer:
    filtered_df = filtered_df[filtered_df["Remixer"].str.contains(remixer, case=False, na=False)]
if label:
    filtered_df = filtered_df[filtered_df["Label"].str.contains(label, case=False, na=False)]
if catalog:
    filtered_df = filtered_df[filtered_df["Catalog Number"].str.contains(catalog, case=False, na=False)]
if release_date:
    filtered_df = filtered_df[filtered_df["Release Date"].astype(str).str.contains(release_date, case=False, na=False)]

st.write(f"### 🎼 Showing {len(filtered_df)} matching track(s)")
st.dataframe(filtered_df)

# Optional download
st.download_button("⬇️ Download Filtered CSV", data=filtered_df.to_csv(index=False), file_name="filtered_collection.csv", mime="text/csv")
