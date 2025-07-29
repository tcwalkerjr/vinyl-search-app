import streamlit as st
import pandas as pd
import random
import requests

CSV_PATH = "merged_12inch_records_only.csv"

st.title("🎧 Vinyl Collection Explorer")

@st.cache_data
def load_data():
    df = pd.read_csv(CSV_PATH)
    df["Album Title"] = df["Album Title"].fillna("").astype(str)
    df["Artist"] = df["Artist"].fillna("").astype(str)
    df["release_id"] = pd.to_numeric(df["release_id"], errors="coerce")
    return df

df = load_data()

# --- 🔍 SEARCH -----------------------------------------------------
st.markdown("## 🔍 Search Your Collection")

search_title = st.text_input("Search by album or track title").lower()
search_artist = st.text_input("Search by artist").lower()

filtered_df = df.copy()
if search_title:
    filtered_df = filtered_df[
        filtered_df["Album Title"].str.lower().str.contains(search_title) |
        filtered_df["Track Title"].str.lower().str.contains(search_title)
    ]
if search_artist:
    filtered_df = filtered_df[filtered_df["Artist"].str.lower().str.contains(search_artist)]

st.markdown(f"### Results: {len(filtered_df)} track(s) found")
st.dataframe(filtered_df)

# --- 🎲 RANDOM ALBUM PICKER -----------------------------------------------
st.markdown("---")
st.markdown("## 🎲 Discover a Random Album")

remixer_filter = st.text_input("Filter by Remixer (optional)").strip().lower()

random_df = df.copy()
if remixer_filter:
    random_df = random_df[random_df["Remixer"].fillna("").str.lower().str.contains(remixer_filter)]

release_groups = random_df.groupby("release_id")

if "random_album_id" not in st.session_state:
    st.session_state.random_album_id = None

col1, col2 = st.columns([1, 1])
with col1:
    if st.button("🎧 Pick a Random Album"):
        if not release_groups:
            st.warning("No albums found with that remixer.")
        else:
            st.session_state.random_album_id = random.choice(list(release_groups.groups.keys()))
with col2:
    if st.button("🔀 Shuffle Again"):
        if not release_groups:
            st.warning("No albums to shuffle.")
        else:
            st.session_state.random_album_id = random.choice(list(release_groups.groups.keys()))

if st.session_state.random_album_id:
    album_df = release_groups.get_group(st.session_state.random_album_id)
    album_info = album_df.iloc[0]

    st.subheader(f"{album_info['Album Title']} — {album_info['Artist']}")
    st.markdown(f"**Label:** {album_info['Label']}  \n"
                f"**Catalog #:** {album_info['Catalog Number']}  \n"
                f"**Year:** {album_info['Release Date']}  \n"
                f"[🔗 View on Discogs](https://www.discogs.com/release/{int(st.session_state.random_album_id)})")

    try:
        release_url = f"https://api.discogs.com/releases/{int(st.session_state.random_album_id)}"
        headers = {"User-Agent": "vinyl-search-app/1.0"}
        r = requests.get(release_url, headers=headers)
        if r.ok:
            img_url = r.json().get("images", [{}])[0].get("uri", "")
            if img_url:
                st.image(img_url, use_column_width=True)
    except Exception as e:
        st.warning("Could not fetch cover image.")

    st.markdown("### Tracklist:")
    for _, row in album_df.iterrows():
        st.markdown(f"- **{row['Track Position']}**: {row['Track Title']}")
