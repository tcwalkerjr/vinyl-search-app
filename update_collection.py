import requests
import pandas as pd
import os
from datetime import datetime

DISCOGS_USER = os.getenv("DISCOGS_USER")
DISCOGS_TOKEN = os.getenv("DISCOGS_TOKEN")
EXISTING_CSV_PATH = "merged_12inch_records_only.csv"
NEW_CSV_PATH = "tracklist_final_cleaned.csv"

BASE_URL = f"https://api.discogs.com/users/{DISCOGS_USER}/collection/folders/0/releases"
HEADERS = {
    "User-Agent": "vinyl-search-app/1.0"
}

def fetch_collection():
    releases = []
    page = 1

    while True:
        response = requests.get(BASE_URL, headers=HEADERS, params={"page": page, "token": DISCOGS_TOKEN})
        response.raise_for_status()
        data = response.json()

        for item in data["releases"]:
            formats = item.get("basic_information", {}).get("formats", [])
            if not any("12\"" in desc for fmt in formats for desc in fmt.get("descriptions", [])):
                continue  # Skip non-12" records

            basic_info = item["basic_information"]
            track = {
                "release_id": basic_info.get("id"),
                "Artist": ", ".join(artist["name"] for artist in basic_info.get("artists", [])),
                "Album Title": basic_info.get("title"),
                "Label": ", ".join(label["name"] for label in basic_info.get("labels", [])),
                "Catalog Number": ", ".join(label["catno"] for label in basic_info.get("labels", [])),
                "Release Date": basic_info.get("year"),
            }
            releases.append(track)

        if page >= data["pagination"]["pages"]:
            break
        page += 1

    return pd.DataFrame(releases)

def merge_new_tracks(existing_df, new_df):
    if "release_id" in existing_df.columns:
        existing_keys = set(zip(existing_df["release_id"], existing_df["Album Title"]))
        new_df = new_df[~new_df.apply(lambda row: (row["release_id"], row["Album Title"]) in existing_keys, axis=1)]
    else:
        existing_keys = set(existing_df["Album Title"])
        new_df = new_df[~new_df["Album Title"].isin(existing_keys)]

    return pd.concat([existing_df, new_df], ignore_index=True)

def main():
    if os.path.exists(EXISTING_CSV_PATH):
        existing_data = pd.read_csv(EXISTING_CSV_PATH)
    else:
        existing_data = pd.DataFrame(columns=["release_id", "Artist", "Album Title", "Label", "Catalog Number", "Release Date"])

    new_data = fetch_collection()
    merged = merge_new_tracks(existing_data, new_data)
    merged.to_csv(EXISTING_CSV_PATH, index=False)

if __name__ == "__main__":
    main()
