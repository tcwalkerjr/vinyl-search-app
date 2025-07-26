
import os
import pandas as pd
import requests
from datetime import datetime

DISCOGS_TOKEN = os.getenv("DISCOGS_TOKEN")
USER_AGENT = "VinylCollectionUpdater/1.0"
HEADERS = {"Authorization": f"Discogs token={DISCOGS_TOKEN}", "User-Agent": USER_AGENT}

def fetch_collection(username, folder_id=0, per_page=100):
    releases = []
    page = 1
    while True:
        url = f"https://api.discogs.com/users/{username}/collection/folders/{folder_id}/releases"
        params = {"page": page, "per_page": per_page}
        r = requests.get(url, headers=HEADERS, params=params)
        data = r.json()
        releases.extend(data["releases"])
        if page >= data["pagination"]["pages"]:
            break
        page += 1
    return releases

def fetch_tracklist(release_id):
    url = f"https://api.discogs.com/releases/{release_id}"
    r = requests.get(url, headers=HEADERS)
    if r.status_code != 200:
        return []
    data = r.json()
    return data.get("tracklist", [])

def enrich_collection(username):
    raw_collection = fetch_collection(username)
    records = []
    for item in raw_collection:
        release = item["basic_information"]
        format_names = " ".join(fmt["name"].lower() for fmt in release.get("formats", []))
        if '12"' not in format_names:
            continue
        release_id = release["id"]
        tracklist = fetch_tracklist(release_id)
        for track in tracklist:
            records.append({
                "release_id": release_id,
                "Album Title": release["title"],
                "Artist": ", ".join(artist["name"] for artist in release["artists"]),
                "Track Title": track.get("title", ""),
                "Position": track.get("position", ""),
                "Producer": "",
                "Remixer": "",
                "Label": ", ".join(label["name"] for label in release.get("labels", [])),
                "Catalog Number": ", ".join(label["catno"] for label in release.get("labels", [])),
                "Release Date": release.get("year", "")
            })
    return pd.DataFrame(records)

def merge_new_tracks(existing_df, new_df):
    # Ensure all expected columns are present in existing_df
    for col in ["release_id", "Track Title"]:
        if col not in existing_df.columns:
            existing_df[col] = ""

    existing_keys = set((row["release_id"], row["Track Title"]) for _, row in existing_df.iterrows())
    new_rows = new_df[~new_df.apply(lambda row: (row["release_id"], row["Track Title"]) in existing_keys, axis=1)]
    return pd.concat([existing_df, new_rows], ignore_index=True)

print("Columns in existing data:", existing_df.columns.tolist())

def main():
    username = os.getenv("DISCOGS_USERNAME")
    if not username:
        raise Exception("DISCOGS_USERNAME environment variable not set")

    new_data = enrich_collection(username)
    existing_path = "merged_12inch_records_only.csv"
    if os.path.exists(existing_path):
        existing_data = pd.read_csv(existing_path)
    else:
        existing_data = pd.DataFrame(columns=new_data.columns)

    merged = merge_new_tracks(existing_data, new_data)
    merged.to_csv(existing_path, index=False)
    print(f"✅ Updated merged_12inch_records_only.csv with {len(merged)} records.")

if __name__ == "__main__":
    main()
