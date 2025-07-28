import requests
import pandas as pd
import os
import time
from datetime import datetime

DISCOGS_USER = os.getenv("DISCOGS_USER")
DISCOGS_TOKEN = os.getenv("DISCOGS_TOKEN")
EXISTING_CSV_PATH = "merged_12inch_records_only.csv"

BASE_URL = f"https://api.discogs.com/users/{DISCOGS_USER}/collection/folders/0/releases"
HEADERS = {
    "User-Agent": "vinyl-search-app/1.0"
}


def fetch_release_tracks(release_id):
    url = f"https://api.discogs.com/releases/{release_id}"
    response = requests.get(url, headers=HEADERS, params={"token": DISCOGS_TOKEN})
    time.sleep(1)
    if response.status_code != 200:
        return []
    data = response.json()
    tracklist = data.get("tracklist", [])
    release_producers = [a.get("name", "") for a in data.get("extraartists", []) if a.get("role", "").lower() == "producer"]
    results = []
    for track in tracklist:
        title = track.get("title", "").strip()
        if not title or title.lower() == "none":
            continue
                # Gather producer and remixer credits if available
        extra_artists = track.get("extraartists", [])
        remixers = []
        producers = []  # Track-level producers, fallback to release-level if empty
        for artist in extra_artists:
            role = artist.get("role", "").lower()
            name = artist.get("name", "")
            if "remix" in role:
                remixers.append(name)
            elif role == "producer" and name not in remixers:
                producers.append(name)

                # Fallback: use release-level producer if none at track level
        if not producers:
            producers = release_producers

        results.append({
            "release_id": release_id,
            "Track Title": title,
            "Track Position": track.get("position", ""),
            "Duration": track.get("duration", ""),
            "Producer": ", ".join(producers),
            "Remixer": ", ".join(remixers),
            "Artist": ", ".join(a.get("name", "") for a in data.get("artists", [])),
            "Album Title": data.get("title", ""),
            "Label": ", ".join(l.get("name", "") for l in data.get("labels", [])),
            "Catalog Number": ", ".join(l.get("catno", "") for l in data.get("labels", [])),
            "Release Date": data.get("year", "")
        })
    return results


def fetch_collection():
    releases = []
    page = 1
    while True:
        response = requests.get(BASE_URL, headers=HEADERS, params={"page": page, "token": DISCOGS_TOKEN})
        time.sleep(1)
        response.raise_for_status()
        data = response.json()

        for item in data["releases"]:
            formats = item.get("basic_information", {}).get("formats", [])
            if not any("Vinyl" in desc for fmt in formats for desc in fmt.get("descriptions", [])):
                continue  # Skip non-12" records
            release_id = item.get("basic_information", {}).get("id")
            track_rows = fetch_release_tracks(release_id)
            releases.extend(track_rows)

        if page >= data["pagination"]["pages"]:
            break
        page += 1

    return pd.DataFrame(releases)


def merge_new_tracks(existing_df, new_df):
    
    if "release_id" in existing_df.columns and "release_id" in new_df.columns:
        existing_df = existing_df[~existing_df["release_id"].isin(new_df["release_id"])]
    return pd.concat([existing_df, new_df], ignore_index=True)


def main():
    if os.path.exists(EXISTING_CSV_PATH):
        existing_data = pd.read_csv(EXISTING_CSV_PATH)
    else:
        existing_data = pd.DataFrame(columns=[
            "release_id", "Artist", "Album Title", "Label", "Catalog Number",
            "Release Date", "Track Title", "Track Position", "Duration", "Producer", "Remixer"])

    new_data = fetch_collection()
    merged = merge_new_tracks(existing_data, new_data)

    # Final clean-up: drop any rows with missing or 'none' Track Title
    if "Track Title" in merged.columns:
        merged = merged[merged["Track Title"].notnull() & (merged["Track Title"].str.lower() != "none") & (merged["Track Title"].str.strip() != "")]

    print(f"Fetched {len(new_data)} new rows")
    print(f"Final merged row count: {len(merged)}")
    merged.to_csv(EXISTING_CSV_PATH, index=False)


if __name__ == "__main__":
    main()
