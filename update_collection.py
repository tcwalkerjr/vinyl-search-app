import requests
import pandas as pd
import os
import time
from datetime import datetime, timedelta

DISCOGS_USER = os.getenv("DISCOGS_USER")
DISCOGS_TOKEN = os.getenv("DISCOGS_TOKEN")
EXISTING_CSV_PATH = "merged_12inch_records_only.csv"

BASE_URL = f"https://api.discogs.com/users/{DISCOGS_USER}/collection/folders/0/releases"
HEADERS = {
    "User-Agent": "vinyl-search-app/1.0"
}

# ✅ Always look back 7 days from "now"
CUTOFF_DATE = datetime.utcnow() - timedelta(days=7)


def is_vinyl_format(formats):
    """Return True if any format looks like Vinyl or 12\"."""
    for fmt in formats:
        name = fmt.get("name", "").lower()
        descriptions = [d.lower() for d in fmt.get("descriptions", [])]
        if "vinyl" in name:
            return True
        if any("vinyl" in d for d in descriptions):
            return True
        if any('12"' in d or "12”" in d for d in descriptions):  # cover both quotes
            return True
    return False


def fetch_release_tracks(release_id):
    url = f"https://api.discogs.com/releases/{release_id}"
    response = requests.get(url, headers=HEADERS, params={"token": DISCOGS_TOKEN})
    time.sleep(1)
    if response.status_code != 200:
        return []
    data = response.json()

    tracklist = data.get("tracklist", [])
    release_extraartists = data.get("extraartists", [])
    release_producers = [a.get("name", "") for a in release_extraartists if a.get("role", "").lower() == "producer"]
    release_remixers = [a.get("name", "") for a in release_extraartists if "remix" in a.get("role", "").lower()]

    results = []
    for track in tracklist:
        title = track.get("title", "").strip()
        if not title or title.lower() == "none":
            continue
        extra_artists = track.get("extraartists", [])
        remixers = []
        producers = []
        for artist in extra_artists:
            role = artist.get("role", "").lower()
            name = artist.get("name", "")
            if "remix" in role:
                remixers.append(name)
            elif role == "producer" and name not in remixers:
                producers.append(name)

        if not producers:
            producers = release_producers
        if not remixers:
            remixers = release_remixers

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


def fetch_new_releases(existing_ids):
    releases = []
    page = 1
    while True:
        response = requests.get(BASE_URL, headers=HEADERS, params={"page": page, "token": DISCOGS_TOKEN})
        time.sleep(1)
        response.raise_for_status()
        data = response.json()

        for item in data["releases"]:
            date_added = item.get("date_added")
            if not date_added:
                continue
            added_date = datetime.strptime(date_added[:10], "%Y-%m-%d")
            if added_date < CUTOFF_DATE:
                # Instead of returning (which stops everything), just skip
                continue

            release_id = item.get("basic_information", {}).get("id")
            if release_id in existing_ids:
                continue

            formats = item.get("basic_information", {}).get("formats", [])
            if not is_vinyl_format(formats):
                print(f"⏭️ Skipping {item['basic_information'].get('title')} ({release_id}), not vinyl/12\"")
                continue

            print(f"✅ Adding {item['basic_information'].get('title')} ({release_id}), added {added_date.date()}")
            track_rows = fetch_release_tracks(release_id)
            releases.extend(track_rows)

        if page >= data["pagination"]["pages"]:
            break
        page += 1
    return releases


def main():
    if os.path.exists(EXISTING_CSV_PATH):
        existing_data = pd.read_csv(EXISTING_CSV_PATH)
    else:
        existing_data = pd.DataFrame(columns=[
            "release_id", "Artist", "Album Title", "Label", "Catalog Number",
            "Release Date", "Track Title", "Track Position", "Duration", "Producer", "Remixer"
        ])

    existing_ids = set(existing_data["release_id"].dropna().astype(int).tolist())
    new_rows = fetch_new_releases(existing_ids)

    if not new_rows:
        print("✅ No new releases found since", CUTOFF_DATE.date())
        return

    new_df = pd.DataFrame(new_rows)
    final = pd.concat([existing_data, new_df], ignore_index=True)

    final = final[final["Track Title"].notna() & (final["Track Title"].str.lower() != "none")]

    print(f"✅ Added {len(new_df)} new rows.")
    print(f"🗂️ Final collection now has {len(final)} rows.")
    final.to_csv(EXISTING_CSV_PATH, index=False)


if __name__ == "__main__":
    main()
