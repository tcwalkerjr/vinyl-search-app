print("🔥 VINYL SYNC — STRICT VINYL MODE 🔥")

import requests
import pandas as pd
import os
import time

DISCOGS_USER = os.getenv("DISCOGS_USER")
DISCOGS_TOKEN = os.getenv("DISCOGS_TOKEN")
EXISTING_CSV_PATH = "merged_12inch_records_only.csv"

BASE_URL = f"https://api.discogs.com/users/{DISCOGS_USER}/collection/folders/0/releases"

HEADERS = {
    "User-Agent": "vinyl-search-app/3.0"
}

# 🎯 STRICT VINYL FILTER
def is_vinyl_format(formats):
    for fmt in formats:
        name = fmt.get("name", "").lower()

        # MUST explicitly be vinyl
        if "vinyl" in name:
            return True

    return False


# 🎯 TRACK FILTER (vinyl-style positions only)
def is_vinyl_track(position):
    if not position:
        return False

    position = str(position).strip().upper()

    # Accept common vinyl formats
    return (
        position.startswith(("A", "B", "C", "D")) or
        position in ["A", "B"]  # some records just use A/B
    )


def fetch_release_tracks(release_id):
    url = f"https://api.discogs.com/releases/{release_id}"

    response = requests.get(
        url,
        headers=HEADERS,
        params={"token": DISCOGS_TOKEN}
    )

    time.sleep(0.5)

    if response.status_code != 200:
        print(f"   ❌ Failed to fetch release {release_id}")
        return []

    data = response.json()
    results = []

    for track in data.get("tracklist", []):
        title = track.get("title", "").strip()
        position = track.get("position", "")

        if not title or title.lower() == "none":
            continue

        # 🎯 FILTER NON-VINYL TRACK STRUCTURES
        if not is_vinyl_track(position):
            continue

        results.append({
            "release_id": str(release_id),
            "Track Title": title,
            "Track Position": position,
            "Duration": track.get("duration", ""),
            "Producer": "",
            "Remixer": "",
            "Artist": ", ".join(a.get("name", "") for a in data.get("artists", [])),
            "Album Title": data.get("title", ""),
            "Label": ", ".join(l.get("name", "") for l in data.get("labels", [])),
            "Catalog Number": ", ".join(l.get("catno", "") for l in data.get("labels", [])),
            "Release Date": data.get("year", "")
        })

    return results


def fetch_all_discogs():
    releases = []
    vinyl_ids = set()
    all_ids = set()
    page = 1

    while True:
        print(f"📄 Fetching page {page}...")

        response = requests.get(
            BASE_URL,
            headers=HEADERS,
            params={"page": page, "token": DISCOGS_TOKEN}
        )

        time.sleep(0.5)
        response.raise_for_status()

        data = response.json()

        for item in data["releases"]:
            info = item.get("basic_information", {})

            release_id = str(info.get("id"))
            title = info.get("title", "Unknown")
            formats = info.get("formats", [])

            all_ids.add(release_id)

            if not is_vinyl_format(formats):
                print(f"   ⏭️ Skipping non-vinyl: {title} ({release_id})")
                continue

            vinyl_ids.add(release_id)

            releases.append({
                "id": release_id,
                "title": title
            })

        if page >= data["pagination"]["pages"]:
            break

        page += 1

    print(f"\n📀 Total collection items: {len(all_ids)}")
    print(f"🎵 Total VINYL items: {len(vinyl_ids)}")

    return releases, vinyl_ids


def main():
    # 📂 Load CSV
    if os.path.exists(EXISTING_CSV_PATH):
        df = pd.read_csv(EXISTING_CSV_PATH, dtype={"release_id": str})
    else:
        df = pd.DataFrame(columns=[
            "release_id", "Artist", "Album Title", "Label", "Catalog Number",
            "Release Date", "Track Title", "Track Position", "Duration", "Producer", "Remixer"
        ])

    existing_ids = set(df["release_id"].dropna())

    # 🌐 Fetch Discogs
    discogs_releases, vinyl_ids = fetch_all_discogs()

    # 🔄 Compare ONLY vinyl IDs
    ids_to_add = vinyl_ids - existing_ids

    print("\n========== 🔄 SYNC SUMMARY ==========")
    print(f"🆕 Releases to add: {len(ids_to_add)}")
    print("====================================\n")

    # ➕ ADD ONLY (🚨 NO AUTO DELETE — protects your data)
    new_rows = []

    if ids_to_add:
        print("🆕 Adding releases:")

        for rel in discogs_releases:
            if rel["id"] not in ids_to_add:
                continue

            print(f"   ➕ {rel['title']} ({rel['id']})")

            tracks = fetch_release_tracks(rel["id"])

            if tracks:
                new_rows.extend(tracks)
            else:
                print(f"   ⚠️ No valid vinyl tracks — skipped")

    else:
        print("🆕 No new releases to add.")

    # 🧩 Merge
    if new_rows:
        df = pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)

    # 🧼 Clean
    df = df[
        df["Track Title"].notna() &
        (df["Track Title"].str.lower() != "none")
    ]

    # 📊 Final
    print("\n========== ✅ FINAL ==========")
    print(f"🗂️ Total rows: {len(df)}")
    print("================================\n")

    # 💾 Save
    df.to_csv(EXISTING_CSV_PATH, index=False)


if __name__ == "__main__":
    main()
