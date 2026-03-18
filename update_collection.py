print("🔥 VINYL SYNC — INCREMENTAL MODE (LAST RUN) 🔥")

import requests
import pandas as pd
import os
import time
from datetime import datetime

DISCOGS_USER = os.getenv("DISCOGS_USER")
DISCOGS_TOKEN = os.getenv("DISCOGS_TOKEN")

EXISTING_CSV_PATH = "merged_12inch_records_only.csv"
LAST_RUN_FILE = "last_run.txt"

BASE_URL = f"https://api.discogs.com/users/{DISCOGS_USER}/collection/folders/0/releases"

HEADERS = {
    "User-Agent": "vinyl-search-app/4.0"
}


# 📅 LAST RUN HANDLING
def get_last_run_date():
    if not os.path.exists(LAST_RUN_FILE):
        return datetime(2000, 1, 1)

    with open(LAST_RUN_FILE, "r") as f:
        return datetime.fromisoformat(f.read().strip())


def save_last_run_date():
    with open(LAST_RUN_FILE, "w") as f:
        f.write(datetime.utcnow().date().isoformat())


# 🎯 STRICT VINYL FILTER
def is_vinyl_format(formats):
    for fmt in formats:
        name = fmt.get("name", "").lower()
        if "vinyl" in name:
            return True
    return False


# 🎯 VINYL TRACK STRUCTURE CHECK
def is_vinyl_track(position):
    if not position:
        return False

    position = str(position).strip().upper()

    return (
        position.startswith(("A", "B", "C", "D")) or
        position in ["A", "B"]
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


def fetch_new_discogs_releases(last_run):
    releases = []
    page = 1

    while True:
        print(f"📄 Fetching page {page}...")

        response = requests.get(
            BASE_URL,
            headers=HEADERS,
            params={
                "page": page,
                "token": DISCOGS_TOKEN,
                "sort": "added",
                "sort_order": "desc"
            }
        )

        time.sleep(0.5)
        response.raise_for_status()

        data = response.json()

        stop_fetching = False

        for item in data["releases"]:
            info = item.get("basic_information", {})

            release_id = str(info.get("id"))
            title = info.get("title", "Unknown")
            formats = info.get("formats", [])
            date_added = item.get("date_added")

            if date_added:
                added_dt = datetime.fromisoformat(date_added.replace("Z", ""))

                # 🛑 STOP once we hit older records
                if added_dt <= last_run:
                    stop_fetching = True
                    break

            # 🎯 VINYL FILTER
            if not is_vinyl_format(formats):
                print(f"   ⏭️ Skipping non-vinyl: {title} ({release_id})")
                continue

            releases.append({
                "id": release_id,
                "title": title
            })

        if stop_fetching or page >= data["pagination"]["pages"]:
            break

        page += 1

    print(f"\n🆕 New vinyl releases found: {len(releases)}")
    return releases


def main():
    last_run = get_last_run_date()
    print(f"⏱️ Last run: {last_run}")

    # 📂 Load CSV
    if os.path.exists(EXISTING_CSV_PATH):
        df = pd.read_csv(EXISTING_CSV_PATH, dtype={"release_id": str})
    else:
        df = pd.DataFrame(columns=[
            "release_id", "Artist", "Album Title", "Label", "Catalog Number",
            "Release Date", "Track Title", "Track Position", "Duration", "Producer", "Remixer"
        ])

    existing_ids = set(df["release_id"].dropna())

    # 🌐 Fetch ONLY new releases
    new_releases = fetch_new_discogs_releases(last_run)

    new_rows = []

    if new_releases:
        print("\n🆕 Adding releases:")

        for rel in new_releases:
            if rel["id"] in existing_ids:
                continue

            print(f"   ➕ {rel['title']} ({rel['id']})")

            tracks = fetch_release_tracks(rel["id"])

            if tracks:
                new_rows.extend(tracks)
            else:
                print(f"   ⚠️ No valid vinyl tracks — skipped")

    else:
        print("🆕 No new releases found.")

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

    # 💾 Save CSV
    df.to_csv(EXISTING_CSV_PATH, index=False)

    # 💾 Save LAST RUN
    save_last_run_date()
    print("💾 Updated last_run.txt")


if __name__ == "__main__":
    main()
