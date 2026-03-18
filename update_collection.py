print("🔥 VINYL SYNC — INCREMENTAL MODE (STABLE v5) 🔥")

import requests
import pandas as pd
import os
import time
from datetime import datetime, timezone

DISCOGS_USER = os.getenv("DISCOGS_USER")
DISCOGS_TOKEN = os.getenv("DISCOGS_TOKEN")

EXISTING_CSV_PATH = "merged_12inch_records_only.csv"
LAST_RUN_FILE = "last_run.txt"

BASE_URL = f"https://api.discogs.com/users/{DISCOGS_USER}/collection/folders/0/releases"

HEADERS = {
    "User-Agent": "vinyl-search-app/5.0"
}


# ================================
# 📅 LAST RUN HANDLING (UTC SAFE)
# ================================
def get_last_run_date():
    if not os.path.exists(LAST_RUN_FILE):
        return datetime(2000, 1, 1, tzinfo=timezone.utc)

    with open(LAST_RUN_FILE, "r") as f:
        return datetime.fromisoformat(f.read().strip()).replace(tzinfo=timezone.utc)


def save_last_run_date():
    with open(LAST_RUN_FILE, "w") as f:
        f.write(datetime.now(timezone.utc).isoformat())


# ================================
# 🎯 VINYL FILTER (balanced)
# ================================
def is_vinyl_format(formats):
    for fmt in formats:
        name = fmt.get("name", "").lower()
        descriptions = [d.lower() for d in fmt.get("descriptions", [])]

        combined = " ".join([name] + descriptions)

        if any(x in combined for x in [
            "vinyl",
            '12"', "12”",
            "lp",
            "ep",
            "single",
            "maxi-single"
        ]):
            return True

    return False


# ================================
# 🎯 TRACK STRUCTURE FILTER
# ================================
def looks_like_vinyl_track(position):
    if not position:
        return False

    pos = str(position).strip().upper()

    return (
        pos.startswith(("A", "B", "C", "D")) or
        pos in ["A", "B"]
    )


# ================================
# 🎵 FETCH TRACKS
# ================================
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

    valid_tracks = 0

    for track in data.get("tracklist", []):
        title = track.get("title", "").strip()
        position = track.get("position", "")

        if not title or title.lower() == "none":
            continue

        if looks_like_vinyl_track(position):
            valid_tracks += 1

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

    # ✅ Require at least ONE vinyl-style track
    if valid_tracks == 0:
        return []

    return results


# ================================
# 🌐 FETCH NEW RELEASES ONLY
# ================================
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

        stop = False

        for item in data["releases"]:
            info = item.get("basic_information", {})

            release_id = str(info.get("id"))
            title = info.get("title", "Unknown")
            formats = info.get("formats", [])
            date_added = item.get("date_added")

            if not date_added:
                continue

            added_dt = datetime.fromisoformat(date_added.replace("Z", "+00:00"))

            # 🛑 stop when older than last run
            if added_dt <= last_run:
                stop = True
                break

            if not is_vinyl_format(formats):
                print(f"   ⏭️ Skipping non-vinyl: {title} ({release_id})")
                continue

            releases.append({
                "id": release_id,
                "title": title
            })

        if stop or page >= data["pagination"]["pages"]:
            break

        page += 1

    print(f"\n🆕 New vinyl releases found: {len(releases)}")
    return releases


# ================================
# 🚀 MAIN
# ================================
def main():
    last_run = get_last_run_date()
    print(f"⏱️ Last run: {last_run}")

    # Load CSV
    if os.path.exists(EXISTING_CSV_PATH):
        df = pd.read_csv(EXISTING_CSV_PATH, dtype={"release_id": str})
    else:
        df = pd.DataFrame(columns=[
            "release_id", "Artist", "Album Title", "Label", "Catalog Number",
            "Release Date", "Track Title", "Track Position", "Duration", "Producer", "Remixer"
        ])

    existing_ids = set(df["release_id"].dropna())

    # Fetch new releases only
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
                print(f"   ⚠️ Not valid vinyl structure — skipped")

    else:
        print("🆕 No new releases found.")

    # Merge
    if new_rows:
        df = pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)

    # Clean
    df = df[
        df["Track Title"].notna() &
        (df["Track Title"].str.lower() != "none")
    ]

    # Final
    print("\n========== ✅ FINAL ==========")
    print(f"🗂️ Total rows: {len(df)}")
    print("================================\n")

    # Save CSV
    df.to_csv(EXISTING_CSV_PATH, index=False)

    # Save last run timestamp
    save_last_run_date()
    print("💾 Updated last_run.txt")


if __name__ == "__main__":
    main()
