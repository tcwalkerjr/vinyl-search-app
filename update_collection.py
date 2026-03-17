import requests
import pandas as pd
import os
import time

DISCOGS_USER = os.getenv("DISCOGS_USER")
DISCOGS_TOKEN = os.getenv("DISCOGS_TOKEN")
EXISTING_CSV_PATH = "merged_12inch_records_only.csv"

BASE_URL = f"https://api.discogs.com/users/{DISCOGS_USER}/collection/folders/0/releases"

HEADERS = {
    "User-Agent": "vinyl-search-app/1.0"
}


def is_vinyl_format(formats):
    """Return True if format is Vinyl or 12\"."""
    for fmt in formats:
        name = fmt.get("name", "").lower()
        descriptions = [d.lower() for d in fmt.get("descriptions", [])]

        if "vinyl" in name:
            return True
        if any("vinyl" in d for d in descriptions):
            return True
        if any('12"' in d or "12”" in d for d in descriptions):
            return True

    return False


def fetch_release_tracks(release_id):
    url = f"https://api.discogs.com/releases/{release_id}"
    response = requests.get(url, headers=HEADERS, params={"token": DISCOGS_TOKEN})
    time.sleep(1)

    if response.status_code != 200:
        return []

    data = response.json()
    results = []

    for track in data.get("tracklist", []):
        title = track.get("title", "").strip()
        if not title or title.lower() == "none":
            continue

        results.append({
            "release_id": str(release_id),
            "Track Title": title,
            "Track Position": track.get("position", ""),
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
    """Fetch all releases + IDs from Discogs"""
    releases = []
    all_ids = set()
    page = 1

    while True:
        print(f"📄 Fetching page {page}...")

        response = requests.get(
            BASE_URL,
            headers=HEADERS,
            params={"page": page, "token": DISCOGS_TOKEN}
        )
        time.sleep(1)
        response.raise_for_status()
        data = response.json()

        for item in data["releases"]:
            release_id = str(item.get("basic_information", {}).get("id"))
            title = item.get("basic_information", {}).get("title", "Unknown")
            formats = item.get("basic_information", {}).get("formats", [])

            all_ids.add(release_id)

            if not is_vinyl_format(formats):
                continue

            releases.append({
                "id": release_id,
                "title": title
            })

        if page >= data["pagination"]["pages"]:
            break

        page += 1

    print(f"📀 Total releases in Discogs: {len(all_ids)}")
    return releases, all_ids


def main():
    # Load existing CSV
    if os.path.exists(EXISTING_CSV_PATH):
        df = pd.read_csv(EXISTING_CSV_PATH, dtype={"release_id": str})
    else:
        df = pd.DataFrame(columns=[
            "release_id", "Artist", "Album Title", "Label", "Catalog Number",
            "Release Date", "Track Title", "Track Position", "Duration", "Producer", "Remixer"
        ])

    existing_ids = set(df["release_id"].dropna())

    # Fetch Discogs data
    discogs_releases, discogs_ids = fetch_all_discogs()

    # Compare sets
    ids_to_remove = existing_ids - discogs_ids
    ids_to_add = discogs_ids - existing_ids

    print("\n========== 🔄 SYNC SUMMARY ==========")
    print(f"🗑️ Releases to remove: {len(ids_to_remove)}")
    print(f"🆕 Releases to add: {len(ids_to_add)}")
    print("====================================\n")

    # 🗑️ Remove deleted releases
    if ids_to_remove:
        print("🗑️ Removing releases:")
        for rid in ids_to_remove:
            print(f"   ❌ {rid}")
        df = df[~df["release_id"].isin(ids_to_remove)]
    else:
        print("🗑️ No releases to remove.")

    # ➕ Add new releases
    new_rows = []

    if ids_to_add:
        print("\n🆕 Adding releases:")

        for rel in discogs_releases:
            if rel["id"] not in ids_to_add:
                continue

            print(f"   ➕ {rel['title']} ({rel['id']})")

            tracks = fetch_release_tracks(rel["id"])
            if tracks:
                new_rows.extend(tracks)
            else:
                print(f"   ⚠️ No tracklist — skipped")

    else:
        print("\n🆕 No new releases to add.")

    # Merge new rows
    if new_rows:
        df = pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)

    # Clean invalid rows
    df = df[
        df["Track Title"].notna() &
        (df["Track Title"].str.lower() != "none")
    ]

    # Final summary
    print("\n========== ✅ FINAL ==========")
    print(f"🗂️ Total rows: {len(df)}")

    if not ids_to_add and not ids_to_remove:
        print("✅ Collection already fully in sync with Discogs")

    print("================================\n")

    # Save
    df.to_csv(EXISTING_CSV_PATH, index=False)


if __name__ == "__main__":
    main()
