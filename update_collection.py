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
    """Return True if any format looks like Vinyl or 12\"."""
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


def fetch_release_title(release_id):
    url = f"https://api.discogs.com/releases/{release_id}"
    response = requests.get(url, headers=HEADERS, params={"token": DISCOGS_TOKEN})
    time.sleep(0.5)

    if response.status_code != 200:
        return f"Unknown ({release_id})"

    data = response.json()
    return data.get("title", f"Unknown ({release_id})")


def fetch_release_tracks(release_id):
    url = f"https://api.discogs.com/releases/{release_id}"
    response = requests.get(url, headers=HEADERS, params={"token": DISCOGS_TOKEN})
    time.sleep(1)

    if response.status_code != 200:
        return []

    data = response.json()

    tracklist = data.get("tracklist", [])
    release_extraartists = data.get("extraartists", [])

    release_producers = [
        a.get("name", "") for a in release_extraartists
        if a.get("role", "").lower() == "producer"
    ]

    release_remixers = [
        a.get("name", "") for a in release_extraartists
        if "remix" in a.get("role", "").lower()
    ]

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
            "release_id": str(release_id),
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


def fetch_all_discogs_release_ids():
    ids = set()
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
            ids.add(release_id)

        if page >= data["pagination"]["pages"]:
            break

        page += 1

    print(f"📀 Total releases in Discogs: {len(ids)}")
    return ids


def main():
    if os.path.exists(EXISTING_CSV_PATH):
        existing_data = pd.read_csv(EXISTING_CSV_PATH, dtype={"release_id": str})
    else:
        existing_data = pd.DataFrame(columns=[
            "release_id", "Artist", "Album Title", "Label", "Catalog Number",
            "Release Date", "Track Title", "Track Position", "Duration", "Producer", "Remixer"
        ])

    existing_ids = set(existing_data["release_id"].dropna())

    # 🔄 Get Discogs IDs
    discogs_ids = fetch_all_discogs_release_ids()

    # Compare
    ids_to_remove = existing_ids - discogs_ids
    ids_to_add = discogs_ids - existing_ids

    print("\n========== 🔄 SYNC SUMMARY ==========")
    print(f"🗑️ Releases to remove: {len(ids_to_remove)}")
    print(f"🆕 Releases to add: {len(ids_to_add)}")
    print("====================================\n")

    # 🗑️ REMOVE
    if ids_to_remove:
        print("🗑️ Removing releases:")
        for rid in ids_to_remove:
            title = fetch_release_title(rid)
            print(f"   ❌ {title} ({rid})")

        existing_data = existing_data[~existing_data["release_id"].isin(ids_to_remove)]
    else:
        print("🗑️ No releases to remove.")

    # ➕ ADD
    new_rows = []

    if ids_to_add:
        print("\n🆕 Adding releases:")

        page = 1
        while True:
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

                if release_id not in ids_to_add:
                    continue

                title = item.get("basic_information", {}).get("title", "Unknown")

                formats = item.get("basic_information", {}).get("formats", [])
                if not is_vinyl_format(formats):
                    print(f"⏭️ Skipping {title} ({release_id}) – not vinyl/12\"")
                    continue

                print(f"   ➕ {title} ({release_id})")

                track_rows = fetch_release_tracks(release_id)

                if track_rows:
                    new_rows.extend(track_rows)
                else:
                    print(f"   ⚠️ No tracklist found for {title}")

            if page >= data["pagination"]["pages"]:
                break
            page += 1

    else:
        print("\n🆕 No new releases to add.")

    # Merge
    if new_rows:
        new_df = pd.DataFrame(new_rows)
        existing_data = pd.concat([existing_data, new_df], ignore_index=True)

    # Clean
    existing_data = existing_data[
        existing_data["Track Title"].notna() &
        (existing_data["Track Title"].str.lower() != "none")
    ]

    print("\n========== ✅ FINAL ==========")
    print(f"🗂️ Total rows: {len(existing_data)}")
    print("================================\n")

    existing_data.to_csv(EXISTING_CSV_PATH, index=False)


if __name__ == "__main__":
    main()
