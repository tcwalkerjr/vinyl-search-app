print("🔥 VINYL SYNC — INCREMENTAL + BACKFILL MODE (v6) 🔥")

import requests
import pandas as pd
import os
import time
import re
from datetime import datetime, timezone

DISCOGS_USER = os.getenv("DISCOGS_USER")
DISCOGS_TOKEN = os.getenv("DISCOGS_TOKEN")

EXISTING_CSV_PATH = "merged_12inch_records_only.csv"
LAST_RUN_FILE = "last_run.txt"

BASE_URL = f"https://api.discogs.com/users/{DISCOGS_USER}/collection/folders/0/releases"

HEADERS = {
    "User-Agent": "vinyl-search-app/6.0"
}

release_cache = {}

# ================================
# 📅 LAST RUN
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
# 🧼 HELPERS
# ================================
def is_blank(value):
    return pd.isna(value) or str(value).strip() == ""


def split_names(raw_name):
    if not raw_name:
        return []

    parts = re.split(r",|&|/| vs\. | feat\. | featuring | and ", raw_name, flags=re.IGNORECASE)
    return [p.strip() for p in parts if p.strip()]


def canonicalize_name(name):
    if not name:
        return ""

    if "," in name:
        parts = [p.strip() for p in name.split(",")]
        if len(parts) == 2:
            name = f"{parts[1]} {parts[0]}"

    return re.sub(r"\s+", " ", name).strip()


def normalize_names(raw_name):
    names = split_names(raw_name)
    clean = set()

    for name in names:
        canon = canonicalize_name(name)
        if canon and len(canon) > 1:
            clean.add(canon)

    return sorted(clean)


# ================================
# 🎯 DETECTORS
# ================================
def detect_mix_type(title):
    t = title.lower()

    if "dub" in t:
        return "Dub"
    if "instrumental" in t:
        return "Instrumental"
    if "radio" in t:
        return "Radio Edit"
    if "club" in t:
        return "Club Mix"
    if "extended" in t:
        return "Extended Mix"
    if "edit" in t:
        return "Edit"
    if "remix" in t:
        return "Remix"

    return ""


def extract_remixers(track, release_data):
    remixers = set()

    for artist in track.get("extraartists", []):
        if "remix" in artist.get("role", "").lower():
            for n in normalize_names(artist.get("name", "")):
                remixers.add(n)

    for artist in release_data.get("extraartists", []):
        if "remix" in artist.get("role", "").lower():
            for n in normalize_names(artist.get("name", "")):
                remixers.add(n)

    matches = re.findall(r"\((.*?)\)", track.get("title", ""))

    for match in matches:
        if any(x in match.lower() for x in ["mix", "remix", "edit", "dub"]):
            cleaned = re.sub(r"\b(mix|remix|edit|version|dub)\b", "", match, flags=re.IGNORECASE)
            for n in normalize_names(cleaned):
                remixers.add(n)

    return ", ".join(sorted(remixers))


def extract_producers(track, release_data):
    producers = set()

    for artist in track.get("extraartists", []):
        if "produc" in artist.get("role", "").lower():
            for n in normalize_names(artist.get("name", "")):
                producers.add(n)

    for artist in release_data.get("extraartists", []):
        if "produc" in artist.get("role", "").lower():
            for n in normalize_names(artist.get("name", "")):
                producers.add(n)

    return ", ".join(sorted(producers))


def is_vinyl_format(formats):
    for fmt in formats:
        combined = " ".join([fmt.get("name", "")] + fmt.get("descriptions", [])).lower()
        if any(x in combined for x in ["vinyl", '12"', "lp", "ep", "single"]):
            return True
    return False


def looks_like_vinyl_track(position):
    if not position:
        return False
    pos = str(position).upper()
    return pos.startswith(("A", "B", "C", "D")) or pos in ["A", "B"]


# ================================
# 🌐 FETCH
# ================================
def fetch_release_data(release_id):
    if release_id in release_cache:
        return release_cache[release_id]

    url = f"https://api.discogs.com/releases/{release_id}"

    r = requests.get(url, headers=HEADERS, params={"token": DISCOGS_TOKEN})
    time.sleep(0.5)

    if r.status_code != 200:
        return None

    data = r.json()
    release_cache[release_id] = data
    return data


def fetch_new_releases(last_run):
    releases = []
    page = 1

    while True:
        print(f"📄 Page {page}")

        r = requests.get(BASE_URL, headers=HEADERS, params={
            "page": page,
            "token": DISCOGS_TOKEN,
            "sort": "added",
            "sort_order": "desc"
        })

        time.sleep(0.5)
        data = r.json()

        stop = False

        for item in data["releases"]:
            added = item.get("date_added")
            if not added:
                continue

            added_dt = datetime.fromisoformat(added.replace("Z", "+00:00"))

            if added_dt <= last_run:
                stop = True
                break

            info = item["basic_information"]

            if not is_vinyl_format(info.get("formats", [])):
                continue

            releases.append({
                "id": str(info["id"]),
                "title": info.get("title", "")
            })

        if stop or page >= data["pagination"]["pages"]:
            break

        page += 1

    print(f"🆕 Found {len(releases)} new releases")
    return releases


# ================================
# 🎵 TRACK PARSE
# ================================
def build_rows(release_id):
    data = fetch_release_data(release_id)
    if not data:
        return []

    rows = []

    for track in data.get("tracklist", []):
        title = track.get("title", "").strip()
        pos = track.get("position", "")

        if not title or not looks_like_vinyl_track(pos):
            continue

        rows.append({
            "release_id": release_id,
            "Artist": ", ".join(a["name"] for a in data.get("artists", [])),
            "Album Title": data.get("title", ""),
            "Label": ", ".join(l["name"] for l in data.get("labels", [])),
            "Catalog Number": ", ".join(l.get("catno", "") for l in data.get("labels", [])),
            "Release Date": data.get("year", ""),
            "Track Title": title,
            "Track Position": pos,
            "Duration": track.get("duration", ""),
            "Producer": extract_producers(track, data),
            "Remixer": extract_remixers(track, data),
            "Mix Type": detect_mix_type(title)
        })

    return rows


# ================================
# 🔧 BACKFILL
# ================================
def backfill(df):
    print("🔧 Backfilling missing fields...")

    for i, row in df.iterrows():
        if not (is_blank(row["Producer"]) or is_blank(row["Remixer"]) or is_blank(row["Mix Type"])):
            continue

        data = fetch_release_data(row["release_id"])
        if not data:
            continue

        for track in data.get("tracklist", []):
            if track.get("title", "").strip() == row["Track Title"]:

                if is_blank(row["Producer"]):
                    df.at[i, "Producer"] = extract_producers(track, data)

                if is_blank(row["Remixer"]):
                    df.at[i, "Remixer"] = extract_remixers(track, data)

                if is_blank(row["Mix Type"]):
                    df.at[i, "Mix Type"] = detect_mix_type(track.get("title", ""))

                break

    return df


# ================================
# 🚀 MAIN
# ================================
def main():
    last_run = get_last_run_date()
    print(f"⏱️ Last run: {last_run}")

    if os.path.exists(EXISTING_CSV_PATH):
        df = pd.read_csv(EXISTING_CSV_PATH, dtype={"release_id": str})
    else:
        df = pd.DataFrame()

    existing_ids = set(df.get("release_id", []))

    new_releases = fetch_new_releases(last_run)

    new_rows = []
    for r in new_releases:
        if r["id"] not in existing_ids:
            new_rows.extend(build_rows(r["id"]))

    if new_rows:
        df = pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)

    df = backfill(df)

    df.to_csv(EXISTING_CSV_PATH, index=False)
    save_last_run_date()

    print("✅ Done")


if __name__ == "__main__":
    main()
