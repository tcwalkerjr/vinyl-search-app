import os
import requests
import pandas as pd

DISCOGS_USERNAME = os.getenv("DISCOGS_USERNAME")
DISCOGS_TOKEN = os.getenv("DISCOGS_USER_TOKEN")
HEADERS = {"Authorization": f"Discogs token={DISCOGS_TOKEN}"}

EXISTING_FILE = "merged_12inch_records_only.csv"
PER_PAGE = 100

def fetch_collection(username):
    releases = []
    page = 1
    while True:
        url = f"https://api.discogs.com/users/{username}/collection/folders/0/releases"
        params = {"per_page": PER_PAGE, "page": page}
        response = requests.get(url, headers=HEADERS, params=params)
        if response.status_code != 200:
            print(f"Failed to fetch page {page}: {response.status_code}")
            break
        data = response.json()
        releases.extend(data.get("releases", []))
        if page >= data.get("pagination", {}).get("pages", 0):
            break
        page += 1
    return releases

def extract_record_info(item):
    basic = item["basic_information"]
    release_id = basic["id"]
    formats = basic.get("formats", [])
    if not any("12"" in fmt.get("descriptions", []) for fmt in formats):
        return None
    title = basic.get("title", "")
    artists = ", ".join(a.get("name", "") for a in basic.get("artists", []))
    labels = ", ".join(l.get("name", "") for l in basic.get("labels", []))
    catno = ", ".join(l.get("catno", "") for l in basic.get("labels", []))
    year = basic.get("year", "")
    return {
        "release_id": release_id,
        "Artist": artists,
        "Album Title": title,
        "Release Date": year,
        "Label": labels,
        "Catalog Number": catno
    }

def main():
    existing_df = pd.read_csv(EXISTING_FILE)
    if "release_id" not in existing_df.columns:
        print("ERROR: 'release_id' column missing in existing CSV.")
        return

    releases = fetch_collection(DISCOGS_USERNAME)
    new_records = []
    existing_ids = set(existing_df["release_id"].astype(str))

    for item in releases:
        info = extract_record_info(item)
        if info and str(info["release_id"]) not in existing_ids:
            new_records.append(info)

    if new_records:
        new_df = pd.DataFrame(new_records)
        updated_df = pd.concat([existing_df, new_df], ignore_index=True)
        updated_df.to_csv(EXISTING_FILE, index=False)
        print(f"Added {len(new_df)} new records.")
    else:
        print("No new records to add.")

if __name__ == "__main__":
    main()
