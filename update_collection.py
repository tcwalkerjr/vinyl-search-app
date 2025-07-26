import pandas as pd
import os

def load_data(file_path):
    return pd.read_csv(file_path)

def filter_12_inch_vinyl(df):
    # Ensure the CollectionFolder column exists
    if "CollectionFolder" in df.columns:
        df = df[df["CollectionFolder"].str.contains("12", case=False, na=False)]
    return df

def merge_new_tracks(existing_df, new_df):
    # Ensure expected columns exist in existing_df
    for col in ["release_id", "Track Title"]:
        if col not in existing_df.columns:
            existing_df[col] = ""

    existing_keys = set((row["release_id"], row["Track Title"]) for _, row in existing_df.iterrows())
    new_rows = new_df[~new_df.apply(lambda row: (row["release_id"], row["Track Title"]) in existing_keys, axis=1)]
    return pd.concat([existing_df, new_rows], ignore_index=True)

def main():
    new_data_path = "tracklist_final_cleaned.csv"
    existing_path = "merged_12inch_records_only.csv"

    new_data = load_data(new_data_path)
    new_data = filter_12_inch_vinyl(new_data)

    if os.path.exists(existing_path):
        existing_data = pd.read_csv(existing_path)
    else:
        existing_data = pd.DataFrame(columns=new_data.columns)

    print("Columns in existing data:", existing_data.columns.tolist())

    merged = merge_new_tracks(existing_data, new_data)
    merged.to_csv(existing_path, index=False)
    print(f"Merged data saved to {existing_path}")

if __name__ == "__main__":
    main()
