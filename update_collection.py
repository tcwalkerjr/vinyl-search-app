on:
  schedule:
    - cron: '0 3 * * 1'  # Runs every Monday at 03:00 UTC
  workflow_dispatch:

jobs:
  update-collection:
    runs-on: ubuntu-latest

    steps:
      - name: Check out repository
        uses: actions/checkout@v3

      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.10'

      - name: Install dependencies
        run: pip install -r requirements.txt

      - name: Run update script
        env:
          DISCOGS_USER: ${{ secrets.DISCOGS_USER }}
          DISCOGS_TOKEN: ${{ secrets.DISCOGS_TOKEN }}
        run: |
          python update_collection.py

      - name: Commit updated CSV
        uses: stefanzweifel/git-auto-commit-action@v5
        with:
          file_pattern: merged_12inch_records_only.csv
          commit_message: "🔄 Auto-update vinyl collection"
          commit_user_name: github-actions[bot]
          commit_user_email: 41898282+github-actions[bot]@users.noreply.github.com
          commit_author: tcwalkerjr <88843202+tcwalkerjr@users.noreply.github.com>
