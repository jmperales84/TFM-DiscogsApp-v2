import json
from discogs_download_json import DiscogsDownloader
import os
from pathlib import Path
import sys

def setup_directories(paths: dict):
    """
    Create required folders if missing:
    - raw/albums
    - landing
    """
    # Create raw/albums and landing if they do not exist
    raw_dir = paths["raw"]
    raw_albums_dir = os.path.join(raw_dir, "albums")

    dirs_to_create = [paths["landing"], raw_albums_dir]

    for path_str in dirs_to_create:
        path = Path(path_str)
        path.mkdir(parents=True, exist_ok=True)
        print(f"[✓] Directory checked or created: {path}")

def main(config: dict) -> None:
    """
    Run the downloader with the provided config.
    """
    setup_directories(config["paths"])

    downloader = DiscogsDownloader(config)
    downloader.download_albums()
    downloader.print_albums()

if __name__ == "__main__":

    # Read config file
    with open("config.json", "r", encoding="utf-8") as f:
        config = json.load(f)

    # Quick check for the required token
    if not os.getenv("DISCOGS_TOKEN"):
        print("ERROR: DISCOGS_TOKEN is not set in the environment.")
        sys.exit(1)

    main(config)
