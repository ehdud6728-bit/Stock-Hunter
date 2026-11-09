"""Fetch only the two public, pinned holdout price files."""

import hashlib
import json
from pathlib import Path
from urllib.request import urlretrieve

HERE = Path(__file__).resolve().parent
COMMIT = "50f4d1a74e87c720a03e576218036b1b8414afa1"


def main():
    folder = HERE / "inputs"
    folder.mkdir(exist_ok=True)
    hashes = json.loads((HERE/"reference/FROZEN_SHA256.json").read_text())["inputs"]
    for year in (2025, 2026):
        name = f"marcap-{year}.parquet"
        dest = folder/name
        if dest.exists() and hashlib.sha256(dest.read_bytes()).hexdigest() == hashes[name]:
            continue
        staging = dest.with_suffix(".download")
        urlretrieve(f"https://raw.githubusercontent.com/FinanceData/marcap/{COMMIT}/data/{name}", staging)
        if hashlib.sha256(staging.read_bytes()).hexdigest() != hashes[name]:
            raise ValueError(f"Incorrect holdout digest: {name}")
        staging.replace(dest)
        print(f"Verified {name}")


if __name__ == "__main__":
    main()
