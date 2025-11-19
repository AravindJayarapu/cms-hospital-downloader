import os
import csv
import re
import json
import requests
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

METASTORE_URL = "https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items"
RUN_METADATA = "run_metadata.json"
DOWNLOAD_DIR = "cms_hospital_data"
RELEVANT_THEME = "Hospitals"

def load_run_metadata():
    if os.path.exists(RUN_METADATA):
        with open(RUN_METADATA, 'r') as f:
            return json.load(f)
    else:
        return {"last_run": None, "datasets": {}}

def save_run_metadata(metadata):
    with open(RUN_METADATA, 'w') as f:
        json.dump(metadata, f, indent=2)

def snake_case(s):
    s = re.sub(r"[’'`]", '', s)
    s = re.sub(r"[^A-Za-z0-9 ]+", ' ', s)
    s = re.sub(r"\s+", ' ', s.strip())
    return s.lower().replace(" ", "_")

def get_hospital_datasets():
    resp = requests.get(METASTORE_URL)
    resp.raise_for_status()
    items = resp.json()  # List structure confirmed by actual endpoint
    hospital_datasets = []
    for item in items:
        # Defensive checks for optional fields
        themes = item.get("theme", [])
        if RELEVANT_THEME not in themes:
            continue
        distributions = item.get("distribution", [])
        csv_url = None
        for dist in distributions:
            if dist.get("mediaType") == "text/csv":
                csv_url = dist.get("downloadURL")
                break
        if not csv_url:
            continue
        hospital_datasets.append({
            "title": item.get("title", "untitled"),
            "id": item.get("uniqueIdentifier", ""),
            "lastModified": item.get("modified", item.get("created")),
            "downloadUrl": csv_url
        })
    return hospital_datasets

def download_and_process_dataset(dataset, run_metadata):
    file_name = f"{snake_case(dataset['title'])}_{dataset['id']}.csv"
    out_path = os.path.join(DOWNLOAD_DIR, file_name)
    modified = dataset["lastModified"]
    dsid = dataset["id"]

    # Only download if modified since last run
    if run_metadata["datasets"].get(dsid) == modified:
        print(f"No update for: {dataset['title']}")
        return None

    try:
        resp = requests.get(dataset["downloadUrl"])
        resp.raise_for_status()
        lines = resp.content.decode('utf-8').splitlines()
        reader = csv.reader(lines)
        headers = next(reader)
        new_headers = [snake_case(h) for h in headers]
        rows = list(reader)

        os.makedirs(DOWNLOAD_DIR, exist_ok=True)
        with open(out_path, 'w', newline='', encoding='utf-8') as wf:
            writer = csv.writer(wf)
            writer.writerow(new_headers)
            writer.writerows(rows)

        print(f"Saved {out_path}")
        return dsid, modified
    except Exception as e:
        print(f"Error downloading {dataset['title']}: {e}")
        return None

def main():
    run_metadata = load_run_metadata()
    datasets = get_hospital_datasets()
    results = []
    with ThreadPoolExecutor(max_workers=5) as pool:
        futures = []
        for dataset in datasets:
            futures.append(pool.submit(
                download_and_process_dataset,
                dataset,
                run_metadata
            ))
        for f in futures:
            result = f.result()
            if result:
                results.append(result)

    # Update run metadata
    for dsid, modified in results:
        run_metadata["datasets"][dsid] = modified
    run_metadata["last_run"] = datetime.utcnow().isoformat()
    save_run_metadata(run_metadata)

if __name__ == "__main__":
    main()
