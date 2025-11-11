import requests
import csv
from pathlib import Path
from datetime import datetime

url = "https://wax.greymass.com/v1/chain/get_table_rows"
payload = {
    "code": "hq.mu",
    "scope": "hq.mu",
    "table": "minepooldata",
    "limit": 1000,
    "reverse": True,
    "json": True
}

csv_file = "minepooldata.csv"
file_exists = Path(csv_file).exists()

try:
    response = requests.post(url, json=payload, timeout=10, headers={"User-Agent": "TLM_Pools Data Collector"})
    response.raise_for_status()
    data = response.json().get("rows", [])

    if not data:
        print("No data found in API response.")
        exit()

    processed_data = []
    for row in data:
        if not all(key in row for key in ["snapshot_id", "snapshot_date", "pool_buckets"]):
            print(f"Skipping row with missing fields: {row}")
            continue

        timestamp = row.get("snapshot_date")
        try:
            date_obj = datetime.fromisoformat(timestamp.replace("T", " "))
            date_str = date_obj.strftime("%Y-%m-%d %H:%M:%S")
        except (ValueError, TypeError, AttributeError) as e:
            print(f"Invalid timestamp format in row {row['snapshot_id']}: {e}")
            date_str = "Invalid timestamp"

        processed_data.append({
            "id": str(row["snapshot_id"]),
            "date": date_str,
            "pool": row["pool_buckets"],
            "raw_timestamp": timestamp
        })


    if processed_data:
        fieldnames = ["id", "date", "pool", "raw_timestamp"]

        existing_ids = set()
        if file_exists:
            try:
                with open(csv_file, "r") as f:
                    reader = csv.DictReader(f)
                    if not reader.fieldnames or "id" not in reader.fieldnames:
                        print("CSV hat keine ID-Spalte, überspringe Duplikatsprüfung")
                    else:
                        existing_ids = {str(row["id"]) for row in reader}
            except (csv.Error, IOError) as e:
                print(f"CSV-Lesefehler: {e}")
                existing_ids = set()

        new_rows = [row for row in processed_data if row["id"] not in existing_ids]

        if new_rows:
            try:
                with open(csv_file, "a" if file_exists else "w", newline="", encoding="utf-8") as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    if not file_exists:
                        writer.writeheader()
                    writer.writerows(new_rows)
                print(f"Added {len(new_rows)} new entries. Total stored: {len(existing_ids) + len(new_rows)}")
            except IOError as e:
                print(f"File write error: {e}")
        else:
            print(f"No new entries found. Existing entries: {len(existing_ids)}")

except requests.exceptions.RequestException as e:
    print(f"Network error occurred: {str(e)}")
except KeyError as e:
    print(f"Critical data structure error: Missing key {e}")
except Exception as e:
    print(f"Unexpected error: {str(e)}")