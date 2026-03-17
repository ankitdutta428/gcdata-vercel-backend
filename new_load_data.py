# load_data.py  –  Create tables and load all CSVs into PostgreSQL.

import os, csv, sys
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from pathlib import Path
from tqdm import tqdm

load_dotenv()

DATA_DIR = Path(__file__).parent / "Dataset-GCData"

def get_conn():
    return psycopg2.connect(
        dbname=os.environ["DB_NAME"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        host=os.environ["DB_HOST"],
        port=os.environ["DB_PORT"],
    )

def clean_str(val):
    return str(val).strip().strip('"') if val and str(val).strip() else None

def load_csv(path):
    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    return rows

# ── THE FIX: This function ignores spaces, caps, and typos in your CSV headers ──
def get_field(r, *possible_names):
    cleaned_row = {str(k).strip().lower(): v for k, v in r.items() if k is not None}
    for name in possible_names:
        if name.lower() in cleaned_row:
            return cleaned_row[name.lower()]
    return None

DDL = """
CREATE TABLE IF NOT EXISTS video_list_data_synthesized (
    headline TEXT, source TEXT, published TEXT, team_name TEXT, type TEXT, uploaded_by TEXT, video_id TEXT, published_platform TEXT, published_url TEXT, language TEXT, channel TEXT, duration_s NUMERIC, created_ts TIMESTAMP, published_ts TIMESTAMP
);
"""

def load_video_list_synthesized(cur):
    path = DATA_DIR / "video_list_data_synthesized_15th_MAR.csv"
    rows = load_csv(path)
    cur.execute("TRUNCATE video_list_data_synthesized") # Wipes the NULLs

    def parse_ts(val):
        if not val or not str(val).strip(): return None
        from datetime import datetime
        for fmt in ("%d/%m/%Y %H:%M:%S", "%d/%m/%Y", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try: return datetime.strptime(str(val).strip(), fmt)
            except ValueError: continue
        return None

    data_to_insert = []
    for r in rows:
        raw_duration = get_field(r, "duration_s", "duration (s)", "duration")
        duration_val = float(raw_duration) if raw_duration and str(raw_duration).strip() else None

        data_to_insert.append((
            clean_str(get_field(r, "headline")),
            clean_str(get_field(r, "source")),
            clean_str(get_field(r, "published")),
            clean_str(get_field(r, "team_name", "team name")),
            clean_str(get_field(r, "type")),
            clean_str(get_field(r, "uploaded_by", "uploaded by")),
            clean_str(get_field(r, "video_id", "video id")),
            clean_str(get_field(r, "published_platform", "published platform")),
            clean_str(get_field(r, "published_url", "published url")),
            clean_str(get_field(r, "language")),
            clean_str(get_field(r, "channel")),
            duration_val,
            parse_ts(get_field(r, "created_ts", "processed/created timestamp", "created timestamp")),
            parse_ts(get_field(r, "published_ts", "published timestamp")),
        ))

    query = """INSERT INTO video_list_data_synthesized
               (headline, source, published, team_name, type, uploaded_by,
                video_id, published_platform, published_url,
                language, channel, duration_s, created_ts, published_ts)
               VALUES %s"""

    batch_size = 500
    print(f"\n  🚀 Batch inserting {len(data_to_insert)} rows into video_list_data_synthesized...")
    for i in tqdm(range(0, len(data_to_insert), batch_size), desc="Uploading", unit="chunk"):
        execute_values(cur, query, data_to_insert[i:i + batch_size])
    print(f"  ✅ Data loaded successfully!\n")

def main():
    print("\n🔌 Connecting to PostgreSQL...")
    conn = get_conn()
    with conn:
        with conn.cursor() as cur:
            cur.execute(DDL)
            load_video_list_synthesized(cur)
        conn.commit()
    conn.close()
    print("🎉 Done!")

if __name__ == "__main__":
    main()