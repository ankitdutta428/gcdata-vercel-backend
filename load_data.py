"""
load_data.py  –  Create tables and load all CSVs into PostgreSQL.

Run:  python load_data.py

Reads DB credentials from .env (DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT).
Duration columns stored as INTERVAL (converted from hh:mm:ss strings automatically).
"""

import os, csv, sys
import psycopg2
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

DATA_DIR = Path(__file__).parent / "Dataset-GCData"

# ── helpers ──────────────────────────────────────────────────────────────────

def get_conn():
    return psycopg2.connect(
        dbname=os.environ["DB_NAME"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        host=os.environ["DB_HOST"],
        port=os.environ["DB_PORT"],
    )

def hms_to_seconds(val: str) -> float:
    """Convert 'hh:mm:ss' or 'h:mm:ss' string to total seconds (float)."""
    if not val or val.strip() == "":
        return 0.0
    parts = val.strip().split(":")
    try:
        if len(parts) == 3:
            h, m, s = parts
            return int(h) * 3600 + int(m) * 60 + float(s)
        elif len(parts) == 2:
            m, s = parts
            return int(m) * 60 + float(s)
    except Exception:
        pass
    return 0.0

def clean_int(val):
    try:
        return int(str(val).strip().replace(",", "").replace('"', ""))
    except Exception:
        return 0

def clean_str(val):
    return str(val).strip().strip('"') if val else None

def load_csv(path):
    """Read CSV and return (headers, rows) with stripped values."""
    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    return rows

def run_ddl(cur, sql):
    cur.execute(sql)

# ── DDL ──────────────────────────────────────────────────────────────────────

DDL = """
-- 1. channel_and_user  (CLIENT 1 combined_data... by channel and user)
CREATE TABLE IF NOT EXISTS channel_and_user (
    channel           TEXT,
    "user"            TEXT,
    uploaded_count    INT,
    created_count     INT,
    published_count   INT,
    uploaded_duration NUMERIC,   -- stored in seconds
    created_duration  NUMERIC,
    published_duration NUMERIC
);

-- 2. input_type
CREATE TABLE IF NOT EXISTS input_type (
    input_type         TEXT,
    uploaded_count     INT,
    created_count      INT,
    published_count    INT,
    uploaded_duration  NUMERIC,
    created_duration   NUMERIC,
    published_duration NUMERIC
);

-- 3. output_type
CREATE TABLE IF NOT EXISTS output_type (
    output_type        TEXT,
    uploaded_count     INT,
    created_count      INT,
    published_count    INT,
    uploaded_duration  NUMERIC,
    created_duration   NUMERIC,
    published_duration NUMERIC
);

-- 4. language
CREATE TABLE IF NOT EXISTS language (
    language           TEXT,
    uploaded_count     INT,
    created_count      INT,
    published_count    INT,
    uploaded_duration  NUMERIC,
    created_duration   NUMERIC,
    published_duration NUMERIC
);

-- 5. channel_wise_publishing
CREATE TABLE IF NOT EXISTS channel_wise_publishing (
    channel    TEXT,
    facebook   INT,
    instagram  INT,
    linkedin   INT,
    reels      INT,
    shorts     INT,
    x          INT,
    youtube    INT,
    threads    INT
);

-- 6. channel_wise_publishing_duration
CREATE TABLE IF NOT EXISTS channel_wise_publishing_duration (
    channel            TEXT,
    facebook_duration  NUMERIC,
    instagram_duration NUMERIC,
    linkedin_duration  NUMERIC,
    reels_duration     NUMERIC,
    shorts_duration    NUMERIC,
    x_duration         NUMERIC,
    youtube_duration   NUMERIC,
    threads_duration   NUMERIC
);

-- 7. month_wise_duration
CREATE TABLE IF NOT EXISTS month_wise_duration (
    month                   TEXT,
    total_uploaded_duration NUMERIC,
    total_created_duration  NUMERIC,
    total_published_duration NUMERIC
);

-- 8. monthly_chart
CREATE TABLE IF NOT EXISTS monthly_chart (
    month          TEXT,
    total_uploaded INT,
    total_created  INT,
    total_published INT
);

-- 9. video_list_data_obfuscated
CREATE TABLE IF NOT EXISTS video_list_data_obfuscated (
    headline           TEXT,
    source             TEXT,
    published          TEXT,
    team_name          TEXT,
    type               TEXT,
    uploaded_by        TEXT,
    video_id           TEXT,
    published_platform TEXT,
    published_url      TEXT
);

-- 10. video_list_data_synthesized (per-video row with timestamps)
CREATE TABLE IF NOT EXISTS video_list_data_synthesized (
    headline           TEXT,
    source             TEXT,
    published          TEXT,
    team_name          TEXT,
    type               TEXT,
    uploaded_by        TEXT,
    video_id           TEXT,
    published_platform TEXT,
    published_url      TEXT,
    language           TEXT,
    channel            TEXT,
    duration_s         NUMERIC,
    created_ts         TIMESTAMP,
    published_ts       TIMESTAMP
);
"""

# ── Loaders ──────────────────────────────────────────────────────────────────

def load_channel_and_user(cur):
    """combined_data... by channel and user (1).csv  — has Channel + User"""
    path = DATA_DIR / "combined_data(2025-3-1-2026-2-28) by channel and user (1).csv"
    rows = load_csv(path)
    cur.execute("TRUNCATE channel_and_user")
    for r in rows:
        cur.execute(
            """INSERT INTO channel_and_user
               (channel, "user", uploaded_count, created_count, published_count,
                uploaded_duration, created_duration, published_duration)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
            (
                clean_str(r.get("Channel")),
                clean_str(r.get("User")),
                clean_int(r.get("Uploaded Count", 0)),
                clean_int(r.get("Created Count", 0)),
                clean_int(r.get("Published Count", 0)),
                hms_to_seconds(r.get("Uploaded Duration (hh:mm:ss)", "")),
                hms_to_seconds(r.get("Created Duration (hh:mm:ss)", "")),
                hms_to_seconds(r.get("Published Duration (hh:mm:ss)", "")),
            )
        )
    print(f"  ✅ channel_and_user  → {len(rows)} rows")


def load_input_type(cur):
    path = DATA_DIR / "combined_data(2025-3-1-2026-2-28) by input type.csv"
    rows = load_csv(path)
    cur.execute("TRUNCATE input_type")
    for r in rows:
        cur.execute(
            """INSERT INTO input_type
               (input_type, uploaded_count, created_count, published_count,
                uploaded_duration, created_duration, published_duration)
               VALUES (%s,%s,%s,%s,%s,%s,%s)""",
            (
                clean_str(r.get("Input Type")),
                clean_int(r.get("Uploaded Count", 0)),
                clean_int(r.get("Created Count", 0)),
                clean_int(r.get("Published Count", 0)),
                hms_to_seconds(r.get("Uploaded Duration (hh:mm:ss)", "")),
                hms_to_seconds(r.get("Created Duration (hh:mm:ss)", "")),
                hms_to_seconds(r.get("Published Duration (hh:mm:ss)", "")),
            )
        )
    print(f"  ✅ input_type        → {len(rows)} rows")


def load_output_type(cur):
    path = DATA_DIR / "combined_data(2025-3-1-2026-2-28) by output type.csv"
    rows = load_csv(path)
    cur.execute("TRUNCATE output_type")
    for r in rows:
        cur.execute(
            """INSERT INTO output_type
               (output_type, uploaded_count, created_count, published_count,
                uploaded_duration, created_duration, published_duration)
               VALUES (%s,%s,%s,%s,%s,%s,%s)""",
            (
                clean_str(r.get("Output Type")),
                clean_int(r.get("Uploaded Count", 0)),
                clean_int(r.get("Created Count", 0)),
                clean_int(r.get("Published Count", 0)),
                hms_to_seconds(r.get("Uploaded Duration (hh:mm:ss)", "")),
                hms_to_seconds(r.get("Created Duration (hh:mm:ss)", "")),
                hms_to_seconds(r.get("Published Duration (hh:mm:ss)", "")),
            )
        )
    print(f"  ✅ output_type       → {len(rows)} rows")


def load_language(cur):
    path = DATA_DIR / "combined_data(2025-3-1-2026-2-28) by language.csv"
    rows = load_csv(path)
    cur.execute("TRUNCATE language")
    for r in rows:
        cur.execute(
            """INSERT INTO language
               (language, uploaded_count, created_count, published_count,
                uploaded_duration, created_duration, published_duration)
               VALUES (%s,%s,%s,%s,%s,%s,%s)""",
            (
                clean_str(r.get("Language")),
                clean_int(r.get("Uploaded Count", 0)),
                clean_int(r.get("Created Count", 0)),
                clean_int(r.get("Published Count", 0)),
                hms_to_seconds(r.get("Uploaded Duration (hh:mm:ss)", "")),
                hms_to_seconds(r.get("Created Duration (hh:mm:ss)", "")),
                hms_to_seconds(r.get("Published Duration (hh:mm:ss)", "")),
            )
        )
    print(f"  ✅ language          → {len(rows)} rows")


def load_channel_wise_publishing(cur):
    path = DATA_DIR / "channel-wise-publishing.csv"
    rows = load_csv(path)
    cur.execute("TRUNCATE channel_wise_publishing")
    for r in rows:
        cur.execute(
            """INSERT INTO channel_wise_publishing
               (channel, facebook, instagram, linkedin, reels, shorts, x, youtube, threads)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (
                clean_str(r.get("Channels")),
                clean_int(r.get("Facebook", 0)),
                clean_int(r.get("Instagram", 0)),
                clean_int(r.get("Linkedin", 0)),
                clean_int(r.get("Reels", 0)),
                clean_int(r.get("Shorts", 0)),
                clean_int(r.get("X", 0)),
                clean_int(r.get("Youtube", 0)),
                clean_int(r.get("Threads", 0)),
            )
        )
    print(f"  ✅ channel_wise_publishing         → {len(rows)} rows")


def load_channel_wise_publishing_duration(cur):
    path = DATA_DIR / "channel-wise-publishing duration.csv"
    rows = load_csv(path)
    cur.execute("TRUNCATE channel_wise_publishing_duration")
    for r in rows:
        cur.execute(
            """INSERT INTO channel_wise_publishing_duration
               (channel, facebook_duration, instagram_duration, linkedin_duration,
                reels_duration, shorts_duration, x_duration, youtube_duration, threads_duration)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (
                clean_str(r.get("Channels")),
                hms_to_seconds(r.get("Facebook Duration", "")),
                hms_to_seconds(r.get("Instagram Duration", "")),
                hms_to_seconds(r.get("Linkedin Duration", "")),
                hms_to_seconds(r.get("Reels Duration", "")),
                hms_to_seconds(r.get("Shorts Duration", "")),
                hms_to_seconds(r.get("X Duration", "")),
                hms_to_seconds(r.get("Youtube Duration", "")),
                hms_to_seconds(r.get("Threads Duration", "")),
            )
        )
    print(f"  ✅ channel_wise_publishing_duration → {len(rows)} rows")


def load_month_wise_duration(cur):
    path = DATA_DIR / "month-wise-duration.csv"
    rows = load_csv(path)
    cur.execute("TRUNCATE month_wise_duration")
    for r in rows:
        cur.execute(
            """INSERT INTO month_wise_duration
               (month, total_uploaded_duration, total_created_duration, total_published_duration)
               VALUES (%s,%s,%s,%s)""",
            (
                clean_str(r.get("Month")),
                hms_to_seconds(r.get("Total Uploaded Duration", "")),
                hms_to_seconds(r.get("Total Created Duration", "")),
                hms_to_seconds(r.get("Total Published Duration", "")),
            )
        )
    print(f"  ✅ month_wise_duration → {len(rows)} rows")


def load_monthly_chart(cur):
    path = DATA_DIR / "monthly-chart.csv"
    rows = load_csv(path)
    cur.execute("TRUNCATE monthly_chart")
    for r in rows:
        cur.execute(
            """INSERT INTO monthly_chart (month, total_uploaded, total_created, total_published)
               VALUES (%s,%s,%s,%s)""",
            (
                clean_str(r.get("Month")),
                clean_int(r.get("Total Uploaded", 0)),
                clean_int(r.get("Total Created", 0)),
                clean_int(r.get("Total Published", 0)),
            )
        )
    print(f"  ✅ monthly_chart      → {len(rows)} rows")


def load_video_list(cur):
    path = DATA_DIR / "video_list_data_obfuscated.csv"
    rows = load_csv(path)
    cur.execute("TRUNCATE video_list_data_obfuscated")
    for r in rows:
        cur.execute(
            """INSERT INTO video_list_data_obfuscated
               (headline, source, published, team_name, type,
                uploaded_by, video_id, published_platform, published_url)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (
                clean_str(r.get("Headline")),
                clean_str(r.get("Source")),
                clean_str(r.get("Published")),
                clean_str(r.get("Team Name")),
                clean_str(r.get("Type")),
                clean_str(r.get("Uploaded By")),
                clean_str(r.get("Video ID")),
                clean_str(r.get("Published Platform")),
                clean_str(r.get("Published URL")),
            )
        )
    print(f"  ✅ video_list_data_obfuscated → {len(rows)} rows")


def load_video_list_synthesized(cur):
    """video_list_data_synthesized_15th_MAR.csv — per-row record with timestamps."""
    path = DATA_DIR / "video_list_data_synthesized_15th_MAR.csv"
    rows = load_csv(path)
    cur.execute("TRUNCATE video_list_data_synthesized")

    def parse_ts(val):
        """Parse DD/MM/YYYY HH:MM:SS into a Python datetime, return None if empty."""
        if not val or not val.strip():
            return None
        from datetime import datetime
        for fmt in ("%d/%m/%Y %H:%M:%S", "%d/%m/%Y"):
            try:
                return datetime.strptime(val.strip(), fmt)
            except ValueError:
                continue
        return None

    for r in rows:
        cur.execute(
            """INSERT INTO video_list_data_synthesized
               (headline, source, published, team_name, type, uploaded_by,
                video_id, published_platform, published_url,
                language, channel, duration_s, created_ts, published_ts)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (
                clean_str(r.get("Headline")),
                clean_str(r.get("Source")),
                clean_str(r.get("Published")),
                clean_str(r.get("Team Name")),
                clean_str(r.get("Type")),
                clean_str(r.get("Uploaded By")),
                clean_str(r.get("Video ID")),
                clean_str(r.get("Published Platform")),
                clean_str(r.get("Published URL")),
                clean_str(r.get("Language")),
                clean_str(r.get("Channel")),
                float(r["Duration (s)"]) if r.get("Duration (s)", "").strip() else None,
                parse_ts(r.get("Processed/Created timestamp")),
                parse_ts(r.get("Published timestamp")),
            )
        )
    print(f"  ✅ video_list_data_synthesized → {len(rows)} rows")


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    import sys
    load_synth = "--synth" in sys.argv or "--all" in sys.argv

    print("\n🔌 Connecting to PostgreSQL...")
    try:
        conn = get_conn()
    except Exception as e:
        print(f"\n❌ Could not connect: {e}")
        print("\nMake sure your .env has the correct DB_NAME / DB_USER / DB_PASSWORD / DB_HOST / DB_PORT")
        sys.exit(1)

    print("✅ Connected!\n")
    print("📐 Creating tables (if not exist)...")
    with conn:
        with conn.cursor() as cur:
            for stmt in DDL.strip().split(";"):
                stmt = stmt.strip()
                if stmt:
                    cur.execute(stmt)
        conn.commit()
    print("✅ Tables ready.\n")

    print("📥 Loading CSV data...\n")
    with conn:
        with conn.cursor() as cur:
            load_channel_and_user(cur)
            load_input_type(cur)
            load_output_type(cur)
            load_language(cur)
            load_channel_wise_publishing(cur)
            load_channel_wise_publishing_duration(cur)
            load_month_wise_duration(cur)
            load_monthly_chart(cur)
            load_video_list(cur)
        conn.commit()

    if load_synth:
        print("\n📥 Loading synthesized video list (with timestamps)...")
        with conn:
            with conn.cursor() as cur:
                load_video_list_synthesized(cur)
            conn.commit()
        print("✅ Synthesized data loaded!\n")
    else:
        print("\n💡 Tip: run  python load_data.py --synth  to also load the")
        print("          video_list_data_synthesized table (needed for time-series KPIs).\n")

    conn.close()
    print("\n🎉 Done! You can now run:  python test_new_kpis.py\n")


if __name__ == "__main__":
    main()
