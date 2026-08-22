#!/usr/bin/env python3
"""Pull VoC volume counts and stratified sample from Snowflake."""
import json
import os
import sys
from datetime import datetime, timedelta

import snowflake.connector

REPORT_DATE = sys.argv[1] if len(sys.argv) > 1 else None
SAMPLE_LIMIT = int(sys.argv[2]) if len(sys.argv) > 2 else 100
OUT_DIR = sys.argv[3] if len(sys.argv) > 3 else "/tmp/voc"

if not REPORT_DATE:
    from zoneinfo import ZoneInfo
    tz = ZoneInfo("Asia/Riyadh")
    REPORT_DATE = (datetime.now(tz) - timedelta(days=1)).strftime("%Y-%m-%d")

os.makedirs(OUT_DIR, exist_ok=True)

START_TS = f"{REPORT_DATE} 00:00:00"
END_TS = f"{REPORT_DATE} 23:59:59.999"

conn = snowflake.connector.connect(
    account=os.environ["SNOWFLAKE_ACCOUNT"],
    user=os.environ["SNOWFLAKE_USER"],
    password=os.environ["SNOWFLAKE_PAT"],
    role=os.environ.get("SNOWFLAKE_ROLE", "ROLE_PRODUCT"),
    database=os.environ.get("SNOWFLAKE_DATABASE", "JEENY_PROD"),
    warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
)
cur = conn.cursor()
cur.execute("USE WAREHOUSE COMPUTE_WH")

BASE_FILTER = """
    TIMESTAMP >= %(start_ts)s
    AND TIMESTAMP < DATEADD(day, 1, %(start_ts)s::DATE)
    AND RATINGS_COMMENTS IS NOT NULL
    AND TRIM(RATINGS_COMMENTS) <> ''
    AND LENGTH(TRIM(RATINGS_COMMENTS)) >= 3
"""

params = {"start_ts": START_TS}

# Total comments
cur.execute(f"""
SELECT COUNT(*) AS total_comments
FROM jeeny_prod.general.feedbackevents
WHERE {BASE_FILTER}
""", params)
total_comments = cur.fetchone()[0]

# Volume by source × persona
cur.execute(f"""
WITH base AS (
  SELECT
    FEEDBACK_SOURCE,
    CASE
      WHEN USER_TYPE = 'driver' THEN 'driver'
      WHEN USER_TYPE IN ('passenger', 'customer') THEN 'rider'
      ELSE 'other'
    END AS persona,
    RATINGS_COMMENTS,
    COALESCE(NULLIF(TRIM(EN_TRANSLATION), ''), RATINGS_COMMENTS) AS text_for_analysis,
    OVERALL_SENTIMENT,
    RATING_STARS,
    GUID,
    CITY,
    COUNTRY,
    TICKET_CATEGORY,
    CATEGORY,
    URGENCY,
    TIMESTAMP
  FROM jeeny_prod.general.feedbackevents
  WHERE {BASE_FILTER}
)
SELECT FEEDBACK_SOURCE, persona, COUNT(*) AS volume
FROM base
WHERE persona IN ('driver', 'rider')
GROUP BY 1, 2
ORDER BY volume DESC
""", params)
volume_rows = [{"feedback_source": r[0], "persona": r[1], "volume": r[2]} for r in cur.fetchall()]

# Stratified sample
cur.execute(f"""
WITH base AS (
  SELECT
    GUID,
    FEEDBACK_SOURCE,
    CASE
      WHEN USER_TYPE = 'driver' THEN 'driver'
      WHEN USER_TYPE IN ('passenger', 'customer') THEN 'rider'
      ELSE 'other'
    END AS persona,
    RATINGS_COMMENTS,
    COALESCE(NULLIF(TRIM(EN_TRANSLATION), ''), RATINGS_COMMENTS) AS text_for_analysis,
    OVERALL_SENTIMENT,
    RATING_STARS,
    CITY,
    COUNTRY,
    TICKET_CATEGORY,
    CATEGORY,
    URGENCY,
    TIMESTAMP,
    ROW_NUMBER() OVER (
      PARTITION BY FEEDBACK_SOURCE,
        CASE
          WHEN USER_TYPE = 'driver' THEN 'driver'
          WHEN USER_TYPE IN ('passenger', 'customer') THEN 'rider'
          ELSE 'other'
        END
      ORDER BY RANDOM()
    ) AS rn
  FROM jeeny_prod.general.feedbackevents
  WHERE {BASE_FILTER}
)
SELECT
  GUID, FEEDBACK_SOURCE, persona, RATINGS_COMMENTS, text_for_analysis,
  OVERALL_SENTIMENT, RATING_STARS, CITY, COUNTRY, TICKET_CATEGORY,
  CATEGORY, URGENCY, TIMESTAMP
FROM base
WHERE persona IN ('driver', 'rider')
  AND rn <= {SAMPLE_LIMIT}
""", params)
cols = [d[0].lower() for d in cur.description]
sample_rows = [dict(zip(cols, row)) for row in cur.fetchall()]

cur.close()
conn.close()

result = {
    "report_date": REPORT_DATE,
    "start_ts": START_TS,
    "end_ts": END_TS,
    "total_comments": total_comments,
    "volume_rows": volume_rows,
    "sample_rows": sample_rows,
    "sample_count": len(sample_rows),
    "sample_limit": SAMPLE_LIMIT,
}

with open(f"{OUT_DIR}/pull_{REPORT_DATE}.json", "w") as f:
    json.dump(result, f, default=str, indent=2)

print(json.dumps({
    "report_date": REPORT_DATE,
    "total_comments": total_comments,
    "sample_count": len(sample_rows),
    "volume_rows": volume_rows,
}, indent=2))
