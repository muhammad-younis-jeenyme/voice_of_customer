#!/usr/bin/env python3
"""Fetch VoC data from Snowflake for daily digest."""
import json
import os
import sys

import snowflake.connector

REPORT_DATE = sys.argv[1] if len(sys.argv) > 1 else "2026-10-08"
SAMPLE_LIMIT = int(sys.argv[2]) if len(sys.argv) > 2 else 100
OUT_DIR = sys.argv[3] if len(sys.argv) > 3 else "/tmp/voc"

VOLUME_SQL = """
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
  WHERE TIMESTAMP >= %(start_ts)s::TIMESTAMP
    AND TIMESTAMP < %(end_ts)s::TIMESTAMP
    AND RATINGS_COMMENTS IS NOT NULL
    AND TRIM(RATINGS_COMMENTS) <> ''
    AND LENGTH(TRIM(RATINGS_COMMENTS)) >= 3
)
SELECT
  FEEDBACK_SOURCE,
  persona,
  COUNT(*) AS volume
FROM base
WHERE persona IN ('driver', 'rider')
GROUP BY 1, 2
ORDER BY volume DESC;
"""

TOTAL_SQL = """
SELECT COUNT(*) AS total_comments
FROM jeeny_prod.general.feedbackevents
WHERE TIMESTAMP >= %(start_ts)s::TIMESTAMP
  AND TIMESTAMP < %(end_ts)s::TIMESTAMP
  AND RATINGS_COMMENTS IS NOT NULL
  AND TRIM(RATINGS_COMMENTS) <> ''
  AND LENGTH(TRIM(RATINGS_COMMENTS)) >= 3;
"""

SAMPLE_SQL = """
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
  WHERE TIMESTAMP >= %(start_ts)s::TIMESTAMP
    AND TIMESTAMP < %(end_ts)s::TIMESTAMP
    AND RATINGS_COMMENTS IS NOT NULL
    AND TRIM(RATINGS_COMMENTS) <> ''
    AND LENGTH(TRIM(RATINGS_COMMENTS)) >= 3
)
SELECT
  GUID, FEEDBACK_SOURCE, persona, RATINGS_COMMENTS, text_for_analysis,
  OVERALL_SENTIMENT, RATING_STARS, CITY, COUNTRY, TICKET_CATEGORY,
  CATEGORY, URGENCY, TIMESTAMP
FROM base
WHERE persona IN ('driver', 'rider')
  AND rn <= %(sample_limit)s;
"""


def connect():
    conn = snowflake.connector.connect(
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PAT"],
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ.get("SNOWFLAKE_DATABASE", "jeeny_prod"),
        role=os.environ.get("SNOWFLAKE_ROLE"),
    )
    cur = conn.cursor()
    cur.execute(f"USE WAREHOUSE {os.environ['SNOWFLAKE_WAREHOUSE']}")
    return conn, cur


def fetch_all(cur, sql, params):
    cur.execute(sql, params)
    cols = [d[0] for d in cur.description]
    rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    return rows


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    start_ts = f"{REPORT_DATE} 00:00:00"
    # next day
    from datetime import datetime, timedelta

    d = datetime.strptime(REPORT_DATE, "%Y-%m-%d")
    end_ts = (d + timedelta(days=1)).strftime("%Y-%m-%d 00:00:00")
    params = {"start_ts": start_ts, "end_ts": end_ts, "sample_limit": SAMPLE_LIMIT}

    conn, cur = connect()
    try:
        volume = fetch_all(cur, VOLUME_SQL, params)
        total = fetch_all(cur, TOTAL_SQL, params)[0]["TOTAL_COMMENTS"]
        sample = fetch_all(cur, SAMPLE_SQL, params)
    finally:
        cur.close()
        conn.close()

    def serialize_row(r):
        out = {}
        for k, v in r.items():
            if hasattr(v, "isoformat"):
                out[k] = v.isoformat()
            else:
                out[k] = v
        return out

    sample = [serialize_row(r) for r in sample]
    with open(f"{OUT_DIR}/volume.json", "w") as f:
        json.dump(volume, f, indent=2)
    with open(f"{OUT_DIR}/total.json", "w") as f:
        json.dump({"total_comments": total, "report_date": REPORT_DATE}, f)
    with open(f"{OUT_DIR}/sample.json", "w") as f:
        json.dump(sample, f)
    print(json.dumps({"total": total, "volume_rows": len(volume), "sample_rows": len(sample)}))


if __name__ == "__main__":
    main()
