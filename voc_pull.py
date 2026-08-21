#!/usr/bin/env python3
"""Pull VoC volume counts and stratified sample from Snowflake."""
import json
import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import snowflake.connector

SAMPLE_LIMIT = 100

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
  WHERE TIMESTAMP >= %(start_ts)s
    AND TIMESTAMP < %(end_ts)s
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
ORDER BY volume DESC
"""

TOTAL_SQL = """
SELECT COUNT(*) AS total_comments
FROM jeeny_prod.general.feedbackevents
WHERE TIMESTAMP >= %(start_ts)s
  AND TIMESTAMP < %(end_ts)s
  AND RATINGS_COMMENTS IS NOT NULL
  AND TRIM(RATINGS_COMMENTS) <> ''
  AND LENGTH(TRIM(RATINGS_COMMENTS)) >= 3
"""

SAMPLE_SQL = f"""
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
  WHERE TIMESTAMP >= %(start_ts)s
    AND TIMESTAMP < %(end_ts)s
    AND RATINGS_COMMENTS IS NOT NULL
    AND TRIM(RATINGS_COMMENTS) <> ''
    AND LENGTH(TRIM(RATINGS_COMMENTS)) >= 3
)
SELECT
  GUID,
  FEEDBACK_SOURCE,
  persona AS PERSONA,
  RATINGS_COMMENTS,
  text_for_analysis AS TEXT_FOR_ANALYSIS,
  OVERALL_SENTIMENT,
  RATING_STARS,
  CITY,
  COUNTRY,
  TICKET_CATEGORY,
  CATEGORY,
  URGENCY,
  TIMESTAMP
FROM base
WHERE persona IN ('driver', 'rider')
  AND rn <= {SAMPLE_LIMIT}
"""


def connect():
    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PAT"],
        role=os.environ.get("SNOWFLAKE_ROLE"),
        database=os.environ.get("SNOWFLAKE_DATABASE", "JEENY_PROD"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
    )
    cur = conn.cursor()
    cur.execute("USE WAREHOUSE COMPUTE_WH")
    return conn, cur


def rows_to_dicts(cur):
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def serialize(rows):
    out = []
    for row in rows:
        clean = {}
        for k, v in row.items():
            if isinstance(v, datetime):
                clean[k] = v.isoformat()
            else:
                clean[k] = v
        out.append(clean)
    return out


def main():
    tz = ZoneInfo("Asia/Riyadh")
    report_date = (datetime.now(tz) - timedelta(days=1)).date()
    start_ts = datetime.combine(report_date, datetime.min.time())
    end_ts = start_ts + timedelta(days=1)
    params = {"start_ts": start_ts, "end_ts": end_ts}

    conn, cur = connect()
    try:
        cur.execute(VOLUME_SQL, params)
        volumes = [[r["FEEDBACK_SOURCE"], r["PERSONA"], r["VOLUME"]] for r in rows_to_dicts(cur)]

        cur.execute(TOTAL_SQL, params)
        total = cur.fetchone()[0]

        cur.execute(SAMPLE_SQL, params)
        sample = serialize(rows_to_dicts(cur))
    finally:
        cur.close()
        conn.close()

    vol_path = f"/tmp/voc_volumes_{report_date}.json"
    sample_path = f"/tmp/voc_sample_{report_date}.json"
    with open(vol_path, "w") as f:
        json.dump({"total": total, "volumes": volumes, "report_date": str(report_date)}, f)
    with open(sample_path, "w") as f:
        json.dump(sample, f)

    print(json.dumps({
        "report_date": str(report_date),
        "total_comments": total,
        "sample_size": len(sample),
        "volume_cells": len(volumes),
        "vol_path": vol_path,
        "sample_path": sample_path,
    }, indent=2))


if __name__ == "__main__":
    main()
