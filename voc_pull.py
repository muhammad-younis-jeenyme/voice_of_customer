#!/usr/bin/env python3
"""Pull VoC volume counts and stratified sample from Snowflake."""
import json
import os
import sys
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import snowflake.connector

SAMPLE_LIMIT = 100
TABLE = "jeeny_prod.general.feedbackevents"


def connect():
    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PAT"],
        role=os.environ.get("SNOWFLAKE_ROLE"),
        database=os.environ.get("SNOWFLAKE_DATABASE"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
    )
    cur = conn.cursor()
    cur.execute("USE WAREHOUSE COMPUTE_WH")
    return conn


def report_window(report_date: str):
    start = f"{report_date} 00:00:00"
    end_dt = datetime.strptime(report_date, "%Y-%m-%d") + timedelta(days=1)
    end = end_dt.strftime("%Y-%m-%d 00:00:00")
    return start, end


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
  FROM {table}
  WHERE TIMESTAMP >= %(start_ts)s
    AND TIMESTAMP < %(end_ts)s
    AND RATINGS_COMMENTS IS NOT NULL
    AND TRIM(RATINGS_COMMENTS) <> ''
    AND LENGTH(TRIM(RATINGS_COMMENTS)) >= 3
)
SELECT FEEDBACK_SOURCE, persona, COUNT(*) AS volume
FROM base
WHERE persona IN ('driver', 'rider')
GROUP BY 1, 2
ORDER BY volume DESC
"""

TOTAL_SQL = """
SELECT COUNT(*) AS total_comments
FROM {table}
WHERE TIMESTAMP >= %(start_ts)s
  AND TIMESTAMP < %(end_ts)s
  AND RATINGS_COMMENTS IS NOT NULL
  AND TRIM(RATINGS_COMMENTS) <> ''
  AND LENGTH(TRIM(RATINGS_COMMENTS)) >= 3
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
  FROM {table}
  WHERE TIMESTAMP >= %(start_ts)s
    AND TIMESTAMP < %(end_ts)s
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
  AND rn <= %(sample_limit)s
"""


def main():
    if len(sys.argv) > 1:
        report_date = sys.argv[1]
    else:
        riyadh = ZoneInfo("Asia/Riyadh")
        report_date = (datetime.now(riyadh).date() - timedelta(days=1)).isoformat()

    start_ts, end_ts = report_window(report_date)
    params = {"start_ts": start_ts, "end_ts": end_ts, "sample_limit": SAMPLE_LIMIT}

    conn = connect()
    cur = conn.cursor()
    cur.execute(TOTAL_SQL.format(table=TABLE), params)
    total_comments = cur.fetchone()[0]

    cur.execute(VOLUME_SQL.format(table=TABLE), params)
    volume_rows = [
        {"feedback_source": r[0], "persona": r[1], "volume": r[2]}
        for r in cur.fetchall()
    ]

    cur.execute(SAMPLE_SQL.format(table=TABLE), params)
    cols = [d[0].lower() for d in cur.description]
    sample_rows = [dict(zip(cols, row)) for row in cur.fetchall()]
    for row in sample_rows:
        if row.get("timestamp"):
            row["timestamp"] = row["timestamp"].isoformat()

    conn.close()

    out = {
        "report_date": report_date,
        "start_ts": start_ts,
        "end_ts": end_ts,
        "total_comments": total_comments,
        "volume": volume_rows,
        "sample": sample_rows,
        "sample_count": len(sample_rows),
    }
    print(json.dumps(out, default=str))


if __name__ == "__main__":
    main()
