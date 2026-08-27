"""Pull VoC data from Snowflake for a given report date."""
import json
import os
import sys
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import snowflake.connector


def get_report_date():
    tz = ZoneInfo("Asia/Riyadh")
    yesterday = datetime.now(tz) - timedelta(days=1)
    return yesterday.strftime("%Y-%m-%d"), yesterday.strftime("%a %d %b %Y")


def connect():
    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PAT"],
        database=os.environ.get("SNOWFLAKE_DATABASE", "JEENY_PROD"),
        role=os.environ.get("SNOWFLAKE_ROLE", "ROLE_PRODUCT"),
    )
    cur = conn.cursor()
    cur.execute(f"USE WAREHOUSE {os.environ.get('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH')}")
    return conn, cur


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
  AND rn <= %(sample_size)s
"""


def main():
    report_date, report_day_name = get_report_date()
    if len(sys.argv) > 1:
        report_date = sys.argv[1]
        dt = datetime.strptime(report_date, "%Y-%m-%d")
        report_day_name = dt.strftime("%a %d %b %Y")

    start_ts = f"{report_date} 00:00:00"
    end_dt = datetime.strptime(report_date, "%Y-%m-%d") + timedelta(days=1)
    end_ts = end_dt.strftime("%Y-%m-%d 00:00:00")
    sample_size = int(os.environ.get("VOC_SAMPLE_SIZE", "100"))

    params = {"start_ts": start_ts, "end_ts": end_ts, "sample_size": sample_size}

    conn, cur = connect()

    cur.execute(TOTAL_SQL, params)
    total_comments = cur.fetchone()[0]

    cur.execute(VOLUME_SQL, params)
    volumes = [
        {"feedback_source": r[0], "persona": r[1], "volume": r[2]}
        for r in cur.fetchall()
    ]

    cur.execute(SAMPLE_SQL, params)
    cols = [d[0] for d in cur.description]
    sample = [dict(zip(cols, row)) for row in cur.fetchall()]

  # serialize timestamps
    for row in sample:
        if row.get("TIMESTAMP"):
            row["TIMESTAMP"] = row["TIMESTAMP"].isoformat()

    conn.close()

    out = {
        "report_date": report_date,
        "report_day_name": report_day_name,
        "total_comments": total_comments,
        "volumes": volumes,
        "sample": sample,
        "sample_size_limit": sample_size,
    }
    print(json.dumps(out, default=str))


if __name__ == "__main__":
    main()
