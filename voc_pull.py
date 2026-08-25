#!/usr/bin/env python3
"""Pull VoC volume counts and stratified sample from Snowflake."""
import json
import os
import sys
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import snowflake.connector

SAMPLE_PER_CELL = 100
MIN_COMMENT_LEN = 3
TABLE = "jeeny_prod.general.feedbackevents"


def get_report_date(date_str: str | None = None) -> tuple[str, str]:
    tz = ZoneInfo("Asia/Riyadh")
    if date_str:
        d = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=tz)
    else:
        d = datetime.now(tz) - timedelta(days=1)
    report_date = d.strftime("%Y-%m-%d")
    report_day = d.strftime("%a %d %b %Y").replace(" 0", " ")
    return report_date, report_day


def connect():
    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PAT"],
        database=os.environ.get("SNOWFLAKE_DATABASE", "JEENY_PROD"),
        role=os.environ.get("SNOWFLAKE_ROLE"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
    )
    cur = conn.cursor()
    cur.execute("USE WAREHOUSE COMPUTE_WH")
    return conn, cur


BASE_CTE = f"""
WITH base AS (
  SELECT
    GUID,
    FEEDBACK_SOURCE,
    USER_TYPE,
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
  FROM {TABLE}
  WHERE TIMESTAMP >= %s
    AND TIMESTAMP < DATEADD(day, 1, %s)
    AND RATINGS_COMMENTS IS NOT NULL
    AND TRIM(RATINGS_COMMENTS) <> ''
    AND LENGTH(TRIM(RATINGS_COMMENTS)) >= {MIN_COMMENT_LEN}
)
"""


def pull(report_date: str) -> dict:
    conn, cur = connect()
    try:
        cur.execute(
            f"""
            SELECT COUNT(*) AS total_comments
            FROM {TABLE}
            WHERE TIMESTAMP >= %s
              AND TIMESTAMP < DATEADD(day, 1, %s)
              AND RATINGS_COMMENTS IS NOT NULL
              AND TRIM(RATINGS_COMMENTS) <> ''
              AND LENGTH(TRIM(RATINGS_COMMENTS)) >= {MIN_COMMENT_LEN}
            """,
            (report_date, report_date),
        )
        total_comments = cur.fetchone()[0]

        cur.execute(
            f"""
            WITH base AS (
              SELECT
                FEEDBACK_SOURCE,
                CASE
                  WHEN USER_TYPE = 'driver' THEN 'driver'
                  WHEN USER_TYPE IN ('passenger', 'customer') THEN 'rider'
                  ELSE 'other'
                END AS persona
              FROM {TABLE}
              WHERE TIMESTAMP >= %s
                AND TIMESTAMP < DATEADD(day, 1, %s)
                AND RATINGS_COMMENTS IS NOT NULL
                AND TRIM(RATINGS_COMMENTS) <> ''
                AND LENGTH(TRIM(RATINGS_COMMENTS)) >= {MIN_COMMENT_LEN}
            )
            SELECT FEEDBACK_SOURCE, persona, COUNT(*) AS volume
            FROM base
            WHERE persona IN ('driver', 'rider')
            GROUP BY 1, 2
            ORDER BY volume DESC
            """,
            (report_date, report_date),
        )
        volume_rows = [
            {"feedback_source": r[0], "persona": r[1], "volume": r[2]}
            for r in cur.fetchall()
        ]

        cur.execute(
            f"""
            {BASE_CTE}
            SELECT
              GUID, FEEDBACK_SOURCE, USER_TYPE, persona, RATINGS_COMMENTS,
              text_for_analysis, OVERALL_SENTIMENT, RATING_STARS, CITY, COUNTRY,
              TICKET_CATEGORY, CATEGORY, URGENCY, TIMESTAMP
            FROM base
            WHERE persona IN ('driver', 'rider')
              AND rn <= {SAMPLE_PER_CELL}
            """,
            (report_date, report_date),
        )
        cols = [d[0] for d in cur.description]
        sample = [dict(zip(cols, row)) for row in cur.fetchall()]
        for row in sample:
            for k, v in row.items():
                if hasattr(v, "isoformat"):
                    row[k] = v.isoformat()

        return {
            "report_date": report_date,
            "total_comments": total_comments,
            "volume_rows": volume_rows,
            "sample": sample,
            "sample_size": len(sample),
        }
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    date_arg = sys.argv[1] if len(sys.argv) > 1 else None
    report_date, report_day = get_report_date(date_arg)
    data = pull(report_date)
    data["report_day_name"] = report_day
    out = sys.argv[2] if len(sys.argv) > 2 else f"/workspace/voc_data_{report_date}.json"
    with open(out, "w") as f:
        json.dump(data, f, indent=2, default=str)
    print(json.dumps({"report_date": report_date, "report_day_name": report_day,
                      "total_comments": data["total_comments"], "sample_size": data["sample_size"],
                      "output": out}))
