#!/usr/bin/env python3
"""Voice of Customer daily pull + analyze + compose."""
import json
import os
import re
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import snowflake.connector

REPORT_TZ = ZoneInfo("Asia/Riyadh")
SAMPLE_LIMIT = 100
TABLE = "jeeny_prod.general.feedbackevents"

# Theme patterns (keyword -> canonical label)
DRIVER_THEMES = {
    "Cancellation fees not received / unfair fines": [
        r"\bcancel", r"\bfine", r"\bpenalt", r"\bfees?\b.*\bnot\b", r"لم يصل", r"غرام",
    ],
    "Earnings & fares (low pay per km, weak offers)": [
        r"\bearn", r"\bpay\b", r"\bfare", r"\boffer", r"\bkm\b", r"\blow\b.*\bprice",
        r"أجر", r"راتب", r"عمولة",
    ],
    "Passenger conduct": [
        r"\brude", r"\babus", r"\bconduct", r"\bbehav", r"\bpassenger.*\b(bad|rude)",
        r"سلوك", r"زبون.*سيء",
    ],
    "Passenger didn't pay (cash)": [
        r"\bdidn.t pay", r"\bno pay", r"\bcash\b.*\b(not|refus)", r"لم يدفع", r"ما دفع",
    ],
    "Fare/charge accuracy": [
        r"\bcharge", r"\bwrong (amount|fare)", r"\bovercharg", r"\brecalcul",
    ],
    "Payment not credited / withdrawals": [
        r"\bwithdraw", r"\bwallet", r"\bcredit", r"\bpayout", r"\btransfer",
        r"سحب", r"محفظ",
    ],
}

RIDER_THEMES = {
    "Overcharging / fare disputes": [
        r"\bovercharg", r"\bfare.*\b(high|wrong|disput)", r"\bcharged\b.*\bmore",
        r"زيادة.*سعر", r"غالي",
    ],
    "Driver conduct": [
        r"\brude", r"\babus", r"\bconduct", r"\bbehav", r"\bdriver.*\b(bad|rude)",
        r"سواق", r"سائق.*سيء",
    ],
    "Wrong driver / car vs app": [
        r"\bwrong (driver|car)", r"\bdifferent (car|driver)", r"\bmismatch",
        r"سيارة.*مختلف", r"سواق.*غير",
    ],
    "Route manipulation / slow driving": [
        r"\broute", r"\bdetour", r"\bslow", r"\blonger", r"\bmanipul", r"طريق.*طويل",
    ],
    "No-show / wrong pickup": [
        r"\bno.?show", r"\bwrong pickup", r"\bdidn.t (come|arrive|show)",
        r"ما جاء", r"لم يحضر",
    ],
    "Cancellation (unfair fees, driver-initiated)": [
        r"\bcancel", r"\bfee", r"\bunfair.*cancel", r"إلغاء",
    ],
    "Payment & charging errors": [
        r"\bpayment", r"\bcharg", r"\bcard\b", r"\bwallet", r"دفع",
    ],
    "Vehicle smell / cleanliness": [
        r"\bsmell", r"\bclean", r"\bdirty", r"\bodor", r"رائحة", r"نظافة",
    ],
}

SAFETY_PATTERNS = {
    "Harassment": [r"harass", r"assault", r"threat", r"insult", r"stalk", r"held hand", r"تحرش", r"تهديد"],
    "Reckless driving": [r"reckless", r"injur", r"accident", r"hit\b", r"child", r"خطير", r"حادث"],
    "Identity mismatch": [r"wrong (driver|car).*safe", r"identity", r"not the (driver|car)", r"مختلف.*سيارة"],
}

ZENDESK_NEUTRAL_CATS = {
    "city update", "work permit", "lost item", "found item", "general inquiry",
    "account update", "document",
}

POSITIVE_KW = [
    r"\bgreat\b", r"\bexcellent\b", r"\bgood\b", r"\bthank", r"\blove\b", r"\bbest\b",
    r"\bممتاز", r"\bشكر", r"\bرائع",
]
NEGATIVE_KW = [
    r"\bbad\b", r"\bterrible\b", r"\bworst\b", r"\bawful\b", r"\bhorrible\b",
    r"\bcomplain", r"\bfrustrat", r"\bangry\b", r"\bdisappoint",
    r"\bسيء", r"\bسيئ", r"\bرديء", r"\bغاضب", r"\bمشكلة",
]


def connect():
    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PAT"],
        role=os.environ.get("SNOWFLAKE_ROLE", "ROLE_PRODUCT"),
        database=os.environ.get("SNOWFLAKE_DATABASE", "JEENY_PROD"),
    )
    cur = conn.cursor()
    cur.execute("USE WAREHOUSE COMPUTE_WH")
    return conn, cur


def classify_sentiment(row):
    existing = (row.get("OVERALL_SENTIMENT") or "").strip()
    if existing in ("Positive", "Neutral", "Negative", "Mixed"):
        return existing

    text = (row.get("TEXT_FOR_ANALYSIS") or "").strip()
    source = row.get("FEEDBACK_SOURCE") or ""
    ticket_cat = (row.get("TICKET_CATEGORY") or "").lower()
    stars = row.get("RATING_STARS")

    if source == "Zendesk":
        if any(n in ticket_cat for n in ZENDESK_NEUTRAL_CATS):
            return "Neutral"
        return "Negative"

    text_l = text.lower()
    pos = sum(1 for p in POSITIVE_KW if re.search(p, text_l, re.I))
    neg = sum(1 for p in NEGATIVE_KW if re.search(p, text_l, re.I))

    if pos and neg:
        sentiment = "Mixed"
    elif neg:
        sentiment = "Negative"
    elif pos:
        sentiment = "Positive"
    elif stars is not None:
        try:
            s = float(stars)
            if s <= 2:
                sentiment = "Negative"
            elif s == 3:
                sentiment = "Neutral"
            elif s >= 4:
                sentiment = "Positive"
            else:
                sentiment = "Neutral"
        except (TypeError, ValueError):
            sentiment = "Unclear"
    else:
        sentiment = "Unclear" if len(text) < 5 else "Neutral"

    # rating tie-break
    if stars is not None and sentiment in ("Neutral", "Unclear"):
        try:
            s = float(stars)
            if s <= 2:
                sentiment = "Negative"
            elif s >= 4:
                sentiment = "Positive"
        except (TypeError, ValueError):
            pass

    return sentiment


def match_themes(text, theme_map):
    if not text:
        return set()
    found = set()
    for label, patterns in theme_map.items():
        for p in patterns:
            if re.search(p, text, re.I):
                found.add(label)
                break
    return found


def pct_neg(counts, n, prefix_tilde=False):
    if n == 0:
        return "n/a"
    p = round(100 * counts.get("Negative", 0) / n)
    tilde = "~" if prefix_tilde or n < 30 else ""
    return f"{tilde}{p}"


def theme_pct(theme_hits, persona_n):
    if persona_n == 0:
        return []
    ranked = sorted(theme_hits.items(), key=lambda x: -x[1])
    result = []
    for label, cnt in ranked[:7]:
        p = round(100 * cnt / persona_n)
        result.append((label, f"~{p}" if persona_n < 30 else f"~{p}"))
    return result


def normalize_source(src):
    if src in ("Google Store", "Apple Store"):
        return "App stores"
    return src


def main():
    report_date = sys.argv[1] if len(sys.argv) > 1 else (
        datetime.now(REPORT_TZ).date() - timedelta(days=1)
    ).isoformat()

    start_ts = f"{report_date} 00:00:00"
    end_ts = f"{report_date} 23:59:59.999"

    conn, cur = connect()

    volume_sql = f"""
    WITH base AS (
      SELECT
        FEEDBACK_SOURCE,
        CASE
          WHEN USER_TYPE = 'driver' THEN 'driver'
          WHEN USER_TYPE IN ('passenger', 'customer') THEN 'rider'
          ELSE 'other'
        END AS persona,
        RATINGS_COMMENTS
      FROM {TABLE}
      WHERE TIMESTAMP >= %s
        AND TIMESTAMP < DATEADD(day, 1, %s::DATE)
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

    total_sql = f"""
    SELECT COUNT(*) AS total_comments
    FROM {TABLE}
    WHERE TIMESTAMP >= %s
      AND TIMESTAMP < DATEADD(day, 1, %s::DATE)
      AND RATINGS_COMMENTS IS NOT NULL
      AND TRIM(RATINGS_COMMENTS) <> ''
      AND LENGTH(TRIM(RATINGS_COMMENTS)) >= 3
    """

    sample_sql = f"""
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
        COALESCE(NULLIF(TRIM(EN_TRANSLATION), ''), RATINGS_COMMENTS) AS TEXT_FOR_ANALYSIS,
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
        AND TIMESTAMP < DATEADD(day, 1, %s::DATE)
        AND RATINGS_COMMENTS IS NOT NULL
        AND TRIM(RATINGS_COMMENTS) <> ''
        AND LENGTH(TRIM(RATINGS_COMMENTS)) >= 3
    )
    SELECT GUID, FEEDBACK_SOURCE, persona, RATINGS_COMMENTS, TEXT_FOR_ANALYSIS,
           OVERALL_SENTIMENT, RATING_STARS, CITY, COUNTRY, TICKET_CATEGORY,
           CATEGORY, URGENCY, TIMESTAMP
    FROM base
    WHERE persona IN ('driver', 'rider')
      AND rn <= {SAMPLE_LIMIT}
    """

    cur.execute(total_sql, (start_ts, report_date))
    total_comments = cur.fetchone()[0]

    cur.execute(volume_sql, (start_ts, report_date))
    volume_rows = cur.fetchall()
    volumes = defaultdict(lambda: {"driver": 0, "rider": 0})
    for src, persona, vol in volume_rows:
        ns = normalize_source(src)
        volumes[ns][persona] += vol

    cur.execute(sample_sql, (start_ts, report_date))
    cols = [d[0] for d in cur.description]
    sample = []
    for row in cur.fetchall():
        r = {k.lower(): v for k, v in zip(cols, row)}
        sample.append(r)

    for row in sample:
        row["sentiment"] = classify_sentiment({
            "OVERALL_SENTIMENT": row.get("overall_sentiment"),
            "TEXT_FOR_ANALYSIS": row.get("text_for_analysis"),
            "FEEDBACK_SOURCE": row.get("feedback_source"),
            "TICKET_CATEGORY": row.get("ticket_category"),
            "RATING_STARS": row.get("rating_stars"),
        })

    # cell-level %neg
    cell_stats = defaultdict(lambda: {"n": 0, "neg": 0})
    for row in sample:
        ns = normalize_source(row["feedback_source"])
        key = (ns, row["persona"])
        cell_stats[key]["n"] += 1
        if row["sentiment"] == "Negative":
            cell_stats[key]["neg"] += 1

    driver_theme_hits = defaultdict(int)
    rider_theme_hits = defaultdict(int)
    driver_n = sum(1 for r in sample if r["persona"] == "driver")
    rider_n = sum(1 for r in sample if r["persona"] == "rider")

    safety_flags = []
    for row in sample:
        if row["sentiment"] not in ("Negative", "Mixed"):
            continue
        text = row.get("text_for_analysis") or ""
        themes = DRIVER_THEMES if row["persona"] == "driver" else RIDER_THEMES
        for label in match_themes(text, themes):
            if row["persona"] == "driver":
                driver_theme_hits[label] += 1
            else:
                rider_theme_hits[label] += 1

        for cat, patterns in SAFETY_PATTERNS.items():
            for p in patterns:
                if re.search(p, text, re.I):
                    city = row.get("city") or "Unknown city"
                    summary = re.sub(r"\s+", " ", text)[:120]
                    safety_flags.append({"category": cat, "city": city, "summary": summary})
                    break

    driver_themes = theme_pct(driver_theme_hits, driver_n)
    rider_themes = theme_pct(rider_theme_hits, rider_n)

    def cell_pct(src, persona):
        key = (src, persona)
        st = cell_stats.get(key, {"n": 0, "neg": 0})
        vol = volumes.get(src, {}).get(persona, 0)
        if vol == 0:
            return vol, "n/a"
        if src == "App stores":
            return vol, "n/a"
        return vol, pct_neg({"Negative": st["neg"]}, st["n"], st["n"] < 30)

    dt = datetime.strptime(report_date, "%Y-%m-%d")
    day_name = dt.strftime("%a %d %b %Y")

    out = {
        "report_date": report_date,
        "report_day_name": day_name,
        "total_comments": total_comments,
        "sample_count": len(sample),
        "volumes": dict(volumes),
        "cell_stats": {f"{k[0]}|{k[1]}": v for k, v in cell_stats.items()},
        "table": {
            "Zendesk": {"driver": cell_pct("Zendesk", "driver"), "rider": cell_pct("Zendesk", "rider")},
            "ride_rating": {"driver": cell_pct("ride_rating", "driver"), "rider": cell_pct("ride_rating", "rider")},
            "in_app": {"driver": cell_pct("in_app", "driver"), "rider": ("—", "—")},
            "App stores": {"driver": cell_pct("App stores", "driver"), "rider": cell_pct("App stores", "rider")},
        },
        "driver_themes": driver_themes,
        "rider_themes": rider_themes,
        "safety_flags": safety_flags[:10],
        "safety_count": len(safety_flags),
    }

    print(json.dumps(out, default=str))
    conn.close()


if __name__ == "__main__":
    main()
