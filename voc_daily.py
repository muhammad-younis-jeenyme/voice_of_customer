#!/usr/bin/env python3
"""Voice of Customer daily digest — Snowflake pull + sentiment/theme analysis."""

import json
import os
import re
import sys
from collections import defaultdict

import snowflake.connector

REPORT_DATE = sys.argv[1] if len(sys.argv) > 1 else "2026-09-09"
SAMPLE_LIMIT = 100

# Zendesk categories treated as neutral/ops (not auto-negative)
ZENDESK_NEUTRAL_CATS = {
    "city update", "work permit", "lost item", "lost items", "general inquiry",
    "account update", "document", "registration", "vehicle update",
}

# Theme keyword patterns: (canonical_label, emoji, keywords)
DRIVER_THEMES = [
    ("Cancellation fees not received / unfair fines", "🚫",
     r"cancel|fine|penalty|fee.*not|unfair.*fine|deduct"),
    ("Earnings & fares (low pay per km, weak offers)", "💰",
     r"earn|low pay|fare.*low|offer|commission|income|salary|km|kilometer|trip.*pay"),
    ("Passenger conduct", "😡",
     r"passenger.*(rude|bad|abuse|drunk|smok|fight)|rider.*(rude|bad|abuse)"),
    ("Passenger didn't pay (cash)", "💵",
     r"didn.?t pay|not pay|cash.*(refus|no)|unpaid|without pay"),
    ("Fare/charge accuracy", "📄",
     r"wrong.*(fare|charge|amount)|incorrect.*(fare|charge)|overcharg.*driver|deduct"),
    ("Payment not credited / withdrawals", "🏦",
     r"withdraw|wallet|not credit|payment.*not|balance|payout|transfer"),
]

RIDER_THEMES = [
    ("Overcharging / fare disputes", "💸",
     r"overcharg|too much|expensive|high.*fare|fare.*high|charged.*more|rip.?off"),
    ("Driver conduct", "😡",
     r"driver.*(rude|bad|abuse|angry|shout|scream|unprofession)|behavio"),
    ("Wrong driver / car vs app", "🚙",
     r"wrong.*(driver|car|vehicle|plate)|different.*(car|driver|vehicle)|not.*match"),
    ("Route manipulation / slow driving", "🗺️",
     r"route|long.*way|detour|slow|traffic.*manip|extra.*km|longer.*route"),
    ("No-show / wrong pickup", "📍",
     r"no.?show|didn.?t come|wrong.*pickup|wait.*long|never.*arriv"),
    ("Cancellation (unfair fees, driver-initiated)", "🚫",
     r"cancel.*(fee|unfair|driver)|driver.*cancel|charged.*cancel"),
    ("Payment & charging errors", "🏦",
     r"payment.*(fail|error|issue)|charg.*(error|twice|double)|wallet|refund"),
    ("Vehicle smell / cleanliness", "🧼",
     r"smell|dirty|clean|hygien|odor|stink"),
]

SAFETY_PATTERNS = [
    ("Harassment", "🆘",
     r"harass|assault|held hand|threaten|hit|insult|stalk|inappropriat|touch"),
    ("Reckless driving", "🏎️",
     r"reckless|speed|dangerous|injur|child.*injur|accident"),
    ("Identity mismatch", "🚙",
     r"wrong.*(driver|car)|different.*(driver|car|plate)|not.*same.*(driver|car)"),
    ("Post-ride contact", "🆘",
     r"call.*after|message.*after|contact.*after|follow.*after.*ride"),
]

ARABIC_NEG = re.compile(
    r"سيء|رديء|فظيع|غبي|كذاب|نصب|احتيال|سرقة|وقح|مشكلة|غش|تأخير|إلغاء|غالي|زيادة"
)


def connect():
    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PAT"],
        database=os.environ.get("SNOWFLAKE_DATABASE", "JEENY_PROD"),
        role=os.environ.get("SNOWFLAKE_ROLE", "ROLE_PRODUCT"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
    )
    cur = conn.cursor()
    cur.execute("USE WAREHOUSE COMPUTE_WH")
    return conn, cur


def normalize_source(src):
    if src in ("Google Store", "Apple Store"):
        return "App stores"
    return src


def classify_sentiment(row):
    """Classify a single row. row: dict with keys from sample query."""
    existing = (row.get("OVERALL_SENTIMENT") or "").strip()
    if existing in ("Positive", "Neutral", "Negative", "Mixed"):
        return existing

    text = (row.get("TEXT_FOR_ANALYSIS") or "").strip()
    source = row.get("FEEDBACK_SOURCE", "")
    stars = row.get("RATING_STARS")
    ticket_cat = (row.get("TICKET_CATEGORY") or "").lower()

    # Zendesk: default negative unless neutral category
    if source == "Zendesk":
        if any(nc in ticket_cat for nc in ZENDESK_NEUTRAL_CATS):
            return "Neutral"
        return "Negative"

    if not text or len(text) < 3:
        return "Unclear"

    text_lower = text.lower()

    # Arabic fallback for very short text
    if len(text) < 5 and ARABIC_NEG.search(text):
        return "Negative"

    pos_kw = re.search(
        r"excellent|great|good|amazing|thank|love|best|perfect|wonderful|ممتاز|رائع|شكر",
        text_lower,
    )
    neg_kw = re.search(
        r"bad|terrible|awful|worst|hate|complain|scam|fraud|poor|horrible|disappoint|"
        r"unfair|never again|refund|problem|issue|angry|frustrat|"
        r"سيء|رديء|فظيع|مشكلة|احتيال|غش",
        text_lower,
    )

    if pos_kw and neg_kw:
        return "Mixed"
    if neg_kw:
        return "Negative"
    if pos_kw:
        return "Positive"

    # Rating tie-break
    if stars is not None:
        try:
            s = float(stars)
            if s <= 2:
                return "Negative"
            if s == 3:
                return "Neutral"
            if s >= 4:
                return "Positive"
        except (ValueError, TypeError):
            pass

    return "Neutral"


def match_themes(text, patterns):
    if not text:
        return []
    text_lower = text.lower()
    matched = []
    for label, emoji, pattern in patterns:
        if re.search(pattern, text_lower, re.IGNORECASE):
            matched.append((label, emoji))
    return matched


def match_safety(text):
    if not text:
        return []
    text_lower = text.lower()
    results = []
    for label, emoji, pattern in SAFETY_PATTERNS:
        if re.search(pattern, text_lower, re.IGNORECASE):
            results.append((label, emoji))
    return results


def anonymize(text, max_len=120):
    if not text:
        return "Customer reported a safety concern."
    t = re.sub(r"\b\d{8,}\b", "[redacted]", text)
    t = re.sub(r"\b[A-Z][a-z]+ [A-Z][a-z]+\b", "[name]", t)
    t = re.sub(r"\+?\d{10,}", "[phone]", t)
    t = t.strip()
    if len(t) > max_len:
        t = t[:max_len].rsplit(" ", 1)[0] + "…"
    return t


def main():
    conn, cur = connect()

    # Volume counts
    vol_sql = f"""
    WITH base AS (
      SELECT
        FEEDBACK_SOURCE,
        CASE
          WHEN USER_TYPE = 'driver' THEN 'driver'
          WHEN USER_TYPE IN ('passenger', 'customer') THEN 'rider'
          ELSE 'other'
        END AS persona,
        RATINGS_COMMENTS
      FROM jeeny_prod.general.feedbackevents
      WHERE TIMESTAMP >= '{REPORT_DATE}'::DATE
        AND TIMESTAMP < '{REPORT_DATE}'::DATE + INTERVAL '1 day'
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
    cur.execute(vol_sql)
    vol_rows = cur.fetchall()
    vol_cols = [d[0] for d in cur.description]

    total_sql = f"""
    SELECT COUNT(*) AS total_comments
    FROM jeeny_prod.general.feedbackevents
    WHERE TIMESTAMP >= '{REPORT_DATE}'::DATE
      AND TIMESTAMP < '{REPORT_DATE}'::DATE + INTERVAL '1 day'
      AND RATINGS_COMMENTS IS NOT NULL
      AND TRIM(RATINGS_COMMENTS) <> ''
      AND LENGTH(TRIM(RATINGS_COMMENTS)) >= 3
    """
    cur.execute(total_sql)
    total_comments = cur.fetchone()[0]

    # Stratified sample
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
      WHERE TIMESTAMP >= '{REPORT_DATE}'::DATE
        AND TIMESTAMP < '{REPORT_DATE}'::DATE + INTERVAL '1 day'
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
      AND rn <= {SAMPLE_LIMIT}
    """
    cur.execute(sample_sql)
    sample_cols = [d[0] for d in cur.description]
    sample_rows = [dict(zip(sample_cols, r)) for r in cur.fetchall()]
    conn.close()

    # Normalize column names for analysis
    for row in sample_rows:
        row["TEXT_FOR_ANALYSIS"] = row.pop("TEXT_FOR_ANALYSIS", row.get("TEXT_FOR_ANALYSIS"))

    # Sentiment per row
    for row in sample_rows:
        row["sentiment"] = classify_sentiment(row)

    sample_count = len(sample_rows)

    # Volume lookup: (normalized_source, persona) -> volume
    vol_map = defaultdict(int)
    for r in vol_rows:
        d = dict(zip(vol_cols, r))
        src = normalize_source(d["FEEDBACK_SOURCE"])
        vol_map[(src, d["PERSONA"])] += d["VOLUME"]

    # Sentiment stats per cell: (source, persona) -> {neg, analyzed}
    cell_stats = defaultdict(lambda: {"neg": 0, "analyzed": 0})
    persona_samples = {"driver": [], "rider": []}

    for row in sample_rows:
        src = normalize_source(row["FEEDBACK_SOURCE"])
        persona = row["PERSONA"]
        sent = row["sentiment"]
        key = (src, persona)
        if sent != "Unclear":
            cell_stats[key]["analyzed"] += 1
            if sent in ("Negative", "Mixed"):
                cell_stats[key]["neg"] += 1
        persona_samples[persona].append(row)

    def pct_neg(src, persona):
        key = (src, persona)
        stats = cell_stats[key]
        if stats["analyzed"] == 0:
            return "n/a"
        pct = round(100 * stats["neg"] / stats["analyzed"])
        prefix = "~" if stats["analyzed"] < 30 else "~"
        return f"{prefix}{pct}%"

    def vol_cell(src, persona):
        return vol_map.get((src, persona), 0)

    # Theme extraction (negative + mixed only)
    def theme_counts(persona, patterns):
        neg_rows = [r for r in persona_samples[persona]
                    if r["sentiment"] in ("Negative", "Mixed")]
        total = len(persona_samples[persona])
        theme_hits = defaultdict(int)
        for row in neg_rows:
            text = row.get("TEXT_FOR_ANALYSIS") or row.get("TEXT_FOR_ANALYSIS", "")
            # handle lowercase key from snowflake
            text = row.get("TEXT_FOR_ANALYSIS") or row.get("text_for_analysis", "")
            for label, emoji in match_themes(text, patterns):
                theme_hits[(label, emoji)] += 1
        ranked = sorted(theme_hits.items(), key=lambda x: -x[1])
        result = []
        for (label, emoji), count in ranked[:6]:
            pct = round(100 * count / total) if total else 0
            result.append({"label": label, "emoji": emoji, "pct": pct, "count": count})
        return result

    driver_themes = theme_counts("driver", DRIVER_THEMES)
    rider_themes = theme_counts("rider", RIDER_THEMES)

    # Safety flags
    safety_items = []
    for row in sample_rows:
        if row["sentiment"] not in ("Negative", "Mixed"):
            continue
        text = row.get("TEXT_FOR_ANALYSIS") or row.get("text_for_analysis", "")
        for label, emoji in match_safety(text):
            safety_items.append({
                "label": label,
                "emoji": emoji,
                "summary": anonymize(text),
            })

    # Dedupe safety by summary
    seen = set()
    unique_safety = []
    for s in safety_items:
        key = (s["label"], s["summary"][:50])
        if key not in seen:
            seen.add(key)
            unique_safety.append(s)

    # Priorities (derived from themes)
    priorities = []
    if driver_themes:
        top_d = driver_themes[0]
        priorities.append({
            "title": top_d["label"],
            "evidence": f"~{top_d['pct']}% of driver sample",
            "action": "Review driver-facing ops and product flows for this pain point.",
        })
    if rider_themes:
        top_r = rider_themes[0]
        priorities.append({
            "title": top_r["label"],
            "evidence": f"~{top_r['pct']}% of rider sample",
            "action": "Audit rider experience and enforcement for this issue.",
        })
    if len(rider_themes) > 1:
        second_r = rider_themes[1]
        priorities.append({
            "title": second_r["label"],
            "evidence": f"~{second_r['pct']}% of rider sample",
            "action": "Strengthen detection and resolution workflows.",
        })
    elif len(driver_themes) > 1:
        second_d = driver_themes[1]
        priorities.append({
            "title": second_d["label"],
            "evidence": f"~{second_d['pct']}% of driver sample",
            "action": "Strengthen detection and resolution workflows.",
        })
    if len(unique_safety) > 0 and len(priorities) < 3:
        priorities.append({
            "title": "Safety escalations",
            "evidence": f"{len(unique_safety)} safety flags in sample",
            "action": "Prioritize trust & safety review for flagged cases.",
        })
    priorities = priorities[:3]

    # Day name
    from datetime import datetime
    dt = datetime.strptime(REPORT_DATE, "%Y-%m-%d")
    day_name = dt.strftime("%a %d %b %Y")

    output = {
        "report_date": REPORT_DATE,
        "day_name": day_name,
        "total_comments": total_comments,
        "sample_count": sample_count,
        "volumes": {f"{k[0]}_{k[1]}": v for k, v in vol_map.items()},
        "cell_stats": {f"{k[0]}_{k[1]}": v for k, v in cell_stats.items()},
        "driver_themes": driver_themes,
        "rider_themes": rider_themes,
        "safety": unique_safety[:5],
        "safety_count": len(unique_safety),
        "priorities": priorities,
        "pct_neg": {
            "zendesk_driver": pct_neg("Zendesk", "driver"),
            "zendesk_rider": pct_neg("Zendesk", "rider"),
            "ride_rating_driver": pct_neg("ride_rating", "driver"),
            "ride_rating_rider": pct_neg("ride_rating", "rider"),
            "in_app_driver": pct_neg("in_app", "driver"),
            "app_stores_driver": "n/a",
            "app_stores_rider": "n/a",
        },
        "vol_display": {
            "zendesk_driver": vol_cell("Zendesk", "driver"),
            "zendesk_rider": vol_cell("Zendesk", "rider"),
            "ride_rating_driver": vol_cell("ride_rating", "driver"),
            "ride_rating_rider": vol_cell("ride_rating", "rider"),
            "in_app_driver": vol_cell("in_app", "driver"),
            "app_stores_driver": vol_cell("App stores", "driver"),
            "app_stores_rider": vol_cell("App stores", "rider"),
        },
    }
    print(json.dumps(output, indent=2, default=str))


if __name__ == "__main__":
    main()
