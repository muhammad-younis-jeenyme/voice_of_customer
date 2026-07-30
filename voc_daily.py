#!/usr/bin/env python3
"""Voice of Customer daily digest pipeline."""
import json
import os
import re
from collections import defaultdict
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import snowflake.connector

REPORT_DATE = "2026-07-29"
SAMPLE_LIMIT = 100
MIN_COMMENT_LEN = 3

# Theme patterns (EN_TRANSLATION / text_for_analysis)
DRIVER_THEMES = {
    "Passenger conduct": [
        r"\b(rude|abusive|insult|disrespect|bad behavior|misbehav|aggressive passenger|threaten)",
        r"\b(passenger).{0,40}(rude|abusive|insult|threaten|fight|argu)",
        r"\b(no.?show|cancelled? on me|waited).{0,30}(passenger|rider)",
    ],
    "Passenger didn't pay (cash)": [
        r"\b(didn'?t|not|never|refus\w*).{0,20}(pay|paid|payment)",
        r"\b(cash).{0,30}(didn'?t|not|refus|without pay)",
        r"\bnon.?payment\b",
    ],
    "Earnings & fares (low pay per km, weak offers)": [
        r"\b(low|cheap|unfair|poor).{0,20}(fare|price|pay|earning|offer|trip)",
        r"\b(commission|deduct|fee).{0,30}(high|too much|unfair)",
        r"\b(offer|bonus|incentive).{0,30}(low|bad|weak)",
        r"\bper km\b",
    ],
    "Cancellation fees not received / unfair fines": [
        r"\bcancel(l)?ation.{0,30}(fee|fine|compensat|not (received|credited|paid))",
        r"\b(fine|penalt).{0,20}(unfair|wrong|incorrect)",
        r"\bno.?show.{0,30}(fee|compensat|not)",
    ],
    "Fare/charge accuracy": [
        r"\b(fare|charge|amount|price|trip cost).{0,30}(wrong|incorrect|miscalculat|not match|disput)",
        r"\b(route|distance|waiting).{0,30}(not|wrong|chang)",
    ],
    "Payment not credited / withdrawals": [
        r"\b(wallet|balance|transfer|withdraw|payout|credit).{0,30}(not|delay|missing|stuck|pending)",
        r"\b(not|never).{0,20}(credited|received|transferred|withdrawn)",
        r"\bbonus.{0,20}(not|missing|unpaid)",
    ],
    "App / technical issues": [
        r"\b(app|gps|map|login|bug|crash|technical|update|glitch|freeze)",
    ],
}

RIDER_THEMES = {
    "Overcharging / fare disputes": [
        r"\b(overcharg|expensive|high fare|extra (cash|money|charge)|more than|higher than)",
        r"\b(quote|estimate|expected).{0,30}(final|charged|paid|higher|more)",
        r"\b(fare|price|amount).{0,30}(too (high|much)|increas|doubl)",
    ],
    "Driver conduct": [
        r"\b(driver).{0,40}(rude|abusive|insult|unprofessional|bad|aggressive|threaten|angry)",
        r"\b(rude|abusive|unprofessional).{0,30}driver",
        r"\b(bad rating|poor service|terrible driver)",
    ],
    "Wrong driver / car vs app": [
        r"\b(wrong|different|another|mismatch).{0,30}(driver|car|vehicle|plate|photo|name)",
        r"\b(driver|car|vehicle).{0,30}(not match|didn'?t match|different)",
        r"\b(plate|color|model).{0,20}(different|wrong)",
    ],
    "Route manipulation / slow driving": [
        r"\b(long route|detour|longer route|manipulat|took longer|unnecessary)",
        r"\b(slow|speed|reckless|dangerous|speeding).{0,20}(driv)",
        r"\btraffic.{0,20}(on purpose|intentional)",
    ],
    "No-show / wrong pickup": [
        r"\b(no.?show|didn'?t (come|arrive|show)|waiting).{0,30}(driver|pickup)",
        r"\b(wrong (location|pickup|address))",
        r"\b(driver).{0,20}(never came|didn'?t come)",
    ],
    "Cancellation (unfair fees, driver-initiated)": [
        r"\b(cancel(l)?ation|cancelled?).{0,30}(fee|charge|unfair|driver)",
        r"\b(driver).{0,30}(cancel|asked me to cancel|forced)",
    ],
    "Payment & charging errors": [
        r"\b(double charg|charged twice|wrong charg|refund|wallet|deduct)",
        r"\b(payment|card).{0,30}(error|fail|problem|issue)",
        r"\b(change|cash).{0,20}(not returned|didn'?t return)",
    ],
    "Vehicle smell / cleanliness": [
        r"\b(smell|dirty|clean|hygien|odor|stink|filthy|unclean)",
        r"\b(ac|air condition).{0,20}(not work|broken|hot)",
    ],
}

SAFETY_PATTERNS = {
    "Harassment": [
        r"\b(harass|sexual|touch(ed)?|grope|stalk|explicit|inappropriat|flirt|whatsapp.{0,20}(photo|message))",
        r"\b(threaten|held (my )?hand|insult)",
    ],
    "Reckless driving": [
        r"\b(reckless|speeding|dangerous|accident|crash|near.?miss|injur|hurt|child)",
        r"\b(phone|distract).{0,20}(driv|wheel)",
        r"\b(intoxicat|drunk|impair|not sober)",
    ],
    "Identity mismatch": [
        r"\b(wrong|different|mismatch).{0,30}(driver|car|vehicle|plate|photo).{0,40}(safety|unsafe|scared|afraid)",
        r"\b(female|male).{0,20}(driver|showed up).{0,20}(different|wrong)",
    ],
}

ZENDESK_NEUTRAL_CATEGORIES = {
    "city update", "work permit", "general inquiry", "account update",
    "document", "registration", "activation",
}


def connect():
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
    return conn, cur


def normalize_source(src):
    if src in ("Google Store", "Apple Store"):
        return "App stores"
    return src


def classify_sentiment(row):
    sent = (row.get("OVERALL_SENTIMENT") or "").strip()
    if sent in ("Positive", "Neutral", "Negative", "Mixed"):
        return sent

    text = (row.get("TEXT_FOR_ANALYSIS") or "").lower()
    stars = row.get("RATING_STARS")
    source = row.get("FEEDBACK_SOURCE")
    category = (row.get("TICKET_CATEGORY") or row.get("CATEGORY") or "").lower()

    if source == "Zendesk":
        if any(nc in category for nc in ZENDESK_NEUTRAL_CATEGORIES):
            return "Neutral"
        return "Negative"

    pos_words = r"\b(good|great|excellent|thank|perfect|nice|best|love|amazing|professional|polite|clean|fast)\b"
    neg_words = r"\b(bad|terrible|awful|worst|horrible|rude|disappoint|unfair|never|refus|complain|problem|issue|wrong|scam|fraud)\b"

    has_pos = bool(re.search(pos_words, text))
    has_neg = bool(re.search(neg_words, text))

    if stars is not None:
        try:
            s = float(stars)
            if s <= 2:
                if has_pos:
                    return "Mixed"
                return "Negative"
            if s == 3:
                if has_neg and has_pos:
                    return "Mixed"
                return "Neutral" if not has_neg else "Mixed"
            if s >= 4:
                if has_neg:
                    return "Mixed"
                return "Positive"
        except (TypeError, ValueError):
            pass

    if has_pos and has_neg:
        return "Mixed"
    if has_neg:
        return "Negative"
    if has_pos:
        return "Positive"
    if len(text.strip()) < 10:
        return "Unclear"
    return "Neutral"


def match_themes(text, theme_dict):
    text_l = (text or "").lower()
    matched = []
    for theme, patterns in theme_dict.items():
        for pat in patterns:
            if re.search(pat, text_l, re.I):
                matched.append(theme)
                break
    return matched


def match_safety(text, city):
    text_l = (text or "").lower()
    results = []
    for category, patterns in SAFETY_PATTERNS.items():
        for pat in patterns:
            if re.search(pat, text_l, re.I):
                results.append((category, city))
                break
    return results


def pct(n, d, use_tilde=True):
    if d == 0:
        return "n/a"
    p = round(100 * n / d)
    prefix = "~" if use_tilde else ""
    return f"{prefix}{p}%"


def main():
    conn, cur = connect()

    start_ts = f"{REPORT_DATE} 00:00:00"
    end_date = (datetime.strptime(REPORT_DATE, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")

    # Total comments
    cur.execute(
        """
        SELECT COUNT(*) FROM jeeny_prod.general.feedbackevents
        WHERE TIMESTAMP >= %s AND TIMESTAMP < %s
          AND RATINGS_COMMENTS IS NOT NULL AND TRIM(RATINGS_COMMENTS) <> ''
          AND LENGTH(TRIM(RATINGS_COMMENTS)) >= %s
        """,
        (start_ts, f"{end_date} 00:00:00", MIN_COMMENT_LEN),
    )
    total_comments = cur.fetchone()[0]

    # Volume by cell
    cur.execute(
        """
        WITH base AS (
          SELECT FEEDBACK_SOURCE,
            CASE WHEN USER_TYPE = 'driver' THEN 'driver'
                 WHEN USER_TYPE IN ('passenger', 'customer') THEN 'rider'
                 ELSE 'other' END AS persona
          FROM jeeny_prod.general.feedbackevents
          WHERE TIMESTAMP >= %s AND TIMESTAMP < %s
            AND RATINGS_COMMENTS IS NOT NULL AND TRIM(RATINGS_COMMENTS) <> ''
            AND LENGTH(TRIM(RATINGS_COMMENTS)) >= %s
        )
        SELECT FEEDBACK_SOURCE, persona, COUNT(*) AS volume
        FROM base WHERE persona IN ('driver', 'rider')
        GROUP BY 1, 2 ORDER BY volume DESC
        """,
        (start_ts, f"{end_date} 00:00:00", MIN_COMMENT_LEN),
    )
    volume_rows = cur.fetchall()

    # Stratified sample
    cur.execute(
        f"""
        WITH base AS (
          SELECT GUID, FEEDBACK_SOURCE,
            CASE WHEN USER_TYPE = 'driver' THEN 'driver'
                 WHEN USER_TYPE IN ('passenger', 'customer') THEN 'rider'
                 ELSE 'other' END AS persona,
            RATINGS_COMMENTS,
            COALESCE(NULLIF(TRIM(EN_TRANSLATION), ''), RATINGS_COMMENTS) AS text_for_analysis,
            OVERALL_SENTIMENT, RATING_STARS, CITY, COUNTRY,
            TICKET_CATEGORY, CATEGORY, URGENCY, TIMESTAMP,
            ROW_NUMBER() OVER (
              PARTITION BY FEEDBACK_SOURCE,
                CASE WHEN USER_TYPE = 'driver' THEN 'driver'
                     WHEN USER_TYPE IN ('passenger', 'customer') THEN 'rider'
                     ELSE 'other' END
              ORDER BY RANDOM()
            ) AS rn
          FROM jeeny_prod.general.feedbackevents
          WHERE TIMESTAMP >= %s AND TIMESTAMP < %s
            AND RATINGS_COMMENTS IS NOT NULL AND TRIM(RATINGS_COMMENTS) <> ''
            AND LENGTH(TRIM(RATINGS_COMMENTS)) >= %s
        )
        SELECT GUID, FEEDBACK_SOURCE, persona, RATINGS_COMMENTS, text_for_analysis,
               OVERALL_SENTIMENT, RATING_STARS, CITY, COUNTRY, TICKET_CATEGORY, CATEGORY, URGENCY
        FROM base WHERE persona IN ('driver', 'rider') AND rn <= {SAMPLE_LIMIT}
        """,
        (start_ts, f"{end_date} 00:00:00", MIN_COMMENT_LEN),
    )
    cols = [d[0] for d in cur.description]
    sample = [dict(zip(cols, r)) for r in cur.fetchall()]
    conn.close()

    # Normalize keys
    for row in sample:
        row["TEXT_FOR_ANALYSIS"] = row.pop("TEXT_FOR_ANALYSIS") if "TEXT_FOR_ANALYSIS" in row else row.get("TEXT_FOR_ANALYSIS")
        if "TEXT_FOR_ANALYSIS" not in row:
            row["TEXT_FOR_ANALYSIS"] = row.get("TEXT_FOR_ANALYSIS") or row.get("text_for_analysis")

    # Fix column name casing from snowflake
    for row in sample:
        if "text_for_analysis" in row:
            row["TEXT_FOR_ANALYSIS"] = row["text_for_analysis"]

    # Volume aggregation
    vol = defaultdict(lambda: {"driver": 0, "rider": 0})
    for src, persona, count in volume_rows:
        ns = normalize_source(src)
        vol[ns][persona] += count

    # Sentiment per cell
    cell_sent = defaultdict(lambda: {"neg": 0, "total": 0})
    for row in sample:
        src = normalize_source(row["FEEDBACK_SOURCE"])
        persona = row["PERSONA"]
        sent = classify_sentiment(row)
        if sent == "Unclear":
            continue
        key = (src, persona)
        cell_sent[key]["total"] += 1
        if sent in ("Negative", "Mixed"):
            cell_sent[key]["neg"] += 1

    # Theme extraction
    driver_neg = [r for r in sample if r["PERSONA"] == "driver" and classify_sentiment(r) in ("Negative", "Mixed")]
    rider_neg = [r for r in sample if r["PERSONA"] == "rider" and classify_sentiment(r) in ("Negative", "Mixed")]

    driver_theme_counts = defaultdict(int)
    rider_theme_counts = defaultdict(int)
    for r in driver_neg:
        for t in match_themes(r["TEXT_FOR_ANALYSIS"], DRIVER_THEMES):
            driver_theme_counts[t] += 1
    for r in rider_neg:
        for t in match_themes(r["TEXT_FOR_ANALYSIS"], RIDER_THEMES):
            rider_theme_counts[t] += 1

    driver_sample_n = sum(1 for r in sample if r["PERSONA"] == "driver")
    rider_sample_n = sum(1 for r in sample if r["PERSONA"] == "rider")

    def theme_pct(counts, base_n, top_n=6):
        ranked = sorted(counts.items(), key=lambda x: -x[1])[:top_n]
        return [(t, round(100 * c / base_n) if base_n else 0, c) for t, c in ranked]

    # Safety
    safety_hits = []
    for r in sample:
        sent = classify_sentiment(r)
        if sent not in ("Negative", "Mixed"):
            continue
        city = r.get("CITY") or r.get("COUNTRY") or "Unknown"
        for cat, c in match_safety(r["TEXT_FOR_ANALYSIS"], city):
            safety_hits.append({"category": cat, "city": c, "text": r["TEXT_FOR_ANALYSIS"][:200]})

    # Build table rows
    sources_order = ["Zendesk", "ride_rating", "in_app", "App stores"]
    table = {}
    for src in sources_order:
        d_vol = vol[src]["driver"]
        r_vol = vol[src]["rider"]
        d_key = (src, "driver")
        r_key = (src, "rider")
        d_pct = pct(cell_sent[d_key]["neg"], cell_sent[d_key]["total"]) if d_vol else "n/a"
        r_pct = pct(cell_sent[r_key]["neg"], cell_sent[r_key]["total"]) if r_vol else "n/a"
        if src == "App stores":
            d_pct = r_pct = "n/a"
        table[src] = {"d_vol": d_vol, "r_vol": r_vol, "d_pct": d_pct, "r_pct": r_pct}

    report_day = datetime.strptime(REPORT_DATE, "%Y-%m-%d").strftime("%a %d %b %Y")

    result = {
        "report_date": REPORT_DATE,
        "report_day_name": report_day,
        "total_comments": total_comments,
        "sample_count": len(sample),
        "volume": dict(vol),
        "table": table,
        "driver_themes": theme_pct(driver_theme_counts, driver_sample_n),
        "rider_themes": theme_pct(rider_theme_counts, rider_sample_n, 7),
        "driver_sample_n": driver_sample_n,
        "rider_sample_n": rider_sample_n,
        "safety_count": len(safety_hits),
        "safety_examples": safety_hits[:5],
        "cell_sent": {f"{k[0]}|{k[1]}": v for k, v in cell_sent.items()},
    }
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
