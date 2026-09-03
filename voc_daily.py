#!/usr/bin/env python3
"""VoC daily digest — Snowflake pull, sentiment, themes, safety flags."""

import json
import os
import re
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import snowflake.connector

SAMPLE_PER_CELL = 100
MIN_COMMENT_LEN = 3

# Zendesk categories treated as neutral/ops (not auto-negative)
ZENDESK_NEUTRAL_CATEGORIES = {
    "city update", "work permit", "lost item", "lost items", "found item",
    "document", "registration", "account update", "general inquiry",
}

# Theme keyword patterns: (canonical_label, persona, keywords)
THEME_PATTERNS = [
    ("Passenger conduct", "driver", [
        r"passenger.*(rude|abusive|aggressive|drunk|smok|fight|insult|harass|bad behavior)",
        r"(rude|abusive|aggressive|drunk|smok|fight|insult).*(passenger|customer|rider)",
        r"passenger conduct", r"bad passenger", r"rude passenger", r"passenger was rude",
        r"passenger.*(scream|yell|spit|hit|attack)", r"inappropriate passenger",
    ]),
    ("Passenger didn't pay (cash)", "driver", [
        r"didn.?t pay", r"not pay", r"refus.*pay", r"no pay", r"without pay",
        r"cash.*(not|didn|refus)", r"passenger.*cash", r"unpaid", r"fare.*not.*paid",
    ]),
    ("Earnings & fares (low pay per km, weak offers)", "driver", [
        r"low (fare|pay|earning|income|price)", r"earn.*low", r"pay.*low",
        r"commission", r"surge", r"bonus", r"incentive", r"offer.*weak",
        r"per km", r"per kilometer", r"fare.*decrease", r"unfair.*fare",
        r"payment.*low", r"not worth", r"underpaid",
    ]),
    ("Cancellation fees not received / unfair fines", "driver", [
        r"cancellation fee", r"cancel.*fee", r"fine", r"penalty", r"deduct",
        r"unfair.*cancel", r"fee.*not.*receiv", r"no.*compensation.*cancel",
    ]),
    ("Fare/charge accuracy", "driver", [
        r"wrong (fare|charge|amount|deduction)", r"incorrect.*(fare|charge|deduction)",
        r"overcharg.*driver", r"fare.*error", r"charge.*error", r"deduction.*wrong",
    ]),
    ("Payment not credited / withdrawals", "driver", [
        r"withdraw", r"wallet", r"not credited", r"payment.*not.*receiv",
        r"money.*not", r"balance.*wrong", r"payout", r"transfer.*fail",
        r"bank.*transfer", r"cashout", r"can.?t withdraw",
    ]),
    ("Overcharging / fare disputes", "rider", [
        r"overcharg", r"too (much|expensive|high)", r"fare.*high", r"charged.*more",
        r"double charg", r"extra charg", r"unfair.*fare", r"fare.*dispute",
        r"bill.*high", r"expensive ride", r"price.*high",
    ]),
    ("Driver conduct", "rider", [
        r"rude driver", r"driver.*(rude|abusive|aggressive|unprofessional|bad)",
        r"(rude|abusive|aggressive).*(driver)", r"driver conduct", r"bad driver",
        r"driver.*(yell|scream|insult|threaten|harass)", r"unprofessional",
        r"driver.*attitude", r"impolite",
    ]),
    ("Wrong driver / car vs app", "rider", [
        r"wrong (driver|car|vehicle|plate)", r"different (car|driver|vehicle)",
        r"car.*not.*match", r"driver.*not.*match", r"plate.*wrong",
        r"identity.*mismatch", r"someone else", r"not the same car",
    ]),
    ("Route manipulation / slow driving", "rider", [
        r"long route", r"detour", r"longer route", r"route.*manipul",
        r"slow driv", r"took longer", r"unnecessary.*route", r"circled",
        r"extra.*km", r"wrong route", r"traffic.*on purpose",
    ]),
    ("No-show / wrong pickup", "rider", [
        r"no.?show", r"wrong pickup", r"pickup.*wrong", r"didn.?t come",
        r"driver.*not.*arriv", r"waited.*long", r"never showed",
    ]),
    ("Cancellation (unfair fees, driver-initiated)", "rider", [
        r"cancel.*fee", r"unfair.*cancel", r"driver.*cancel", r"cancelled.*ride",
        r"cancellation.*charg", r"charged.*cancel", r"driver.*ask.*cancel",
    ]),
    ("Payment & charging errors", "rider", [
        r"payment.*fail", r"charged.*twice", r"double.*charg", r"card.*charg",
        r"wallet.*error", r"refund", r"payment.*error", r"not.*refund",
    ]),
    ("Vehicle smell / cleanliness", "rider", [
        r"smell", r"dirty", r"clean", r"hygien", r"odor", r"stink",
        r"filthy", r"unclean", r"cigarette", r"smoking.*car",
    ]),
]

SAFETY_PATTERNS = [
    ("Harassment", [
        r"harass", r"assault", r"held (my )?hand", r"threaten", r"insult",
        r"touch", r"inappropriate", r"sexual", r"stalk", r"molest",
    ]),
    ("Reckless driving", [
        r"reckless", r"speed", r"accident", r"crash", r"injur", r"child.*injur",
        r"dangerous driv", r"almost hit", r"ran.*red",
    ]),
    ("Identity mismatch", [
        r"wrong (driver|car)", r"different (driver|car)", r"not.*same.*(driver|car)",
        r"identity", r"fake driver", r"someone else.*driv",
    ]),
]

ARABIC_NEGATIVE = [
    "سيء", "سيئة", "سيئ", "فظيع", "اسوأ", "أسوأ", "غبي", "كذاب", "نصاب",
    "سرقة", "محتال", "لا انصح", "لا أنصح", "تأخير", "متأخر", "وقح", "بخيل",
    "مزعج", "مخرب", "قذر", "كريه", "غش", "احتيال",
]

ZENDESK_TICKET_CATEGORY_MAP = {
    "passenger conduct": ("Passenger conduct", "driver"),
    "passenger didn't pay": ("Passenger didn't pay (cash)", "driver"),
    "earnings": ("Earnings & fares (low pay per km, weak offers)", "driver"),
    "cancellation": ("Cancellation fees not received / unfair fines", "driver"),
    "fare": ("Fare/charge accuracy", "driver"),
    "payment": ("Payment not credited / withdrawals", "driver"),
    "withdrawal": ("Payment not credited / withdrawals", "driver"),
    "driver conduct": ("Driver conduct", "rider"),
    "overcharg": ("Overcharging / fare disputes", "rider"),
    "route": ("Route manipulation / slow driving", "rider"),
    "wrong driver": ("Wrong driver / car vs app", "rider"),
    "wrong car": ("Wrong driver / car vs app", "rider"),
    "clean": ("Vehicle smell / cleanliness", "rider"),
    "smell": ("Vehicle smell / cleanliness", "rider"),
}


def connect():
    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PAT"],
        role=os.environ.get("SNOWFLAKE_ROLE", "ROLE_PRODUCT"),
        database=os.environ.get("SNOWFLAKE_DATABASE", "JEENY_PROD"),
    )
    cur = conn.cursor()
    cur.execute(f"USE WAREHOUSE {os.environ.get('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH')}")
    return conn, cur


def classify_sentiment(row):
    """Classify sentiment for a single row."""
    existing = (row.get("OVERALL_SENTIMENT") or "").strip()
    if existing in ("Positive", "Neutral", "Negative", "Mixed"):
        return existing

    source = row.get("FEEDBACK_SOURCE") or ""
    text = (row.get("TEXT_FOR_ANALYSIS") or "").strip()
    rating = row.get("RATING_STARS")
    ticket_cat = (row.get("TICKET_CATEGORY") or "").lower()
    text_lower = text.lower()

    # Zendesk default negative unless neutral category
    if source == "Zendesk":
        if any(nc in ticket_cat for nc in ZENDESK_NEUTRAL_CATEGORIES):
            return "Neutral"
        return "Negative"

    if not text or len(text) < 3:
        if rating is not None:
            if rating <= 2:
                return "Negative"
            if rating == 3:
                return "Neutral"
            if rating >= 4:
                return "Positive"
        return "Unclear"

    # Arabic keyword check for short comments
    if len(text) < 5:
        for kw in ARABIC_NEGATIVE:
            if kw in text:
                return "Negative"
        if rating is not None:
            if rating <= 2:
                return "Negative"
            if rating >= 4:
                return "Positive"
        return "Unclear"

    pos_signals = [
        r"\b(great|excellent|good|amazing|wonderful|perfect|thank|thanks|love|best|awesome|nice|helpful|professional|clean|fast|polite)\b",
        r"\b(ممتاز|رائع|جيد|شكرا|شكراً|مشكور)\b",
    ]
    neg_signals = [
        r"\b(bad|terrible|awful|horrible|worst|hate|disgusting|rude|scam|fraud|cheat|unfair|slow|dirty|smell|never again|disappoint)\b",
        r"\b(سيء|فظيع|اسوأ|أسوأ|نصاب|سرقة|احتيال|وقح|قذر)\b",
        r"didn.?t", r"not (good|working|paid|help)", r"no (show|pay|refund)",
        r"overcharg", r"cancel", r"fine", r"complain",
    ]

    pos_count = sum(1 for p in pos_signals if re.search(p, text_lower, re.I))
    neg_count = sum(1 for p in neg_signals if re.search(p, text_lower, re.I))

    if pos_count > 0 and neg_count > 0:
        sentiment = "Mixed"
    elif neg_count > pos_count:
        sentiment = "Negative"
    elif pos_count > neg_count:
        sentiment = "Positive"
    elif neg_count > 0:
        sentiment = "Negative"
    elif pos_count > 0:
        sentiment = "Positive"
    else:
        sentiment = "Neutral"

    # Rating tie-break
    if rating is not None:
        if rating <= 2 and sentiment in ("Neutral", "Positive", "Unclear"):
            sentiment = "Negative"
        elif rating >= 4 and sentiment in ("Neutral", "Unclear"):
            sentiment = "Positive"
        elif rating == 3 and sentiment == "Unclear":
            sentiment = "Neutral"

    return sentiment


def extract_themes(row, persona):
    """Return set of canonical theme labels for a row."""
    themes = set()
    text = (row.get("TEXT_FOR_ANALYSIS") or "").lower()
    ticket_cat = (row.get("TICKET_CATEGORY") or "").lower()
    category = (row.get("CATEGORY") or "").lower()

    for label, p, patterns in THEME_PATTERNS:
        if p != persona:
            continue
        for pat in patterns:
            if re.search(pat, text, re.I):
                themes.add(label)
                break

    combined = f"{ticket_cat} {category}"
    for key, (label, p) in ZENDESK_TICKET_CATEGORY_MAP.items():
        if p == persona and key in combined:
            themes.add(label)

    return themes


def extract_safety(row):
    """Return list of (category, anonymized_summary) for safety flags."""
    text = (row.get("TEXT_FOR_ANALYSIS") or "").strip()
    if not text:
        return []
    text_lower = text.lower()
    results = []
    for category, patterns in SAFETY_PATTERNS:
        for pat in patterns:
            if re.search(pat, text_lower, re.I):
                # Anonymize: truncate and remove potential PII
                summary = re.sub(r"\b\d{8,}\b", "[redacted]", text)
                summary = re.sub(r"\b[A-Z][a-z]+ [A-Z][a-z]+\b", "[person]", summary)
                if len(summary) > 120:
                    summary = summary[:117] + "..."
                results.append((category, summary))
                break
    return results


def normalize_source(src):
    if src in ("Google Store", "Apple Store"):
        return "App stores"
    return src


def main():
    tz = ZoneInfo("Asia/Riyadh")
    if len(sys.argv) > 1:
        report_date = datetime.strptime(sys.argv[1], "%Y-%m-%d").date()
    else:
        report_date = (datetime.now(tz) - timedelta(days=1)).date()

    start_ts = f"{report_date} 00:00:00"
    end_ts = report_date + timedelta(days=1)
    end_ts_str = f"{end_ts} 00:00:00"
    report_day_name = report_date.strftime("%a %d %b %Y")

    conn, cur = connect()

    volume_sql = """
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
      WHERE TIMESTAMP >= %s
        AND TIMESTAMP < %s
        AND RATINGS_COMMENTS IS NOT NULL
        AND TRIM(RATINGS_COMMENTS) <> ''
        AND LENGTH(TRIM(RATINGS_COMMENTS)) >= %s
    )
    SELECT FEEDBACK_SOURCE, persona, COUNT(*) AS volume
    FROM base
    WHERE persona IN ('driver', 'rider')
    GROUP BY 1, 2
    ORDER BY volume DESC
  """

    total_sql = """
    SELECT COUNT(*) AS total_comments
    FROM jeeny_prod.general.feedbackevents
    WHERE TIMESTAMP >= %s
      AND TIMESTAMP < %s
      AND RATINGS_COMMENTS IS NOT NULL
      AND TRIM(RATINGS_COMMENTS) <> ''
      AND LENGTH(TRIM(RATINGS_COMMENTS)) >= %s
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
      WHERE TIMESTAMP >= %s
        AND TIMESTAMP < %s
        AND RATINGS_COMMENTS IS NOT NULL
        AND TRIM(RATINGS_COMMENTS) <> ''
        AND LENGTH(TRIM(RATINGS_COMMENTS)) >= %s
    )
    SELECT GUID, FEEDBACK_SOURCE, persona, RATINGS_COMMENTS, text_for_analysis,
           OVERALL_SENTIMENT, RATING_STARS, CITY, COUNTRY, TICKET_CATEGORY,
           CATEGORY, URGENCY, TIMESTAMP
    FROM base
    WHERE persona IN ('driver', 'rider')
      AND rn <= {SAMPLE_PER_CELL}
  """

    cur.execute(total_sql, (start_ts, end_ts_str, MIN_COMMENT_LEN))
    total_comments = cur.fetchone()[0]

    cur.execute(volume_sql, (start_ts, end_ts_str, MIN_COMMENT_LEN))
    volume_rows = cur.fetchall()
    volumes = defaultdict(lambda: {"driver": 0, "rider": 0})
    for src, persona, vol in volume_rows:
        ns = normalize_source(src)
        volumes[ns][persona] += vol

    cur.execute(sample_sql, (start_ts, end_ts_str, MIN_COMMENT_LEN))
    cols = [d[0] for d in cur.description]
    sample = []
    for row in cur.fetchall():
        r = {k.upper(): v for k, v in zip(cols, row)}
        if "TEXT_FOR_ANALYSIS" not in r:
            r["TEXT_FOR_ANALYSIS"] = r.get("TEXT_FOR_ANALYSIS")
        sample.append(r)

    conn.close()

    # Sentiment per cell
    cell_sentiment = defaultdict(lambda: {"negative": 0, "analyzed": 0})
    persona_neg_mixed = {"driver": [], "rider": []}
    theme_counts = {"driver": defaultdict(int), "rider": defaultdict(int)}
    persona_sample_size = {"driver": 0, "rider": 0}
    safety_flags = []

    for row in sample:
        persona = row["PERSONA"]
        src = normalize_source(row["FEEDBACK_SOURCE"])
        sentiment = classify_sentiment(row)
        persona_sample_size[persona] += 1

        cell_key = (src, persona)
        if sentiment != "Unclear":
            cell_sentiment[cell_key]["analyzed"] += 1
            if sentiment in ("Negative", "Mixed"):
                cell_sentiment[cell_key]["negative"] += 1

        if sentiment in ("Negative", "Mixed"):
            persona_neg_mixed[persona].append(row)
            for theme in extract_themes(row, persona):
                theme_counts[persona][theme] += 1
            for cat, summary in extract_safety(row):
                safety_flags.append({"category": cat, "summary": summary, "persona": persona})

    def pct_neg(src, persona):
        key = (src, persona)
        data = cell_sentiment[key]
        if data["analyzed"] == 0:
            return "n/a"
        pct = round(100 * data["negative"] / data["analyzed"])
        prefix = "~" if data["analyzed"] < 30 else ""
        return f"{prefix}{pct}%"

    def theme_pct(persona, label):
        denom = persona_sample_size[persona]
        if denom == 0:
            return 0
        return round(100 * theme_counts[persona][label] / denom)

    def top_themes(persona, n=6):
        items = sorted(theme_counts[persona].items(), key=lambda x: -x[1])
        return [(label, theme_pct(persona, label)) for label, _ in items[:n]]

    # Dedupe safety summaries
    seen_safety = set()
    unique_safety = []
    for s in safety_flags:
        key = (s["category"], s["summary"][:80])
        if key not in seen_safety:
            seen_safety.add(key)
            unique_safety.append(s)

    result = {
        "report_date": str(report_date),
        "report_day_name": report_day_name,
        "total_comments": total_comments,
        "sample_size": len(sample),
        "volumes": dict(volumes),
        "cell_sentiment": {f"{k[0]}|{k[1]}": v for k, v in cell_sentiment.items()},
        "pct_neg": {
            "Zendesk_driver": pct_neg("Zendesk", "driver"),
            "Zendesk_rider": pct_neg("Zendesk", "rider"),
            "ride_rating_driver": pct_neg("ride_rating", "driver"),
            "ride_rating_rider": pct_neg("ride_rating", "rider"),
            "in_app_driver": pct_neg("in_app", "driver"),
            "in_app_rider": pct_neg("in_app", "rider"),
            "App stores_driver": pct_neg("App stores", "driver"),
            "App stores_rider": pct_neg("App stores", "rider"),
        },
        "driver_themes": top_themes("driver"),
        "rider_themes": top_themes("rider"),
        "safety_count": len(unique_safety),
        "safety_examples": unique_safety[:3],
        "persona_sample_size": persona_sample_size,
    }

    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
