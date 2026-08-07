#!/usr/bin/env python3
"""Voice of Customer daily digest — Snowflake pull + sentiment/theme analysis."""

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
RIYADH = ZoneInfo("Asia/Riyadh")

# Zendesk categories treated as neutral/ops (not default-negative)
ZENDESK_NEUTRAL_CATEGORIES = {
    "city update", "work permit", "lost item", "lost & found", "lost and found",
    "general inquiry", "account update", "document", "registration",
    "want to update my operating city", "passenger lost an item", "work permit policies",
    "updating my personal photo", "personal photo", "phone number", "car and license",
    "new car information", "driver's license", "stc pay", "voucher",
    "my account has been blocked",
}

SAFETY_KEYWORDS = [
    (r"\bharass", "Harassment"),
    (r"\bassault", "Harassment"),
    (r"held (my |her |his )?hand", "Harassment"),
    (r"\bthreaten", "Harassment"),
    (r"\binsult", "Harassment"),
    (r"\breckless", "Reckless driving"),
    (r"\binjur", "Reckless driving"),
    (r"child (was )?injur", "Reckless driving"),
    (r"\bstalk", "Harassment"),
    (r"repeated (calls|messages)", "Harassment"),
    (r"wrong (driver|car|vehicle)", "Identity mismatch"),
    (r"different (driver|car|vehicle)", "Identity mismatch"),
    (r"not the (driver|car)", "Identity mismatch"),
    (r"\bhit\b", "Reckless driving"),
]

# Zendesk TICKET_CATEGORY → (label, emoji) per persona
ZENDESK_DRIVER_CAT_MAP = {
    "passenger did not pay": ("Passenger didn't pay (cash)", "💵"),
    "did not get paid the cancellation": ("Cancellation fees not received / unfair fines", "🚫"),
    "ride value is less than actual": ("Earnings & fares (low pay per km, weak offers)", "💰"),
    "passenger's behaviour": ("Passenger conduct", "😡"),
    "passenger behaviour": ("Passenger conduct", "😡"),
    "amount has been deducted": ("Fare/charge accuracy", "📄"),
    "did not receive credit card payment": ("Payment not credited / withdrawals", "🏦"),
    "did not receive discount amount": ("Payment not credited / withdrawals", "🏦"),
    "funds have not been transferred": ("Payment not credited / withdrawals", "🏦"),
    "ride value is higher than actual": ("Fare/charge accuracy", "📄"),
    "did not win although the target": ("Earnings & fares (low pay per km, weak offers)", "💰"),
    "intercity surcharge": ("Earnings & fares (low pay per km, weak offers)", "💰"),
}

ZENDESK_RIDER_CAT_MAP = {
    "price was way higher": ("Overcharging / fare disputes", "💸"),
    "driver charged me a higher": ("Overcharging / fare disputes", "💸"),
    "driver overcharged": ("Overcharging / fare disputes", "💸"),
    "driver took a long route": ("Route manipulation / slow driving", "🗺️"),
    "charges to increase": ("Route manipulation / slow driving", "🗺️"),
    "driver was rude": ("Driver conduct", "😡"),
    "driver was not matching": ("Wrong driver / car vs app", "🚙"),
    "car was not matching": ("Wrong driver / car vs app", "🚙"),
    "driver/c car did not match": ("Wrong driver / car vs app", "🚙"),
    "did not show up at the pickup": ("No-show / wrong pickup", "📍"),
    "driver asked to cancel": ("Cancellation (unfair fees, driver-initiated)", "🚫"),
    "driver cancelled and i was charged": ("Cancellation (unfair fees, driver-initiated)", "🚫"),
    "driver cancelled without reaching": ("Cancellation (unfair fees, driver-initiated)", "🚫"),
    "driver started/ended the ride before": ("Cancellation (unfair fees, driver-initiated)", "🚫"),
    "charged multiple times": ("Payment & charging errors", "🏦"),
    "authorized amount not reversed": ("Payment & charging errors", "🏦"),
    "car is dirty": ("Vehicle smell / cleanliness", "🧼"),
    "bad smell": ("Vehicle smell / cleanliness", "🧼"),
    "driver was driving recklessly": ("Driver conduct", "😡"),
    "driver discriminated": ("Driver conduct", "😡"),
    "serious incident": ("Driver conduct", "😡"),
    "wallet through credit card": ("Payment & charging errors", "🏦"),
    "technical issue": ("Payment & charging errors", "🏦"),
}

DRIVER_TEXT_THEMES = [
    (r"cancel fee|cancellation fee|did not get paid|unfair fine|غرامة|إلغاء", "Cancellation fees not received / unfair fines", "🚫"),
    (r"earn|low pay|pay per km|weak offer|fare too low|income|commission|أجر|راتب|قيمة الرحلة", "Earnings & fares (low pay per km, weak offers)", "💰"),
    (r"passenger|rider|راكب|سلوك|conduct|behav|rude|aggressive|وقح|غير أخلاق", "Passenger conduct", "😡"),
    (r"didn.?t pay|not pay|no pay|refused to pay|no payment|مادفع|لم يدفع|لم يدفع", "Passenger didn't pay (cash)", "💵"),
    (r"deduct|wrong fare|charge accuracy|overcharge|خصم|مبلغ", "Fare/charge accuracy", "📄"),
    (r"withdraw|wallet|not credited|balance|payout|محفظة|تحويل", "Payment not credited / withdrawals", "🏦"),
]

RIDER_TEXT_THEMES = [
    (r"overcharg|fare dispute|charged more|extra charge|price shock|زياده|زيادة|أكثر|مبلغ زياده|بريزه", "Overcharging / fare disputes", "💸"),
    (r"driver conduct|rude|aggressive|bad attitude|impolite|وقح|سلوك|تجربة سيئ", "Driver conduct", "😡"),
    (r"wrong (driver|car|vehicle)|different (driver|car)|not the (driver|car)|سيارة|سائق مختلف", "Wrong driver / car vs app", "🚙"),
    (r"long route|slow driv|took longer|unnecessary route|detour|طريق اطول|طريق طويل", "Route manipulation / slow driving", "🗺️"),
    (r"no.?show|wrong pickup|pickup point|لم ياتي|لم يأتي|نقطة الانطلاق", "No-show / wrong pickup", "📍"),
    (r"cancel|cancellation fee|unfair fee|driver cancel|إلغاء|رسوم", "Cancellation (unfair fees, driver-initiated)", "🚫"),
    (r"charged twice|payment error|wallet|card (declin|fail)|billing|محفظة|دفع", "Payment & charging errors", "🏦"),
    (r"smell|dirty|cleanliness|hygiene|stink|رائحة|نظافة", "Vehicle smell / cleanliness", "🧼"),
]


def _text(row):
    """Lowercase analysis text from Snowflake row."""
    t = row.get("TEXT_FOR_ANALYSIS") or row.get("text_for_analysis") or ""
    return str(t).lower().strip()


def connect():
    conn = snowflake.connector.connect(
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PAT"],
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        database=os.environ.get("SNOWFLAKE_DATABASE", "jeeny_prod"),
        role=os.environ.get("SNOWFLAKE_ROLE"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
    )
    cur = conn.cursor()
    cur.execute(f"USE WAREHOUSE {os.environ.get('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH')}")
    return conn, cur


def classify_sentiment(row):
    existing = row.get("OVERALL_SENTIMENT") or row.get("overall_sentiment")
    if existing and str(existing).strip() in ("Positive", "Neutral", "Negative", "Mixed"):
        return str(existing).strip()

    text = _text(row)
    stars = row.get("RATING_STARS") or row.get("rating_stars")
    source = (row.get("FEEDBACK_SOURCE") or row.get("feedback_source") or "").lower()
    ticket_cat = (row.get("TICKET_CATEGORY") or row.get("ticket_category") or "").lower()

    # Zendesk default negative unless neutral category
    if source == "zendesk":
        if any(n in ticket_cat for n in ZENDESK_NEUTRAL_CATEGORIES):
            return "Neutral"
        return "Negative"

    pos_kw = r"\b(great|excellent|good|thank|love|amazing|perfect|wonderful|best|happy|satisfied|professional|polite)\b"
    neg_kw = r"\b(bad|terrible|awful|worst|horrible|disappoint|frustrat|angry|unfair|refuse|scam|cheat|never again|disgust)\b"

    pos = bool(re.search(pos_kw, text))
    neg = bool(re.search(neg_kw, text))

    if stars is not None:
        try:
            s = int(stars)
            if s <= 2:
                neg = True
            elif s >= 4:
                pos = True
            elif s == 3:
                if not pos and not neg:
                    return "Neutral"
        except (TypeError, ValueError):
            pass

    if pos and neg:
        return "Mixed"
    if neg:
        return "Negative"
    if pos:
        return "Positive"
    if len(text) < 10:
        return "Unclear"
    return "Neutral"


def match_text_themes(text, theme_defs):
    matched = []
    for pattern, label, emoji in theme_defs:
        if re.search(pattern, text, re.I):
            if label not in [m[0] for m in matched]:
                matched.append((label, emoji))
    return matched


def match_zendesk_category(ticket_cat, cat_map):
    if not ticket_cat:
        return []
    tc = ticket_cat.lower().strip()
    matched = []
    for key, (label, emoji) in cat_map.items():
        if key in tc:
            if label not in [m[0] for m in matched]:
                matched.append((label, emoji))
    return matched


def extract_themes_for_row(row):
    """Return list of (label, emoji) for a negative/mixed row."""
    persona = row.get("PERSONA", "")
    source = (row.get("FEEDBACK_SOURCE") or "").lower()
    text = _text(row)
    ticket_cat = row.get("TICKET_CATEGORY") or ""
    themes = []

    if source == "zendesk":
        if ticket_cat.lower().strip() == "bad rating":
            themes = match_text_themes(
                text,
                DRIVER_TEXT_THEMES if persona == "driver" else RIDER_TEXT_THEMES,
            )
        else:
            cat_map = ZENDESK_DRIVER_CAT_MAP if persona == "driver" else ZENDESK_RIDER_CAT_MAP
            themes = match_zendesk_category(ticket_cat, cat_map)
            if not themes:
                themes = match_text_themes(
                    text,
                    DRIVER_TEXT_THEMES if persona == "driver" else RIDER_TEXT_THEMES,
                )
    else:
        themes = match_text_themes(
            text,
            DRIVER_TEXT_THEMES if persona == "driver" else RIDER_TEXT_THEMES,
        )

    # ride_rating 1-2 stars with no keyword match: infer from stars
    stars = row.get("RATING_STARS")
    if not themes and source == "ride_rating" and stars in (1, 2):
        if persona == "driver":
            themes = [("Passenger conduct", "😡")]
        else:
            themes = [("Driver conduct", "😡")]

    return themes


def anonymize(text, max_len=120):
    t = str(text or "")
    t = re.sub(r"\b\d{8,}\b", "[ID]", t)
    t = re.sub(r"\b0?5\d{8}\b", "[phone]", t)
    t = re.sub(r"\b[A-Z][a-z]+ [A-Z][a-z]+\b", "a user", t)
    t = re.sub(r"other :: [a-z-]+", "", t)
    t = re.sub(r"\s+", " ", t).strip()
    if len(t) > max_len:
        t = t[:max_len - 3] + "..."
    return t


SAFETY_CAT_MAP = {
    "driver was driving recklessly": "Reckless driving",
    "driver was not matching the profile": "Identity mismatch",
    "car was not matching the information": "Identity mismatch",
    "driver/car did not match the app": "Identity mismatch",
    "driver discriminated": "Harassment",
}


def detect_safety(row):
    text = _text(row)
    ticket_cat = (row.get("TICKET_CATEGORY") or "").lower()
    for key, category in SAFETY_CAT_MAP.items():
        if key in ticket_cat:
            summary = anonymize(row.get("TEXT_FOR_ANALYSIS") or row.get("RATINGS_COMMENTS"))
            if summary:
                return {"category": category, "summary": summary}
    for pattern, category in SAFETY_KEYWORDS:
        if re.search(pattern, text, re.I):
            return {
                "category": category,
                "summary": anonymize(row.get("TEXT_FOR_ANALYSIS") or row.get("RATINGS_COMMENTS")),
            }
    return None


def main():
    report_date = (datetime.now(RIYADH) - timedelta(days=1)).date()
    if len(sys.argv) > 1:
        report_date = datetime.strptime(sys.argv[1], "%Y-%m-%d").date()

    start_ts = datetime(report_date.year, report_date.month, report_date.day, tzinfo=RIYADH)
    end_ts = start_ts + timedelta(days=1)
    day_name = start_ts.strftime("%a %d %b %Y")

    base_filter = """
        TIMESTAMP >= %(start)s AND TIMESTAMP < %(end)s
        AND RATINGS_COMMENTS IS NOT NULL
        AND TRIM(RATINGS_COMMENTS) <> ''
        AND LENGTH(TRIM(RATINGS_COMMENTS)) >= %(min_len)s
    """
    params = {"start": start_ts.strftime("%Y-%m-%d %H:%M:%S"),
              "end": end_ts.strftime("%Y-%m-%d %H:%M:%S"),
              "min_len": MIN_COMMENT_LEN,
              "sample": SAMPLE_PER_CELL}

    conn, cur = connect()

    cur.execute(f"""
        SELECT COUNT(*) AS total_comments
        FROM jeeny_prod.general.feedbackevents
        WHERE {base_filter}
    """, params)
    total_comments = cur.fetchone()[0]

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
            GUID, CITY, COUNTRY, TICKET_CATEGORY, CATEGORY, URGENCY, TIMESTAMP
          FROM jeeny_prod.general.feedbackevents
          WHERE {base_filter}
        )
        SELECT FEEDBACK_SOURCE, persona, COUNT(*) AS volume
        FROM base
        WHERE persona IN ('driver', 'rider')
        GROUP BY 1, 2
        ORDER BY volume DESC
    """, params)
    volumes = {(r[0], r[1]): r[2] for r in cur.fetchall()}

    cur.execute(f"""
        WITH base AS (
          SELECT
            GUID, FEEDBACK_SOURCE,
            CASE
              WHEN USER_TYPE = 'driver' THEN 'driver'
              WHEN USER_TYPE IN ('passenger', 'customer') THEN 'rider'
              ELSE 'other'
            END AS persona,
            RATINGS_COMMENTS,
            COALESCE(NULLIF(TRIM(EN_TRANSLATION), ''), RATINGS_COMMENTS) AS text_for_analysis,
            OVERALL_SENTIMENT, RATING_STARS, CITY, COUNTRY,
            TICKET_CATEGORY, CATEGORY, URGENCY, TIMESTAMP,
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
          WHERE {base_filter}
        )
        SELECT GUID, FEEDBACK_SOURCE, persona, RATINGS_COMMENTS, text_for_analysis,
               OVERALL_SENTIMENT, RATING_STARS, CITY, COUNTRY,
               TICKET_CATEGORY, CATEGORY, URGENCY, TIMESTAMP
        FROM base
        WHERE persona IN ('driver', 'rider') AND rn <= %(sample)s
    """, params)

    cols = [d[0] for d in cur.description]
    rows = []
    for r in cur.fetchall():
        row = {c.upper(): v for c, v in zip(cols, r)}
        rows.append(row)
    conn.close()

    # Normalize volumes for digest table
    def norm_source(src):
        if src in ("Google Store", "Apple Store"):
            return "App stores"
        return src

    digest_volumes = defaultdict(lambda: {"driver": 0, "rider": 0})
    for (src, persona), vol in volumes.items():
        ds = norm_source(src)
        digest_volumes[ds][persona] += vol

    # Sentiment per cell
    cells = defaultdict(list)
    for row in rows:
        src = norm_source(row["FEEDBACK_SOURCE"])
        persona = row["PERSONA"]
        sentiment = classify_sentiment(row)
        row["sentiment"] = sentiment
        cells[(src, persona)].append(row)

    def pct_neg(cell_rows):
        analyzed = [r for r in cell_rows if r["sentiment"] != "Unclear"]
        if not analyzed:
            return "n/a", 0
        neg = sum(1 for r in analyzed if r["sentiment"] in ("Negative", "Mixed"))
        pct = round(100 * neg / len(analyzed))
        prefix = "~" if len(analyzed) < 30 else "~"
        return f"{prefix}{pct}%", pct

    sentiment_table = {}
    for src in ["Zendesk", "ride_rating", "in_app", "App stores"]:
        for persona in ["driver", "rider"]:
            key = (src, persona)
            cell_rows = cells.get(key, [])
            pct_str, _ = pct_neg(cell_rows)
            vol = digest_volumes[src][persona]
            sentiment_table[key] = {"vol": vol, "pct_neg": pct_str, "sample": len(cell_rows)}

    # Theme extraction (negative + mixed only, per persona)
    driver_neg = [r for r in rows if r["PERSONA"] == "driver" and r["sentiment"] in ("Negative", "Mixed")]
    rider_neg = [r for r in rows if r["PERSONA"] == "rider" and r["sentiment"] in ("Negative", "Mixed")]

    def theme_counts(neg_rows, persona):
        counts = defaultdict(int)
        for r in neg_rows:
            for label, emoji in extract_themes_for_row(r):
                counts[(label, emoji)] += 1
        denom = len([r for r in rows if r["PERSONA"] == persona])
        ranked = sorted(counts.items(), key=lambda x: -x[1])
        result = []
        for (label, emoji), cnt in ranked[:7]:
            pct = round(100 * cnt / denom) if denom else 0
            prefix = "~" if denom < 30 else "~"
            result.append({"label": label, "emoji": emoji, "pct": f"{prefix}{pct}%", "count": cnt})
        return result, denom

    driver_themes, driver_denom = theme_counts(driver_neg, "driver")
    rider_themes, rider_denom = theme_counts(rider_neg, "rider")

    # Safety escalations
    safety = []
    seen = set()
    for r in rows:
        if r["sentiment"] not in ("Negative", "Mixed"):
            continue
        hit = detect_safety(r)
        if hit and hit["summary"] not in seen:
            seen.add(hit["summary"])
            safety.append(hit)

    out = {
        "report_date": str(report_date),
        "report_day_name": day_name,
        "total_comments": total_comments,
        "sample_count": len(rows),
        "volumes": dict(digest_volumes),
        "sentiment_table": {f"{k[0]}_{k[1]}": v for k, v in sentiment_table.items()},
        "driver_themes": driver_themes,
        "rider_themes": rider_themes,
        "driver_denom": driver_denom,
        "rider_denom": rider_denom,
        "safety": safety[:10],
        "safety_count": len(safety),
    }
    print(json.dumps(out, indent=2, default=str))


if __name__ == "__main__":
    main()
