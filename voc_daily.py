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

# --- Theme patterns (EN_TRANSLATION / text_for_analysis) ---
DRIVER_THEMES = {
    "Passenger conduct": [
        r"\b(rude|disrespect|abusive|aggressive|bad behavior|misbehav|scream|shout|fight|argu|unprofessional|impolite)\w*\b",
        r"\b(passenger|client|customer).{0,30}(rude|bad|terrible|awful|disrespect)\b",
        r"\b(smok\w*|eating|food|dirty|mess)\b.*\b(passenger|car)\b",
        r"(غير محترم|وقح|اسلوب|مزعج|سيء التعامل|ضرب الباب|قليل ادب|غير لبق)",
        r"(rude|impolite|hit.*door|bad attitude)",
    ],
    "Passenger didn't pay (cash)": [
        r"\b(didn'?t|not|never|refus\w*|fail\w*)\s+pay\b",
        r"\bcash\b.*\b(didn'?t|not|refus|without)\b",
        r"\bunpaid\b",
        r"\bno\s+payment\b",
        r"(لم يدفع|لم يتم الدفع|لم تدفع|عدم الدفع|لم ادفع|non.?payment)",
    ],
    "Earnings & fares (low pay per km, weak offers)": [
        r"\b(low|poor|bad|weak|unfair)\s+(pay|fare|earning|income|offer|price|rate)\b",
        r"\bearn\w*\b.*\b(low|little|poor)\b",
        r"\bper\s*km\b",
        r"\bcommission\b",
        r"\bsurge\b",
        r"(الاجره غير عادله|سعر رخيص|رخيص جدا|قليل|ضعيف|مستحيل سعر)",
        r"(unfair fare|low price|cheap fare|long trip.*cheap)",
    ],
    "Cancellation fees not received / unfair fines": [
        r"\bcancel\w*\b.*\b(fee|fine|penalt|charge)\b",
        r"\b(fee|fine|penalt)\b.*\bcancel\w*\b",
        r"\bunfair\s+(fine|fee|penalt)\b",
        r"\bdidn'?t\s+(receive|get)\b.*\b(fee|fine)\b",
        r"(رسوم الالغاء|رسوم الغاء|لم احصل.*الغاء|cancellation fee)",
    ],
    "Fare/charge accuracy": [
        r"\b(wrong|incorrect|error|mismatch)\b.*\b(fare|charge|amount|bill)\b",
        r"\b(fare|charge|amount)\b.*\b(wrong|incorrect|error)\b",
        r"\bovercharg\w*\b",
        r"\bdeduct\w*\b",
        r"(مبلغ غلط|خطأ|غلط|wrong amount|wrong location.*price)",
    ],
    "Payment not credited / withdrawals": [
        r"\b(withdraw|wallet|credit|transfer|payout|balance)\b.*\b(not|fail|delay|missing|stuck)\b",
        r"\b(not|fail|delay|missing|stuck)\b.*\b(withdraw|wallet|credit|transfer|payout|balance)\b",
        r"\bpayment\s+not\s+(received|credited)\b",
        r"(لم يتم نزول|لم ينزل|wallet|حسابي)",
    ],
}

RIDER_THEMES = {
    "Overcharging / fare disputes": [
        r"\bovercharg\w*\b",
        r"\b(too\s+high|expensive|extra)\b.*\b(fare|charge|price|amount|cost)\b",
        r"\b(fare|charge|price|amount)\b.*\b(too\s+high|expensive|extra|wrong)\b",
        r"\bfare\s+disput\w*\b",
        r"(غالي|غااالي|مبالغ|زياده|أعلى|اعلى|expensive|higher than)",
    ],
    "Driver conduct": [
        r"\b(rude|unprofessional|disrespect|abusive|aggressive|angry|shout|scream|insult)\w*\b",
        r"\b(driver).{0,30}(rude|bad|terrible|awful|unprofessional|disrespect)\b",
        r"\b(bad|poor|terrible)\s+(attitude|behavior|service)\b",
        r"(اسلوب|غير لائق|وقح|مزعج|بخزي|unprofessional|impolite|rude)",
    ],
    "Wrong driver / car vs app": [
        r"\bwrong\s+(driver|car|vehicle|plate|number)\b",
        r"\b(different|another)\s+(driver|car|vehicle)\b",
        r"\bnot\s+match\w*\b.*\b(app|photo|picture)\b",
        r"\bidentity\b",
    ],
    "Route manipulation / slow driving": [
        r"\b(long|wrong|extra)\s+route\b",
        r"\broute\s+(manipulat|deviat|longer|wrong)\w*\b",
        r"\b(slow|delay|detour|circle|round)\b.*\b(driv|route)\b",
        r"\btraffic\s+scam\b",
        r"(متهور|سرعه|speed|reckless|fast driving)",
    ],
    "No-show / wrong pickup": [
        r"\bno[\s-]?show\b",
        r"\bwrong\s+(pickup|location|place)\b",
        r"\bdidn'?t\s+(come|arrive|show)\b",
        r"\bwait\w*\b.*\b(minute|hour|long)\b",
    ],
    "Cancellation (unfair fees, driver-initiated)": [
        r"\bcancel\w*\b.*\b(fee|fine|charge|unfair)\b",
        r"\b(driver).{0,20}cancel\w*\b",
        r"\bunfair\s+cancel\w*\b",
    ],
    "Payment & charging errors": [
        r"\b(payment|charge|card|wallet)\b.*\b(fail|error|declin|problem|issue)\b",
        r"\b(double|duplicate)\s+charg\w*\b",
        r"\brefund\b",
    ],
    "Vehicle smell / cleanliness": [
        r"\b(smell|dirty|clean|hygien|odor|stink)\w*\b",
        r"\b(car|vehicle|interior)\b.*\b(dirty|smell|clean)\b",
        r"(رائحة|نظافة|قذر|dirty|smell)",
    ],
}

SAFETY_PATTERNS = {
    "Harassment": [
        r"\b(harass|assault|molest|touch|held\s+hand|threaten|stalk|insult|abuse)\w*\b",
        r"\b(repeated|keep)\s+(call|message|contact)\b",
    ],
    "Reckless driving": [
        r"\b(reckless|dangerous|speed|accident|crash|injur|hit)\w*\b",
        r"\b(child|kid)\b.*\binjur\w*\b",
    ],
    "Identity mismatch": [
        r"\bwrong\s+(driver|car|vehicle)\b.*\b(safe|scared|afraid|danger)\b",
        r"\b(safe|scared|afraid|danger)\b.*\bwrong\s+(driver|car|vehicle)\b",
        r"\bnot\s+the\s+same\s+(driver|car)\b",
    ],
}

ZENDESK_NEUTRAL_CATEGORIES = {
    "city update", "work permit", "lost item", "lost and found", "general inquiry",
    "information", "account update", "document", "registration", "want to update my operating city",
    "passenger lost an item", "i lost an item", "report a technical issue",
    "i entered a wrong amount on payment screen",
}

ZENDESK_DRIVER_CATEGORY_MAP = {
    "passenger did not pay trip fare in full / part of the trip fare": "Passenger didn't pay (cash)",
    "i did not get paid the cancellation fees": "Cancellation fees not received / unfair fines",
    "report an issue with the passenger's behaviour": "Passenger conduct",
    "report an issue with the passenger's behavior": "Passenger conduct",
    "ride value is less than actual": "Earnings & fares (low pay per km, weak offers)",
    "amount has been deducted from my balance": "Payment not credited / withdrawals",
    "i did not receive credit card payment amount in wallet": "Payment not credited / withdrawals",
    "bad rating": None,  # fall through to text
}

ZENDESK_RIDER_CATEGORY_MAP = {
    "price was way higher than the estimate": "Overcharging / fare disputes",
    "driver cancelled and i was charged with cancellation fees": "Cancellation (unfair fees, driver-initiated)",
    "i cancelled because driver did not show up at the pickup point": "No-show / wrong pickup",
    "driver was not matching the profile in the app": "Wrong driver / car vs app",
    "amount has been deducted from my balance": "Payment & charging errors",
    "bad rating": None,
}

POSITIVE_WORDS = re.compile(
    r"\b(great|excellent|good|amazing|wonderful|fantastic|perfect|thank|thanks|appreciate|"
    r"helpful|polite|professional|clean|fast|quick|smooth|love|best|awesome|nice)\w*\b",
    re.I,
)
NEGATIVE_WORDS = re.compile(
    r"\b(bad|terrible|awful|horrible|worst|hate|angry|frustrat|disappoint|complain|"
    r"unfair|unprofessional|rude|disrespect|scam|fraud|cheat|steal|refus|fail|problem|"
    r"issue|wrong|poor|slow|delay|cancel|overcharg|harass|danger|scared|afraid)\w*\b",
    re.I,
)


def get_report_date(tz_name: str = "Asia/Riyadh") -> tuple[str, str]:
    tz = ZoneInfo(tz_name)
    yesterday = (datetime.now(tz) - timedelta(days=1)).date()
    day_name = yesterday.strftime("%a %d %b %Y")
    return str(yesterday), day_name


def connect():
    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PAT"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ.get("SNOWFLAKE_DATABASE", "JEENY_PROD"),
        role=os.environ.get("SNOWFLAKE_ROLE"),
    )
    cur = conn.cursor()
    cur.execute(f"USE WAREHOUSE {os.environ['SNOWFLAKE_WAREHOUSE']}")
    return conn, cur


def normalize_source(src: str) -> str:
    if src in ("Google Store", "Apple Store"):
        return "App stores"
    return src


def classify_sentiment(row: dict) -> str:
    existing = (row.get("OVERALL_SENTIMENT") or "").strip()
    if existing in ("Positive", "Neutral", "Negative", "Mixed"):
        return existing

    text = (row.get("TEXT_FOR_ANALYSIS") or "").strip()
    source = row.get("FEEDBACK_SOURCE", "")
    category = (row.get("TICKET_CATEGORY") or "").lower()
    stars = row.get("RATING_STARS")

    # Zendesk default negative unless neutral category
    if source == "Zendesk":
        if any(n in category for n in ZENDESK_NEUTRAL_CATEGORIES):
            return "Neutral"
        return "Negative"

    if not text or len(text) < 3:
        return "Unclear"

    pos = len(POSITIVE_WORDS.findall(text))
    neg = len(NEGATIVE_WORDS.findall(text))

    if pos > 0 and neg > 0:
        base = "Mixed"
    elif neg > pos:
        base = "Negative"
    elif pos > neg:
        base = "Positive"
    elif stars is not None:
        if stars <= 2:
            base = "Negative"
        elif stars == 3:
            base = "Neutral"
        elif stars >= 4:
            base = "Positive"
        else:
            base = "Neutral"
    else:
        base = "Neutral"

    # Rating tie-break
    if stars is not None:
        if stars <= 2 and base in ("Positive", "Neutral"):
            base = "Negative"
        elif stars >= 4 and base == "Negative" and pos > 0:
            base = "Mixed"
        elif stars >= 4 and base == "Negative":
            base = "Positive"

    return base


def match_themes(text: str, theme_map: dict, category: str | None = None, persona: str = "driver") -> set[str]:
    found = set()
    cat = (category or "").strip().lower()
    cat_map = ZENDESK_DRIVER_CATEGORY_MAP if persona == "driver" else ZENDESK_RIDER_CATEGORY_MAP
    if cat in cat_map and cat_map[cat]:
        found.add(cat_map[cat])
    if not text:
        return found
    for theme, patterns in theme_map.items():
        for pat in patterns:
            if re.search(pat, text, re.I):
                found.add(theme)
                break
    return found


def pct(n: int, denom: int, small_sample: bool) -> str:
    if denom == 0:
        return "n/a"
    val = round(100 * n / denom)
    return f"~{val}%" if small_sample else f"{val}%"


def main():
    report_date, report_day_name = get_report_date()
    if len(sys.argv) > 1:
        report_date = sys.argv[1]
        d = datetime.strptime(report_date, "%Y-%m-%d").date()
        report_day_name = d.strftime("%a %d %b %Y")

    start_ts = f"{report_date} 00:00:00"
    conn, cur = connect()

    cur.execute(f"""
    SELECT COUNT(*) FROM jeeny_prod.general.feedbackevents
    WHERE TIMESTAMP >= '{start_ts}'
      AND TIMESTAMP < DATEADD(day, 1, '{start_ts}'::timestamp)
      AND RATINGS_COMMENTS IS NOT NULL
      AND TRIM(RATINGS_COMMENTS) <> ''
      AND LENGTH(TRIM(RATINGS_COMMENTS)) >= 3
    """)
    total_comments = cur.fetchone()[0]

    cur.execute(f"""
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
      WHERE TIMESTAMP >= '{start_ts}'
        AND TIMESTAMP < DATEADD(day, 1, '{start_ts}'::timestamp)
        AND RATINGS_COMMENTS IS NOT NULL
        AND TRIM(RATINGS_COMMENTS) <> ''
        AND LENGTH(TRIM(RATINGS_COMMENTS)) >= 3
    )
    SELECT GUID, FEEDBACK_SOURCE, persona, RATINGS_COMMENTS, text_for_analysis,
           OVERALL_SENTIMENT, RATING_STARS, CITY, COUNTRY, TICKET_CATEGORY, CATEGORY, URGENCY
    FROM base
    WHERE persona IN ('driver', 'rider') AND rn <= {SAMPLE_PER_CELL}
    """)

    cols = [d[0] for d in cur.description]
    rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    conn.close()

    # Volume from full population (re-query volumes)
    conn, cur = connect()
    cur.execute(f"""
    WITH base AS (
      SELECT FEEDBACK_SOURCE,
        CASE WHEN USER_TYPE = 'driver' THEN 'driver'
             WHEN USER_TYPE IN ('passenger', 'customer') THEN 'rider'
             ELSE 'other' END AS persona
      FROM jeeny_prod.general.feedbackevents
      WHERE TIMESTAMP >= '{start_ts}'
        AND TIMESTAMP < DATEADD(day, 1, '{start_ts}'::timestamp)
        AND RATINGS_COMMENTS IS NOT NULL AND TRIM(RATINGS_COMMENTS) <> ''
        AND LENGTH(TRIM(RATINGS_COMMENTS)) >= 3
    )
    SELECT FEEDBACK_SOURCE, persona, COUNT(*) FROM base
    WHERE persona IN ('driver', 'rider')
    GROUP BY 1, 2
    """)
    vol_raw = cur.fetchall()
    conn.close()

    volumes = defaultdict(lambda: {"driver": 0, "rider": 0})
    for src, persona, vol in vol_raw:
        ns = normalize_source(src)
        volumes[ns][persona] += vol

    # Sentiment per cell
    cell_sentiment = defaultdict(lambda: {"neg": 0, "total": 0})
    driver_neg_mixed = []
    rider_neg_mixed = []
    safety_flags = []

    for row in rows:
        row["TEXT_FOR_ANALYSIS"] = row.pop("TEXT_FOR_ANALYSIS", row.get("TEXT_FOR_ANALYSIS"))
        # fix column name casing
        row["TEXT_FOR_ANALYSIS"] = row.get("TEXT_FOR_ANALYSIS") or row.get("text_for_analysis", "")
        sent = classify_sentiment({
            "OVERALL_SENTIMENT": row.get("OVERALL_SENTIMENT"),
            "TEXT_FOR_ANALYSIS": row["TEXT_FOR_ANALYSIS"],
            "FEEDBACK_SOURCE": row["FEEDBACK_SOURCE"],
            "TICKET_CATEGORY": row.get("TICKET_CATEGORY"),
            "RATING_STARS": row.get("RATING_STARS"),
        })
        row["sentiment"] = sent
        ns = normalize_source(row["FEEDBACK_SOURCE"])
        persona = row["PERSONA"]
        if sent != "Unclear":
            cell_sentiment[(ns, persona)]["total"] += 1
            if sent in ("Negative", "Mixed"):
                cell_sentiment[(ns, persona)]["neg"] += 1

        if persona == "driver" and sent in ("Negative", "Mixed"):
            driver_neg_mixed.append(row)
        elif persona == "rider" and sent in ("Negative", "Mixed"):
            rider_neg_mixed.append(row)

        if sent in ("Negative", "Mixed"):
            text = row["TEXT_FOR_ANALYSIS"]
            for cat, patterns in SAFETY_PATTERNS.items():
                for pat in patterns:
                    if re.search(pat, text, re.I):
                        summary = anonymize_summary(text, row.get("CITY"))
                        safety_flags.append({"category": cat, "summary": summary})
                        break

    def theme_counts(neg_rows, theme_map, persona_sample_size, persona):
        counts = defaultdict(int)
        for row in neg_rows:
            themes = match_themes(
                row["TEXT_FOR_ANALYSIS"],
                theme_map,
                row.get("TICKET_CATEGORY"),
                persona,
            )
            for t in themes:
                counts[t] += 1
        ranked = sorted(counts.items(), key=lambda x: -x[1])
        return [
            {"theme": t, "count": c, "pct": pct(c, persona_sample_size, persona_sample_size < 30)}
            for t, c in ranked
        ]

    driver_sample = sum(1 for r in rows if r["PERSONA"] == "driver")
    rider_sample = sum(1 for r in rows if r["PERSONA"] == "rider")
    driver_themes = theme_counts(driver_neg_mixed, DRIVER_THEMES, driver_sample, "driver")
    rider_themes = theme_counts(rider_neg_mixed, RIDER_THEMES, rider_sample, "rider")

    def cell_pctneg(src, persona):
        c = cell_sentiment.get((src, persona), {"neg": 0, "total": 0})
        small = c["total"] < 30
        if c["total"] == 0:
            return "n/a"
        return pct(c["neg"], c["total"], small).replace("%", "").replace("~", "")

    result = {
        "report_date": report_date,
        "report_day_name": report_day_name,
        "total_comments": total_comments,
        "sample_count": len(rows),
        "volumes": dict(volumes),
        "cell_pctneg": {
            f"{s}_{p}": cell_pctneg(s, p)
            for s in ["Zendesk", "ride_rating", "in_app", "App stores"]
            for p in ["driver", "rider"]
        },
        "driver_themes": driver_themes[:6],
        "rider_themes": rider_themes[:7],
        "safety_count": len(safety_flags),
        "safety_examples": safety_flags[:3],
        "driver_sample": driver_sample,
        "rider_sample": rider_sample,
    }
    print(json.dumps(result, indent=2, default=str))


def anonymize_summary(text: str, city: str | None) -> str:
    t = text[:200].strip()
    t = re.sub(r"\b\d{8,}\b", "[redacted]", t)
    t = re.sub(r"\b[A-Z][a-z]+\s+[A-Z][a-z]+\b", "a user", t)
    if city:
        t = f"[{city}] {t}"
    return t[:120] + ("…" if len(t) > 120 else "")


if __name__ == "__main__":
    main()
