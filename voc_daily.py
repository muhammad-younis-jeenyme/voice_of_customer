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

REPORT_DATE = sys.argv[1] if len(sys.argv) > 1 else None
SAMPLE_PER_CELL = 100

VALID_SENTIMENTS = {"Positive", "Neutral", "Negative", "Mixed"}

ZENDESK_NEUTRAL_CATS = {"city update", "work permit", "lost item", "lost and found", "document",
                        "registration", "want to update my operating city", "updating my personal photo",
                        "passenger lost an item", "i lost an item"}

# Map TICKET_CATEGORY (lowercase substring match) -> canonical theme
TICKET_TO_DRIVER = [
    ("Cancellation fees not received / unfair fines", [
        "cancellation fee", "did not get paid the cancellation", "deducted from my balance",
        "unfair fine", "penalty", "amount has been deducted",
    ]),
    ("Earnings & fares (low pay per km, weak offers)", [
        "ride value is less", "low pay", "earning", "commission", "target was achieved",
        "weak offer", "fare too low",
    ]),
    ("Passenger conduct", [
        "passenger's behaviour", "passenger behavior", "passenger conduct", "rude passenger",
    ]),
    ("Passenger didn't pay (cash)", [
        "did not pay", "not pay trip fare", "passenger did not pay", "cash",
    ]),
    ("Fare/charge accuracy", [
        "fare accuracy", "wrong fare", "incorrect charge", "meter",
    ]),
    ("Payment not credited / withdrawals", [
        "not been transferred", "wallet", "stc pay", "withdraw", "not credited",
        "discount amount in wallet", "funds have not",
    ]),
]

TICKET_TO_RIDER = [
    ("Overcharging / fare disputes", [
        "price was way higher", "overcharged", "extra money", "higher than the estimate",
        "fare dispute", "too expensive",
    ]),
    ("Driver conduct", [
        "driver behaviour", "driver behavior", "rude driver", "bad attitude",
    ]),
    ("Wrong driver / car vs app", [
        "not matching the profile", "not matching the information", "wrong driver",
        "wrong car", "car was not matching",
    ]),
    ("Route manipulation / slow driving", [
        "long route", "detour", "slow", "route", "took longer",
    ]),
    ("No-show / wrong pickup", [
        "no show", "no-show", "wrong pickup", "waited",
    ]),
    ("Cancellation (unfair fees, driver-initiated)", [
        "driver asked to cancel", "charged with cancellation", "cancellation fee",
        "unfair cancel",
    ]),
    ("Payment & charging errors", [
        "payment", "charged twice", "double charge", "wallet", "refund", "stc pay",
        "card charge",
    ]),
    ("Vehicle smell / cleanliness", [
        "smell", "dirty", "cleanliness", "unclean",
    ]),
]

# Keyword fallback on uppercased text
DRIVER_KW = [
    ("Cancellation fees not received / unfair fines", ["CANCEL FEE", "CANCELLATION FEE", "FINE", "PENALTY", "DEDUCT", "غرامة", "الغاء"]),
    ("Earnings & fares (low pay per km, weak offers)", ["LOW PAY", "EARNING", "COMMISSION", "WEAK OFFER", "PER KM", "أجر", "ربح"]),
    ("Passenger conduct", ["RUDE PASSENGER", "PASSENGER BEHAV", "BAD PASSENGER", "سلوك", "وقح"]),
    ("Passenger didn't pay (cash)", ["DIDN'T PAY", "DID NOT PAY", "REFUSED TO PAY", "لم يدفع", "ما دفع"]),
    ("Fare/charge accuracy", ["WRONG FARE", "FARE ACCURACY", "METER", "عداد"]),
    ("Payment not credited / withdrawals", ["WITHDRAW", "WALLET", "NOT CREDITED", "BALANCE", "PAYOUT", "محفظة", "سحب"]),
]

RIDER_KW = [
    ("Overcharging / fare disputes", ["OVERCHARG", "TOO MUCH", "EXPENSIVE", "HIGH FARE", "EXTRA CHARGE", "غالي", "زيادة"]),
    ("Driver conduct", ["RUDE DRIVER", "DRIVER BEHAV", "BAD ATTITUDE", "سلوك السائق"]),
    ("Wrong driver / car vs app", ["WRONG DRIVER", "WRONG CAR", "DIFFERENT CAR", "LICENSE PLATE", "سيارة مختلفة"]),
    ("Route manipulation / slow driving", ["LONG ROUTE", "DETOUR", "SLOW DRIV", "CIRCLING", "طريق طويل", "بطيء"]),
    ("No-show / wrong pickup", ["NO SHOW", "NO-SHOW", "WRONG PICKUP", "WAITED"]),
    ("Cancellation (unfair fees, driver-initiated)", ["CANCEL FEE", "DRIVER CANCEL", "CANCELLED", "الغاء"]),
    ("Payment & charging errors", ["PAYMENT ERROR", "CHARGED TWICE", "DOUBLE CHARGE", "REFUND", "دفع"]),
    ("Vehicle smell / cleanliness", ["SMELL", "DIRTY", "CLEANLINESS", "UNCLEAN", "رائحة", "قذر"]),
]

SAFETY_PATTERNS = [
    ("Harassment", ["HARASS", "ASSAULT", "HELD HAND", "THREATEN", "INSULT", "STALK", "TOUCHED", "SEXUAL", "تحرش", "تهديد"]),
    ("Reckless driving", ["RECKLESS", "SPEED", "ACCIDENT", "INJUR", "CRASH", "DANGEROUS", "سرعة", "حادث"]),
    ("Identity mismatch", ["WRONG DRIVER", "DIFFERENT CAR", "IDENTITY", "NOT THE DRIVER", "FAKE DRIVER", "سيارة مختلفة"]),
]

EMOJI = {
    "Cancellation fees not received / unfair fines": "🚫",
    "Earnings & fares (low pay per km, weak offers)": "💰",
    "Passenger conduct": "😡",
    "Passenger didn't pay (cash)": "💵",
    "Fare/charge accuracy": "📄",
    "Payment not credited / withdrawals": "🏦",
    "Cancellation (unfair fees, driver-initiated)": "🚫",
    "Overcharging / fare disputes": "💸",
    "Driver conduct": "😡",
    "Wrong driver / car vs app": "🚙",
    "Route manipulation / slow driving": "🗺️",
    "No-show / wrong pickup": "📍",
    "Payment & charging errors": "🏦",
    "Vehicle smell / cleanliness": "🧼",
    "Harassment": "🆘",
    "Reckless driving": "🏎️",
    "Identity mismatch": "🚙",
}


def connect():
    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PAT"],
        database=os.environ.get("SNOWFLAKE_DATABASE", "jeeny_prod"),
        role=os.environ.get("SNOWFLAKE_ROLE"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
    )
    cur = conn.cursor()
    cur.execute(f"USE WAREHOUSE {os.environ.get('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH')}")
    return conn, cur


def normalize_source(src):
    if src in ("Google Store", "Apple Store"):
        return "App stores"
    return src


def classify_sentiment(row):
    existing = row.get("OVERALL_SENTIMENT")
    if existing and str(existing).strip() in VALID_SENTIMENTS:
        return str(existing).strip()

    text = (row.get("TEXT_FOR_ANALYSIS") or "").strip()
    stars = row.get("RATING_STARS")
    source = row.get("FEEDBACK_SOURCE", "")
    ticket_cat = (row.get("TICKET_CATEGORY") or "").lower()

    if source == "Zendesk":
        if any(nc in ticket_cat for nc in ZENDESK_NEUTRAL_CATS):
            return "Neutral"
        return "Negative"

    text_up = text.upper()
    neg_kw = ["BAD", "TERRIBLE", "WORST", "HATE", "AWFUL", "RUDE", "SCAM", "FRAUD", "NOT GOOD",
              "DISAPPOINT", "UNFAIR", "REFUSE", "COMPLAIN", "ANGRY", "HORRIBLE", "SLOW",
              "سيء", "رديء", "مزعج", "غش", "احتيال", "لم يدفع", "غالي"]
    pos_kw = ["GOOD", "GREAT", "EXCELLENT", "THANK", "LOVE", "AMAZING", "PERFECT", "NICE",
              "ممتاز", "شكر", "رائع", "جيد"]

    neg_hits = sum(1 for k in neg_kw if k in text_up or k in text)
    pos_hits = sum(1 for k in pos_kw if k in text_up or k in text)

    if neg_hits and pos_hits:
        return "Mixed"
    if neg_hits:
        return "Negative"
    if pos_hits:
        return "Positive"

    if stars is not None:
        try:
            s = float(stars)
            if s <= 2:
                return "Negative"
            if s == 3:
                return "Neutral"
            if s >= 4:
                return "Positive"
        except (TypeError, ValueError):
            pass

    if len(text) < 5:
        return "Unclear"
    return "Neutral"


def map_ticket_category(ticket_cat, persona):
    if not ticket_cat:
        return []
    tc = ticket_cat.lower()
    mapping = TICKET_TO_DRIVER if persona == "driver" else TICKET_TO_RIDER
    matched = []
    for label, patterns in mapping:
        if any(p in tc for p in patterns):
            matched.append(label)
    return matched


def match_keywords(text, kw_list):
    if not text:
        return []
    t = text.upper()
    matched = []
    for label, patterns in kw_list:
        if any(p in t or p in text for p in patterns):
            matched.append(label)
    return matched


def extract_themes(row, persona):
    themes = set()
    themes.update(map_ticket_category(row.get("TICKET_CATEGORY"), persona))
    kw = DRIVER_KW if persona == "driver" else RIDER_KW
    themes.update(match_keywords(row.get("TEXT_FOR_ANALYSIS"), kw))
    # ride_rating low stars without theme -> infer from short text patterns
    if not themes and row.get("FEEDBACK_SOURCE") == "ride_rating":
        stars = row.get("RATING_STARS")
        try:
            if stars is not None and float(stars) <= 2:
                if persona == "driver":
                    themes.add("Passenger conduct")
                else:
                    themes.add("Driver conduct")
        except (TypeError, ValueError):
            pass
    return list(themes)


def anonymize(text, city=None):
    s = (text or "").strip()[:120]
    s = re.sub(r"\d{6,}", "[redacted]", s)
    loc = city or "Unknown"
    return f"{loc}: {s}..." if s else f"{loc}: [redacted]"


def main():
    tz = ZoneInfo("Asia/Riyadh")
    report_date = datetime.strptime(REPORT_DATE, "%Y-%m-%d").date() if REPORT_DATE else (datetime.now(tz) - timedelta(days=1)).date()
    start_ts = f"{report_date} 00:00:00"
    end_ts = f"{report_date + timedelta(days=1)} 00:00:00"
    day_name = report_date.strftime("%a %d %b %Y")

    conn, cur = connect()

    cur.execute(
        """WITH base AS (
          SELECT FEEDBACK_SOURCE,
            CASE WHEN USER_TYPE='driver' THEN 'driver'
                 WHEN USER_TYPE IN ('passenger','customer') THEN 'rider' ELSE 'other' END AS persona,
            RATINGS_COMMENTS
          FROM jeeny_prod.general.feedbackevents
          WHERE TIMESTAMP >= %s AND TIMESTAMP < %s
            AND RATINGS_COMMENTS IS NOT NULL AND TRIM(RATINGS_COMMENTS) <> ''
            AND LENGTH(TRIM(RATINGS_COMMENTS)) >= 3
        )
        SELECT FEEDBACK_SOURCE, persona, COUNT(*) FROM base
        WHERE persona IN ('driver','rider') GROUP BY 1,2 ORDER BY 3 DESC""",
        (start_ts, end_ts),
    )
    volume_rows = cur.fetchall()

    cur.execute(
        """SELECT COUNT(*) FROM jeeny_prod.general.feedbackevents
        WHERE TIMESTAMP >= %s AND TIMESTAMP < %s
          AND RATINGS_COMMENTS IS NOT NULL AND TRIM(RATINGS_COMMENTS) <> ''
          AND LENGTH(TRIM(RATINGS_COMMENTS)) >= 3""",
        (start_ts, end_ts),
    )
    total_comments = cur.fetchone()[0]

    cur.execute(
        f"""WITH base AS (
          SELECT GUID, FEEDBACK_SOURCE,
            CASE WHEN USER_TYPE='driver' THEN 'driver'
                 WHEN USER_TYPE IN ('passenger','customer') THEN 'rider' ELSE 'other' END AS persona,
            RATINGS_COMMENTS,
            COALESCE(NULLIF(TRIM(EN_TRANSLATION), ''), RATINGS_COMMENTS) AS text_for_analysis,
            OVERALL_SENTIMENT, RATING_STARS, CITY, COUNTRY, TICKET_CATEGORY, CATEGORY, URGENCY, TIMESTAMP,
            ROW_NUMBER() OVER (
              PARTITION BY FEEDBACK_SOURCE,
                CASE WHEN USER_TYPE='driver' THEN 'driver'
                     WHEN USER_TYPE IN ('passenger','customer') THEN 'rider' ELSE 'other' END
              ORDER BY RANDOM()) AS rn
          FROM jeeny_prod.general.feedbackevents
          WHERE TIMESTAMP >= %s AND TIMESTAMP < %s
            AND RATINGS_COMMENTS IS NOT NULL AND TRIM(RATINGS_COMMENTS) <> ''
            AND LENGTH(TRIM(RATINGS_COMMENTS)) >= 3
        )
        SELECT GUID, FEEDBACK_SOURCE, persona, text_for_analysis, OVERALL_SENTIMENT, RATING_STARS,
               CITY, COUNTRY, TICKET_CATEGORY, CATEGORY, URGENCY
        FROM base WHERE persona IN ('driver','rider') AND rn <= {SAMPLE_PER_CELL}""",
        (start_ts, end_ts),
    )
    cols = [d[0] for d in cur.description]
    sample = [dict(zip(cols, row)) for row in cur.fetchall()]
    conn.close()

    vol = defaultdict(lambda: {"driver": 0, "rider": 0})
    for src, persona, v in volume_rows:
        vol[normalize_source(src)][persona] += v

    cell_sent = defaultdict(lambda: {"neg": 0, "total": 0})
    persona_data = {
        "driver": {"sample_total": 0, "neg_mixed": 0, "themes": defaultdict(set)},
        "rider": {"sample_total": 0, "neg_mixed": 0, "themes": defaultdict(set)},
    }
    safety_flags = []

    for row in sample:
        persona = row["PERSONA"]
        persona_data[persona]["sample_total"] += 1

        sent = classify_sentiment({
            "OVERALL_SENTIMENT": row["OVERALL_SENTIMENT"],
            "TEXT_FOR_ANALYSIS": row["TEXT_FOR_ANALYSIS"],
            "RATING_STARS": row["RATING_STARS"],
            "FEEDBACK_SOURCE": row["FEEDBACK_SOURCE"],
            "TICKET_CATEGORY": row["TICKET_CATEGORY"],
        })

        ns = normalize_source(row["FEEDBACK_SOURCE"])
        if sent != "Unclear":
            cell_sent[(ns, persona)]["total"] += 1
            if sent in ("Negative", "Mixed"):
                cell_sent[(ns, persona)]["neg"] += 1
                persona_data[persona]["neg_mixed"] += 1
                for theme in extract_themes(row, persona):
                    persona_data[persona]["themes"][theme].add(row["GUID"])

                text_up = (row["TEXT_FOR_ANALYSIS"] or "").upper()
                for cat, pats in SAFETY_PATTERNS:
                    if any(p in text_up or p in (row["TEXT_FOR_ANALYSIS"] or "") for p in pats):
                        safety_flags.append({
                            "category": cat,
                            "summary": anonymize(row["TEXT_FOR_ANALYSIS"], row.get("CITY") or row.get("COUNTRY")),
                        })
                        break

    def cell_pct(ns, persona):
        c = cell_sent[(ns, persona)]
        if c["total"] == 0:
            return "n/a", 0
        p = round(100 * c["neg"] / c["total"])
        return (f"~{p}%" if c["total"] < 30 else f"{p}%"), p

    def theme_list(persona, top_n=6):
        total = persona_data[persona]["sample_total"] or 1
        counts = sorted(
            ((k, len(v)) for k, v in persona_data[persona]["themes"].items()),
            key=lambda x: -x[1],
        )
        out = []
        for label, n in counts[:top_n]:
            p = round(100 * n / total)
            out.append({"theme": label, "pct": f"{'~' if total < 30 else ''}{p}%"})
        return out

    driver_themes = theme_list("driver")
    rider_themes = theme_list("rider", 7)

    # Build priorities from evidence
    priorities = []
    if rider_themes:
        rt = rider_themes[0]
        second = rider_themes[1]["pct"] if len(rider_themes) > 1 else None
        ev = f"{rt['theme']} ≈ {rt['pct']} of rider sample"
        if second:
            ev += f"; {rider_themes[1]['theme']} ≈ {second}"
        priorities.append({
            "title": rt["theme"],
            "evidence": ev,
            "action": "Audit fare recalculation, payment flows, and support routing for top rider complaints.",
        })
    if driver_themes:
        dt = driver_themes[0]
        priorities.append({
            "title": dt["theme"],
            "evidence": f"{dt['theme']} ≈ {dt['pct']} of driver sample",
            "action": "Review cancellation-fee policy, wallet crediting, and driver payout SLAs.",
        })
    if safety_flags:
        priorities.append({
            "title": "Safety escalations",
            "evidence": f"{len(safety_flags)} safety-related flags in today's sample",
            "action": "Triage flagged cases and reinforce safety response SLAs.",
        })
    # fill to 3
    extras = [t for t in (driver_themes[1:] + rider_themes[1:]) if len(priorities) < 3]
    for t in extras:
        if len(priorities) >= 3:
            break
        priorities.append({
            "title": t["theme"],
            "evidence": f"{t['pct']} of sample",
            "action": "Prioritize in next product/ops review.",
        })

    def vol_cell(ns, persona):
        return vol.get(ns, {}).get(persona, 0)

    result = {
        "report_date": str(report_date),
        "day_name": day_name,
        "total_comments": total_comments,
        "sample_count": len(sample),
        "table": {
            "Zendesk": {"driver": (*cell_pct("Zendesk", "driver"), vol_cell("Zendesk", "driver")),
                        "rider": (*cell_pct("Zendesk", "rider"), vol_cell("Zendesk", "rider"))},
            "ride_rating": {"driver": (*cell_pct("ride_rating", "driver"), vol_cell("ride_rating", "driver")),
                            "rider": (*cell_pct("ride_rating", "rider"), vol_cell("ride_rating", "rider"))},
            "in_app": {"driver": (*cell_pct("in_app", "driver"), vol_cell("in_app", "driver"))},
            "App stores": {"driver": ("n/a", 0, vol_cell("App stores", "driver")),
                           "rider": ("n/a", 0, vol_cell("App stores", "rider"))},
        },
        "driver_themes": driver_themes,
        "rider_themes": rider_themes,
        "safety_count": len(safety_flags),
        "safety_examples": safety_flags[:3],
        "priorities": priorities[:3],
        "emoji": EMOJI,
    }
    print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
