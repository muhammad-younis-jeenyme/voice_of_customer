#!/usr/bin/env python3
"""Voice of Customer daily digest — Snowflake pull + sentiment/theme analysis."""

import json
import os
import re
import sys
from collections import defaultdict

import snowflake.connector

REPORT_DATE = "2026-09-15"
REPORT_DAY_NAME = "Tue 15 Sep 2026"
SAMPLE_PER_CELL = 100
MIN_COMMENT_LEN = 3

VALID_SENTIMENTS = {"Positive", "Neutral", "Negative", "Mixed"}

# Zendesk neutral/ops categories
ZENDESK_NEUTRAL_CATS = {
    "city update", "work permit", "lost item", "lost & found", "lost and found",
    "general inquiry", "account update", "document", "registration",
}

# TICKET_CATEGORY → canonical theme
DRIVER_CAT_MAP = {
    "i did not get paid the cancellation fees": "Cancellation fees not received / unfair fines",
    "report an issue with the passenger's behaviour": "Passenger conduct",
    "report an issue with the passenger's behavior": "Passenger conduct",
    "ride value is less than actual": "Earnings & fares (low pay per km, weak offers)",
    "ride value is higher than actual": "Fare/charge accuracy",
    "amount has been deducted from my balance": "Payment not credited / withdrawals",
    "passenger did not pay trip fare in full / part of the trip fare": "Passenger didn't pay (cash)",
    "funds have not been transferred to my wallet": "Payment not credited / withdrawals",
    "i did not receive discount amount in wallet": "Payment not credited / withdrawals",
    "my account is blocked": "Payment not credited / withdrawals",
}

RIDER_CAT_MAP = {
    "price was way higher than the estimate": "Overcharging / fare disputes",
    "driver overcharged me for the trip/took extra money from me": "Overcharging / fare disputes",
    "driver was not matching the profile in the app": "Wrong driver / car vs app",
    "car was not matching the information in the app": "Wrong driver / car vs app",
    "i cancelled because driver did not show up at the pickup point": "No-show / wrong pickup",
    "driver asked to cancel and i was charged with cancellation fees": "Cancellation (unfair fees, driver-initiated)",
    "driver cancelled and i was charged with cancellation fees": "Cancellation (unfair fees, driver-initiated)",
    "driver took a long route/ driver caused the charges to increase": "Route manipulation / slow driving",
    "driver was rude/unprofessional": "Driver conduct",
    "report a technical issue": "Payment & charging errors",
    "report a technical issue": "Payment & charging errors",
    "bad rating": "Driver conduct",
}

# Driver theme patterns (order matters for priority in ties)
DRIVER_THEMES = [
    ("Cancellation fees not received / unfair fines", ["cancel", "cancellation", "fine", "penalty", "fee not", "unfair fee", "deduct"]),
    ("Earnings & fares (low pay per km, weak offers)", ["earning", "low pay", "low fare", "weak offer", "commission", "income", "salary", "pay per km", "bonus"]),
    ("Passenger conduct", ["passenger", "rude", "abuse", "insult", "behavior", "conduct", "disrespect", "smoking", "drunk"]),
    ("Passenger didn't pay (cash)", ["didn't pay", "did not pay", "not pay", "cash", "refuse to pay", "unpaid", "no payment"]),
    ("Fare/charge accuracy", ["fare", "charge", "overcharge", "wrong amount", "billing", "price", "meter", "recalculat"]),
    ("Payment not credited / withdrawals", ["withdraw", "wallet", "not credited", "balance", "payout", "transfer", "bank"]),
]

# Rider theme patterns
RIDER_THEMES = [
    ("Overcharging / fare disputes", ["overcharg", "fare dispute", "too much", "expensive", "high fare", "double charg", "wrong fare", "price"]),
    ("Driver conduct", ["driver rude", "driver behavior", "unprofessional", "insult", "abuse", "harass", "smoking", "phone while driving"]),
    ("Wrong driver / car vs app", ["wrong driver", "wrong car", "different car", "different driver", "plate", "identity", "not the same"]),
    ("Route manipulation / slow driving", ["route", "long way", "detour", "slow", "traffic", "manipulat", "extra km", "longer route"]),
    ("No-show / wrong pickup", ["no show", "no-show", "wrong pickup", "didn't come", "did not arrive", "waiting", "late"]),
    ("Cancellation (unfair fees, driver-initiated)", ["cancel", "cancellation fee", "driver cancel", "unfair fee", "charged for cancel"]),
    ("Payment & charging errors", ["payment", "charg", "card", "wallet", "double", "failed payment", "refund", "deduct"]),
    ("Vehicle smell / cleanliness", ["smell", "dirty", "clean", "hygien", "odor", "filthy"]),
]

SAFETY_PATTERNS = [
    ("Harassment", ["harass", "assault", "held hand", "threaten", "hit", "insult", "stalk", "inappropriate touch", "sexual"]),
    ("Reckless driving", ["reckless", "speed", "accident", "injur", "child injured", "dangerous driving", "crash"]),
    ("Identity mismatch", ["wrong driver", "different car", "not the same person", "fake driver", "identity"]),
]

ARABIC_NEG = ["سيء", "رديء", "غير راض", "مشكلة", "احتيال", "غش", "سرقة", "وقح", "كذب", "غالي", "فظ", "تهديد"]


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


def classify_sentiment(row):
    existing = (row.get("OVERALL_SENTIMENT") or "").strip()
    if existing in VALID_SENTIMENTS:
        return existing

    text = (row.get("TEXT_FOR_ANALYSIS") or "").lower()
    stars = row.get("RATING_STARS")
    source = row.get("FEEDBACK_SOURCE") or ""
    ticket_cat = (row.get("TICKET_CATEGORY") or "").lower()

    # Zendesk default negative unless neutral ops category
    if source == "Zendesk":
        if any(cat in ticket_cat for cat in ZENDESK_NEUTRAL_CATS):
            return "Neutral"
        return "Negative"

    if not text or len(text.strip()) < 2:
        if stars is not None:
            if stars <= 2:
                return "Negative"
            if stars >= 4:
                return "Positive"
        return "Unclear"

    pos_words = ["good", "great", "excellent", "thank", "love", "best", "amazing", "perfect", "nice", "happy", "ممتاز", "شكر", "رائع", "جيد"]
    neg_words = ["bad", "terrible", "awful", "worst", "hate", "scam", "fraud", "cheat", "angry", "frustrat", "complain", "poor", "slow", "rude"] + ARABIC_NEG

    pos = sum(1 for w in pos_words if w in text)
    neg = sum(1 for w in neg_words if w in text)

    if pos > 0 and neg > 0:
        return "Mixed"
    if neg > pos:
        return "Negative"
    if pos > neg:
        return "Positive"
    if neg == pos and neg > 0:
        return "Mixed"

    # tie-break with stars
    if stars is not None:
        if stars <= 2:
            return "Negative"
        if stars == 3:
            return "Neutral"
        if stars >= 4:
            return "Positive"

    return "Neutral"


def normalize_source(src):
    if src in ("Google Store", "Apple Store"):
        return "App stores"
    return src


def match_themes(text, themes, ticket_category=None, cat_map=None, persona=None):
    matched = []
    cat_l = (ticket_category or "").lower().strip()
    if cat_map and cat_l in cat_map:
        matched.append(cat_map[cat_l])
    text_l = (text or "").lower()
    for label, keywords in themes:
        if any(kw in text_l for kw in keywords):
            if label not in matched:
                matched.append(label)
    if persona == "driver":
        if "دفع" in text_l or "pay" in text_l:
            if "Passenger didn't pay (cash)" not in matched:
                matched.append("Passenger didn't pay (cash)")
        if "انتظار" in text_l or "waiting" in text_l or "تاخير" in text_l:
            if "Passenger conduct" not in matched:
                matched.append("Passenger conduct")
        if "غالي" in text_l or "expensive" in text_l:
            if "Earnings & fares (low pay per km, weak offers)" not in matched:
                matched.append("Earnings & fares (low pay per km, weak offers)")
    return matched


def pct_str(neg_count, total, sample_n):
    if total == 0:
        return "n/a"
    pct = round(100 * neg_count / total)
    prefix = "~" if sample_n < 30 else "~"
    return f"{prefix}{pct}%"


def theme_pct(theme_hits, persona_total):
    results = []
    for label, count in sorted(theme_hits.items(), key=lambda x: -x[1]):
        pct = round(100 * count / persona_total) if persona_total else 0
        results.append((label, pct, count))
    return results


def main():
    conn, cur = connect()

    start_ts = f"{REPORT_DATE} 00:00:00"
    end_ts = f"{REPORT_DATE} 23:59:59.999"

    # Volume counts
    vol_sql = """
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
      WHERE TIMESTAMP >= %(start)s
        AND TIMESTAMP < DATEADD(day, 1, %(start)s::DATE)
        AND RATINGS_COMMENTS IS NOT NULL
        AND TRIM(RATINGS_COMMENTS) <> ''
        AND LENGTH(TRIM(RATINGS_COMMENTS)) >= %(min_len)s
    )
    SELECT FEEDBACK_SOURCE, persona, COUNT(*) AS volume
    FROM base
    WHERE persona IN ('driver', 'rider')
    GROUP BY 1, 2
    ORDER BY volume DESC
    """
    cur.execute(vol_sql, {"start": REPORT_DATE, "min_len": MIN_COMMENT_LEN})
    vol_rows = cur.fetchall()
    vol_cols = [d[0] for d in cur.description]

    total_sql = """
    SELECT COUNT(*) AS total_comments
    FROM jeeny_prod.general.feedbackevents
    WHERE TIMESTAMP >= %(start)s
      AND TIMESTAMP < DATEADD(day, 1, %(start)s::DATE)
      AND RATINGS_COMMENTS IS NOT NULL
      AND TRIM(RATINGS_COMMENTS) <> ''
      AND LENGTH(TRIM(RATINGS_COMMENTS)) >= %(min_len)s
    """
    cur.execute(total_sql, {"start": REPORT_DATE, "min_len": MIN_COMMENT_LEN})
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
      WHERE TIMESTAMP >= %(start)s
        AND TIMESTAMP < DATEADD(day, 1, %(start)s::DATE)
        AND RATINGS_COMMENTS IS NOT NULL
        AND TRIM(RATINGS_COMMENTS) <> ''
        AND LENGTH(TRIM(RATINGS_COMMENTS)) >= %(min_len)s
    )
    SELECT *
    FROM base
    WHERE persona IN ('driver', 'rider')
      AND rn <= {SAMPLE_PER_CELL}
    """
    cur.execute(sample_sql, {"start": REPORT_DATE, "min_len": MIN_COMMENT_LEN})
    sample_cols = [d[0] for d in cur.description]
    sample_rows = [dict(zip(sample_cols, r)) for r in cur.fetchall()]
    conn.close()

    # Build volume table
    vol = defaultdict(lambda: {"driver": 0, "rider": 0})
    for row in vol_rows:
        d = dict(zip(vol_cols, row))
        src = normalize_source(d["FEEDBACK_SOURCE"])
        vol[src][d["PERSONA"]] = d["VOLUME"]

    # Sentiment analysis on sample
    cell_sentiment = defaultdict(lambda: {"neg": 0, "total": 0})
    driver_sample = []
    rider_sample = []
    neg_mixed_rows = []

    for row in sample_rows:
        row["TEXT_FOR_ANALYSIS"] = row.get("TEXT_FOR_ANALYSIS") or row.get("RATINGS_COMMENTS")
        sent = classify_sentiment(row)
        row["sentiment"] = sent
        src = normalize_source(row["FEEDBACK_SOURCE"])
        persona = row["PERSONA"]
        cell_key = (src, persona)
        cell_sentiment[cell_key]["total"] += 1
        if sent in ("Negative", "Mixed"):
            cell_sentiment[cell_key]["neg"] += 1
            neg_mixed_rows.append(row)

        if persona == "driver":
            driver_sample.append(row)
        else:
            rider_sample.append(row)

    # Theme extraction
    driver_theme_hits = defaultdict(int)
    rider_theme_hits = defaultdict(int)
    for row in driver_sample:
        if row["sentiment"] in ("Negative", "Mixed"):
            for t in match_themes(
                row["TEXT_FOR_ANALYSIS"], DRIVER_THEMES,
                row.get("TICKET_CATEGORY"), DRIVER_CAT_MAP, "driver",
            ):
                driver_theme_hits[t] += 1
    for row in rider_sample:
        if row["sentiment"] in ("Negative", "Mixed"):
            for t in match_themes(
                row["TEXT_FOR_ANALYSIS"], RIDER_THEMES,
                row.get("TICKET_CATEGORY"), RIDER_CAT_MAP, "rider",
            ):
                rider_theme_hits[t] += 1

    driver_themes = theme_pct(driver_theme_hits, len(driver_sample))[:6]
    rider_themes = theme_pct(rider_theme_hits, len(rider_sample))[:7]

    # Safety escalations
    safety_flags = []
    for row in neg_mixed_rows:
        text = (row.get("TEXT_FOR_ANALYSIS") or "").lower()
        for cat, keywords in SAFETY_PATTERNS:
            if any(kw in text for kw in keywords):
                city = row.get("CITY") or row.get("COUNTRY") or "Unknown"
                summary = re.sub(r"\s+", " ", (row.get("TEXT_FOR_ANALYSIS") or "")[:120]).strip()
                # anonymize - remove potential phone/name patterns
                summary = re.sub(r"\b\d{8,}\b", "[redacted]", summary)
                safety_flags.append({"category": cat, "summary": f"{city}: {summary}"})
                break

    # Emoji mapping
    theme_emoji = {
        "Cancellation fees not received / unfair fines": "🚫",
        "Earnings & fares (low pay per km, weak offers)": "💰",
        "Passenger conduct": "😡",
        "Passenger didn't pay (cash)": "💵",
        "Fare/charge accuracy": "📄",
        "Payment not credited / withdrawals": "🏦",
        "Overcharging / fare disputes": "💸",
        "Driver conduct": "😡",
        "Wrong driver / car vs app": "🚙",
        "Route manipulation / slow driving": "🗺️",
        "No-show / wrong pickup": "📍",
        "Cancellation (unfair fees, driver-initiated)": "🚫",
        "Payment & charging errors": "🏦",
        "Vehicle smell / cleanliness": "🧼",
    }
    safety_emoji = {"Harassment": "🆘", "Reckless driving": "🏎️", "Identity mismatch": "🚙"}

    def table_cell(src, persona):
        v = vol.get(src, {}).get(persona, 0)
        if src == "App stores":
            return f"{v} · n/a"
        cs = cell_sentiment.get((src, persona), {"neg": 0, "total": 0})
        if v == 0:
            return "0 · n/a"
        return f"{v} · {pct_str(cs['neg'], cs['total'], cs['total'])}"

    # Build digest
    lines = [
        f"## 📊 {REPORT_DAY_NAME}",
        "",
        f"{total_comments} comments · stratified sample ~{len(sample_rows)} analyzed (up to 100 per source×user-type cell). Zendesk = support tickets (complaint-only by nature, so its %neg is always very high); ride_rating is the truer sentiment gauge.",
        "",
        "|Source|Driver (vol · %neg)|Rider (vol · %neg)|",
        "|  ---  |  ---  |  ---  |",
        f"|Zendesk|{table_cell('Zendesk', 'driver')}|{table_cell('Zendesk', 'rider')}|",
        f"|ride_rating|{table_cell('ride_rating', 'driver')}|{table_cell('ride_rating', 'rider')}|",
        f"|in_app|{table_cell('in_app', 'driver')}|—|",
        f"|App stores|{table_cell('App stores', 'driver')}|{table_cell('App stores', 'rider')}|",
        "",
        "### 🏎️ Drivers — top issues",
        "",
        "*(% of driver sample)*",
        "",
    ]
    for label, pct, _ in driver_themes:
        em = theme_emoji.get(label, "•")
        lines.append(f"* {em} {label} — **~{pct}%**")

    lines += ["", "### 🧍 Riders — top issues", "", "*(% of rider sample)*", ""]
    for label, pct, _ in rider_themes:
        em = theme_emoji.get(label, "•")
        lines.append(f"* {em} {label} — **~{pct}%**")

    lines += ["", f"### ⚠️ Safety escalations (~{len(safety_flags)} flagged)", ""]
    shown = 0
    seen_cats = set()
    for flag in safety_flags:
        if shown >= 3:
            break
        cat = flag["category"]
        if cat in seen_cats:
            continue
        seen_cats.add(cat)
        em = safety_emoji.get(cat, "🆘")
        lines.append(f"* {em} **{cat}** — {flag['summary']}")
        shown += 1
    if not safety_flags:
        lines.append("* No critical safety flags in today's sample.")

    # Priorities
    top_driver = driver_themes[0] if driver_themes else ("N/A", 0, 0)
    top_rider = rider_themes[0] if rider_themes else ("N/A", 0, 0)
    second_rider = rider_themes[1] if len(rider_themes) > 1 else ("N/A", 0, 0)

    rider_actions = {
        "Driver conduct": "Launch driver professionalism coaching and in-app reporting follow-up.",
        "Overcharging / fare disputes": "Audit final-fare recalculation and route-deviation detection.",
        "Payment & charging errors": "Fix wallet/card double-charge flows and refund SLAs.",
        "Wrong driver / car vs app": "Strengthen driver verification and in-ride identity checks.",
        "Route manipulation / slow driving": "Improve route-deviation alerts and fare transparency.",
        "Cancellation (unfair fees, driver-initiated)": "Review cancellation-fee policy and driver-initiated cancel penalties.",
    }
    driver_actions = {
        "Passenger didn't pay (cash)": "Enforce cash-payment confirmation and passenger penalty workflow.",
        "Payment not credited / withdrawals": "Audit wallet crediting pipeline and withdrawal delays.",
        "Passenger conduct": "Expand passenger conduct reporting and enforcement.",
        "Cancellation fees not received / unfair fines": "Review cancellation-fee payout rules and dispute handling.",
        "Earnings & fares (low pay per km, weak offers)": "Review fare floors, surge logic, and per-km minimums.",
        "Fare/charge accuracy": "Audit fare calculation and trip finalization logic.",
    }
    priorities = [
        f"1. **{top_rider[0]}** — ~{top_rider[1]}% of rider sample; Zendesk + ride_rating. {rider_actions.get(top_rider[0], 'Investigate root cause and assign owner.')}",
        f"2. **{top_driver[0]}** — ~{top_driver[1]}% of driver sample; ride_rating + in_app. {driver_actions.get(top_driver[0], 'Investigate root cause and assign owner.')}",
    ]
    if len(safety_flags) > 0:
        priorities.append(f"3. **Safety response** — {len(safety_flags)} flagged case(s) in sample. Escalate to safety team within SLA and review identity-verification gaps.")
    elif second_rider[1] >= 5:
        priorities.append(f"3. **{second_rider[0]}** — ~{second_rider[1]}% of rider negatives. {rider_actions.get(second_rider[0], 'Investigate root cause and assign owner.')}")
    elif len(driver_themes) > 1:
        sd = driver_themes[1]
        priorities.append(f"3. **{sd[0]}** — ~{sd[1]}% of driver negatives. {driver_actions.get(sd[0], 'Investigate root cause and assign owner.')}")

    lines += ["", "### 🎯 Priorities", ""]
    lines.extend(priorities)
    lines += ["", "---", ""]

    digest = "\n".join(lines)

    result = {
        "report_date": REPORT_DATE,
        "report_day_name": REPORT_DAY_NAME,
        "total_comments": total_comments,
        "sample_size": len(sample_rows),
        "digest": digest,
        "priorities": priorities[:3],
        "driver_themes": driver_themes,
        "rider_themes": rider_themes,
        "safety_count": len(safety_flags),
        "vol": dict(vol),
        "cell_sentiment": {f"{k[0]}|{k[1]}": v for k, v in cell_sentiment.items()},
    }
    print(json.dumps(result, default=str, indent=2))


if __name__ == "__main__":
    main()
