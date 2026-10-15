#!/usr/bin/env python3
"""Voice of Customer daily digest — Snowflake pull + sentiment/theme analysis."""
import json
import os
import re
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import snowflake.connector

SAMPLE_PER_CELL = 100
MIN_COMMENT_LEN = 3

# Zendesk TICKET_CATEGORY → canonical theme
ZENDESK_CATEGORY_THEMES = {
    "Passenger did not pay trip fare in full / part of the trip fare": "Passenger didn't pay (cash)",
    "I did not get paid the cancellation fees": "Cancellation fees not received / unfair fines",
    "Report an issue with the passenger's behaviour": "Passenger conduct",
    "Ride value is less than actual": "Earnings & fares (low pay per km, weak offers)",
    "Funds have not been transferred to my wallet": "Payment not credited / withdrawals",
    "I did not receive credit card payment amount in wallet": "Payment not credited / withdrawals",
    "Amount has been deducted from my balance": "Fare/charge accuracy",
    "I did not receive discount amount in wallet": "Payment not credited / withdrawals",
    "Price was way higher than the estimate": "Overcharging / fare disputes",
    "Driver charged me a higher amount": "Overcharging / fare disputes",
    "Driver was rude/unprofessional": "Driver conduct",
    "Driver was not matching the profile in the app": "Wrong driver / car vs app",
    "Driver took a long route/ Driver caused the charges to increase": "Route manipulation / slow driving",
    "I cancelled because driver did not show up at the pickup point": "No-show / wrong pickup",
    "Driver cancelled and I was charged with cancellation fees": "Cancellation (unfair fees, driver-initiated)",
    "Driver asked to cancel and I was charged with cancellation fees": "Cancellation (unfair fees, driver-initiated)",
    "Driver cancelled without reaching pickup point": "Cancellation (unfair fees, driver-initiated)",
    "Driver started/ended the ride before I got in the car": "No-show / wrong pickup",
    "STC Pay": "Payment & charging errors",
}

# Theme keyword patterns (EN_TRANSLATION / text_for_analysis, incl. Arabic)
DRIVER_THEMES = {
    "Payment not credited / withdrawals": [
        r"withdraw", r"wallet", r"not credited", r"payment not", r"balance",
        r"transfer", r"payout", r"didn't receive", r"not received",
        r"محفظ", r"لم يصل", r"سحب",
    ],
    "Passenger didn't pay (cash)": [
        r"didn't pay", r"not pay", r"refused to pay", r"no payment", r"cash",
        r"without paying", r"did not pay", r"لم يدفع", r"ما دفع",
    ],
    "Cancellation fees not received / unfair fines": [
        r"cancel", r"cancellation fee", r"fine", r"penalty", r"deduct",
        r"unfair", r"compensation", r"الغاء", r"غرامة",
    ],
    "Passenger conduct": [
        r"rude", r"bad behavior", r"conduct", r"insult", r"harass",
        r"aggressive", r"inappropriate", r"سلوك", r"غير مهذب",
    ],
    "Earnings & fares (low pay per km, weak offers)": [
        r"low pay", r"low fare", r"earnings", r"commission", r"offer",
        r"per km", r"income", r"salary", r"bonus", r"قليل", r"سعر الرحله",
    ],
    "Fare/charge accuracy": [
        r"wrong amount", r"incorrect charge", r"overcharge", r"fare accuracy",
        r"charged wrong", r"double charge",
    ],
}

RIDER_THEMES = {
    "Overcharging / fare disputes": [
        r"overcharge", r"too much", r"high fare", r"expensive", r"fare dispute",
        r"charged more", r"extra charge", r"unfair price", r"زياده", r"اخذ زياده",
        r"مبلغ", r"فلوس", r"خلل",
    ],
    "Driver conduct": [
        r"rude", r"bad behavior", r"conduct", r"unprofessional", r"insult",
        r"aggressive", r"inappropriate driver", r"اسوا", r"غير مهذب",
    ],
    "Wrong driver / car vs app": [
        r"wrong car", r"wrong driver", r"different car", r"not the same",
        r"vehicle mismatch", r"car shown", r"license plate", r"number plate",
        r"registered vehicle", r"غير ذلك", r"صورة", r"سعودي", r"بنقالي",
        r"اجنبي", r"authorized driver",
    ],
    "Route manipulation / slow driving": [
        r"long route", r"detour", r"slow", r"manipulat", r"extra distance",
        r"took longer", r"wrong route", r"missed the route", r"اطال", r"طريق",
        r"reckless_driving",
    ],
    "No-show / wrong pickup": [
        r"no show", r"didn't come", r"wrong pickup", r"wrong location",
        r"never arrived", r"didn't arrive",
    ],
    "Cancellation (unfair fees, driver-initiated)": [
        r"cancel", r"cancellation fee", r"driver cancel", r"unfair cancel",
        r"cancelled by driver",
    ],
    "Payment & charging errors": [
        r"payment fail", r"charged twice", r"double charge", r"card",
        r"wallet", r"payment error", r"not refunded", r"stc pay", r"دفع",
    ],
    "Vehicle smell / cleanliness": [
        r"smell", r"dirty", r"clean", r"hygiene", r"odor", r"filthy",
        r"نظيف", r"رائحة", r"قذر", r"مكسور",
    ],
}

SAFETY_PATTERNS = {
    "Harassment": [
        r"harass", r"assault", r"threaten", r"held hand", r"stalk",
        r"inappropriate touch", r"sexual",
    ],
    "Reckless driving": [
        r"reckless", r"speeding", r"accident", r"injur", r"hit", r"crash",
        r"dangerous driving", r"reckless_driving", r"متهور",
    ],
    "Identity mismatch": [
        r"wrong driver", r"wrong car", r"different person", r"not the same",
        r"fake driver", r"identity", r"not the authorized", r"number plate",
        r"registered vehicle", r"غير ذلك", r"صورة",
    ],
}

ZENDESK_NEUTRAL_CATS = {
    "city update", "work permit", "lost item", "lost and found",
    "general inquiry", "account update", "document",
}

VALID_SENTIMENTS = {"Positive", "Neutral", "Negative", "Mixed"}


def get_report_date():
    """Yesterday in Asia/Riyadh (UTC+3)."""
    riyadh = timezone(timedelta(hours=3))
    now = datetime.now(riyadh)
    yesterday = now - timedelta(days=1)
    return yesterday.strftime("%Y-%m-%d"), yesterday.strftime("%a %d %b %Y")


def connect():
    conn = snowflake.connector.connect(
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PAT"],
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ.get("SNOWFLAKE_DATABASE", "jeeny_prod"),
        role=os.environ.get("SNOWFLAKE_ROLE"),
    )
    cur = conn.cursor()
    cur.execute("USE WAREHOUSE COMPUTE_WH")
    return conn, cur


def classify_sentiment(row):
    """Classify a single row using OVERALL_SENTIMENT, text, rating, Zendesk rules."""
    text = (row.get("TEXT_FOR_ANALYSIS") or "").lower()
    source = row.get("FEEDBACK_SOURCE") or ""
    existing = row.get("OVERALL_SENTIMENT")
    stars = row.get("RATING_STARS")
    ticket_cat = (row.get("TICKET_CATEGORY") or "").lower()

    if existing and existing in VALID_SENTIMENTS:
        return existing

    # Zendesk: default negative unless neutral category
    if source == "Zendesk":
        if any(cat in ticket_cat for cat in ZENDESK_NEUTRAL_CATS):
            return "Neutral"
        return "Negative"

    # Rating tie-break
    rating_lean = None
    if stars is not None:
        try:
            s = float(stars)
            if s <= 2:
                rating_lean = "Negative"
            elif s == 3:
                rating_lean = "Neutral"
            elif s >= 4:
                rating_lean = "Positive"
        except (TypeError, ValueError):
            pass

    # Text-based keywords
    neg_kw = [
        r"bad", r"terrible", r"awful", r"worst", r"horrible", r"disappoint",
        r"complain", r"frustrat", r"angry", r"unfair", r"refund", r"scam",
        r"cheat", r"never again", r"disgust", r"hate", r"problem", r"issue",
        r"not good", r"poor", r"unacceptable",
    ]
    pos_kw = [
        r"great", r"excellent", r"amazing", r"wonderful", r"perfect", r"love",
        r"thank", r"best", r"happy", r"good service", r"professional",
        r"recommend", r"fantastic", r"awesome",
    ]

    neg_score = sum(1 for p in neg_kw if re.search(p, text))
    pos_score = sum(1 for p in pos_kw if re.search(p, text))

    if neg_score > 0 and pos_score > 0:
        return "Mixed"
    if neg_score > pos_score:
        return "Negative"
    if pos_score > neg_score:
        return "Positive"
    if rating_lean:
        return rating_lean
    if len(text.strip()) < 10:
        return "Unclear"
    return "Neutral"


def match_themes(text, theme_dict):
    text_lower = text.lower()
    matched = []
    for theme, patterns in theme_dict.items():
        if any(re.search(p, text_lower) for p in patterns):
            matched.append(theme)
    return matched


def pct_str(count, total, prefix="~"):
    if total == 0:
        return "n/a"
    p = round(100 * count / total)
    if total < 30:
        return f"{prefix}{p}%"
    return f"{prefix}{p}%"


def normalize_source(src):
    if src in ("Google Store", "Apple Store"):
        return "App stores"
    return src


def pull_data(cur, report_date):
    start_ts = f"{report_date} 00:00:00"

    cur.execute(f"""
        SELECT COUNT(*) FROM jeeny_prod.general.feedbackevents
        WHERE TIMESTAMP >= '{start_ts}'
          AND TIMESTAMP < '{report_date}'::date + INTERVAL '1 day'
          AND RATINGS_COMMENTS IS NOT NULL
          AND TRIM(RATINGS_COMMENTS) <> ''
          AND LENGTH(TRIM(RATINGS_COMMENTS)) >= 3
    """)
    total_comments = cur.fetchone()[0]

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
          WHERE TIMESTAMP >= '{start_ts}'
            AND TIMESTAMP < '{report_date}'::date + INTERVAL '1 day'
            AND RATINGS_COMMENTS IS NOT NULL
            AND TRIM(RATINGS_COMMENTS) <> ''
            AND LENGTH(TRIM(RATINGS_COMMENTS)) >= 3
        )
        SELECT GUID, FEEDBACK_SOURCE, persona, RATINGS_COMMENTS, text_for_analysis,
               OVERALL_SENTIMENT, RATING_STARS, CITY, COUNTRY,
               TICKET_CATEGORY, CATEGORY, URGENCY, TIMESTAMP
        FROM base
        WHERE persona IN ('driver', 'rider') AND rn <= {SAMPLE_PER_CELL}
    """)

    cols = [d[0] for d in cur.description]
    rows = [dict(zip(cols, r)) for r in cur.fetchall()]

    cur.execute(f"""
        WITH base AS (
          SELECT
            FEEDBACK_SOURCE,
            CASE
              WHEN USER_TYPE = 'driver' THEN 'driver'
              WHEN USER_TYPE IN ('passenger', 'customer') THEN 'rider'
              ELSE 'other'
            END AS persona
          FROM jeeny_prod.general.feedbackevents
          WHERE TIMESTAMP >= '{start_ts}'
            AND TIMESTAMP < '{report_date}'::date + INTERVAL '1 day'
            AND RATINGS_COMMENTS IS NOT NULL
            AND TRIM(RATINGS_COMMENTS) <> ''
            AND LENGTH(TRIM(RATINGS_COMMENTS)) >= 3
        )
        SELECT FEEDBACK_SOURCE, persona, COUNT(*) AS volume
        FROM base WHERE persona IN ('driver', 'rider')
        GROUP BY 1, 2
    """)
    volumes = {(r[0], r[1]): r[2] for r in cur.fetchall()}

    return total_comments, volumes, rows


def analyze(rows):
    for row in rows:
        row["SENTIMENT"] = classify_sentiment(row)

    # Volume table cells
    table_sources = ["Zendesk", "ride_rating", "in_app", "App stores"]
    vol_table = {}
    pct_table = {}

    for src in table_sources:
        vol_table[src] = {"driver": 0, "rider": 0}
        pct_table[src] = {"driver": "n/a", "rider": "n/a"}

    # Aggregate app stores volumes from raw data (passed separately)
    return rows


def build_volume_table(volumes, rows):
    """Build volume and %neg for digest table."""
    # Combine app store sources
    combined_vols = defaultdict(int)
    for (src, persona), vol in volumes.items():
        norm = normalize_source(src)
        combined_vols[(norm, persona)] += vol

    table = {}
    for src in ["Zendesk", "ride_rating", "in_app", "App stores"]:
        table[src] = {
            "driver_vol": combined_vols.get((src, "driver"), 0),
            "rider_vol": combined_vols.get((src, "rider"), 0),
            "driver_pct": "n/a",
            "rider_pct": "n/a",
        }

    # %neg from sample per cell
    cell_data = defaultdict(lambda: {"neg": 0, "total": 0})
    for row in rows:
        src = normalize_source(row["FEEDBACK_SOURCE"])
        persona = row["PERSONA"]
        if row["SENTIMENT"] in ("Negative", "Mixed"):
            cell_data[(src, persona)]["neg"] += 1
        if row["SENTIMENT"] != "Unclear":
            cell_data[(src, persona)]["total"] += 1

    for (src, persona), data in cell_data.items():
        if src in table:
            pct = pct_str(data["neg"], data["total"])
            if persona == "driver":
                table[src]["driver_pct"] = pct
            else:
                table[src]["rider_pct"] = pct

    # App stores: n/a for %neg
    table["App stores"]["driver_pct"] = "n/a"
    table["App stores"]["rider_pct"] = "n/a"

    return table


THEME_EMOJI = {
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


def themes_for_row(row, theme_dict):
    """Extract themes from Zendesk category + text keywords."""
    found = set()
    persona = row.get("PERSONA")
    cat = row.get("TICKET_CATEGORY") or ""
    if cat == "STC Pay":
        if persona == "driver":
            found.add("Payment not credited / withdrawals")
        else:
            found.add("Payment & charging errors")
    elif cat in ZENDESK_CATEGORY_THEMES:
        found.add(ZENDESK_CATEGORY_THEMES[cat])
    text = row.get("TEXT_FOR_ANALYSIS") or ""
    orig = row.get("RATINGS_COMMENTS") or ""
    combined = f"{text} {orig}"
    for theme in match_themes(combined, theme_dict):
        found.add(theme)
    return found


def extract_themes(rows, persona, theme_dict):
    persona_rows = [r for r in rows if r["PERSONA"] == persona]
    sample_size = len(persona_rows)
    neg_mixed = [r for r in persona_rows if r["SENTIMENT"] in ("Negative", "Mixed")]

    theme_counts = defaultdict(int)
    for row in neg_mixed:
        for theme in themes_for_row(row, theme_dict):
            theme_counts[theme] += 1

    ranked = sorted(theme_counts.items(), key=lambda x: -x[1])
    results = []
    for theme, count in ranked[:7]:
        pct = pct_str(count, sample_size)
        emoji = THEME_EMOJI.get(theme, "•")
        results.append((emoji, theme, pct))
    return results, sample_size


def anonymize_summary(text):
    summary = re.sub(r"^\w+_driving\s*::\s*", "", text, flags=re.I)
    summary = summary.replace("-", " ").strip()
    summary = re.sub(r"\b\d{8,}\b", "[phone]", summary)
    summary = re.sub(r"\b[A-Z][a-z]+ [A-Z][a-z]+\b", "[person]", summary)
    if len(summary) > 100:
        summary = summary[:100] + "..."
    return summary


SAFETY_SUMMARY_TEMPLATES = {
    "Harassment": "Harassment or threatening behavior reported during or after ride",
    "Reckless driving": "Reckless or dangerous driving reported with speeding or unsafe maneuvers",
    "Identity mismatch": "Vehicle or driver did not match app profile at pickup",
}


def flag_safety(rows):
    flagged = []
    for row in rows:
        if row["SENTIMENT"] not in ("Negative", "Mixed"):
            continue
        text = row.get("TEXT_FOR_ANALYSIS") or ""
        orig = row.get("RATINGS_COMMENTS") or ""
        combined = f"{text} {orig}".lower()
        city = row.get("CITY") or "unknown city"
        for category, patterns in SAFETY_PATTERNS.items():
            if any(re.search(p, combined) for p in patterns):
                summary = SAFETY_SUMMARY_TEMPLATES.get(category, "Safety concern reported")
                flagged.append({
                    "category": category,
                    "summary": f"{summary} ({city})",
                })
                break
    return flagged


PRIORITY_ACTIONS = {
    "Wrong driver / car vs app": "Tighten identity verification at pickup and flag mismatched vehicle/plate before trip start.",
    "Overcharging / fare disputes": "Audit final-fare vs estimate recalculation and surface fare breakdown before trip confirmation.",
    "Payment not credited / withdrawals": "Review wallet payout pipeline and STC Pay reconciliation for stuck transfers.",
    "Passenger didn't pay (cash)": "Strengthen cash-trip enforcement and passenger payment dispute workflow.",
    "Cancellation fees not received / unfair fines": "Audit cancellation-fee crediting rules and driver compensation SLAs.",
    "Driver conduct": "Reinforce conduct enforcement and faster escalation for rude/unprofessional behavior.",
    "Route manipulation / slow driving": "Improve route-deviation detection and auto-adjust fare when excessive detours detected.",
    "Cancellation (unfair fees, driver-initiated)": "Review driver-initiated cancellation policy and rider fee waiver rules.",
    "No-show / wrong pickup": "Improve pickup-point validation and driver arrival tracking with rider notifications.",
    "Payment & charging errors": "Fix double-charge and wallet credit failures; prioritize STC Pay error handling.",
}


def compose_priorities(driver_themes, rider_themes, safety_count, table):
    candidates = []
    for emoji, theme, pct in rider_themes[:3]:
        candidates.append((theme, pct, "rider"))
    for emoji, theme, pct in driver_themes[:3]:
        candidates.append((theme, pct, "driver"))

    priorities = []
    used = set()
    for theme, pct, persona in candidates:
        if theme in used:
            continue
        used.add(theme)
        action = PRIORITY_ACTIONS.get(theme, "Investigate root causes and deploy targeted fixes.")
        n = len(priorities) + 1
        priorities.append(
            f"{n}. **{theme}** — {pct} of {persona} sample. {action}"
        )
        if len(priorities) >= 3:
            break

    if safety_count >= 3 and len(priorities) < 3:
        n = len(priorities) + 1
        priorities.append(
            f"{n}. **Safety escalation review** — {safety_count} flagged cases in sample. "
            "Prioritize safety team triage and faster response SLAs."
        )

    while len(priorities) < 3:
        n = len(priorities) + 1
        priorities.append(
            f"{n}. **Monitor feedback trends** — Continue tracking volume shifts across sources."
        )

    return priorities[:3]


def compose_digest(report_day_name, total_comments, sample_count, table,
                   driver_themes, rider_themes, safety_flags, priorities):
    lines = [
        f"## 📊 {report_day_name}",
        "",
        f"{total_comments:,} comments · stratified sample ~{sample_count} analyzed "
        f"(up to {SAMPLE_PER_CELL} per source×user-type cell). "
        "Zendesk = support tickets (complaint-only by nature, so its %neg is always very high); "
        "ride_rating is the truer sentiment gauge.",
        "",
        "|Source|Driver (vol · %neg)|Rider (vol · %neg)|",
        "|  ---  |  ---  |  ---  |",
    ]

    for src in ["Zendesk", "ride_rating", "in_app", "App stores"]:
        t = table[src]
        d_cell = f"{t['driver_vol']:,} · {t['driver_pct']}"
        if src == "in_app":
            r_cell = "—"
        else:
            r_cell = f"{t['rider_vol']:,} · {t['rider_pct']}"
        lines.append(f"|{src}|{d_cell}|{r_cell}|")

    lines += ["", "### 🏎️ Drivers — top issues", "", "*(% of driver sample)*", ""]
    for emoji, theme, pct in driver_themes:
        lines.append(f"* {emoji} {theme} — **{pct}**")

    lines += ["", "### 🧍 Riders — top issues", "", "*(% of rider sample)*", ""]
    for emoji, theme, pct in rider_themes:
        lines.append(f"* {emoji} {theme} — **{pct}**")

    lines += ["", f"### ⚠️ Safety escalations (~{len(safety_flags)} flagged)", ""]
    safety_emoji = {"Harassment": "🆘", "Reckless driving": "🏎️", "Identity mismatch": "🚙"}
    shown = 0
    for flag in safety_flags[:3]:
        cat = flag["category"]
        emoji = safety_emoji.get(cat, "⚠️")
        lines.append(f"* {emoji} **{cat}** — {flag['summary']}")
        shown += 1
    if shown == 0:
        lines.append("* No critical safety escalations flagged in today's sample.")

    lines += ["", "### 🎯 Priorities", ""]
    for p in priorities:
        lines.append(p)

    lines += ["", "---", ""]
    return "\n".join(lines)


def main():
    report_date, report_day_name = get_report_date()
    conn, cur = connect()
    total_comments, volumes, rows = pull_data(cur, report_date)
    conn.close()

    for row in rows:
        row["SENTIMENT"] = classify_sentiment(row)

    sample_count = len(rows)
    table = build_volume_table(volumes, rows)
    driver_themes, _ = extract_themes(rows, "driver", DRIVER_THEMES)
    rider_themes, _ = extract_themes(rows, "rider", RIDER_THEMES)
    safety_flags = flag_safety(rows)
    priorities = compose_priorities(driver_themes, rider_themes, len(safety_flags), table)
    digest = compose_digest(
        report_day_name, total_comments, sample_count, table,
        driver_themes, rider_themes, safety_flags, priorities,
    )

    result = {
        "report_date": report_date,
        "report_day_name": report_day_name,
        "total_comments": total_comments,
        "sample_count": sample_count,
        "digest": digest,
        "priorities": priorities,
        "driver_themes": driver_themes,
        "rider_themes": rider_themes,
        "safety_count": len(safety_flags),
        "table": table,
    }
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
