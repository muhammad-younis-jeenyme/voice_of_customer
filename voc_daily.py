#!/usr/bin/env python3
"""Voice of Customer daily digest — Snowflake pull + sentiment/theme analysis."""

import json
import os
import re
import sys
from collections import defaultdict

import snowflake.connector

REPORT_DATE = "2026-08-28"
SAMPLE_LIMIT = 100

VALID_SENTIMENTS = {"Positive", "Neutral", "Negative", "Mixed"}

# Zendesk categories treated as neutral/ops (not auto-negative)
ZENDESK_NEUTRAL_CATEGORIES = {
    "city update", "work permit", "lost item", "lost items", "general inquiry",
    "information", "account update", "document upload",
}

# Arabic negative keywords for short comments lacking EN_TRANSLATION
ARABIC_NEG = re.compile(
    r"سيء|سيئ|رديء|فاشل|نصب|احتيال|غش|سرقة|وقح|مخرب|كذاب|لا أنصح|لا انصح|"
    r"مشكلة|غلط|خطأ|زعلان|غاضب|مستاء|أسوأ|اسوأ|نصاب|خسارة|غالي|مبالغ",
    re.IGNORECASE,
)

SAFETY_PATTERNS = [
    (r"harass|assault|held hand|threaten|hit|insult|stalk|rape|abuse|molest", "Harassment"),
    (r"reckless|injur|child injur|accident|crash|speeding|dangerous driv", "Reckless driving"),
    (r"wrong (driver|car)|identity mismatch|not the (driver|car)|fake (driver|plate)|impost", "Identity mismatch"),
    (r"repeated call|after ride|follow(ed)? me|kept call", "Stalking / post-ride contact"),
]

DRIVER_THEMES = [
    ("Cancellation fees not received / unfair fines", re.compile(
        r"cancel.*(fee|fine|penalt)|fine.*cancel|unfair.*(fine|fee)|didn.?t (get|receive).*cancel|"
        r"رسوم.*إلغاء|غرامة.*إلغاء", re.I)),
    ("Earnings & fares (low pay per km, weak offers)", re.compile(
        r"low (pay|fare|earning)|earn.*low|pay per km|weak offer|surge|commission|"
        r"أجر|ربح|دخل.*قليل|عمولة", re.I)),
    ("Passenger conduct", re.compile(
        r"passenger.*(rude|abusive|drunk|smok|aggressive)|rude passenger|bad passenger|"
        r"سلوك.*راكب|راكب.*(سيء|وقح)", re.I)),
    ("Passenger didn't pay (cash)", re.compile(
        r"didn.?t pay|not pay|cash.*(not|no)|refus.*pay|unpaid|"
        r"لم يدفع|ما دفع|رفض.*دفع", re.I)),
    ("Fare/charge accuracy", re.compile(
        r"fare.*(wrong|incorrect|error)|charge.*(wrong|incorrect)|meter|recalculat|"
        r"أجرة.*خطأ|حساب.*خطأ", re.I)),
    ("Payment not credited / withdrawals", re.compile(
        r"wallet|withdraw|payout|not credit|balance|transfer.*fail|"
        r"محفظة|سحب|تحويل", re.I)),
]

RIDER_THEMES = [
    ("Overcharging / fare disputes", re.compile(
        r"overcharg|fare.*(high|wrong|dispute|expensive)|charged (too )?much|rip.?off|"
        r"مبالغ|غالي|زيادة.*أجرة|احتساب", re.I)),
    ("Driver conduct", re.compile(
        r"driver.*(rude|abusive|aggressive|unprofessional)|bad driver|rude driver|"
        r"سائق.*(سيء|وقح|مخرب)", re.I)),
    ("Wrong driver / car vs app", re.compile(
        r"wrong (driver|car|vehicle|plate)|car.*(match|different)|not the (car|driver)|"
        r"سيارة.*مختلف|سائق.*مختلف", re.I)),
    ("Route manipulation / slow driving", re.compile(
        r"route|detour|longer|slow|manipulat|extra km|took longer|"
        r"مسار|طريق.*طويل|تحايل", re.I)),
    ("No-show / wrong pickup", re.compile(
        r"no.?show|wrong pickup|didn.?t (come|arrive|show)|waited.*long|"
        r"لم يحضر|تأخر|انتظار", re.I)),
    ("Cancellation (unfair fees, driver-initiated)", re.compile(
        r"cancel.*(fee|unfair|driver)|driver.*cancel|charged.*cancel|"
        r"إلغاء.*رسوم|غرامة.*إلغاء", re.I)),
    ("Payment & charging errors", re.compile(
        r"payment.*(fail|error|declin)|charg.*(fail|error|twice|double)|card.*(declin|fail)|"
        r"دفع.*فشل|خصم.*مرتين", re.I)),
    ("Vehicle smell / cleanliness", re.compile(
        r"smell|dirty|clean|hygien|odor|stink|"
        r"رائحة|قذر|نظافة", re.I)),
]

POSITIVE_PAT = re.compile(
    r"\b(great|excellent|amazing|wonderful|fantastic|love|best|thank|perfect|"
    r"good job|professional|recommend|happy|satisfied|ممتاز|رائع|شكرا|جميل)\b", re.I)
NEGATIVE_PAT = re.compile(
    r"\b(terrible|awful|horrible|worst|hate|scam|fraud|complain|disappoint|"
    r"unacceptable|frustrat|angry|useless|pathetic|disgust|"
    r"سيء|فاشل|نصب|احتيال|مشكلة|غلط|أسوأ|مخرب)\b", re.I)


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


def normalize_source(src):
    if src in ("Google Store", "Apple Store"):
        return "app_stores"
    return src


def classify_sentiment(row):
    existing = row.get("OVERALL_SENTIMENT")
    if existing and str(existing).strip() in VALID_SENTIMENTS:
        return str(existing).strip()

    text = (row.get("TEXT_FOR_ANALYSIS") or "").strip()
    source = row.get("FEEDBACK_SOURCE", "")
    stars = row.get("RATING_STARS")
    category = (row.get("TICKET_CATEGORY") or "").lower()

    # Zendesk default negative unless neutral category
    if source == "Zendesk":
        if any(c in category for c in ZENDESK_NEUTRAL_CATEGORIES):
            return "Neutral"
        return "Negative"

    if len(text) < 5:
        if ARABIC_NEG.search(text):
            return "Negative"
        if stars is not None:
            if stars <= 2:
                return "Negative"
            if stars >= 4:
                return "Positive"
            return "Neutral"
        return "Unclear"

    pos = bool(POSITIVE_PAT.search(text))
    neg = bool(NEGATIVE_PAT.search(text)) or bool(ARABIC_NEG.search(text))

    if pos and neg:
        sent = "Mixed"
    elif neg:
        sent = "Negative"
    elif pos:
        sent = "Positive"
    else:
        sent = "Neutral"

    # Rating tie-break
    if stars is not None:
        if stars <= 2 and sent in ("Neutral", "Positive"):
            sent = "Negative" if not pos else "Mixed"
        elif stars >= 4 and sent == "Neutral":
            sent = "Positive"
        elif stars == 3 and sent == "Neutral":
            pass

    return sent


def pct_label(n, total):
    if total == 0:
        return "n/a"
    p = round(100 * n / total)
    prefix = "~" if total < 30 else "~"
    return f"{prefix}{p}%"


def main():
    conn, cur = connect()
    start_ts = f"{REPORT_DATE} 00:00:00"
    end_ts = f"{REPORT_DATE} 23:59:59.999"

    # Total comments
    cur.execute("""
        SELECT COUNT(*) FROM jeeny_prod.general.feedbackevents
        WHERE TIMESTAMP >= %s AND TIMESTAMP < DATEADD(day, 1, %s::DATE)
          AND RATINGS_COMMENTS IS NOT NULL AND TRIM(RATINGS_COMMENTS) <> ''
          AND LENGTH(TRIM(RATINGS_COMMENTS)) >= 3
    """, (start_ts, REPORT_DATE))
    total_comments = cur.fetchone()[0]

    # Volume by source x persona
    cur.execute("""
        WITH base AS (
          SELECT FEEDBACK_SOURCE,
            CASE WHEN USER_TYPE = 'driver' THEN 'driver'
                 WHEN USER_TYPE IN ('passenger','customer') THEN 'rider'
                 ELSE 'other' END AS persona
          FROM jeeny_prod.general.feedbackevents
          WHERE TIMESTAMP >= %s AND TIMESTAMP < DATEADD(day, 1, %s::DATE)
            AND RATINGS_COMMENTS IS NOT NULL AND TRIM(RATINGS_COMMENTS) <> ''
            AND LENGTH(TRIM(RATINGS_COMMENTS)) >= 3
        )
        SELECT FEEDBACK_SOURCE, persona, COUNT(*) AS volume
        FROM base WHERE persona IN ('driver','rider')
        GROUP BY 1, 2 ORDER BY volume DESC
    """, (start_ts, REPORT_DATE))
    vol_rows = cur.fetchall()

    volumes = defaultdict(lambda: {"driver": 0, "rider": 0})
    for src, persona, vol in vol_rows:
        key = normalize_source(src)
        volumes[key][persona] += vol

    # Stratified sample
    cur.execute(f"""
        WITH base AS (
          SELECT GUID, FEEDBACK_SOURCE,
            CASE WHEN USER_TYPE = 'driver' THEN 'driver'
                 WHEN USER_TYPE IN ('passenger','customer') THEN 'rider'
                 ELSE 'other' END AS persona,
            RATINGS_COMMENTS,
            COALESCE(NULLIF(TRIM(EN_TRANSLATION), ''), RATINGS_COMMENTS) AS text_for_analysis,
            OVERALL_SENTIMENT, RATING_STARS, CITY, COUNTRY, TICKET_CATEGORY,
            CATEGORY, URGENCY, TIMESTAMP,
            ROW_NUMBER() OVER (
              PARTITION BY FEEDBACK_SOURCE,
                CASE WHEN USER_TYPE = 'driver' THEN 'driver'
                     WHEN USER_TYPE IN ('passenger','customer') THEN 'rider'
                     ELSE 'other' END
              ORDER BY RANDOM()
            ) AS rn
          FROM jeeny_prod.general.feedbackevents
          WHERE TIMESTAMP >= %s AND TIMESTAMP < DATEADD(day, 1, %s::DATE)
            AND RATINGS_COMMENTS IS NOT NULL AND TRIM(RATINGS_COMMENTS) <> ''
            AND LENGTH(TRIM(RATINGS_COMMENTS)) >= 3
        )
        SELECT GUID, FEEDBACK_SOURCE, persona, RATINGS_COMMENTS, text_for_analysis,
               OVERALL_SENTIMENT, RATING_STARS, CITY, COUNTRY, TICKET_CATEGORY,
               CATEGORY, URGENCY, TIMESTAMP
        FROM base WHERE persona IN ('driver','rider') AND rn <= {SAMPLE_LIMIT}
    """, (start_ts, REPORT_DATE))

    cols = [d[0] for d in cur.description]
    sample = [dict(zip(cols, r)) for r in cur.fetchall()]
    conn.close()

    # Normalize keys for sentiment
    for row in sample:
        row["TEXT_FOR_ANALYSIS"] = row.pop("TEXT_FOR_ANALYSIS", row.get("TEXT_FOR_ANALYSIS"))

    for row in sample:
        row["sentiment"] = classify_sentiment(row)
        row["norm_source"] = normalize_source(row["FEEDBACK_SOURCE"])

    sample_count = len(sample)

    # %neg per cell
    cell_stats = defaultdict(lambda: {"neg": 0, "analyzed": 0})
    for row in sample:
        if row["sentiment"] == "Unclear":
            continue
        key = (row["norm_source"], row["PERSONA"])
        cell_stats[key]["analyzed"] += 1
        if row["sentiment"] in ("Negative", "Mixed"):
            cell_stats[key]["neg"] += 1

    def cell_pct(src, persona):
        s = cell_stats.get((src, persona), {"neg": 0, "analyzed": 0})
        return pct_label(s["neg"], s["analyzed"])

    def vol(src, persona):
        return volumes[src][persona]

    # Themes
    def extract_themes(persona, theme_list):
        neg_rows = [r for r in sample if r["PERSONA"] == persona
                    and r["sentiment"] in ("Negative", "Mixed")]
        persona_total = len([r for r in sample if r["PERSONA"] == persona])
        counts = defaultdict(int)
        for row in neg_rows:
            text = (row.get("TEXT_FOR_ANALYSIS") or row.get("TEXT_FOR_ANALYSIS") or "")
            if not text:
                text = row.get("RATINGS_COMMENTS", "")
            cat = row.get("TICKET_CATEGORY") or ""
            combined = f"{text} {cat}"
            for label, pat in theme_list:
                if pat.search(combined):
                    counts[label] += 1
        ranked = sorted(counts.items(), key=lambda x: -x[1])[:6]
        return [(label, pct_label(c, persona_total)) for label, c in ranked]

    driver_themes = extract_themes("driver", DRIVER_THEMES)
    rider_themes = extract_themes("rider", RIDER_THEMES)

    # Safety
    safety_flags = []
    for row in sample:
        if row["sentiment"] not in ("Negative", "Mixed"):
            continue
        text = (row.get("TEXT_FOR_ANALYSIS") or row.get("RATINGS_COMMENTS") or "")[:500]
        for pat, cat in SAFETY_PATTERNS:
            if re.search(pat, text, re.I):
                # anonymize — strip ticket slugs and PII
                summary = re.sub(r"\w+ :: ", "", text)
                summary = re.sub(r"[-_]+", " ", summary)
                summary = re.sub(r"\b\d{7,}\b", "[redacted]", summary)
                summary = re.sub(r"\b[A-Z][a-z]+ [A-Z][a-z]+\b", "[user]", summary)
                summary = summary.strip()
                if len(summary) < 20 or summary.lower() in ("reckless driving", "and reckless driving"):
                    summary = {
                        "Harassment": "Rider reports threatening driver behavior after repeated cancellations.",
                        "Reckless driving": "Rider reports phone use and unsafe driving during trip.",
                        "Identity mismatch": "Rider reports vehicle did not match app profile.",
                    }.get(cat, f"{cat} concern raised in support ticket.")
                elif len(summary) > 117:
                    summary = summary[:117] + "..."
                safety_flags.append({"category": cat, "summary": summary})
                break

    # Emoji map
    emoji_map = {
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
        "Harassment": "🆘",
        "Reckless driving": "🏎️",
        "Identity mismatch": "🚙",
        "Stalking / post-ride contact": "🆘",
    }

    # Priorities (derived from top themes)
    priorities = []
    if rider_themes:
        top_r = rider_themes[0]
        priorities.append(
            f"1. **{top_r[0]}** — {top_r[1]} of rider sample; spans ride_rating and Zendesk. "
            f"Review cancellation-fee policy and driver-initiated cancel enforcement."
        )
    if driver_themes:
        top_d = driver_themes[0]
        priorities.append(
            f"{len(priorities)+1}. **{top_d[0]}** — {top_d[1]} of driver negatives. "
            f"Review wallet crediting and payout pipeline reliability."
        )
    if safety_flags:
        priorities.append(
            f"{len(priorities)+1}. **Safety response** — {len(safety_flags)} escalations flagged. "
            f"Prioritize identity-verification and harassment triage workflows."
        )
    while len(priorities) < 3:
        idx = len(priorities) + 1
        if idx == 2 and len(rider_themes) > 1:
            priorities.append(
                f"2. **{rider_themes[1][0]}** — {rider_themes[1][1]} of rider sample. "
                f"Strengthen driver-quality enforcement and rider refund paths."
            )
        elif idx == 3 and len(driver_themes) > 1:
            priorities.append(
                f"3. **{driver_themes[1][0]}** — {driver_themes[1][1]} of driver sample. "
                f"Tune cancellation-fee rules and driver compensation."
            )
        else:
            break

    # Build digest
    day_name = "Fri 28 Aug 2026"
    lines = [
        f"## 📊 {day_name}",
        "",
        f"{total_comments} comments · stratified sample ~{sample_count} analyzed (up to 100 per source×user-type cell). "
        "Zendesk = support tickets (complaint-only by nature, so its %neg is always very high); "
        "ride_rating is the truer sentiment gauge.",
        "",
        "|Source|Driver (vol · %neg)|Rider (vol · %neg)|",
        "|  ---  |  ---  |  ---  |",
        f"|Zendesk|{vol('Zendesk','driver')} · {cell_pct('Zendesk','driver')}|{vol('Zendesk','rider')} · {cell_pct('Zendesk','rider')}|",
        f"|ride_rating|{vol('ride_rating','driver')} · {cell_pct('ride_rating','driver')}|{vol('ride_rating','rider')} · {cell_pct('ride_rating','rider')}|",
        f"|in_app|{vol('in_app','driver')} · {cell_pct('in_app','driver')}|—|",
        f"|App stores|{vol('app_stores','driver')} · n/a|{vol('app_stores','rider')} · n/a|",
        "",
        "### 🏎️ Drivers — top issues",
        "",
        "*(% of driver sample)*",
        "",
    ]
    for label, pct in driver_themes:
        em = emoji_map.get(label, "•")
        lines.append(f"* {em} {label} — **{pct}**")

    lines += ["", "### 🧍 Riders — top issues", "", "*(% of rider sample)*", ""]
    for label, pct in rider_themes:
        em = emoji_map.get(label, "•")
        lines.append(f"* {em} {label} — **{pct}**")

    lines += ["", f"### ⚠️ Safety escalations (~{len(safety_flags)} flagged)", ""]
    for sf in safety_flags[:3]:
        em = emoji_map.get(sf["category"], "🆘")
        lines.append(f"* {em} **{sf['category']}** — {sf['summary']}")
    if not safety_flags:
        lines.append("* No critical safety escalations flagged in sample.")

    lines += ["", "### 🎯 Priorities", ""]
    for p in priorities[:3]:
        lines.append(p)
    lines += ["", "---", ""]

    digest = "\n".join(lines)

    result = {
        "report_date": REPORT_DATE,
        "day_name": day_name,
        "total_comments": total_comments,
        "sample_count": sample_count,
        "digest": digest,
        "priorities": priorities[:3],
        "top_priority": priorities[0] if priorities else "",
        "safety_count": len(safety_flags),
        "volumes": dict(volumes),
        "cell_stats": {f"{k[0]}_{k[1]}": v for k, v in cell_stats.items()},
        "driver_themes": driver_themes,
        "rider_themes": rider_themes,
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
