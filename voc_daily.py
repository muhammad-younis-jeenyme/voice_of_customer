#!/usr/bin/env python3
"""Voice of Customer daily digest — Snowflake pull + sentiment/theme analysis."""

import json
import os
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import snowflake.connector

SAMPLE_PER_CELL = 100
MIN_COMMENT_LEN = 3
VALID_SENTIMENTS = {"Positive", "Neutral", "Negative", "Mixed"}

# Theme keyword patterns (text_for_analysis, case-insensitive)
DRIVER_THEMES = [
    ("Cancellation fees not received / unfair fines", [
        r"cancel", r"fine", r"penalty", r"fee not", r"didn.?t receive.*fee",
        r"unfair.*fine", r"cancellation fee",
    ]),
    ("Payment not credited / withdrawals", [
        r"withdraw", r"wallet", r"not credited", r"payment not", r"payout",
        r"balance", r"transfer",
    ]),
    ("Earnings & fares (low pay per km, weak offers)", [
        r"earn", r"low pay", r"fare.*low", r"offer", r"commission", r"income",
        r"per km", r"per kilometer", r"weak offer",
    ]),
    ("Passenger conduct", [
        r"passenger.*(rude|bad|conduct|behavior|behaviour|smell|drunk|fight)",
        r"rider.*(rude|bad|conduct|behavior|behaviour|smell|drunk|fight)",
        r"customer.*(rude|bad|conduct)",
    ]),
    ("Passenger didn't pay (cash)", [
        r"didn.?t pay", r"not pay", r"cash.*(not|no)", r"without pay", r"refused to pay",
        r"no payment", r"did not pay",
    ]),
    ("Fare/charge accuracy", [
        r"wrong (fare|charge|amount|price)", r"fare.*(wrong|incorrect|error)",
        r"charge.*(wrong|incorrect|error)", r"overcharg", r"undercharg",
    ]),
]

RIDER_THEMES = [
    ("Overcharging / fare disputes", [
        r"overcharg", r"fare.*(high|wrong|dispute|expensive|too much)",
        r"charged.*(more|extra|twice)", r"price.*(high|wrong)",
    ]),
    ("Driver conduct", [
        r"driver.*(rude|bad|conduct|behavior|behaviour|angry|aggressive|impolite)",
        r"unprofessional", r"insult", r"shout", r"yell",
    ]),
    ("Wrong driver / car vs app", [
        r"wrong (driver|car|vehicle|plate)", r"different (driver|car|vehicle)",
        r"not the (driver|car)", r"identity", r"plate number", r"car.*match",
    ]),
    ("Route manipulation / slow driving", [
        r"long route", r"detour", r"longer route", r"slow driv", r"manipulat",
        r"extra (km|kilometer|distance)", r"took longer", r"roundabout",
    ]),
    ("No-show / wrong pickup", [
        r"no show", r"no-show", r"didn.?t (come|arrive|show)", r"wrong pickup",
        r"pickup.*(wrong|late)", r"waited.*(long|forever)",
    ]),
    ("Cancellation (unfair fees, driver-initiated)", [
        r"cancel.*(fee|charge|unfair)", r"driver.*cancel", r"cancelled by driver",
        r"cancellation.*(fee|unfair)", r"charged.*cancel",
    ]),
    ("Payment & charging errors", [
        r"payment.*(fail|error|issue|problem)", r"charged.*(twice|double)",
        r"card.*(declin|fail|charge)", r"wallet.*(error|issue)",
    ]),
    ("Vehicle smell / cleanliness", [
        r"smell", r"dirty", r"clean", r"hygien", r"odor", r"odour", r"filthy",
    ]),
]

ZENDESK_NEUTRAL_CATS = {
    "city update", "work permit", "lost item", "lost and found", "general inquiry",
    "information", "account update", "document", "registration",
}

SAFETY_PATTERNS = [
    ("Harassment", [
        r"harass", r"assault", r"held (my )?hand", r"threaten", r"hit me", r"insult",
        r"stalk", r"inappropriate touch", r"sexual", r"molest",
    ]),
    ("Reckless driving", [
        r"reckless", r"accident", r"crash", r"injur", r"child.*injur", r"speed",
        r"dangerous driv", r"almost hit",
    ]),
    ("Identity mismatch", [
        r"wrong (driver|car|vehicle).*safety", r"different (driver|car).*scared",
        r"not the registered", r"fake driver", r"impersonat",
    ]),
    ("Stalking / post-ride contact", [
        r"after the ride.*(call|message|contact)", r"repeated (call|message)",
        r"keeps calling", r"followed me",
    ]),
]

ARABIC_NEGATIVE = [
    "سيء", "سيئ", "مشكلة", "غش", "نصب", "سرقة", "وقح", "فظ", "احتيال", "غير مهذب",
    "لا يرد", "لم يأت", "لم يحضر", "زيادة", "غالي", "مبالغ",
]


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
    cur.execute(f"USE WAREHOUSE {os.environ['SNOWFLAKE_WAREHOUSE']}")
    return conn, cur


def report_dates():
    tz = ZoneInfo("Asia/Riyadh")
    today = datetime.now(tz).date()
    report_date = today - timedelta(days=1)
    day_name = report_date.strftime("%a %d %b %Y")
    return report_date.isoformat(), day_name


def classify_sentiment(row):
    existing = (row.get("OVERALL_SENTIMENT") or "").strip()
    if existing in VALID_SENTIMENTS:
        return existing

    text = (row.get("TEXT_FOR_ANALYSIS") or "").strip()
    source = (row.get("FEEDBACK_SOURCE") or "")
    stars = row.get("RATING_STARS")
    ticket_cat = (row.get("TICKET_CATEGORY") or "").lower()

    if source == "Zendesk":
        if any(n in ticket_cat for n in ZENDESK_NEUTRAL_CATS):
            return "Neutral"
        return "Negative"

    text_lower = text.lower()
    if len(text) < 5:
        for kw in ARABIC_NEGATIVE:
            if kw in text:
                return "Negative"
        if stars is not None:
            if stars <= 2:
                return "Negative"
            if stars >= 4:
                return "Positive"
        return "Unclear"

    pos_words = ["good", "great", "excellent", "thank", "thanks", "perfect", "love", "best", "nice", "happy", "ممتاز", "شكر"]
    neg_words = ["bad", "worst", "terrible", "awful", "hate", "problem", "complaint", "disappoint", "fraud", "scam", "rude", "slow", "late", "never", "poor"]

    pos = sum(1 for w in pos_words if w in text_lower)
    neg = sum(1 for w in neg_words if w in text_lower)
    for kw in ARABIC_NEGATIVE:
        if kw in text:
            neg += 1

    if pos > 0 and neg > 0:
        sentiment = "Mixed"
    elif neg > pos:
        sentiment = "Negative"
    elif pos > neg:
        sentiment = "Positive"
    elif stars is not None:
        if stars <= 2:
            sentiment = "Negative"
        elif stars == 3:
            sentiment = "Neutral"
        elif stars >= 4:
            sentiment = "Positive"
        else:
            sentiment = "Neutral"
    else:
        sentiment = "Neutral"

    # rating tie-break
    if stars is not None and sentiment in ("Neutral", "Unclear"):
        if stars <= 2:
            sentiment = "Negative"
        elif stars >= 4:
            sentiment = "Positive"
    return sentiment


def match_themes(text, theme_defs):
    if not text:
        return []
    t = text.lower()
    matched = []
    for label, patterns in theme_defs:
        for pat in patterns:
            if re.search(pat, t, re.I):
                matched.append(label)
                break
    return matched


def normalize_source(src):
    if src in ("Google Store", "Apple Store"):
        return "App stores"
    return src


def pct_neg(counts, use_tilde):
    neg = counts.get("Negative", 0)
    analyzed = sum(v for k, v in counts.items() if k != "Unclear")
    if analyzed == 0:
        return "n/a"
    pct = round(100 * neg / analyzed)
    return f"~{pct}%" if use_tilde else f"{pct}%"


def anonymize(text, max_len=120):
    if not text:
        return "Customer reported a safety concern."
    t = re.sub(r"\b\d{7,}\b", "[redacted]", text)
    t = re.sub(r"[\w.-]+@[\w.-]+", "[email]", t)
    t = re.sub(r"\+?\d[\d\s-]{8,}\d", "[phone]", t)
    t = re.sub(r"\b[A-Z][a-z]+ [A-Z][a-z]+\b", "A user", t)
    t = t.strip()
    if len(t) > max_len:
        t = t[: max_len - 3].rsplit(" ", 1)[0] + "..."
    return t


def main():
    report_date, day_name = report_dates()
    start_ts = f"{report_date} 00:00:00"
    end_ts = f"{report_date} 23:59:59.999"

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

    total_sql = """
    SELECT COUNT(*) AS total_comments
    FROM jeeny_prod.general.feedbackevents
    WHERE TIMESTAMP >= %(start)s
      AND TIMESTAMP < DATEADD(day, 1, %(start)s::DATE)
      AND RATINGS_COMMENTS IS NOT NULL
      AND TRIM(RATINGS_COMMENTS) <> ''
      AND LENGTH(TRIM(RATINGS_COMMENTS)) >= %(min_len)s
    """

    sample_sql = """
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
      AND rn <= %(sample_n)s
    """

    params = {"start": start_ts, "min_len": MIN_COMMENT_LEN, "sample_n": SAMPLE_PER_CELL}

    cur.execute(total_sql, params)
    total_comments = cur.fetchone()[0]

    cur.execute(volume_sql, params)
    volumes_raw = cur.fetchall()

    cur.execute(sample_sql, params)
    cols = [d[0] for d in cur.description]
    sample_rows = [dict(zip(cols, row)) for row in cur.fetchall()]
    conn.close()

    # Build volume table
    vol = defaultdict(lambda: {"driver": 0, "rider": 0})
    for src, persona, count in volumes_raw:
        key = normalize_source(src)
        vol[key][persona] += count

    # Analyze sample
    cell_sentiment = defaultdict(Counter)
    driver_sample = []
    rider_sample = []
    driver_neg_mixed = []
    rider_neg_mixed = []
    safety_flags = []

    for row in sample_rows:
        row["TEXT_FOR_ANALYSIS"] = row.pop("TEXT_FOR_ANALYSIS") if "TEXT_FOR_ANALYSIS" in row else row.get("TEXT_FOR_ANALYSIS")
        if "TEXT_FOR_ANALYSIS" not in row:
            row["TEXT_FOR_ANALYSIS"] = row.get("TEXT_FOR_ANALYSIS") or row.get("TEXT_FOR_ANALYSIS")
        # snowflake returns uppercase
        row_norm = {k.upper(): v for k, v in row.items()}
        row_norm["TEXT_FOR_ANALYSIS"] = row_norm.get("TEXT_FOR_ANALYSIS") or row_norm.get("TEXT_FOR_ANALYSIS")

        text = row_norm.get("TEXT_FOR_ANALYSIS") or row_norm.get("TEXT_FOR_ANALYSIS")
        if text is None:
            text = row_norm.get("TEXT_FOR_ANALYSIS")
        # fix key from query alias
        if row_norm.get("TEXT_FOR_ANALYSIS") is None:
            for k in row_norm:
                if k.lower() == "text_for_analysis":
                    row_norm["TEXT_FOR_ANALYSIS"] = row_norm[k]
                    break

        sentiment = classify_sentiment({
            "OVERALL_SENTIMENT": row_norm.get("OVERALL_SENTIMENT"),
            "TEXT_FOR_ANALYSIS": row_norm.get("TEXT_FOR_ANALYSIS"),
            "FEEDBACK_SOURCE": row_norm.get("FEEDBACK_SOURCE"),
            "RATING_STARS": row_norm.get("RATING_STARS"),
            "TICKET_CATEGORY": row_norm.get("TICKET_CATEGORY"),
        })

        src = normalize_source(row_norm.get("FEEDBACK_SOURCE", ""))
        persona = row_norm.get("PERSONA", "")
        cell_key = (src, persona)
        cell_sentiment[cell_key][sentiment] += 1

        enriched = {
            **row_norm,
            "SENTIMENT": sentiment,
            "TEXT_FOR_ANALYSIS": row_norm.get("TEXT_FOR_ANALYSIS") or row_norm.get("TEXT_FOR_ANALYSIS"),
        }
        if persona == "driver":
            driver_sample.append(enriched)
            if sentiment in ("Negative", "Mixed"):
                driver_neg_mixed.append(enriched)
        elif persona == "rider":
            rider_sample.append(enriched)
            if sentiment in ("Negative", "Mixed"):
                rider_neg_mixed.append(enriched)

        if sentiment in ("Negative", "Mixed"):
            text_for_safety = (enriched.get("TEXT_FOR_ANALYSIS") or "")
            for cat, patterns in SAFETY_PATTERNS:
                for pat in patterns:
                    if re.search(pat, text_for_safety, re.I):
                        safety_flags.append({
                            "category": cat,
                            "summary": anonymize(text_for_safety),
                            "persona": persona,
                            "city": enriched.get("CITY"),
                        })
                        break

    # Theme extraction
    def theme_stats(neg_mixed_rows, theme_defs, full_sample_size):
        theme_counts = Counter()
        for row in neg_mixed_rows:
            text = row.get("TEXT_FOR_ANALYSIS") or ""
            ticket = (row.get("TICKET_CATEGORY") or "")
            themes = set(match_themes(text, theme_defs))
            # Zendesk category hints
            if row.get("FEEDBACK_SOURCE") == "Zendesk" and ticket:
                tl = ticket.lower()
                for label, _ in theme_defs:
                    if any(w in tl for w in label.lower().split("/")[0].split()[:2]):
                        themes.add(label)
            for t in themes:
                theme_counts[t] += 1
        results = []
        for label, _ in theme_defs:
            if theme_counts[label]:
                pct = round(100 * theme_counts[label] / max(full_sample_size, 1))
                results.append((label, pct))
        results.sort(key=lambda x: -x[1])
        return results[:6]

    driver_themes = theme_stats(driver_neg_mixed, DRIVER_THEMES, len(driver_sample))
    rider_themes = theme_stats(rider_neg_mixed, RIDER_THEMES, len(rider_sample))

    # Table cells
    sources_order = ["Zendesk", "ride_rating", "in_app", "App stores"]
    table_lines = []
    for src in sources_order:
        d_vol = vol[src]["driver"]
        r_vol = vol[src]["rider"]
        d_cell = cell_sentiment.get((src, "driver"), Counter())
        r_cell = cell_sentiment.get((src, "rider"), Counter())
        d_analyzed = sum(d_cell.values())
        r_analyzed = sum(r_cell.values())
        if src == "App stores":
            d_pct = "n/a"
            r_pct = "n/a"
        elif src == "in_app":
            d_pct = pct_neg(d_cell, d_analyzed < 30)
            r_pct = "—"
        else:
            d_pct = pct_neg(d_cell, d_analyzed < 30)
            r_pct = pct_neg(r_cell, r_analyzed < 30) if r_analyzed else "n/a"
        table_lines.append(f"|{src}|{d_vol} · {d_pct}|{r_vol} · {r_pct if r_vol or src != 'in_app' else '—'}|")

    THEME_EMOJI = {
        "Cancellation fees not received / unfair fines": "🚫",
        "Payment not credited / withdrawals": "🏦",
        "Earnings & fares (low pay per km, weak offers)": "💰",
        "Passenger conduct": "😡",
        "Passenger didn't pay (cash)": "💵",
        "Fare/charge accuracy": "📄",
        "Overcharging / fare disputes": "💸",
        "Driver conduct": "😡",
        "Wrong driver / car vs app": "🚙",
        "Route manipulation / slow driving": "🗺️",
        "No-show / wrong pickup": "📍",
        "Cancellation (unfair fees, driver-initiated)": "🚫",
        "Payment & charging errors": "🏦",
        "Vehicle smell / cleanliness": "🧼",
    }

    SAFETY_EMOJI = {
        "Harassment": "🆘",
        "Reckless driving": "🏎️",
        "Identity mismatch": "🚙",
        "Stalking / post-ride contact": "🆘",
    }

    driver_bullets = "\n".join(
        f"* {THEME_EMOJI.get(label, '•')} {label} — **~{pct}%**"
        for label, pct in driver_themes
    ) or "* No dominant themes in sample"

    rider_bullets = "\n".join(
        f"* {THEME_EMOJI.get(label, '•')} {label} — **~{pct}%**"
        for label, pct in rider_themes
    ) or "* No dominant themes in sample"

    # Dedupe safety by summary
    seen_safety = set()
    safety_examples = []
    for s in safety_flags:
        key = (s["category"], s["summary"][:60])
        if key in seen_safety:
            continue
        seen_safety.add(key)
        safety_examples.append(s)
        if len(safety_examples) >= 3:
            break

    safety_bullets = "\n".join(
        f"* {SAFETY_EMOJI.get(s['category'], '🆘')} **{s['category']}** — {s['summary']}"
        for s in safety_examples
    ) or "* No critical safety keywords detected in sample"

    # Priorities (heuristic from top themes + safety)
    priorities = []
    if rider_themes:
        top_r = rider_themes[0]
        priorities.append(
            f"1. **{top_r[0]}** — ~{top_r[1]}% of rider sample; spans ride_rating and Zendesk. "
            f"Audit fare calculation, receipt transparency, and dispute resolution SLAs."
        )
    if driver_themes:
        top_d = driver_themes[0]
        priorities.append(
            f"{len(priorities)+1}. **{top_d[0]}** — ~{top_d[1]}% of driver negatives. "
            f"Review cancellation-fee crediting workflow and driver comms on penalty rules."
        )
    if len(safety_flags) >= 5:
        priorities.append(
            f"{len(priorities)+1}. **Safety escalation review** — {len(safety_flags)} flagged in sample. "
            f"Triage harassment/reckless-driving tickets within 24h and verify identity-match checks."
        )
    elif len(rider_themes) > 1:
        top2 = rider_themes[1]
        priorities.append(
            f"{len(priorities)+1}. **{top2[0]}** — ~{top2[1]}% of rider sample. "
            f"Strengthen driver quality signals and in-ride reporting."
        )
    while len(priorities) < 3 and driver_themes:
        for dt in driver_themes[1:]:
            if len(priorities) >= 3:
                break
            if not any(dt[0] in p for p in priorities):
                priorities.append(
                    f"{len(priorities)+1}. **{dt[0]}** — ~{dt[1]}% of driver sample. "
                    f"Targeted ops review and product fix."
                )

    priority_text = "\n".join(priorities[:3])

    sample_count = len(sample_rows)
    digest = f"""## 📊 {day_name}

{total_comments} comments · stratified sample ~{sample_count} analyzed (up to 100 per source×user-type cell). Zendesk = support tickets (complaint-only by nature, so its %neg is always very high); ride_rating is the truer sentiment gauge.

|Source|Driver (vol · %neg)|Rider (vol · %neg)|
|  ---  |  ---  |  ---  |
{chr(10).join(table_lines)}

### 🏎️ Drivers — top issues

*(% of driver sample)*

{driver_bullets}

### 🧍 Riders — top issues

*(% of rider sample)*

{rider_bullets}

### ⚠️ Safety escalations (~{len(safety_flags)} flagged)

{safety_bullets}

### 🎯 Priorities

{priority_text}

---
"""

    result = {
        "report_date": report_date,
        "day_name": day_name,
        "total_comments": total_comments,
        "sample_count": sample_count,
        "digest": digest,
        "priorities": priorities[:3],
        "volumes": dict(vol),
        "cell_sentiment": {f"{k[0]}|{k[1]}": dict(v) for k, v in cell_sentiment.items()},
        "driver_themes": driver_themes,
        "rider_themes": rider_themes,
        "safety_count": len(safety_flags),
    }
    print(json.dumps(result, default=str))


if __name__ == "__main__":
    main()
