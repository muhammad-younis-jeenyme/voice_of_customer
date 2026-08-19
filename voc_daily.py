#!/usr/bin/env python3
"""Voice of Customer daily digest — Snowflake pull + sentiment/theme analysis."""

import json
import os
import re
import sys
from collections import defaultdict
from datetime import datetime, timedelta

import snowflake.connector

REPORT_DATE = sys.argv[1] if len(sys.argv) > 1 else None
SAMPLE_PER_CELL = 100

# Theme keyword patterns (EN_TRANSLATION text)
DRIVER_THEMES = {
    "Cancellation fees not received / unfair fines": [
        r"cancel.*fee", r"fine", r"penalt", r"unfair.*cancel", r"didn.?t receive.*fee",
        r"charged.*cancel", r"cancellation.*not.*paid", r"deduct",
    ],
    "Earnings & fares (low pay per km, weak offers)": [
        r"low.*pay", r"earn", r"fare.*low", r"offer", r"commission", r"income",
        r"pay.*km", r"weak.*offer", r"bonus", r"incentive",
    ],
    "Passenger conduct": [
        r"passenger.*(rude|bad|conduct|behav)", r"rude.*passenger", r"customer.*(rude|abusive)",
        r"passenger.*(smok|drunk|fight)", r"disrespect",
    ],
    "Passenger didn't pay (cash)": [
        r"didn.?t pay", r"not pay", r"cash.*(didn|refus|without)", r"unpaid", r"no payment",
        r"refus.*pay", r"without paying",
    ],
    "Fare/charge accuracy": [
        r"wrong.*(fare|charge|amount)", r"fare.*(wrong|incorrect)", r"overcharg",
        r"charged.*wrong", r"bill.*wrong", r"incorrect.*fare",
    ],
    "Payment not credited / withdrawals": [
        r"withdraw", r"wallet", r"not.*credit", r"payment.*not.*receiv", r"payout",
        r"balance", r"transfer", r"bank.*not",
    ],
}

RIDER_THEMES = {
    "Overcharging / fare disputes": [
        r"overcharg", r"fare.*(high|wrong|extra|dispute)", r"charged.*more", r"too much",
        r"bill.*high", r"price.*high", r"unfair.*fare", r"extra.*charge",
    ],
    "Driver conduct": [
        r"driver.*(rude|bad|conduct|behav|aggress)", r"rude.*driver", r"driver.*(smok|angry)",
        r"unprofessional", r"disrespect", r"driver.*(shout|yell|insult)",
    ],
    "Wrong driver / car vs app": [
        r"wrong.*(driver|car|vehicle)", r"different.*(driver|car|vehicle)", r"not.*match",
        r"another.*driver", r"car.*(different|wrong)", r"plate.*(wrong|different)",
    ],
    "Route manipulation / slow driving": [
        r"long.*route", r"detour", r"slow.*driv", r"manipulat.*route", r"unnecessary.*route",
        r"took.*long", r"longer.*route", r"traffic.*(on purpose|deliberate)",
    ],
    "No-show / wrong pickup": [
        r"no.?show", r"didn.?t (come|arrive|show)", r"wrong.*pickup", r"wait.*long",
        r"never.*arriv", r"driver.*not.*come",
    ],
    "Cancellation (unfair fees, driver-initiated)": [
        r"driver.*cancel", r"cancel.*fee", r"unfair.*cancel", r"cancelled.*by.*driver",
        r"charged.*cancel", r"penalt.*cancel", r"driver.*ask.*cancel",
    ],
    "Payment & charging errors": [
        r"payment.*(fail|error|issue|problem)", r"charg.*(fail|error|twice|double)",
        r"wallet", r"card.*(declin|fail|charg)", r"refund",
    ],
    "Vehicle smell / cleanliness": [
        r"smell", r"dirty", r"clean", r"hygien", r"odor", r"stink", r"filthy", r"unclean",
    ],
}

SAFETY_PATTERNS = {
    "Harassment": [
        r"harass", r"assault", r"touch", r"held.*hand", r"threaten", r"insult", r"stalk",
        r"inappropriat", r"sexual", r"abuse", r"hit me", r"beat",
    ],
    "Reckless driving": [
        r"reckless", r"speed", r"accident", r"injur", r"child.*injur", r"dangerous.*driv",
        r"crash", r"almost.*hit",
    ],
    "Identity mismatch": [
        r"wrong.*(driver|car).*safety", r"different.*person", r"not.*same.*driver",
        r"fake.*driver", r"scared.*driver", r"unsafe.*driver",
    ],
    "Post-ride contact": [
        r"call.*after", r"message.*after", r"contact.*after", r"whatsapp.*after",
        r"repeated.*call", r"stalk",
    ],
}

ZENDESK_CATEGORY_THEMES = {
    "driver": {
        "cancellation": "Cancellation fees not received / unfair fines",
        "cancel": "Cancellation fees not received / unfair fines",
        "fine": "Cancellation fees not received / unfair fines",
        "earning": "Earnings & fares (low pay per km, weak offers)",
        "fare": "Earnings & fares (low pay per km, weak offers)",
        "commission": "Earnings & fares (low pay per km, weak offers)",
        "passenger": "Passenger conduct",
        "customer": "Passenger conduct",
        "payment": "Payment not credited / withdrawals",
        "wallet": "Payment not credited / withdrawals",
        "withdraw": "Payment not credited / withdrawals",
        "cash": "Passenger didn't pay (cash)",
    },
    "rider": {
        "overcharg": "Overcharging / fare disputes",
        "fare": "Overcharging / fare disputes",
        "driver": "Driver conduct",
        "conduct": "Driver conduct",
        "wrong": "Wrong driver / car vs app",
        "identity": "Wrong driver / car vs app",
        "route": "Route manipulation / slow driving",
        "cancel": "Cancellation (unfair fees, driver-initiated)",
        "payment": "Payment & charging errors",
        "clean": "Vehicle smell / cleanliness",
        "smell": "Vehicle smell / cleanliness",
        "safety": "Driver conduct",
        "harass": "Driver conduct",
    },
}

PRIORITY_ACTIONS = {
    "Driver conduct": "Tighten driver conduct enforcement, repeat-offender flags, and post-ride CS follow-up.",
    "Payment & charging errors": "Audit payment gateway failures, double-charge detection, and wallet reconciliation.",
    "Passenger didn't pay (cash)": "Strengthen cash-trip verification and non-payment reporting workflow for drivers.",
    "Cancellation fees not received / unfair fines": "Audit cancellation-fee crediting rules and driver payout SLAs.",
    "Earnings & fares (low pay per km, weak offers)": "Review offer pricing, per-km rates, and incentive alignment in low-EPH zones.",
    "Overcharging / fare disputes": "Audit final-fare recalculation and route-deviation detection.",
    "Wrong driver / car vs app": "Strengthen driver identity verification and in-app mismatch alerts.",
    "Vehicle smell / cleanliness": "Reinforce vehicle cleanliness standards and driver coaching.",
    "Route manipulation / slow driving": "Improve route-deviation detection and rider fare-protection rules.",
    "Cancellation (unfair fees, driver-initiated)": "Review driver-initiated cancellation patterns and unfair fee reversals.",
    "No-show / wrong pickup": "Improve pickup geofencing and no-show dispute handling.",
}

ZENDESK_NEUTRAL_CATS = {
    "city update", "work permit", "lost item", "found item", "document", "registration",
    "account update", "profile update", "general inquiry", "information",
}


def extract_themes(row, theme_dict, persona):
    themes = set()
    text = row.get("text_for_analysis") or ""
    for theme in match_themes(text, theme_dict):
        themes.add(theme)
    ticket_cat = (row.get("ticket_category") or "").lower()
    cat_map = ZENDESK_CATEGORY_THEMES.get(persona, {})
    for key, theme in cat_map.items():
        if key in ticket_cat:
            themes.add(theme)
    category = (row.get("category") or "").lower()
    for key, theme in cat_map.items():
        if key in category:
            themes.add(theme)
    return themes


def clean_safety_text(text):
    return re.sub(r"^[a-z_]+ ::\s*", "", (text or "").strip())


def row_for_sentiment(r):
    return {
        "OVERALL_SENTIMENT": r.get("overall_sentiment"),
        "TEXT_FOR_ANALYSIS": r.get("text_for_analysis"),
        "FEEDBACK_SOURCE": r.get("feedback_source"),
        "RATING_STARS": r.get("rating_stars"),
        "TICKET_CATEGORY": r.get("ticket_category"),
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
    cur.execute("USE WAREHOUSE COMPUTE_WH")
    return conn, cur


def classify_sentiment(row):
    """Classify sentiment using OVERALL_SENTIMENT, text, rating, and Zendesk rules."""
    existing = (row.get("OVERALL_SENTIMENT") or "").strip()
    if existing in ("Positive", "Neutral", "Negative", "Mixed"):
        return existing

    text = (row.get("TEXT_FOR_ANALYSIS") or "").lower()
    source = row.get("FEEDBACK_SOURCE") or ""
    stars = row.get("RATING_STARS")
    ticket_cat = (row.get("TICKET_CATEGORY") or "").lower()

    if source == "Zendesk":
        if any(n in ticket_cat for n in ZENDESK_NEUTRAL_CATS):
            return "Neutral"
        if text and len(text) < 5:
            return "Unclear"
        return "Negative"

    pos_kw = r"\b(good|great|excellent|thank|perfect|nice|best|love|happy|professional|clean|fast|amazing|wonderful)\b"
    neg_kw = r"\b(bad|terrible|awful|worst|horrible|rude|slow|dirty|smell|overcharg|cancel|refus|unfair|disappoint|angry|never|hate|poor|disgust)\b"

    pos = bool(re.search(pos_kw, text))
    neg = bool(re.search(neg_kw, text))

    if pos and neg:
        sentiment = "Mixed"
    elif neg:
        sentiment = "Negative"
    elif pos:
        sentiment = "Positive"
    elif stars is not None:
        try:
            s = float(stars)
            if s <= 2:
                sentiment = "Negative"
            elif s == 3:
                sentiment = "Neutral"
            elif s >= 4:
                sentiment = "Positive"
            else:
                sentiment = "Unclear"
        except (TypeError, ValueError):
            sentiment = "Unclear"
    else:
        sentiment = "Unclear"

    # Rating tie-break
    if stars is not None and sentiment in ("Unclear", "Neutral"):
        try:
            s = float(stars)
            if s <= 2:
                sentiment = "Negative"
            elif s >= 4 and sentiment != "Negative":
                sentiment = "Positive"
        except (TypeError, ValueError):
            pass

    return sentiment


def match_themes(text, theme_dict):
    text_l = (text or "").lower()
    matched = []
    for theme, patterns in theme_dict.items():
        for pat in patterns:
            if re.search(pat, text_l):
                matched.append(theme)
                break
    return matched


def match_safety(text):
    text_l = (text or "").lower()
    flags = []
    for category, patterns in SAFETY_PATTERNS.items():
        for pat in patterns:
            if re.search(pat, text_l):
                flags.append(category)
                break
    return flags


def anonymize_summary(text, city=None):
    t = (text or "").strip()
    t = re.sub(r"\b\d{8,}\b", "[ID]", t)
    t = re.sub(r"\+?\d[\d\s\-]{7,}\d", "[phone]", t)
    t = re.sub(r"\b[A-Z][a-z]+ [A-Z][a-z]+\b", "A customer", t)
    if city:
        t = f"{t[:120]}… ({city})" if len(t) > 120 else f"{t} ({city})"
    else:
        t = t[:140] + "…" if len(t) > 140 else t
    return t


def digest_source(source):
    if source in ("Google Store", "Apple Store"):
        return "App stores"
    return source


def pct_label(count, total, sample_size):
    if total == 0:
        return "n/a"
    p = round(100 * count / total)
    prefix = "~" if sample_size < 30 else ""
    return f"{prefix}{p}%"


def main():
    if not REPORT_DATE:
        print("Usage: voc_daily.py YYYY-MM-DD", file=sys.stderr)
        sys.exit(1)

    report_date = datetime.strptime(REPORT_DATE, "%Y-%m-%d")
    report_day_name = report_date.strftime("%a %d %b %Y")
    start_ts = report_date.strftime("%Y-%m-%d 00:00:00")
    end_ts = (report_date + timedelta(days=1)).strftime("%Y-%m-%d 00:00:00")

    conn, cur = connect()

    base_filter = """
        TIMESTAMP >= %s AND TIMESTAMP < %s
        AND RATINGS_COMMENTS IS NOT NULL
        AND TRIM(RATINGS_COMMENTS) <> ''
        AND LENGTH(TRIM(RATINGS_COMMENTS)) >= 3
    """

    cur.execute(
        f"SELECT COUNT(*) FROM jeeny_prod.general.feedbackevents WHERE {base_filter}",
        (start_ts, end_ts),
    )
    total_comments = cur.fetchone()[0]

    cur.execute(
        f"""
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
            GUID,
            CITY,
            COUNTRY,
            TICKET_CATEGORY,
            CATEGORY,
            URGENCY,
            TIMESTAMP
          FROM jeeny_prod.general.feedbackevents
          WHERE {base_filter}
        )
        SELECT FEEDBACK_SOURCE, persona, COUNT(*) AS volume
        FROM base
        WHERE persona IN ('driver', 'rider')
        GROUP BY 1, 2
        ORDER BY volume DESC
        """,
        (start_ts, end_ts),
    )
    volume_rows = cur.fetchall()

    cur.execute(
        f"""
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
          WHERE {base_filter}
        )
        SELECT GUID, FEEDBACK_SOURCE, persona, RATINGS_COMMENTS, text_for_analysis,
               OVERALL_SENTIMENT, RATING_STARS, CITY, COUNTRY, TICKET_CATEGORY,
               CATEGORY, URGENCY, TIMESTAMP
        FROM base
        WHERE persona IN ('driver', 'rider') AND rn <= {SAMPLE_PER_CELL}
        """,
        (start_ts, end_ts),
    )
    cols = [d[0].lower() for d in cur.description]
    sample = [dict(zip(cols, row)) for row in cur.fetchall()]
    conn.close()

    # Volume table
    vol = defaultdict(lambda: {"driver": 0, "rider": 0})
    for src, persona, vol_n in volume_rows:
        ds = digest_source(src)
        vol[ds][persona] += vol_n

    # Sentiment per cell
    cell_sentiment = defaultdict(lambda: {"neg": 0, "total": 0})
    for row in sample:
        ds = digest_source(row["feedback_source"])
        persona = row["persona"]
        sent = classify_sentiment(row_for_sentiment(row))
        key = (ds, persona)
        if sent != "Unclear":
            cell_sentiment[key]["total"] += 1
            if sent in ("Negative", "Mixed"):
                cell_sentiment[key]["neg"] += 1

    def cell_pct(ds, persona):
        k = (ds, persona)
        t = cell_sentiment[k]["total"]
        n = cell_sentiment[k]["neg"]
        return pct_label(n, t, t)

    # Themes
    driver_neg = [r for r in sample if r["persona"] == "driver"]
    rider_neg = [r for r in sample if r["persona"] == "rider"]

    def theme_counts(rows, theme_dict, persona):
        neg_mixed = []
        for r in rows:
            sent = classify_sentiment(row_for_sentiment(r))
            if sent in ("Negative", "Mixed"):
                neg_mixed.append(r)
        total_sample = len([r for r in rows if classify_sentiment(row_for_sentiment(r)) != "Unclear"])
        counts = defaultdict(int)
        for r in neg_mixed:
            for theme in extract_themes(r, theme_dict, persona):
                counts[theme] += 1
        ranked = sorted(counts.items(), key=lambda x: -x[1])
        result = []
        for theme, cnt in ranked[:7]:
            pct = pct_label(cnt, len(rows), len(rows))
            result.append((theme, pct))
        return result, len(neg_mixed), total_sample

    driver_themes, driver_neg_n, _ = theme_counts(driver_neg, DRIVER_THEMES, "driver")
    rider_themes, rider_neg_n, _ = theme_counts(rider_neg, RIDER_THEMES, "rider")

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

    SAFETY_EMOJI = {"Harassment": "🆘", "Reckless driving": "🏎️", "Identity mismatch": "🚙", "Post-ride contact": "🆘"}

    # Safety flags
    safety_items = []
    for r in sample:
        sent = classify_sentiment(row_for_sentiment(r))
        if sent not in ("Negative", "Mixed"):
            continue
        cats = match_safety(r.get("text_for_analysis"))
        for cat in cats:
            safety_items.append({
                "category": cat,
                "summary": anonymize_summary(clean_safety_text(r.get("text_for_analysis")), r.get("city")),
            })

    # Dedupe similar safety summaries
    seen = set()
    unique_safety = []
    for item in safety_items:
        key = (item["category"], item["summary"][:60])
        if key not in seen:
            seen.add(key)
            unique_safety.append(item)

    # Priorities from top themes across personas
    all_themes = []
    for theme, pct in rider_themes[:3]:
        all_themes.append((theme, pct, "rider"))
    for theme, pct in driver_themes[:3]:
        all_themes.append((theme, pct, "driver"))
    all_themes.sort(key=lambda x: -int(re.sub(r"[^\d]", "", x[1]) or 0))

    priorities = []
    used = set()
    for theme, pct, persona in all_themes:
        if theme in used or len(priorities) >= 3:
            continue
        used.add(theme)
        action = PRIORITY_ACTIONS.get(theme, "Targeted ops review and product instrumentation.")
        priorities.append(
            f"{len(priorities)+1}. **{theme}** — {pct} of {persona} sample. {action}"
        )

    # Build digest
    sources_order = ["Zendesk", "ride_rating", "in_app", "App stores"]
    table_lines = ["|Source|Driver (vol · %neg)|Rider (vol · %neg)|", "|  ---  |  ---  |  ---  |"]
    for src in sources_order:
        d_vol = vol[src]["driver"]
        r_vol = vol[src]["rider"]
        if src == "App stores":
            d_pct = "n/a"
            r_pct = cell_pct(src, "rider") if r_vol else "n/a"
            table_lines.append(f"|{src}|{d_vol} · {d_pct}|{r_vol} · {r_pct}|")
        elif src == "in_app":
            table_lines.append(f"|{src}|{d_vol} · {cell_pct(src, 'driver')}|—|")
        else:
            table_lines.append(
                f"|{src}|{d_vol} · {cell_pct(src, 'driver')}|{r_vol} · {cell_pct(src, 'rider')}|"
            )

    driver_bullets = "\n".join(
        f"* {THEME_EMOJI.get(t, '•')} {t} — **{p}**" for t, p in driver_themes[:6]
    ) or "* No dominant themes in sample"

    rider_bullets = "\n".join(
        f"* {THEME_EMOJI.get(t, '•')} {t} — **{p}**" for t, p in rider_themes[:7]
    ) or "* No dominant themes in sample"

    if unique_safety:
        safety_bullets = "\n".join(
            f"* {SAFETY_EMOJI.get(s['category'], '⚠️')} **{s['category']}** — {s['summary']}"
            for s in unique_safety[:3]
        )
    else:
        safety_bullets = "* No safety escalations flagged in today's sample."

    priority_text = "\n".join(priorities[:3])

    digest = f"""## 📊 {report_day_name}

{total_comments} comments · stratified sample ~{len(sample)} analyzed (up to 100 per source×user-type cell). Zendesk = support tickets (complaint-only by nature, so its %neg is always very high); ride_rating is the truer sentiment gauge.

{chr(10).join(table_lines)}

### 🏎️ Drivers — top issues

*(% of driver sample)*

{driver_bullets}

### 🧍 Riders — top issues

*(% of rider sample)*

{rider_bullets}

### ⚠️ Safety escalations (~{len(unique_safety)} flagged)

{safety_bullets}

### 🎯 Priorities

{priority_text}

---
"""

    output = {
        "report_date": REPORT_DATE,
        "report_day_name": report_day_name,
        "total_comments": total_comments,
        "sample_size": len(sample),
        "digest": digest,
        "priorities": priorities[:3],
        "volume": dict(vol),
        "cell_sentiment": {f"{k[0]}|{k[1]}": v for k, v in cell_sentiment.items()},
        "safety_count": len(unique_safety),
    }
    print(json.dumps(output, indent=2, default=str))


if __name__ == "__main__":
    main()
