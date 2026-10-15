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

# Zendesk categories treated as neutral/ops (not auto-negative)
ZENDESK_NEUTRAL_CATEGORIES = {
    "city update", "work permit", "lost item", "lost items", "found item",
    "general inquiry", "account update", "document", "registration",
    "partnership", "feedback positive", "compliment", "thank you",
}

# Theme keyword patterns (applied to text_for_analysis, case-insensitive)
DRIVER_THEMES = {
    "Cancellation fees not received / unfair fines": [
        r"cancel", r"fine", r"penalt", r"fee.*not", r"unfair.*fee",
        r"deduct", r"suspend", r"block",
    ],
    "Earnings & fares (low pay per km, weak offers)": [
        r"earn", r"low pay", r"pay.*low", r"fare.*low", r"offer",
        r"commission", r"bonus", r"incentive", r"per km", r"per kilomet",
        r"income", r"salary", r"trip.*few",
    ],
    "Passenger conduct": [
        r"passenger.*(rude|bad|drunk|smok|abuse|fight|argu)",
        r"rude.*passenger", r"customer.*(rude|bad|drunk|smok)",
        r"disrespect", r"insult", r"harass",
    ],
    "Passenger didn't pay (cash)": [
        r"didn.?t pay", r"not pay", r"no pay", r"refus.*pay",
        r"cash.*(not|no|refus)", r"unpaid", r"without pay",
    ],
    "Fare/charge accuracy": [
        r"fare.*(wrong|incorrect|error|mismatch|high|over)",
        r"charg.*(wrong|incorrect|error|extra)",
        r"overcharg", r"bill.*wrong", r"amount.*wrong",
    ],
    "Payment not credited / withdrawals": [
        r"wallet", r"withdraw", r"not credit", r"payment.*not",
        r"money.*not", r"balance", r"payout", r"transfer",
    ],
}

RIDER_THEMES = {
    "Overcharging / fare disputes": [
        r"overcharg", r"fare.*(high|wrong|expensive|too much|increase)",
        r"charg.*(too|high|extra|wrong)", r"price.*(high|wrong|expensive)",
        r"bill.*(high|wrong)", r"cost.*(high|wrong)", r"expensive",
        r"double.*charg", r"extra.*charg", r"fare.*disput",
    ],
    "Driver conduct": [
        r"driver.*(rude|bad|angry|aggress|abuse|shout|yell|insult)",
        r"rude.*driver", r"unprofessional", r"disrespect",
        r"smok", r"phone.*driv", r"attitude",
    ],
    "Wrong driver / car vs app": [
        r"wrong.*(driver|car|vehicle|plate|taxi)",
        r"different.*(driver|car|vehicle)", r"not.*match",
        r"another.*driver", r"identity", r"plate.*number",
        r"car.*(color|model|type).*wrong",
    ],
    "Route manipulation / slow driving": [
        r"route", r"long.*way", r"detour", r"circulat",
        r"slow.*driv", r"delay", r"traffic.*(fake|unnecessary)",
        r"manipulat", r"longer.*(route|distance|time)",
        r"gps.*(wrong|off)", r"went.*around",
    ],
    "No-show / wrong pickup": [
        r"no.?show", r"didn.?t (come|arrive|show)", r"wait.*long",
        r"pickup.*(wrong|late)", r"never.*came", r"cancel.*after.*wait",
        r"arriv.*late", r"not.*arriv",
    ],
    "Cancellation (unfair fees, driver-initiated)": [
        r"cancel", r"cancell", r"fee.*cancel", r"unfair.*cancel",
        r"driver.*cancel", r"charged.*cancel",
    ],
    "Payment & charging errors": [
        r"payment.*(fail|error|problem|issue|declin)",
        r"charg.*(fail|error|twice|double)", r"card.*(declin|fail|error)",
        r"wallet.*(error|problem|not)", r"deduct.*wrong",
    ],
    "Vehicle smell / cleanliness": [
        r"smell", r"dirty", r"clean", r"hygien", r"odor", r"stink",
        r"filthy", r"trash", r"garbage",
    ],
}

SAFETY_PATTERNS = {
    "Harassment": [
        r"harass", r"assault", r"touch", r"held.*hand", r"threaten",
        r"stalk", r"inappropriat", r"sexual", r"molest",
    ],
    "Reckless driving": [
        r"reckless", r"speed", r"accident", r"crash", r"injur",
        r"child.*injur", r"dangerous.*driv", r"hit.*(me|us|car)",
    ],
    "Identity mismatch": [
        r"wrong.*(driver|car|vehicle|plate)", r"different.*(driver|car)",
        r"not.*match.*(app|photo|plate)", r"fake.*driver", r"impersonat",
        r"safety.*(concern|issue)", r"scared", r"afraid", r"unsafe",
    ],
}

# Zendesk TICKET_CATEGORY -> theme mapping
ZENDESK_CATEGORY_THEMES = {
    "driver": {
        "cancellation": "Cancellation fees not received / unfair fines",
        "fine": "Cancellation fees not received / unfair fines",
        "earnings": "Earnings & fares (low pay per km, weak offers)",
        "payment": "Payment not credited / withdrawals",
        "passenger": "Passenger conduct",
        "fare": "Fare/charge accuracy",
        "cash": "Passenger didn't pay (cash)",
    },
    "rider": {
        "fare": "Overcharging / fare disputes",
        "overcharg": "Overcharging / fare disputes",
        "driver": "Driver conduct",
        "route": "Route manipulation / slow driving",
        "cancel": "Cancellation (unfair fees, driver-initiated)",
        "payment": "Payment & charging errors",
        "vehicle": "Vehicle smell / cleanliness",
        "car": "Wrong driver / car vs app",
        "pickup": "No-show / wrong pickup",
        "safety": "Harassment",
    },
}


def get_report_date():
    now = datetime.now(RIYADH)
    yesterday = (now - timedelta(days=1)).date()
    day_name = yesterday.strftime("%a %d %b %Y")
    return yesterday.isoformat(), day_name


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


def classify_sentiment(row):
    """Classify sentiment for a single row."""
    existing = row.get("OVERALL_SENTIMENT")
    if existing and str(existing).strip() in ("Positive", "Neutral", "Negative", "Mixed"):
        return existing

    text = (row.get("TEXT_FOR_ANALYSIS") or "").lower()
    stars = row.get("RATING_STARS")
    source = row.get("FEEDBACK_SOURCE", "")
    category = (row.get("TICKET_CATEGORY") or "").lower()

    # Zendesk: default negative unless neutral category
    if source == "Zendesk":
        if any(nc in category for nc in ZENDESK_NEUTRAL_CATEGORIES):
            return "Neutral"
        return "Negative"

    # Keyword-based classification
    pos_words = r"\b(good|great|excellent|amazing|wonderful|thank|love|best|perfect|happy|satisfied|nice|awesome|fantastic)\b"
    neg_words = r"\b(bad|terrible|awful|horrible|worst|hate|angry|frustrat|disappoint|poor|unacceptable|complain|scam|cheat|fraud|rubbish|garbage)\b"

    has_pos = bool(re.search(pos_words, text))
    has_neg = bool(re.search(neg_words, text))

    if has_pos and has_neg:
        return "Mixed"
    if has_neg:
        return "Negative"
    if has_pos:
        return "Positive"

    # Rating tie-break
    if stars is not None:
        try:
            s = float(stars)
            if s <= 2:
                return "Negative"
            if s == 3:
                return "Neutral"
            if s >= 4:
                return "Positive"
        except (ValueError, TypeError):
            pass

    if len(text.strip()) < 10:
        return "Unclear"
    return "Neutral"


def match_themes(text, persona, ticket_category=None):
    """Return set of matched theme labels."""
    themes = set()
    text_lower = (text or "").lower()
    theme_dict = DRIVER_THEMES if persona == "driver" else RIDER_THEMES

    for theme, patterns in theme_dict.items():
        for pat in patterns:
            if re.search(pat, text_lower, re.IGNORECASE):
                themes.add(theme)
                break

    # Zendesk category hints
    if ticket_category:
        cat_lower = ticket_category.lower()
        cat_map = ZENDESK_CATEGORY_THEMES.get(persona, {})
        for key, theme in cat_map.items():
            if key in cat_lower:
                themes.add(theme)

    return themes


def match_safety(text):
    """Return list of (category, snippet) for safety flags."""
    text_lower = (text or "").lower()
    flags = []
    for category, patterns in SAFETY_PATTERNS.items():
        for pat in patterns:
            m = re.search(pat, text_lower, re.IGNORECASE)
            if m:
                # Anonymize: take a short generic snippet
                snippet = text[:120].strip()
                snippet = re.sub(r"\b\d{8,}\b", "[phone]", snippet)
                snippet = re.sub(r"\b[A-Z][a-z]+ [A-Z][a-z]+\b", "[name]", snippet)
                if len(snippet) > 80:
                    snippet = snippet[:77] + "..."
                flags.append((category, snippet))
                break
    return flags


def normalize_source(src):
    if src in ("Google Store", "Apple Store"):
        return "App stores"
    return src


def pct_str(n, total):
    if total == 0:
        return "n/a"
    pct = round(100 * n / total)
    prefix = "~" if total < 30 else ""
    return f"{prefix}{pct}%"


def main():
    report_date, report_day_name = get_report_date()
    start_ts = f"{report_date} 00:00:00"
    end_ts = (datetime.strptime(report_date, "%Y-%m-%d").date() + timedelta(days=1)).isoformat()

    print(f"Report date: {report_date} ({report_day_name})", file=sys.stderr)

    conn, cur = connect()

    # Total comments
    cur.execute("""
        SELECT COUNT(*) AS total_comments
        FROM jeeny_prod.general.feedbackevents
        WHERE TIMESTAMP >= %s
          AND TIMESTAMP < %s
          AND RATINGS_COMMENTS IS NOT NULL
          AND TRIM(RATINGS_COMMENTS) <> ''
          AND LENGTH(TRIM(RATINGS_COMMENTS)) >= %s
    """, (start_ts, end_ts + " 00:00:00", MIN_COMMENT_LEN))
    total_comments = cur.fetchone()[0]

    # Volume by source x persona
    cur.execute("""
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
    """, (start_ts, end_ts + " 00:00:00", MIN_COMMENT_LEN))
    volume_rows = cur.fetchall()

    # Stratified sample
    cur.execute("""
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
          AND rn <= %s
    """, (start_ts, end_ts + " 00:00:00", MIN_COMMENT_LEN, SAMPLE_PER_CELL))

    cols = [d[0] for d in cur.description]
    sample = [dict(zip(cols, row)) for row in cur.fetchall()]
    conn.close()

    sample_count = len(sample)

    # Build volume table
    volumes = defaultdict(lambda: {"driver": 0, "rider": 0})
    for src, persona, vol in volume_rows:
        norm = normalize_source(src)
        volumes[norm][persona] += vol

    # Sentiment analysis per cell
    cells = defaultdict(list)
    for row in sample:
        row["TEXT_FOR_ANALYSIS"] = row.pop("TEXT_FOR_ANALYSIS", row.get("TEXT_FOR_ANALYSIS"))
        sentiment = classify_sentiment({
            "OVERALL_SENTIMENT": row.get("OVERALL_SENTIMENT"),
            "TEXT_FOR_ANALYSIS": row.get("TEXT_FOR_ANALYSIS"),
            "RATING_STARS": row.get("RATING_STARS"),
            "FEEDBACK_SOURCE": row.get("FEEDBACK_SOURCE"),
            "TICKET_CATEGORY": row.get("TICKET_CATEGORY"),
        })
        row["sentiment"] = sentiment
        norm_src = normalize_source(row["FEEDBACK_SOURCE"])
        cells[(norm_src, row["PERSONA"])].append(row)

    def cell_pctneg(src, persona):
        rows = cells.get((src, persona), [])
        analyzed = [r for r in rows if r["sentiment"] != "Unclear"]
        if not analyzed:
            return "n/a", 0
        neg = sum(1 for r in analyzed if r["sentiment"] in ("Negative", "Mixed"))
        return pct_str(neg, len(analyzed)), len(analyzed)

    # Theme extraction
    driver_sample = [r for r in sample if r["PERSONA"] == "driver"]
    rider_sample = [r for r in sample if r["PERSONA"] == "rider"]
    driver_neg_mixed = [r for r in driver_sample if r["sentiment"] in ("Negative", "Mixed")]
    rider_neg_mixed = [r for r in rider_sample if r["sentiment"] in ("Negative", "Mixed")]

    def theme_counts(neg_mixed_rows, persona, persona_sample_size):
        counts = defaultdict(int)
        for r in neg_mixed_rows:
            themes = match_themes(
                r.get("TEXT_FOR_ANALYSIS"),
                persona,
                r.get("TICKET_CATEGORY"),
            )
            for t in themes:
                counts[t] += 1
        total = persona_sample_size if persona_sample_size else 1
        ranked = sorted(counts.items(), key=lambda x: -x[1])
        return [(t, pct_str(c, total)) for t, c in ranked]

    driver_themes = theme_counts(driver_neg_mixed, "driver", len(driver_sample))
    rider_themes = theme_counts(rider_neg_mixed, "rider", len(rider_sample))

    # Safety flags
    safety_flags = []
    seen_snippets = set()
    for r in driver_neg_mixed + rider_neg_mixed:
        for cat, snippet in match_safety(r.get("TEXT_FOR_ANALYSIS")):
            key = (cat, snippet[:50])
            if key not in seen_snippets:
                seen_snippets.add(key)
                safety_flags.append({"category": cat, "snippet": snippet})

  # Emoji mapping
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

    SAFETY_EMOJI = {"Harassment": "🆘", "Reckless driving": "🏎️", "Identity mismatch": "🚙"}

    # Build digest table rows
    sources = ["Zendesk", "ride_rating", "in_app", "App stores"]
    table_lines = []
    for src in sources:
        d_vol = volumes[src]["driver"]
        r_vol = volumes[src]["rider"]
        if src == "App stores":
            table_lines.append(f"|{src}|{d_vol} · n/a|{r_vol} · n/a|")
        elif src == "in_app":
            d_pct, _ = cell_pctneg(src, "driver")
            table_lines.append(f"|{src}|{d_vol} · {d_pct}|—|")
        else:
            d_pct, _ = cell_pctneg(src, "driver")
            r_pct, _ = cell_pctneg(src, "rider")
            table_lines.append(f"|{src}|{d_vol} · {d_pct}|{r_vol} · {r_pct}|")

    # Driver themes (top 5-6)
    driver_bullets = []
    for theme, pct in driver_themes[:6]:
        emoji = THEME_EMOJI.get(theme, "•")
        driver_bullets.append(f"* {emoji} {theme} — **{pct}**")

    # Rider themes (top 5-7)
    rider_bullets = []
    for theme, pct in rider_themes[:7]:
        emoji = THEME_EMOJI.get(theme, "•")
        rider_bullets.append(f"* {emoji} {theme} — **{pct}**")

    # Safety section
    safety_bullets = []
    for flag in safety_flags[:3]:
        emoji = SAFETY_EMOJI.get(flag["category"], "⚠️")
        safety_bullets.append(f"* {emoji} **{flag['category']}** — {flag['snippet']}")

    # Priorities
    top_rider = rider_themes[:3] if rider_themes else []
    top_driver = driver_themes[:3] if driver_themes else []

    ACTION_HINTS = {
        "Overcharging / fare disputes": "Audit final-fare recalculation and surface fare breakdown before trip confirmation.",
        "Driver conduct": "Strengthen in-ride reporting and driver behavior enforcement with faster ops review.",
        "Wrong driver / car vs app": "Tighten identity verification at pickup and flag plate/car mismatches automatically.",
        "Route manipulation / slow driving": "Improve route-deviation detection and rider notification during the trip.",
        "Passenger conduct": "Expand passenger rating consequences and driver-side incident reporting.",
        "Cancellation fees not received / unfair fines": "Clarify cancellation fee policy and ensure timely driver payouts.",
        "Earnings & fares (low pay per km, weak offers)": "Review incentive tiers and per-km minimum pay in low-demand zones.",
    }

    def priority_line(n, theme, pct, evidence_extra=""):
        action = ACTION_HINTS.get(theme, "Review ops workflow and product fixes for this theme.")
        return f"{n}. **{theme}** — {pct} of rider sample{evidence_extra}. {action}"

    priorities = []
    if top_rider:
        t, p = top_rider[0]
        priorities.append(priority_line(1, t, p))
    if len(top_rider) > 1:
        t, p = top_rider[1]
        priorities.append(priority_line(2, t, p))
    elif top_driver:
        t, p = top_driver[0]
        priorities.append(
            f"2. **{t}** — {p} of driver sample. "
            f"{ACTION_HINTS.get(t, 'Review driver complaint handling.')}"
        )
    if len(top_rider) > 2:
        t, p = top_rider[2]
        priorities.append(priority_line(3, t, p))
    elif len(top_driver) > 1:
        t, p = top_driver[1]
        priorities.append(
            f"3. **{t}** — {p} of driver sample. "
            f"{ACTION_HINTS.get(t, 'Review driver complaint handling.')}"
        )
    else:
        priorities.append(
            "3. **Safety escalations** — Review flagged safety cases and ensure 24h response SLA."
        )

    while len(priorities) < 3:
        priorities.append(
            f"{len(priorities)+1}. **Monitor volume trends** — Continue tracking daily sentiment shifts."
        )

    digest = f"""## 📊 {report_day_name}

{total_comments} comments · stratified sample ~{sample_count} analyzed (up to 100 per source×user-type cell). Zendesk = support tickets (complaint-only by nature, so its %neg is always very high); ride_rating is the truer sentiment gauge.

|Source|Driver (vol · %neg)|Rider (vol · %neg)|
|  ---  |  ---  |  ---  |
{chr(10).join(table_lines)}

### 🏎️ Drivers — top issues

*(% of driver sample)*

{chr(10).join(driver_bullets) if driver_bullets else '* No significant themes detected'}

### 🧍 Riders — top issues

*(% of rider sample)*

{chr(10).join(rider_bullets) if rider_bullets else '* No significant themes detected'}

### ⚠️ Safety escalations (~{len(safety_flags)} flagged)

{chr(10).join(safety_bullets) if safety_bullets else '* No safety escalations flagged in sample'}

### 🎯 Priorities

{chr(10).join(priorities)}

---
"""

    result = {
        "report_date": report_date,
        "report_day_name": report_day_name,
        "total_comments": total_comments,
        "sample_count": sample_count,
        "digest": digest,
        "top_priority": priorities[0].split("**")[1] if priorities else "N/A",
        "priorities": priorities[:3],
        "safety_count": len(safety_flags),
        "volumes": dict(volumes),
        "driver_themes": driver_themes[:6],
        "rider_themes": rider_themes[:7],
    }
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
