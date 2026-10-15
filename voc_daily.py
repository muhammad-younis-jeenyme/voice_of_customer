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
RIYADH = ZoneInfo("Asia/Riyadh")

VALID_SENTIMENTS = {"Positive", "Neutral", "Negative", "Mixed"}

ZENDESK_NEUTRAL_CATEGORIES = {
    "city update", "work permit", "lost item", "lost items", "general inquiry",
    "account update", "document", "registration", "information",
}

DRIVER_THEME_PATTERNS = {
    "Passenger conduct": [
        r"\b(rude|abusive|aggressive|disrespect|insult|scream|yell|fight|argu)\w*",
        r"\b(passenger|customer|rider)\b.{0,30}\b(bad|terrible|awful|horrible|nasty)\b",
        r"\b(smok|drunk|intoxicat)\w*",
    ],
    "Passenger didn't pay (cash)": [
        r"\b(didn'?t|not|never|refus\w*)\s+pay",
        r"\b(cash|unpaid|non.?payment|without pay)",
        r"\b(run away|ran away|escape)\b.{0,20}\bpay",
    ],
    "Earnings & fares (low pay per km, weak offers)": [
        r"\b(low|poor|bad|unfair)\s+(pay|fare|earning|income|rate|price)",
        r"\b(earn|payment|fare|offer)\b.{0,25}\b(low|less|poor|weak|bad)\b",
        r"\b(commission|surge|bonus|incentive)\b",
        r"\bper\s*km\b",
    ],
    "Cancellation fees not received / unfair fines": [
        r"\b(cancel|cancellation)\b.{0,30}\b(fee|fine|charge|penalt)",
        r"\b(fee|fine)\b.{0,20}\b(cancel|unfair|not received|didn'?t receive)",
        r"\bunfair\s+(fine|fee|penalt)",
    ],
    "Fare/charge accuracy": [
        r"\b(wrong|incorrect|error|miscalculat)\w*\s+(fare|charge|amount|bill)",
        r"\b(fare|charge|bill)\b.{0,20}\b(wrong|incorrect|error)",
    ],
    "Payment not credited / withdrawals": [
        r"\b(withdraw|wallet|credit|payout|transfer)\b.{0,30}\b(not|fail|delay|stuck|pending|missing)",
        r"\b(not|never)\s+(credit|receive|get)\b.{0,20}\b(pay|money|amount|earning)",
    ],
}

RIDER_THEME_PATTERNS = {
    "Overcharging / fare disputes": [
        r"\b(overcharg|over.?charg|too much|expensive|high fare|rip.?off|scam)\w*",
        r"\b(fare|price|charge|bill|amount)\b.{0,25}\b(high|expensive|wrong|more|extra)",
        r"\bcharged\s+(more|extra|double)",
    ],
    "Driver conduct": [
        r"\b(rude|abusive|aggressive|disrespect|insult|scream|yell|fight|argu|unprofession)\w*",
        r"\b(driver)\b.{0,30}\b(bad|terrible|awful|horrible|nasty|rude)\b",
        r"\b(smok|drunk|intoxicat)\w*",
    ],
    "Wrong driver / car vs app": [
        r"\b(wrong|different)\s+(driver|car|vehicle|plate|number)",
        r"\b(driver|car|vehicle|plate)\b.{0,20}\b(not match|mismatch|different|wrong)",
        r"\bnot\s+the\s+(same|driver|car)\b",
    ],
    "Route manipulation / slow driving": [
        r"\b(long route|detour|extra route|manipulat|longer way|took long)",
        r"\b(slow|delay|late|traffic)\b.{0,20}\b(driver|driving|ride)",
        r"\b(go around|longer distance|unnecessary route)",
    ],
    "No-show / wrong pickup": [
        r"\b(no.?show|didn'?t come|never came|not arrive|wrong pickup|wrong location)",
        r"\b(pickup|pick.?up)\b.{0,20}\b(wrong|miss|incorrect)",
    ],
    "Cancellation (unfair fees, driver-initiated)": [
        r"\b(cancel|cancellation)\b.{0,30}\b(driver|unfair|fee|fine|charge)",
        r"\b(driver)\b.{0,20}\b(cancel|refus)",
    ],
    "Payment & charging errors": [
        r"\b(payment|charge|wallet|card|double)\b.{0,25}\b(error|fail|wrong|twice|duplicate)",
        r"\b(charged|deduct)\w*\s+(twice|double|wrong)",
    ],
    "Vehicle smell / cleanliness": [
        r"\b(dirty|smell|stink|filthy|unclean|hygien|odor|odour)\w*",
        r"\b(clean|cleanliness)\b.{0,15}\b(bad|poor|terrible)",
    ],
}

SAFETY_PATTERNS = {
    "Harassment": [
        r"\b(harass|assault|molest|touch|held hand|threaten|stalk|insult|abuse)\w*",
        r"\b(repeated call|repeated message|after ride)\b",
    ],
    "Reckless driving": [
        r"\b(reckless|dangerous|accident|crash|hit|injur|speed|fast driving)\w*",
        r"\b(child|kid)\b.{0,15}\binjur",
    ],
    "Identity mismatch": [
        r"\b(wrong driver|different driver|not the driver|fake driver|impersonat)\w*",
        r"\b(safety|unsafe|scared|afraid|fear)\b.{0,30}\b(driver|car|vehicle)",
    ],
}

ARABIC_NEGATIVE = [
    "سيء", "سيئة", "فظيع", "مزعج", "غش", "احتيال", "سرقة", "وقح", "مشكلة",
    "غالي", "زيادة", "ما دفع", "لم يدفع", "إلغاء", "تهديد", "تحرش", "خرا",
    "عميل سيئ", "غير محترم", "مش محترم",
]

# Zendesk TICKET_CATEGORY → canonical theme
DRIVER_CATEGORY_MAP = {
    "passenger did not pay": "Passenger didn't pay (cash)",
    "passenger's behaviour": "Passenger conduct",
    "passenger behaviour": "Passenger conduct",
    "cancellation fee": "Cancellation fees not received / unfair fines",
    "ride value is less": "Earnings & fares (low pay per km, weak offers)",
    "ride value is higher": "Fare/charge accuracy",
    "did not win although the target": "Earnings & fares (low pay per km, weak offers)",
    "discount amount in wallet": "Payment not credited / withdrawals",
    "deducted from my balance": "Payment not credited / withdrawals",
    "funds have not been transferred": "Payment not credited / withdrawals",
    "credit card payment amount": "Payment not credited / withdrawals",
    "intercity surcharge": "Earnings & fares (low pay per km, weak offers)",
}

RIDER_CATEGORY_MAP = {
    "overcharged": "Overcharging / fare disputes",
    "price was way higher": "Overcharging / fare disputes",
    "extra money": "Overcharging / fare disputes",
    "driver made an unscheduled stop": "Route manipulation / slow driving",
    "longer route": "Route manipulation / slow driving",
    "wrong driver": "Wrong driver / car vs app",
    "car does not match": "Wrong driver / car vs app",
    "cancellation": "Cancellation (unfair fees, driver-initiated)",
    "cancelled by driver": "Cancellation (unfair fees, driver-initiated)",
    "payment": "Payment & charging errors",
    "charged twice": "Payment & charging errors",
    "no show": "No-show / wrong pickup",
    "driver did not arrive": "No-show / wrong pickup",
    "bad rating": None,  # resolved via tag
}

TAG_THEME_MAP = {
    "driver_behaviour": ("rider", "Driver conduct"),
    "driver_behavior": ("rider", "Driver conduct"),
    "driver_hygiene": ("rider", "Vehicle smell / cleanliness"),
    "passenger_behaviour": ("driver", "Passenger conduct"),
    "passenger_behavior": ("driver", "Passenger conduct"),
    "reckless_driving": ("rider", "Route manipulation / slow driving"),
    "overcharge": ("rider", "Overcharging / fare disputes"),
    "cancellation": ("rider", "Cancellation (unfair fees, driver-initiated)"),
    "wrong_car": ("rider", "Wrong driver / car vs app"),
    "identity": ("rider", "Wrong driver / car vs app"),
}


def get_report_date() -> tuple[str, str]:
    today = datetime.now(RIYADH).date()
    report = today - timedelta(days=1)
    day_name = report.strftime("%a %d %b %Y")
    heading = f"## 📊 {day_name}"
    return report.isoformat(), heading


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


def normalize_source(src: str) -> str:
    if src in ("Google Store", "Apple Store"):
        return "App stores"
    return src


def classify_sentiment(row: dict) -> str:
    existing = (row.get("OVERALL_SENTIMENT") or "").strip()
    if existing in VALID_SENTIMENTS:
        return existing

    text = (row.get("TEXT_FOR_ANALYSIS") or "").strip()
    source = row.get("FEEDBACK_SOURCE", "")
    rating = row.get("RATING_STARS")
    ticket_cat = (row.get("TICKET_CATEGORY") or "").lower()
    text_lower = text.lower()

    if source == "Zendesk":
        if any(neu in ticket_cat for neu in ZENDESK_NEUTRAL_CATEGORIES):
            return "Neutral"
        return "Negative"

    if len(text) < 5:
        for kw in ARABIC_NEGATIVE:
            if kw in text:
                return "Negative"
        if rating is not None:
            if rating <= 2:
                return "Negative"
            if rating >= 4:
                return "Positive"
            return "Neutral"
        return "Unclear"

    pos_words = r"\b(good|great|excellent|amazing|wonderful|thank|love|best|perfect|nice|helpful|fast|quick|clean|professional)\w*"
    neg_words = r"\b(bad|terrible|awful|horrible|worst|hate|slow|late|rude|scam|fraud|dirty|smell|overcharg|unfair|refus|didn'?t|never|poor|disappoint|angry|frustrat|complain)\w*"

    pos = len(re.findall(pos_words, text_lower))
    neg = len(re.findall(neg_words, text_lower))

    for kw in ARABIC_NEGATIVE:
        if kw in text:
            neg += 1

    if pos > 0 and neg > 0:
        sentiment = "Mixed"
    elif neg > pos:
        sentiment = "Negative"
    elif pos > neg:
        sentiment = "Positive"
    elif rating is not None:
        if rating <= 2:
            sentiment = "Negative"
        elif rating >= 4:
            sentiment = "Positive"
        elif rating == 3:
            sentiment = "Neutral"
        else:
            sentiment = "Unclear"
    else:
        sentiment = "Neutral" if not text else "Unclear"

    if sentiment == "Unclear" and rating is not None:
        if rating <= 2:
            return "Negative"
        if rating >= 4:
            return "Positive"
    return sentiment


def category_to_themes(ticket_category: str | None, persona: str) -> list[str]:
    if not ticket_category:
        return []
    cat = ticket_category.lower()
    cmap = DRIVER_CATEGORY_MAP if persona == "driver" else RIDER_CATEGORY_MAP
    themes = []
    for key, theme in cmap.items():
        if key in cat and theme:
            themes.append(theme)
    return themes


def tag_to_themes(text: str) -> list[tuple[str, str]]:
    """Parse structured tags like driver_behaviour :: ..."""
    found = []
    for tag, (persona, theme) in TAG_THEME_MAP.items():
        if re.search(rf"\b{re.escape(tag)}\b", text, re.I):
            found.append((persona, theme))
    return found


def match_themes(text: str, patterns: dict, ticket_category: str | None = None, persona: str = "") -> list[str]:
    matched = set(category_to_themes(ticket_category, persona))
    text_lower = text.lower()
    for theme, pats in patterns.items():
        for pat in pats:
            if re.search(pat, text_lower, re.I):
                matched.add(theme)
                break
    # Arabic keyword boosts
    ar_driver = {
        "لم يدفع": "Passenger didn't pay (cash)",
        "ما دفع": "Passenger didn't pay (cash)",
        "عميل سيئ": "Passenger conduct",
        "غير محترم": "Passenger conduct",
        "رسم الإلغاء": "Cancellation fees not received / unfair fines",
        "إلغاء": "Cancellation fees not received / unfair fines",
        "رخيص": "Earnings & fares (low pay per km, weak offers)",
        "التحويل": "Payment not credited / withdrawals",
    }
    ar_rider = {
        "زياده سعر": "Overcharging / fare disputes",
        "غالي": "Overcharging / fare disputes",
        "طريق": "Route manipulation / slow driving",
        "رائحة": "Vehicle smell / cleanliness",
    }
    ar_map = ar_driver if persona == "driver" else ar_rider
    for kw, theme in ar_map.items():
        if kw in text:
            matched.add(theme)
    return list(matched)


def pct_neg(counts: dict, source: str, persona: str) -> str:
    neg = counts.get((source, persona, "Negative"), 0)
    mixed = counts.get((source, persona, "Mixed"), 0)
    total = sum(
        counts.get((source, persona, s), 0)
        for s in ("Positive", "Neutral", "Negative", "Mixed")
    )
    if total == 0:
        return "n/a"
    pct = round(100 * (neg + mixed) / total)
    prefix = "~" if total < 30 else ""
    return f"{prefix}{pct}%"


def theme_pct(theme_counts: Counter, sample_size: int) -> str:
    if sample_size == 0:
        return "0%"
    pct = round(100 * theme_counts / sample_size)
    prefix = "~" if sample_size < 30 else ""
    return f"{prefix}{pct}%"


def anonymize(text: str, max_len: int = 120) -> str:
    t = re.sub(r"\b\d{8,}\b", "[redacted]", text)
    t = re.sub(r"\b[A-Z][a-z]+\s+[A-Z][a-z]+\b", "[name]", t)
    t = t.strip()
    if len(t) > max_len:
        t = t[: max_len - 3] + "..."
    return t


def run(report_date: str | None = None):
    if report_date:
        d = datetime.strptime(report_date, "%Y-%m-%d").date()
    else:
        d = datetime.now(RIYADH).date() - timedelta(days=1)
    start_ts = f"{d.isoformat()} 00:00:00"
    end_ts = (d + timedelta(days=1)).isoformat() + " 00:00:00"
    day_heading = f"## 📊 {d.strftime('%a %d %b %Y')}"

    conn, cur = connect()

    cur.execute(
        """
        SELECT COUNT(*) FROM jeeny_prod.general.feedbackevents
        WHERE TIMESTAMP >= %s AND TIMESTAMP < %s
          AND RATINGS_COMMENTS IS NOT NULL AND TRIM(RATINGS_COMMENTS) <> ''
          AND LENGTH(TRIM(RATINGS_COMMENTS)) >= %s
        """,
        (start_ts, end_ts, MIN_COMMENT_LEN),
    )
    total_comments = cur.fetchone()[0]

    cur.execute(
        """
        WITH base AS (
          SELECT FEEDBACK_SOURCE,
            CASE WHEN USER_TYPE = 'driver' THEN 'driver'
                 WHEN USER_TYPE IN ('passenger', 'customer') THEN 'rider'
                 ELSE 'other' END AS persona
          FROM jeeny_prod.general.feedbackevents
          WHERE TIMESTAMP >= %s AND TIMESTAMP < %s
            AND RATINGS_COMMENTS IS NOT NULL AND TRIM(RATINGS_COMMENTS) <> ''
            AND LENGTH(TRIM(RATINGS_COMMENTS)) >= %s
        )
        SELECT FEEDBACK_SOURCE, persona, COUNT(*) AS volume
        FROM base WHERE persona IN ('driver', 'rider')
        GROUP BY 1, 2 ORDER BY volume DESC
        """,
        (start_ts, end_ts, MIN_COMMENT_LEN),
    )
    volume_rows = cur.fetchall()
    volumes = defaultdict(lambda: {"driver": 0, "rider": 0})
    for src, persona, vol in volume_rows:
        ns = normalize_source(src)
        volumes[ns][persona] += vol

    cur.execute(
        f"""
        WITH base AS (
          SELECT GUID, FEEDBACK_SOURCE,
            CASE WHEN USER_TYPE = 'driver' THEN 'driver'
                 WHEN USER_TYPE IN ('passenger', 'customer') THEN 'rider'
                 ELSE 'other' END AS persona,
            RATINGS_COMMENTS,
            COALESCE(NULLIF(TRIM(EN_TRANSLATION), ''), RATINGS_COMMENTS) AS text_for_analysis,
            OVERALL_SENTIMENT, RATING_STARS, CITY, COUNTRY,
            TICKET_CATEGORY, CATEGORY, URGENCY, TIMESTAMP,
            ROW_NUMBER() OVER (
              PARTITION BY FEEDBACK_SOURCE,
                CASE WHEN USER_TYPE = 'driver' THEN 'driver'
                     WHEN USER_TYPE IN ('passenger', 'customer') THEN 'rider'
                     ELSE 'other' END
              ORDER BY RANDOM()
            ) AS rn
          FROM jeeny_prod.general.feedbackevents
          WHERE TIMESTAMP >= %s AND TIMESTAMP < %s
            AND RATINGS_COMMENTS IS NOT NULL AND TRIM(RATINGS_COMMENTS) <> ''
            AND LENGTH(TRIM(RATINGS_COMMENTS)) >= %s
        )
        SELECT GUID, FEEDBACK_SOURCE, persona, RATINGS_COMMENTS, text_for_analysis,
               OVERALL_SENTIMENT, RATING_STARS, CITY, COUNTRY,
               TICKET_CATEGORY, CATEGORY, URGENCY, TIMESTAMP
        FROM base WHERE persona IN ('driver', 'rider') AND rn <= {SAMPLE_PER_CELL}
        """,
        (start_ts, end_ts, MIN_COMMENT_LEN),
    )
    cols = [d[0] for d in cur.description]
    sample = [dict(zip(cols, row)) for row in cur.fetchall()]
    cur.close()
    conn.close()

    for row in sample:
        row["TEXT_FOR_ANALYSIS"] = row.pop("TEXT_FOR_ANALYSIS", row.get("TEXT_FOR_ANALYSIS"))

    sentiment_counts = Counter()
    driver_neg_mixed = []
    rider_neg_mixed = []
    driver_themes = Counter()
    rider_themes = Counter()
    safety_flags = []

    driver_sample_n = sum(1 for r in sample if r["PERSONA"] == "driver")
    rider_sample_n = sum(1 for r in sample if r["PERSONA"] == "rider")

    for row in sample:
        row["SENTIMENT"] = classify_sentiment(row)
        src = normalize_source(row["FEEDBACK_SOURCE"])
        persona = row["PERSONA"]
        sentiment_counts[(src, persona, row["SENTIMENT"])] += 1

        text = row.get("TEXT_FOR_ANALYSIS") or row.get("TEXT_FOR_ANALYSIS", "")
        ticket_cat = row.get("TICKET_CATEGORY")
        if row["SENTIMENT"] in ("Negative", "Mixed"):
            if persona == "driver":
                driver_neg_mixed.append(row)
                for t in match_themes(text, DRIVER_THEME_PATTERNS, ticket_cat, "driver"):
                    driver_themes[t] += 1
            else:
                rider_neg_mixed.append(row)
                for t in match_themes(text, RIDER_THEME_PATTERNS, ticket_cat, "rider"):
                    rider_themes[t] += 1

            for tag_persona, theme in tag_to_themes(text):
                if tag_persona == persona:
                    if persona == "driver":
                        driver_themes[theme] += 1
                    else:
                        rider_themes[theme] += 1

            for cat, pats in SAFETY_PATTERNS.items():
                for pat in pats:
                    if re.search(pat, text.lower(), re.I):
                        safety_flags.append({
                            "category": cat,
                            "summary": anonymize(text),
                            "city": row.get("CITY", ""),
                        })
                        break
                if re.search(r"\breckless_driving\b", text, re.I):
                    safety_flags.append({
                        "category": "Reckless driving",
                        "summary": anonymize(text),
                        "city": row.get("CITY", ""),
                    })
                    break

    def vol_cell(src, persona):
        return volumes[src][persona]

  # Build digest table rows
    table_sources = ["Zendesk", "ride_rating", "in_app", "App stores"]
    table_lines = ["|Source|Driver (vol · %neg)|Rider (vol · %neg)|", "|  ---  |  ---  |  ---  |"]
    for src in table_sources:
        d_vol = vol_cell(src, "driver")
        r_vol = vol_cell(src, "rider")
        if src == "in_app":
            table_lines.append(
                f"|{src}|{d_vol} · {pct_neg(sentiment_counts, src, 'driver')}|—|"
            )
        elif src == "App stores":
            d_pct = pct_neg(sentiment_counts, src, "driver") if d_vol else "n/a"
            r_pct = pct_neg(sentiment_counts, src, "rider") if r_vol else "n/a"
            table_lines.append(f"|{src}|{d_vol} · {d_pct}|{r_vol} · {r_pct}|")
        else:
            table_lines.append(
                f"|{src}|{d_vol} · {pct_neg(sentiment_counts, src, 'driver')}|"
                f"{r_vol} · {pct_neg(sentiment_counts, src, 'rider')}|"
            )

    driver_emoji = {
        "Passenger conduct": "😡",
        "Passenger didn't pay (cash)": "💵",
        "Earnings & fares (low pay per km, weak offers)": "💰",
        "Cancellation fees not received / unfair fines": "🚫",
        "Fare/charge accuracy": "📄",
        "Payment not credited / withdrawals": "🏦",
    }
    rider_emoji = {
        "Overcharging / fare disputes": "💸",
        "Driver conduct": "😡",
        "Wrong driver / car vs app": "🚙",
        "Route manipulation / slow driving": "🗺️",
        "No-show / wrong pickup": "📍",
        "Cancellation (unfair fees, driver-initiated)": "🚫",
        "Payment & charging errors": "🏦",
        "Vehicle smell / cleanliness": "🧼",
    }

    driver_issues = []
    for theme, cnt in driver_themes.most_common(6):
        emoji = driver_emoji.get(theme, "•")
        driver_issues.append(
            f"* {emoji} {theme} — **{theme_pct(cnt, driver_sample_n)}**"
        )

    rider_issues = []
    for theme, cnt in rider_themes.most_common(7):
        emoji = rider_emoji.get(theme, "•")
        rider_issues.append(
            f"* {emoji} {theme} — **{theme_pct(cnt, rider_sample_n)}**"
        )

    safety_lines = []
    seen_cats = set()
    for flag in safety_flags:
        if flag["category"] in seen_cats:
            continue
        seen_cats.add(flag["category"])
        cat_emoji = {"Harassment": "🆘", "Reckless driving": "🏎️", "Identity mismatch": "🚙"}
        safety_lines.append(
            f"* {cat_emoji.get(flag['category'], '⚠️')} **{flag['category']}** — {flag['summary']}"
        )
        if len(safety_lines) >= 3:
            break

    priority_actions = {
        "Passenger conduct": "Reinforce passenger conduct policies and in-app reporting for drivers.",
        "Passenger didn't pay (cash)": "Tighten cash-trip payment enforcement and dispute workflows.",
        "Earnings & fares (low pay per km, weak offers)": "Review fare floors, bonus targets, and per-km economics.",
        "Cancellation fees not received / unfair fines": "Audit cancellation-fee crediting and driver compensation SLAs.",
        "Payment not credited / withdrawals": "Fix wallet credit delays and withdrawal pipeline monitoring.",
        "Overcharging / fare disputes": "Audit final-fare recalculation and route-deviation detection.",
        "Driver conduct": "Escalate driver conduct coaching and strike policy for repeat offenders.",
        "Wrong driver / car vs app": "Strengthen identity verification at trip start.",
        "Route manipulation / slow driving": "Improve route-deviation alerts and fare adjustment rules.",
        "Cancellation (unfair fees, driver-initiated)": "Review driver-initiated cancellation fees and rider refunds.",
        "Vehicle smell / cleanliness": "Expand hygiene audits and vehicle cleanliness checks.",
    }

    ranked = []
    for theme, cnt in driver_themes.most_common(3):
        ranked.append((theme, theme_pct(cnt, driver_sample_n), "driver", cnt))
    for theme, cnt in rider_themes.most_common(3):
        ranked.append((theme, theme_pct(cnt, rider_sample_n), "rider", cnt))
    ranked.sort(key=lambda x: x[3], reverse=True)

    priorities = []
    for i, (theme, pct, persona, _) in enumerate(ranked[:3], 1):
        action = priority_actions.get(theme, "Targeted ops review and product fixes.")
        src_note = "Zendesk + ride_rating" if persona == "driver" else "ride_rating + Zendesk"
        priorities.append(
            f"{i}. **{theme}** — ~{pct} of {persona} sample ({src_note}). {action}"
        )

    digest = f"""{day_heading}

{total_comments} comments · stratified sample ~{len(sample)} analyzed (up to 100 per source×user-type cell). Zendesk = support tickets (complaint-only by nature, so its %neg is always very high); ride_rating is the truer sentiment gauge.

{chr(10).join(table_lines)}


### 🏎️ Drivers — top issues

*(% of driver sample)*

{chr(10).join(driver_issues) if driver_issues else '* No dominant themes in sample'}

### 🧍 Riders — top issues

*(% of rider sample)*

{chr(10).join(rider_issues) if rider_issues else '* No dominant themes in sample'}

### ⚠️ Safety escalations (~{len(safety_flags)} flagged)

{chr(10).join(safety_lines) if safety_lines else '* No safety escalations flagged in sample'}

### 🎯 Priorities

{chr(10).join(priorities)}

---
"""

    result = {
        "report_date": d.isoformat(),
        "day_heading": day_heading,
        "total_comments": total_comments,
        "sample_size": len(sample),
        "digest": digest,
        "priorities": priorities[:3],
        "safety_count": len(safety_flags),
        "driver_themes": dict(driver_themes.most_common(5)),
        "rider_themes": dict(rider_themes.most_common(5)),
        "volumes": dict(volumes),
    }
    return result


if __name__ == "__main__":
    report_date = sys.argv[1] if len(sys.argv) > 1 else None
    out = run(report_date)
    print(json.dumps(out, indent=2, default=str))
