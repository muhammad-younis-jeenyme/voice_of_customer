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

SAMPLE_LIMIT = 100
MIN_COMMENT_LEN = 3

# Theme keyword patterns (canonical label -> patterns)
DRIVER_THEMES = {
    "Cancellation fees not received / unfair fines": [
        r"cancel", r"fine", r"penalty", r"fee.*not", r"unfair.*fee", r"deduct",
        r"compensation", r"refund.*cancel",
    ],
    "Earnings & fares (low pay per km, weak offers)": [
        r"earn", r"low.*pay", r"fare.*low", r"offer", r"km", r"kilometer",
        r"commission", r"income", r"bonus", r"surge", r"price.*low",
    ],
    "Passenger conduct": [
        r"passenger.*(rude|bad|conduct|behavior|behaviour|disrespect|abuse|fight|argue)",
        r"rider.*(rude|bad|conduct|behavior|behaviour|disrespect|abuse|fight|argue)",
        r"customer.*(rude|bad|conduct|behavior|behaviour|disrespect|abuse)",
        r"passenger.*(smok|drunk|aggressive|violent|insult)",
    ],
    "Passenger didn't pay (cash)": [
        r"didn.?t pay", r"not pay", r"cash.*(not|refus|deni)", r"non.?payment",
        r"without pay", r"no pay", r"refus.*pay", r"unpaid",
    ],
    "Fare/charge accuracy": [
        r"wrong.*(fare|charge|amount|price)", r"incorrect.*(fare|charge|amount|price)",
        r"overcharg", r"double.*charg", r"fare.*(error|wrong|incorrect)",
    ],
    "Payment not credited / withdrawals": [
        r"withdraw", r"wallet", r"not credit", r"payment.*not.*(receiv|credit|reflect)",
        r"payout", r"transfer.*fail", r"balance",
    ],
}

RIDER_THEMES = {
    "Overcharging / fare disputes": [
        r"overcharg", r"fare.*(high|wrong|incorrect|dispute|too much|expensive)",
        r"charged.*(more|extra|double)", r"price.*(high|wrong|too much)",
    ],
    "Driver conduct": [
        r"driver.*(rude|bad|conduct|behavior|behaviour|disrespect|abuse|aggressive|violent|insult|unprofessional)",
        r"rude.*driver", r"bad.*driver", r"driver.*(smok|drunk|sleep|phone|text)",
    ],
    "Wrong driver / car vs app": [
        r"wrong.*(driver|car|vehicle|plate|number)", r"different.*(driver|car|vehicle)",
        r"not.*(same|match).*driver", r"identity", r"fake.*driver", r"car.*(different|wrong)",
    ],
    "Route manipulation / slow driving": [
        r"route.*(long|wrong|manipul|detour|extra)", r"slow.*(drive|driving|driver)",
        r"longer.*route", r"detour", r"took.*long", r"circled", r"traffic.*(on purpose|deliberate)",
    ],
    "No-show / wrong pickup": [
        r"no.?show", r"didn.?t (come|arrive|show)", r"wait.*(long|forever|hour)",
        r"wrong.*(pickup|location|place)", r"never.*(came|arrived|showed)",
    ],
    "Cancellation (unfair fees, driver-initiated)": [
        r"cancel.*(fee|charge|fine|penalty)", r"driver.*cancel", r"unfair.*cancel",
        r"charged.*cancel", r"cancelled.*(without|for no)",
    ],
    "Payment & charging errors": [
        r"payment.*(fail|error|issue|problem|declin)", r"charg.*(fail|error|twice|double)",
        r"card.*(declin|fail|error)", r"wallet.*(error|issue|problem)",
    ],
    "Vehicle smell / cleanliness": [
        r"smell", r"dirty", r"clean", r"hygien", r"odor", r"stink", r"messy", r"filthy",
    ],
}

TICKET_SAFETY_CATEGORIES = {
    "Harassment": [
        "driver discriminated against me",
        "i had a serious incident",
        "report an issue with the passenger's behaviour",
    ],
    "Reckless driving": [
        "reckless_driving",
    ],
    "Identity mismatch": [
        "driver was not matching the profile",
        "car was not matching the information",
        "driver/car did not match the app",
        "incorrect_driver",
        "wrong_car",
    ],
}

SAFETY_PATTERNS = {
    "Harassment": [
        r"harass", r"assault", r"held.*hand", r"threaten", r"\bhit\b", r"stalk",
        r"inappropriat", r"touch", r"sexual", r"molest", r"discriminat",
        r"serious incident", r"after.*ride.*(call|message|contact)",
    ],
    "Reckless driving": [
        r"reckless", r"reckless_driving", r"accident", r"crash", r"injur",
        r"child.*injur", r"dangerous.*(drive|driving)", r"almost.*(hit|crash)",
        r"unsafe.*(drive|driving|speed)",
    ],
    "Identity mismatch": [
        r"wrong_car", r"incorrect_driver", r"not matching", r"did not match",
        r"not the same driver", r"different driver", r"wrong driver",
        r"car.*not matching", r"driver.*not matching", r"vehicle.*not matching",
        r"السائق.*(غير|لا).*مطابق", r"السيار.*(غير|لا).*مطابق", r"مو نفس",
    ],
}

ZENDESK_NEUTRAL_CATEGORIES = {
    "city update", "work permit", "lost item", "lost and found", "general inquiry",
    "account update", "document", "registration", "want to update my operating city",
    "updating my personal photo", "new car information", "passenger lost an item",
}

# Zendesk TICKET_CATEGORY -> canonical theme
TICKET_CATEGORY_THEMES = {
    "driver": {
        "Cancellation fees not received / unfair fines": [
            "i did not get paid the cancellation fees",
            "amount has been deducted from my balance",
        ],
        "Earnings & fares (low pay per km, weak offers)": [
            "ride value is less than actual",
            "i did not win although the target was achieved",
            "intercity surcharge was not added",
        ],
        "Passenger conduct": [
            "report an issue with the passenger's behaviour",
        ],
        "Passenger didn't pay (cash)": [
            "passenger did not pay trip fare",
        ],
        "Fare/charge accuracy": [
            "i entered a wrong amount on payment screen",
        ],
        "Payment not credited / withdrawals": [
            "i did not receive discount amount in wallet",
            "funds have not been transferred to my wallet",
            "i did not receive credit card payment amount in wallet",
            "stc pay",
        ],
    },
    "rider": {
        "Overcharging / fare disputes": [
            "price was way higher than the estimate",
            "driver overcharged me",
            "driver took a long route",
            "driver caused the charges to increase",
        ],
        "Driver conduct": [
            "driver was rude/unprofessional",
            "driver discriminated against me",
            "driver offered services outside the app",
            "bad rating",
        ],
        "Wrong driver / car vs app": [
            "driver was not matching the profile",
            "car was not matching the information",
            "driver/car did not match the app",
            "incorrect_driver",
            "wrong_car",
        ],
        "Route manipulation / slow driving": [
            "driver took a long route",
            "driver caused the charges to increase",
        ],
        "No-show / wrong pickup": [
            "driver did not show up at the pickup",
            "driver started/ended the ride before i got in",
        ],
        "Cancellation (unfair fees, driver-initiated)": [
            "driver asked to cancel and i was charged",
            "driver cancelled and i was charged",
            "i was charged for a cancelled ride",
        ],
        "Payment & charging errors": [
            "i was charged multiple times",
            "report a technical issue",
            "authorized amount not reversed",
        ],
    },
}

ARABIC_NEGATIVE = [
    "سيء", "سيئ", "مشكلة", "مشاكل", "غش", "احتيال", "سرقة", "وقح", "فظ", "غبي",
    "لا أنصح", "أسوأ", "مخالف", "غير محترم", "زيادة", "غالي", "expensive",
]

VALID_SENTIMENTS = {"Positive", "Neutral", "Negative", "Mixed"}


def connect():
    conn = snowflake.connector.connect(
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PAT"],
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        database=os.environ.get("SNOWFLAKE_DATABASE", "jeeny_prod"),
        role=os.environ.get("SNOWFLAKE_ROLE"),
    )
    cur = conn.cursor()
    cur.execute(f"USE WAREHOUSE {os.environ.get('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH')}")
    return conn, cur


def classify_sentiment(row):
    """Classify sentiment for a single row."""
    existing = (row.get("OVERALL_SENTIMENT") or "").strip()
    if existing in VALID_SENTIMENTS:
        return existing

    text = (row.get("TEXT_FOR_ANALYSIS") or "").strip()
    source = (row.get("FEEDBACK_SOURCE") or "")
    rating = row.get("RATING_STARS")
    ticket_cat = (row.get("TICKET_CATEGORY") or "").lower()

    # Zendesk default negative unless neutral category
    if source == "Zendesk":
        if any(cat in ticket_cat for cat in ZENDESK_NEUTRAL_CATEGORIES):
            return "Neutral"
        return "Negative"

    if not text or len(text) < 3:
        return "Unclear"

    text_lower = text.lower()

    # Arabic negative keywords when short
    if len(text) < 5:
        if any(kw in text for kw in ARABIC_NEGATIVE):
            return "Negative"
        if rating is not None:
            if rating <= 2:
                return "Negative"
            if rating >= 4:
                return "Positive"
        return "Unclear"

    positive_kw = [
        "thank", "great", "excellent", "good", "love", "best", "awesome", "perfect",
        "helpful", "professional", "recommend", "happy", "satisfied", "nice", "fast",
        "ممتاز", "شكر", "جيد", "رائع",
    ]
    negative_kw = [
        "bad", "worst", "terrible", "horrible", "awful", "hate", "disappoint", "frustrat",
        "complain", "problem", "issue", "scam", "fraud", "rude", "never again", "refund",
        "overcharg", "cancel", "fine", "unfair", "slow", "wait", "dirty", "smell",
        "dangerous", "unsafe", "harass", "assault", "threat",
    ]

    pos_hits = sum(1 for kw in positive_kw if kw in text_lower)
    neg_hits = sum(1 for kw in negative_kw if kw in text_lower)

    # Rating tie-break
    if rating is not None:
        if rating <= 2:
            if pos_hits > neg_hits:
                return "Mixed"
            return "Negative"
        if rating == 3:
            if neg_hits > 0 and pos_hits > 0:
                return "Mixed"
            if neg_hits > 0:
                return "Negative"
            if pos_hits > 0:
                return "Positive"
            return "Neutral"
        if rating >= 4:
            if neg_hits > pos_hits:
                return "Mixed"
            return "Positive"

    if pos_hits > 0 and neg_hits > 0:
        return "Mixed"
    if neg_hits > 0:
        return "Negative"
    if pos_hits > 0:
        return "Positive"
    return "Neutral"


def match_themes(text, theme_dict, ticket_category="", persona=""):
    matched = []
    cat_lower = (ticket_category or "").lower()
    text_lower = (text or "").lower()

    # Zendesk category mapping first
    if cat_lower and persona in TICKET_CATEGORY_THEMES:
        for label, cat_patterns in TICKET_CATEGORY_THEMES[persona].items():
            for pat in cat_patterns:
                if pat in cat_lower or re.search(re.escape(pat), cat_lower):
                    matched.append(label)
                    break

    if not text_lower:
        return list(dict.fromkeys(matched))

    for label, patterns in theme_dict.items():
        if label in matched:
            continue
        for pat in patterns:
            if re.search(pat, text_lower, re.IGNORECASE):
                matched.append(label)
                break
    return list(dict.fromkeys(matched))


def match_safety(text, ticket_cat=""):
    combined = f"{text} {ticket_cat}".lower()
    results = []
    cat_lower = (ticket_cat or "").lower()
    for category, cats in TICKET_SAFETY_CATEGORIES.items():
        for pat in cats:
            if pat in cat_lower or pat in combined:
                results.append(category)
                break
    if results:
        return results
    for category, patterns in SAFETY_PATTERNS.items():
        for pat in patterns:
            if re.search(pat, combined, re.IGNORECASE):
                results.append(category)
                break
    return results


def anonymize_summary(text, city=None):
    """Create one-line anonymized safety summary."""
    t = (text or "")[:200]
    t = re.sub(r"\b\d{8,}\b", "[redacted]", t)
    t = re.sub(r"\+?\d[\d\s\-]{7,}\d", "[phone]", t)
    t = re.sub(r"\b[A-Z][a-z]+\s+[A-Z][a-z]+\b", "[person]", t)
    if city:
        t = f"[{city}] {t}"
    return t[:120].strip()


def normalize_source(src):
    if src in ("Google Store", "Apple Store"):
        return "App stores"
    return src


def pct_neg(neg, total):
    if total == 0:
        return "n/a"
    pct = round(100 * neg / total)
    prefix = "~" if total < 30 else ""
    return f"{prefix}{pct}%"


def theme_pct(count, sample_total):
    if sample_total == 0:
        return 0
    return round(100 * count / sample_total)


def pull_data(report_date):
    start_ts = f"{report_date} 00:00:00"
    end_ts = (datetime.strptime(report_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d 00:00:00")

    conn, cur = connect()

    base_cte = """
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
        TIMESTAMP
      FROM jeeny_prod.general.feedbackevents
      WHERE TIMESTAMP >= %s
        AND TIMESTAMP < %s
        AND RATINGS_COMMENTS IS NOT NULL
        AND TRIM(RATINGS_COMMENTS) <> ''
        AND LENGTH(TRIM(RATINGS_COMMENTS)) >= %s
    )
    """

    # Total count
    cur.execute(
        f"""SELECT COUNT(*) FROM jeeny_prod.general.feedbackevents
        WHERE TIMESTAMP >= %s AND TIMESTAMP < %s
          AND RATINGS_COMMENTS IS NOT NULL AND TRIM(RATINGS_COMMENTS) <> ''
          AND LENGTH(TRIM(RATINGS_COMMENTS)) >= %s""",
        (start_ts, end_ts, MIN_COMMENT_LEN),
    )
    total_comments = cur.fetchone()[0]

    # Volume by source x persona
    cur.execute(
        base_cte
        + """SELECT FEEDBACK_SOURCE, persona, COUNT(*) AS volume
        FROM base WHERE persona IN ('driver', 'rider')
        GROUP BY 1, 2 ORDER BY volume DESC""",
        (start_ts, end_ts, MIN_COMMENT_LEN),
    )
    volumes = {}
    for src, persona, vol in cur.fetchall():
        key = (src, persona)
        volumes[key] = vol

    # Stratified sample
    cur.execute(
        base_cte
        + f"""SELECT
          GUID, FEEDBACK_SOURCE, persona, RATINGS_COMMENTS, text_for_analysis,
          OVERALL_SENTIMENT, RATING_STARS, CITY, COUNTRY, TICKET_CATEGORY,
          CATEGORY, URGENCY, TIMESTAMP,
          ROW_NUMBER() OVER (
            PARTITION BY FEEDBACK_SOURCE, persona ORDER BY RANDOM()
          ) AS rn
        FROM base
        WHERE persona IN ('driver', 'rider')
        QUALIFY rn <= {SAMPLE_LIMIT}""",
        (start_ts, end_ts, MIN_COMMENT_LEN),
    )
    cols = [d[0] for d in cur.description]
    sample = [dict(zip(cols, row)) for row in cur.fetchall()]

    cur.close()
    conn.close()
    return total_comments, volumes, sample


def analyze(total_comments, volumes, sample):
    # Sentiment per cell
    cell_sentiments = defaultdict(lambda: {"neg": 0, "total": 0})
    driver_theme_counts = Counter()
    rider_theme_counts = Counter()
    safety_flags = []
    driver_sample_total = 0
    rider_sample_total = 0

    for row in sample:
        persona = row["PERSONA"]
        src = row["FEEDBACK_SOURCE"]
        norm_src = normalize_source(src)
        cell_key = (norm_src, persona)

        row_dict = {
            "TEXT_FOR_ANALYSIS": row["TEXT_FOR_ANALYSIS"],
            "OVERALL_SENTIMENT": row["OVERALL_SENTIMENT"],
            "FEEDBACK_SOURCE": src,
            "RATING_STARS": row["RATING_STARS"],
            "TICKET_CATEGORY": row["TICKET_CATEGORY"] or "",
        }
        sentiment = classify_sentiment(row_dict)

        if sentiment != "Unclear":
            cell_sentiments[cell_key]["total"] += 1
            if sentiment in ("Negative", "Mixed"):
                cell_sentiments[cell_key]["neg"] += 1

        if persona == "driver":
            driver_sample_total += 1
        else:
            rider_sample_total += 1

        text = row["TEXT_FOR_ANALYSIS"] or ""
        ticket_cat = row["TICKET_CATEGORY"] or ""

        if sentiment in ("Negative", "Mixed"):
            theme_dict = DRIVER_THEMES if persona == "driver" else RIDER_THEMES
            for theme in match_themes(text, theme_dict, ticket_cat, persona):
                if persona == "driver":
                    driver_theme_counts[theme] += 1
                else:
                    rider_theme_counts[theme] += 1

            safety_cats = match_safety(text, ticket_cat)
            if safety_cats:
                safety_flags.append({
                    "categories": safety_cats,
                    "summary": anonymize_summary(text, row.get("CITY")),
                    "persona": persona,
                    "source": src,
                })

    # Build volume table
    def vol_cell(src, persona):
        if src == "App stores":
            v = volumes.get(("Google Store", persona), 0) + volumes.get(("Apple Store", persona), 0)
        else:
            v = volumes.get((src, persona), 0)
        return v

    def neg_cell(src, persona):
        key = (src, persona)
        c = cell_sentiments[key]
        return pct_neg(c["neg"], c["total"])

    table = {
        "Zendesk": {
            "driver": (vol_cell("Zendesk", "driver"), neg_cell("Zendesk", "driver")),
            "rider": (vol_cell("Zendesk", "rider"), neg_cell("Zendesk", "rider")),
        },
        "ride_rating": {
            "driver": (vol_cell("ride_rating", "driver"), neg_cell("ride_rating", "driver")),
            "rider": (vol_cell("ride_rating", "rider"), neg_cell("ride_rating", "rider")),
        },
        "in_app": {
            "driver": (vol_cell("in_app", "driver"), neg_cell("in_app", "driver")),
            "rider": ("—", "—"),
        },
        "App stores": {
            "driver": (vol_cell("App stores", "driver"), "n/a"),
            "rider": (vol_cell("App stores", "rider"), "n/a"),
        },
    }

    driver_themes = [
        (label, theme_pct(driver_theme_counts[label], driver_sample_total))
        for label in sorted(driver_theme_counts, key=driver_theme_counts.get, reverse=True)
    ][:6]

    rider_themes = [
        (label, theme_pct(rider_theme_counts[label], rider_sample_total))
        for label in sorted(rider_theme_counts, key=rider_theme_counts.get, reverse=True)
    ][:7]

    # Priorities
    top_driver = driver_themes[:3] if driver_themes else []
    top_rider = rider_themes[:3] if rider_themes else []
    priorities = build_priorities(top_driver, top_rider, len(safety_flags), table)

    return {
        "total_comments": total_comments,
        "sample_count": len(sample),
        "table": table,
        "driver_themes": driver_themes,
        "rider_themes": rider_themes,
        "driver_sample_total": driver_sample_total,
        "rider_sample_total": rider_sample_total,
        "safety_flags": safety_flags,
        "priorities": priorities,
    }


def build_priorities(top_driver, top_rider, safety_count, table):
    priorities = []

    if safety_count >= 3:
        priorities.append(
            "Safety escalation review — "
            f"{safety_count} flagged cases in sample (harassment, reckless driving, identity mismatch). "
            "Prioritize ops review and driver/rider safety workflows."
        )

    if top_rider:
        label, pct = top_rider[0]
        rec = {
            "Driver conduct": "Strengthen driver coaching, deactivation triggers, and in-ride reporting.",
            "Overcharging / fare disputes": "Audit final-fare recalculation and route-deviation detection.",
            "Payment & charging errors": "Review payment gateway reconciliation and duplicate-charge reversal SLAs.",
        }.get(label, "Review ops playbooks and rider communication.")
        priorities.append(
            f"{label} for riders — ~{pct}% of rider sample mentions this theme. {rec}"
        )

    if top_driver:
        label, pct = top_driver[0]
        rec = {
            "Cancellation fees not received / unfair fines": "Review cancellation-fee payout rules and driver wallet crediting.",
            "Earnings & fares (low pay per km, weak offers)": "Review fare floors, offer density, and km-rate transparency.",
            "Passenger conduct": "Improve passenger reporting and repeat-offender enforcement.",
        }.get(label, "Review policy clarity and compensation triggers.")
        priorities.append(
            f"{label} for drivers — ~{pct}% of driver sample. {rec}"
        )

    # Fill to 3
    extras = []
    if len(top_rider) > 1:
        label, pct = top_rider[1]
        extras.append(
            f"{label} — ~{pct}% of rider negatives. Tighten enforcement and in-app reporting."
        )
    if len(top_driver) > 1:
        label, pct = top_driver[1]
        extras.append(
            f"{label} — ~{pct}% of driver negatives. Review ops playbooks and payout rules."
        )
    if len(top_rider) > 2:
        label, pct = top_rider[2]
        extras.append(
            f"{label} — ~{pct}% of rider sample. Improve detection and rider communication."
        )

    for e in extras:
        if len(priorities) >= 3:
            break
        priorities.append(e)

    return priorities[:3]


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

SAFETY_EMOJI = {
    "Harassment": "🆘",
    "Reckless driving": "🏎️",
    "Identity mismatch": "🚙",
}


def compose_digest(report_day_name, analysis):
    t = analysis["table"]
    lines = [
        f"## 📊 {report_day_name}",
        "",
        f"{analysis['total_comments']} comments · stratified sample ~{analysis['sample_count']} analyzed (up to 100 per source×user-type cell). Zendesk = support tickets (complaint-only by nature, so its %neg is always very high); ride_rating is the truer sentiment gauge.",
        "",
        "|Source|Driver (vol · %neg)|Rider (vol · %neg)|",
        "|  ---  |  ---  |  ---  |",
    ]

    for src in ["Zendesk", "ride_rating", "in_app", "App stores"]:
        d_vol, d_pct = t[src]["driver"]
        r_vol, r_pct = t[src]["rider"]
        d_str = f"{d_vol} · {d_pct}" if d_pct != "n/a" else f"{d_vol} · n/a"
        if r_vol == "—":
            r_str = "—"
        elif r_pct == "n/a":
            r_str = f"{r_vol} · n/a"
        else:
            r_str = f"{r_vol} · {r_pct}"
        lines.append(f"|{src}|{d_str}|{r_str}|")

    lines += ["", "### 🏎️ Drivers — top issues", "", "*(% of driver sample)*", ""]

    for label, pct in analysis["driver_themes"]:
        emoji = THEME_EMOJI.get(label, "•")
        lines.append(f"* {emoji} {label} — **~{pct}%**")

    if not analysis["driver_themes"]:
        lines.append("* No dominant themes in sample")

    lines += ["", "### 🧍 Riders — top issues", "", "*(% of rider sample)*", ""]

    for label, pct in analysis["rider_themes"]:
        emoji = THEME_EMOJI.get(label, "•")
        lines.append(f"* {emoji} {label} — **~{pct}%**")

    if not analysis["rider_themes"]:
        lines.append("* No dominant themes in sample")

    safety_count = len(analysis["safety_flags"])
    lines += ["", f"### ⚠️ Safety escalations (~{safety_count} flagged)", ""]

    shown = set()
    examples = []
    # Prefer one clear example per category
    category_best = {}
    for flag in analysis["safety_flags"]:
        for cat in flag["categories"]:
            summary = flag["summary"]
            score = 0
            s_lower = summary.lower()
            if cat == "Reckless driving" and "reckless" in s_lower:
                score += 10
            if cat == "Identity mismatch" and any(
                x in s_lower for x in ("not matching", "different driver", "wrong_car", "incorrect_driver", "not the same")
            ):
                score += 10
            if cat == "Harassment" and any(
                x in s_lower for x in ("harass", "assault", "discriminat", "threaten", "almost hit")
            ):
                score += 10
            if cat not in category_best or score > category_best[cat][0]:
                category_best[cat] = (score, summary)

    for cat in ["Harassment", "Reckless driving", "Identity mismatch"]:
        if cat in category_best and len(examples) < 3:
            emoji = SAFETY_EMOJI.get(cat, "⚠️")
            examples.append(f"* {emoji} **{cat}** — {category_best[cat][1]}")

    if not examples:
        lines.append("* No safety escalations flagged in sample")
    else:
        lines.extend(examples)

    lines += ["", "### 🎯 Priorities", ""]

    for i, p in enumerate(analysis["priorities"], 1):
        parts = p.split(" — ", 1)
        if len(parts) == 2:
            lines.append(f"{i}. **{parts[0]}** — {parts[1]}")
        else:
            lines.append(f"{i}. {p}")

    lines += ["", "---", ""]
    return "\n".join(lines)


def main():
    report_date = sys.argv[1] if len(sys.argv) > 1 else None
    if not report_date:
        tz = ZoneInfo("Asia/Riyadh")
        report_date = (datetime.now(tz) - timedelta(days=1)).strftime("%Y-%m-%d")

    report_day = datetime.strptime(report_date, "%Y-%m-%d")
    report_day_name = report_day.strftime("%a %d %b %Y")

    print(f"Report date: {report_date} ({report_day_name})", file=sys.stderr)
    total, volumes, sample = pull_data(report_date)
    print(f"Total comments: {total}, sample: {len(sample)}", file=sys.stderr)

    analysis = analyze(total, volumes, sample)
    digest = compose_digest(report_day_name, analysis)

    output = {
        "report_date": report_date,
        "report_day_name": report_day_name,
        "total_comments": total,
        "sample_count": len(sample),
        "analysis": analysis,
        "digest": digest,
        "top_priorities": analysis["priorities"][:3],
    }
    print(json.dumps(output, indent=2, default=str))


if __name__ == "__main__":
    main()
