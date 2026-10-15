#!/usr/bin/env python3
"""Voice of Customer daily digest — Snowflake pull + sentiment/theme analysis."""

import json
import os
import re
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta

import snowflake.connector

REPORT_DATE = sys.argv[1] if len(sys.argv) > 1 else None
SAMPLE_PER_CELL = 100
MIN_LEN = 3

# --- Theme keyword patterns ---
DRIVER_THEMES = {
    "Cancellation fees not received / unfair fines": [
        r"cancel(lation)?\s*(fee|fine)", r"no[- ]show\s*(fee|compensation)",
        r"fine", r"penalty", r"unfair\s*(fine|cancel)", r"cancel.*not\s*(received|credited)",
    ],
    "Earnings & fares (low pay per km, weak offers)": [
        r"low\s*(fare|pay|price|earning)", r"earn(ing)?s?", r"commission",
        r"pay\s*per\s*km", r"weak\s*offer", r"bonus", r"incentive", r"reward",
        r"trip\s*price", r"underpaid", r"not\s*worth",
    ],
    "Passenger conduct": [
        r"rude", r"abusive", r"aggressive", r"bad\s*behavior", r"conduct",
        r"passenger.*(rude|abuse|fight|argue)", r"no[- ]show", r"disrespect",
    ],
    "Passenger didn't pay (cash)": [
        r"didn'?t\s*pay", r"not\s*pay", r"unpaid", r"cash.*(not|no)", r"refused\s*to\s*pay",
        r"non[- ]payment", r"without\s*paying",
    ],
    "Fare/charge accuracy": [
        r"fare\s*(calculation|accuracy|wrong|error)", r"charge\s*(wrong|error|accuracy)",
        r"route\s*change", r"waiting\s*time", r"distance", r"recalculat",
    ],
    "Payment not credited / withdrawals": [
        r"wallet", r"withdraw", r"not\s*credited", r"balance", r"transfer.*delay",
        r"payout", r"payment\s*not\s*(received|credited)", r"deducted", r"deduction",
    ],
    "App / technical issues": [
        r"app\s*(crash|bug|error|issue|problem|freeze)", r"technical", r"gps",
        r"login", r"update", r"not\s*working", r"stuck",
    ],
}

RIDER_THEMES = {
    "Overcharging / fare disputes": [
        r"overcharg", r"fare\s*(dispute|high|increase|wrong|more)", r"charged\s*more",
        r"final\s*(fare|price|amount)", r"quote", r"estimate", r"extra\s*(cash|money|charge)",
        r"price\s*(higher|increase)", r"expensive",
    ],
    "Driver conduct": [
        r"rude", r"abusive", r"aggressive", r"unprofessional", r"bad\s*behavior",
        r"driver.*(rude|abuse|argue|fight|insult)", r"conduct", r"disrespect",
        r"inappropriate", r"harass",
    ],
    "Wrong driver / car vs app": [
        r"wrong\s*(driver|car|vehicle)", r"different\s*(driver|car|vehicle|plate)",
        r"not\s*match", r"mismatch", r"identity", r"photo", r"profile",
        r"another\s*(driver|car|person)", r"plate\s*number",
    ],
    "Route manipulation / slow driving": [
        r"route\s*(manipul|change|deviat|longer)", r"slow\s*driv", r"detour",
        r"long\s*route", r"unnecessary\s*route", r"took\s*longer",
    ],
    "No-show / wrong pickup": [
        r"no[- ]show", r"wrong\s*pickup", r"pickup\s*(wrong|location)",
        r"didn'?t\s*(arrive|come|show)", r"waited\s*for\s*driver",
    ],
    "Cancellation (unfair fees, driver-initiated)": [
        r"cancel(lation)?", r"cancel\s*fee", r"driver\s*cancel", r"unfair\s*fee",
        r"forced\s*to\s*cancel", r"charged\s*for\s*cancel",
    ],
    "Payment & charging errors": [
        r"double\s*charge", r"charged\s*(twice|without)", r"refund", r"wallet",
        r"payment\s*(error|issue|problem)", r"deducted", r"no\s*change",
    ],
    "Vehicle smell / cleanliness": [
        r"smell", r"dirty", r"cleanliness", r"unclean", r"odor", r"stink",
        r"cigarette", r"smoke", r"filthy",
    ],
    "App / technical issues": [
        r"app\s*(crash|bug|error|issue|problem|freeze)", r"technical", r"gps",
        r"login", r"not\s*working", r"stuck",
    ],
}

SAFETY_PATTERNS = {
    "Harassment": [
        r"harass", r"assault", r"held\s*(my|her|his)\s*hand", r"threaten",
        r"insult", r"sexual", r"explicit", r"stalk", r"inappropriate\s*advanc",
        r"touch(ed)?\s*(me|her|leg)", r"whatsapp.*(photo|message|call)",
    ],
    "Reckless driving": [
        r"reckless", r"speed(ing)?", r"near[- ]accident", r"crash", r"collision",
        r"dangerous\s*driv", r"swerv", r"harsh\s*brak", r"phone\s*while\s*driv",
        r"drunk", r"drugs", r"took\s*drugs", r"looks\s*drunk", r"intoxicat", r"حادث", r"accident",
    ],
    "Identity mismatch": [
        r"wrong\s*(driver|car|vehicle)", r"different\s*(driver|car|vehicle)",
        r"not\s*match", r"mismatch", r"another\s*(driver|person|car)",
        r"female\s*driver.*male", r"gender",
    ],
}

ZENDESK_NEUTRAL_CATS = [
    r"city\s*update", r"work\s*permit", r"document", r"registration",
    r"account\s*update", r"information", r"want to update", r"lost an item",
    r"passenger lost an item", r"stc pay",
]

# Zendesk TICKET_CATEGORY → canonical theme
ZENDESK_DRIVER_CAT = {
    "passenger did not pay trip fare in full / part of the trip fare": "Passenger didn't pay (cash)",
    "i did not get paid the cancellation fees": "Cancellation fees not received / unfair fines",
    "ride value is less than actual": "Earnings & fares (low pay per km, weak offers)",
    "report an issue with the passenger's behaviour": "Passenger conduct",
    "amount has been deducted from my balance": "Payment not credited / withdrawals",
    "funds have not been transferred to my wallet": "Payment not credited / withdrawals",
    "i did not receive credit card payment amount in wallet": "Payment not credited / withdrawals",
    "i did not win although the target was achieved": "Earnings & fares (low pay per km, weak offers)",
    "i did not receive discount amount in wallet": "Payment not credited / withdrawals",
    "intercity surcharge was not added": "Fare/charge accuracy",
    "report a technical issue": "App / technical issues",
}

ZENDESK_RIDER_CAT = {
    "price was way higher than the estimate": "Overcharging / fare disputes",
    "driver was not matching the profile in the app": "Wrong driver / car vs app",
    "i cancelled because driver did not show up at the pickup point": "No-show / wrong pickup",
    "driver asked to cancel and i was charged with cancellation fees": "Cancellation (unfair fees, driver-initiated)",
    "driver cancelled and i was charged with cancellation fees": "Cancellation (unfair fees, driver-initiated)",
    "driver started/ended the ride before i got in the car": "No-show / wrong pickup",
    "report a technical issue": "App / technical issues",
    "bad rating": None,  # parsed via subcategory
}

BAD_RATING_SUB = {
    "incorrect_driver": "Wrong driver / car vs app",
    "car_cleanliness": "Vehicle smell / cleanliness",
    "rude_driver": "Driver conduct",
    "rude": "Driver conduct",
    "overcharging": "Overcharging / fare disputes",
    "route_manipulation": "Route manipulation / slow driving",
    "slow_driving": "Route manipulation / slow driving",
    "no_air_conditioner": "Vehicle smell / cleanliness",
    "no-air-conditioner": "Vehicle smell / cleanliness",
    "harassment": "Driver conduct",
    "reckless_driving": "Driver conduct",
    "cancellation": "Cancellation (unfair fees, driver-initiated)",
    "payment": "Payment & charging errors",
    "wrong_pickup": "No-show / wrong pickup",
    "driver_no_show": "No-show / wrong pickup",
}

# Arabic + English keyword fallbacks for ride_rating / unmatched text
AR_EN_KEYWORDS = {
    "Passenger didn't pay (cash)": [
        r"didn'?t\s*pay", r"not\s*pay", r"unpaid", r"لم\s*يدفع", r"ما\s*دفع", r"رفض\s*الدفع",
        r"without\s*paying", r"no\s*pay",
    ],
    "Passenger conduct": [
        r"rude", r"abusive", r"aggressive", r"smok", r"تدخين", r"سلوك", r"مش\s*محترم",
    ],
    "Cancellation fees not received / unfair fines": [
        r"cancel(lation)?\s*fee", r"cancellation fees", r"رسوم\s*الالغاء", r"الغاء",
    ],
    "Earnings & fares (low pay per km, weak offers)": [
        r"low\s*(fare|pay|price)", r"bonus", r"incentive", r"reward", r"سعر\s*الرحلة",
        r"أقل\s*من", r"commission", r"earn",
    ],
    "Payment not credited / withdrawals": [
        r"wallet", r"withdraw", r"not\s*credited", r"balance", r"deducted", r"محفظ",
        r"transfer", r"payout",
    ],
    "Fare/charge accuracy": [
        r"fare\s*(wrong|error)", r"charge\s*(wrong|error)", r"recalculat", r"route\s*change",
    ],
    "Overcharging / fare disputes": [
        r"overcharg", r"higher\s*than", r"charged\s*more", r"extra\s*money", r"expensive",
        r"مرتفع", r"سعر\s*ثاني", r"quote", r"estimate",
    ],
    "Driver conduct": [
        r"rude", r"abusive", r"harass", r"unprofessional", r"insult", r"aggressive",
        r"get\s*out", r"وقف", r"محترم", r"سلوك", r"inappropriate",
    ],
    "Wrong driver / car vs app": [
        r"wrong\s*(driver|car|vehicle)", r"different\s*(driver|car|vehicle|plate)",
        r"not\s*match", r"mismatch", r"incorrect_driver", r"مختلف", r"مو\s*نفس",
        r"لوحة\s*مختلف",
    ],
    "Route manipulation / slow driving": [
        r"route\s*(manipul|deviat|longer)", r"detour", r"slow\s*driv", r"confused\s*the\s*road",
    ],
    "No-show / wrong pickup": [
        r"no[- ]show", r"wrong\s*pickup", r"didn'?t\s*show", r"بعيد", r"لم\s*يأتي",
        r"wrong\s*place", r"wrong\s*location",
    ],
    "Cancellation (unfair fees, driver-initiated)": [
        r"cancel(lation)?\s*fee", r"driver\s*cancel", r"forced\s*to\s*cancel",
        r"charged.*cancel", r"كانسل",
    ],
    "Payment & charging errors": [
        r"double\s*charge", r"refund", r"charged\s*twice", r"deducted", r"no\s*change",
    ],
    "Vehicle smell / cleanliness": [
        r"smell", r"dirty", r"cleanliness", r"odor", r"stink", r"cigarette", r"smoke",
        r"no-air-conditioner", r"air-conditioner",
    ],
    "App / technical issues": [
        r"app\s*(crash|bug|error|issue)", r"technical", r"gps", r"not\s*working",
    ],
}


def themes_from_row(row, persona):
    """Return list of canonical themes for a negative/mixed row."""
    themes = set()
    text = (row.get("text_for_analysis") or "") + " " + (row.get("ratings_comments") or "")
    text_lower = text.lower()
    ticket_cat = (row.get("ticket_category") or "").strip()
    ticket_lower = ticket_cat.lower()

    if row.get("feedback_source") == "Zendesk" and ticket_cat:
        if persona == "driver":
            mapped = ZENDESK_DRIVER_CAT.get(ticket_lower)
            if mapped:
                themes.add(mapped)
        else:
            if ticket_lower == "bad rating":
                if " :: " in ticket_cat:
                    sub = ticket_cat.split(" :: ")[0].strip().lower()
                    mapped = BAD_RATING_SUB.get(sub)
                    if mapped:
                        themes.add(mapped)
                for sub_key, theme in BAD_RATING_SUB.items():
                    if sub_key in text_lower:
                        themes.add(theme)
            else:
                mapped = ZENDESK_RIDER_CAT.get(ticket_lower)
                if mapped:
                    themes.add(mapped)

    # Keyword fallback on combined text (persona-scoped only)
    theme_pool = DRIVER_THEMES if persona == "driver" else RIDER_THEMES
    for theme_name in theme_pool:
        patterns = AR_EN_KEYWORDS.get(theme_name, []) + theme_pool.get(theme_name, [])
        for pat in patterns:
            if re.search(pat, text_lower):
                themes.add(theme_name)
                break

    # Rating-based hints for short ride_rating comments
    stars = row.get("rating_stars")
    source = row.get("feedback_source")
    if source == "ride_rating" and stars is not None:
        try:
            s = float(stars)
            if s <= 2 and persona == "driver":
                if not themes and re.search(r"rude|pay|لم", text_lower):
                    if re.search(r"pay|دفع", text_lower):
                        themes.add("Passenger didn't pay (cash)")
                    elif re.search(r"rude", text_lower):
                        themes.add("Passenger conduct")
            if s <= 2 and persona == "rider":
                if re.search(r"بعيد|wrong|rude|fare|سعر", text_lower):
                    if re.search(r"بعيد|wrong|pickup", text_lower):
                        themes.add("No-show / wrong pickup")
                    elif re.search(r"rude|conduct", text_lower):
                        themes.add("Driver conduct")
        except (ValueError, TypeError):
            pass

    return list(themes)


def connect():
    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PAT"],
        database=os.environ.get("SNOWFLAKE_DATABASE", "jeeny_prod"),
        role=os.environ.get("SNOWFLAKE_ROLE"),
    )
    cur = conn.cursor()
    warehouse = os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
    cur.execute(f"USE WAREHOUSE {warehouse}")
    return conn, cur


def normalize_source(src):
    if src in ("Google Store", "Apple Store"):
        return "app_stores"
    return src


def classify_sentiment(row):
    text = (row.get("text_for_analysis") or "").lower()
    existing = row.get("overall_sentiment")
    if existing in ("Positive", "Neutral", "Negative", "Mixed"):
        return existing

    source = row.get("feedback_source")
    stars = row.get("rating_stars")
    ticket_cat = (row.get("ticket_category") or "").lower()

    # Zendesk: default negative unless neutral category
    if source == "Zendesk":
        for pat in ZENDESK_NEUTRAL_CATS:
            if re.search(pat, ticket_cat):
                return "Neutral"
        return "Negative"

    # Keyword-based classification
    pos_words = len(re.findall(
        r"\b(good|great|excellent|amazing|thank|happy|satisfied|best|love|perfect|nice)\b", text
    ))
    neg_words = len(re.findall(
        r"\b(bad|terrible|awful|horrible|worst|angry|frustrat|disappoint|unfair|refund|complaint|problem|issue)\b", text
    ))

    if pos_words > 0 and neg_words > 0:
        base = "Mixed"
    elif neg_words > pos_words:
        base = "Negative"
    elif pos_words > neg_words:
        base = "Positive"
    else:
        base = "Neutral"

    # Rating tie-break
    if stars is not None:
        try:
            s = float(stars)
            if s <= 2:
                if base in ("Positive", "Neutral"):
                    base = "Negative"
            elif s >= 4:
                if base in ("Negative", "Neutral") and neg_words == 0:
                    base = "Positive"
            elif s == 3:
                if base == "Positive" and neg_words > 0:
                    base = "Mixed"
        except (ValueError, TypeError):
            pass

    return base


def match_themes(text, theme_dict):
    text_lower = text.lower()
    matched = []
    for theme, patterns in theme_dict.items():
        for pat in patterns:
            if re.search(pat, text_lower):
                matched.append(theme)
                break
    return matched


def pct(n, total, prefix="~"):
    if total == 0:
        return "n/a"
    p = round(100 * n / total)
    if total < 30:
        return f"{prefix}{p}%"
    return f"{prefix}{p}%"


def main():
    if REPORT_DATE:
        report_date = date.fromisoformat(REPORT_DATE)
    else:
        from zoneinfo import ZoneInfo
        tz = ZoneInfo("Asia/Riyadh")
        report_date = (datetime.now(tz) - timedelta(days=1)).date()

    start_ts = f"{report_date.isoformat()} 00:00:00"
    end_ts = (report_date + timedelta(days=1)).isoformat() + " 00:00:00"
    day_name = report_date.strftime("%a %d %b %Y")

    conn, cur = connect()

    # Total comments
    cur.execute(
        """
        SELECT COUNT(*) AS total_comments
        FROM jeeny_prod.general.feedbackevents
        WHERE TIMESTAMP >= %s AND TIMESTAMP < %s
          AND RATINGS_COMMENTS IS NOT NULL
          AND TRIM(RATINGS_COMMENTS) <> ''
          AND LENGTH(TRIM(RATINGS_COMMENTS)) >= %s
        """,
        (start_ts, end_ts, MIN_LEN),
    )
    total_comments = cur.fetchone()[0]

    # Volume by source x persona
    cur.execute(
        """
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
          WHERE TIMESTAMP >= %s AND TIMESTAMP < %s
            AND RATINGS_COMMENTS IS NOT NULL
            AND TRIM(RATINGS_COMMENTS) <> ''
            AND LENGTH(TRIM(RATINGS_COMMENTS)) >= %s
        )
        SELECT FEEDBACK_SOURCE, persona, COUNT(*) AS volume
        FROM base
        WHERE persona IN ('driver', 'rider')
        GROUP BY 1, 2
        ORDER BY volume DESC
        """,
        (start_ts, end_ts, MIN_LEN),
    )
    volumes = defaultdict(lambda: defaultdict(int))
    for src, persona, vol in cur.fetchall():
        volumes[normalize_source(src)][persona] += vol

    # Stratified sample
    cur.execute(
        """
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
          WHERE TIMESTAMP >= %s AND TIMESTAMP < %s
            AND RATINGS_COMMENTS IS NOT NULL
            AND TRIM(RATINGS_COMMENTS) <> ''
            AND LENGTH(TRIM(RATINGS_COMMENTS)) >= %s
        )
        SELECT GUID, FEEDBACK_SOURCE, persona, RATINGS_COMMENTS, text_for_analysis,
               OVERALL_SENTIMENT, RATING_STARS, CITY, COUNTRY, TICKET_CATEGORY,
               CATEGORY, URGENCY, TIMESTAMP
        FROM base
        WHERE persona IN ('driver', 'rider') AND rn <= %s
        """,
        (start_ts, end_ts, MIN_LEN, SAMPLE_PER_CELL),
    )
    cols = [d[0].lower() for d in cur.description]
    rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    conn.close()

    sample_count = len(rows)

    # Sentiment per cell
    cell_sentiment = defaultdict(lambda: {"neg": 0, "total": 0})
    for row in rows:
        src = normalize_source(row["feedback_source"])
        persona = row["persona"]
        sent = classify_sentiment(row)
        cell_sentiment[(src, persona)]["total"] += 1
        if sent in ("Negative", "Mixed"):
            cell_sentiment[(src, persona)]["neg"] += 1

    # Theme extraction on negative/mixed
    driver_neg = []
    rider_neg = []
    safety_flags = []

    for row in rows:
        sent = classify_sentiment(row)
        text = row.get("text_for_analysis") or ""
        persona = row["persona"]
        city = row.get("city") or ""
        country = row.get("country") or ""
        geo = city or country or "Unknown"

        if sent in ("Negative", "Mixed"):
            if persona == "driver":
                driver_neg.append((text, geo, row))
            else:
                rider_neg.append((text, geo, row))

            # Safety scan on full text
            full_text = text + " " + (row.get("ratings_comments") or "")
            for cat, patterns in SAFETY_PATTERNS.items():
                for pat in patterns:
                    if re.search(pat, full_text.lower()):
                        safety_flags.append({
                            "category": cat,
                            "text": text[:300],
                            "geo": geo,
                            "guid": row["guid"],
                        })
                        break

    # Count themes
    def theme_counts(neg_rows, persona):
        counts = defaultdict(int)
        total = len(neg_rows)
        for _, _, row in neg_rows:
            for theme in themes_from_row(row, persona):
                counts[theme] += 1
        return counts, total

    driver_themes, driver_neg_total = theme_counts(driver_neg, "driver")
    rider_themes, rider_neg_total = theme_counts(rider_neg, "rider")

  # Dedupe safety by guid+category
    seen_safety = set()
    unique_safety = []
    for s in safety_flags:
        key = (s["guid"], s["category"])
        if key not in seen_safety:
            seen_safety.add(key)
            unique_safety.append(s)

    def table_cell(src_key, persona):
        vol = volumes.get(src_key, {}).get(persona, 0)
        cs = cell_sentiment.get((src_key, persona), {"neg": 0, "total": 0})
        if vol == 0:
            return "0 · n/a"
        pct_str = pct(cs["neg"], cs["total"])
        return f"{vol:,} · {pct_str}"

    # Build digest table rows
    zendesk_d = table_cell("Zendesk", "driver")
    zendesk_r = table_cell("Zendesk", "rider")
    ride_d = table_cell("ride_rating", "driver")
    ride_r = table_cell("ride_rating", "rider")
    inapp_d = table_cell("in_app", "driver")
    app_d_vol = volumes.get("app_stores", {}).get("driver", 0)
    app_r_vol = volumes.get("app_stores", {}).get("rider", 0)
    app_d = f"{app_d_vol:,} · n/a" if app_d_vol else "0 · n/a"
    app_r = f"{app_r_vol:,} · n/a" if app_r_vol else "0 · n/a"

    # Top themes sorted
    driver_top = sorted(
        [(t, c) for t, c in driver_themes.items() if t in DRIVER_THEMES and c >= 2],
        key=lambda x: -x[1],
    )[:6]
    rider_top = sorted(
        [(t, c) for t, c in rider_themes.items() if t in RIDER_THEMES and c >= 2],
        key=lambda x: -x[1],
    )[:7]

    EMOJI = {
        "Cancellation fees not received / unfair fines": "🚫",
        "Earnings & fares (low pay per km, weak offers)": "💰",
        "Passenger conduct": "😡",
        "Passenger didn't pay (cash)": "💵",
        "Fare/charge accuracy": "📄",
        "Payment not credited / withdrawals": "🏦",
        "App / technical issues": "📱",
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

    def theme_lines(top_themes, neg_total):
        lines = []
        for theme, count in top_themes:
            em = EMOJI.get(theme, "•")
            p = pct(count, neg_total)
            lines.append(f"* {em} {theme} — **{p}**")
        return "\n".join(lines)

    def anonymize_safety(s):
        text = s["text"][:200].strip()
        text = re.sub(r"\b\d{10,}\b", "[phone]", text)
        text = re.sub(r"\b[A-Z][a-z]+\s+[A-Z][a-z]+\b", "user", text)
        geo = s["geo"]
        cat = s["category"]
        em = SAFETY_EMOJI.get(cat, "⚠️")
        # Summarize rather than dump raw text
        if cat == "Harassment":
            summary = f"{geo} rider reports inappropriate driver behavior during or after the trip."
        elif cat == "Reckless driving":
            summary = f"{geo} rider flags reckless or impaired driving — described the driver as dangerous."
        elif cat == "Identity mismatch":
            summary = f"{geo} rider reports a different vehicle and driver than shown in-app."
        else:
            summary = text[:120]
        return f"* {em} **{cat}** — {summary}"

    safety_lines = []
    shown_cats = set()
    for s in unique_safety[:10]:
        if s["category"] not in shown_cats or len(safety_lines) < 3:
            safety_lines.append(anonymize_safety(s))
            shown_cats.add(s["category"])
        if len(safety_lines) >= 3:
            break

    # Priorities from top themes (numbered 1–3)
    priority_items = []
    rider_over = rider_themes.get("Overcharging / fare disputes", 0)
    rider_noshow = rider_themes.get("No-show / wrong pickup", 0)
    if rider_over / max(rider_neg_total, 1) >= 0.08:
        op = pct(rider_over, rider_neg_total)
        np_ = pct(rider_noshow, rider_neg_total)
        priority_items.append(
            f"**Fare integrity for riders** — overcharging / estimate-vs-final gaps are {op} of the rider sample, with no-show friction another {np_}; audit final-fare recalculation and pickup SLA enforcement."
        )
    driver_pay = driver_themes.get("Payment not credited / withdrawals", 0)
    driver_cancel = driver_themes.get("Cancellation fees not received / unfair fines", 0)
    driver_cash = driver_themes.get("Passenger didn't pay (cash)", 0)
    if (driver_pay + driver_cancel + driver_cash) / max(driver_neg_total, 1) >= 0.08:
        wp = pct(driver_pay, driver_neg_total)
        cp = pct(driver_cancel, driver_neg_total)
        cash_p = pct(driver_cash, driver_neg_total)
        priority_items.append(
            f"**Driver payout & cash collection** — unpaid cash fares ({cash_p}), wallet non-credits ({wp}), and missing cancellation fees ({cp}) dominate driver negatives; tighten auto-compensation and wallet credit SLAs."
        )
    rider_identity = rider_themes.get("Wrong driver / car vs app", 0)
    if rider_identity / max(rider_neg_total, 1) >= 0.05:
        ip = pct(rider_identity, rider_neg_total)
        priority_items.append(
            f"**Identity mismatch enforcement** — {ip} of rider negatives report wrong driver/car vs app; pair with safety flags and tighten verification before trip start."
        )
    rider_dc = rider_themes.get("Driver conduct", 0)
    if rider_dc / max(rider_neg_total, 1) >= 0.08:
        dp = pct(rider_dc, rider_neg_total)
        priority_items.append(
            f"**Driver conduct enforcement** — {dp} of the rider sample cites rude/unprofessional driver behavior; pair with reckless-driving safety flags."
        )

    priorities = [
        f"{i + 1}. {item}" for i, item in enumerate(priority_items[:3])
    ]
    while len(priorities) < 3:
        n = len(priorities) + 1
        priorities.append(f"{n}. **Review emerging themes** — continue monitoring daily feedback trends.")

    digest = f"""## 📊 {day_name}

{total_comments:,} comments · stratified sample ~{sample_count} analyzed (up to 100 per source×user-type cell). Zendesk = support tickets (complaint-only by nature, so its %neg is always very high); ride_rating is the truer sentiment gauge.

|Source|Driver (vol · %neg)|Rider (vol · %neg)|
|  ---  |  ---  |  ---  |
|Zendesk|{zendesk_d}|{zendesk_r}|
|ride_rating|{ride_d}|{ride_r}|
|in_app|{inapp_d}|—|
|App stores|{app_d}|{app_r}|


### 🏎️ Drivers — top issues

*(% of driver sample)*

{theme_lines(driver_top, driver_neg_total)}

### 🧍 Riders — top issues

*(% of rider sample)*

{theme_lines(rider_top, rider_neg_total)}

### ⚠️ Safety escalations (~{len(unique_safety)} flagged)

{chr(10).join(safety_lines) if safety_lines else "* No critical safety flags in sample."}

### 🎯 Priorities

{chr(10).join(priorities)}

---
"""

    result = {
        "report_date": report_date.isoformat(),
        "day_name": day_name,
        "total_comments": total_comments,
        "sample_count": sample_count,
        "digest": digest,
        "priorities": priorities,
        "driver_neg_total": driver_neg_total,
        "rider_neg_total": rider_neg_total,
        "safety_count": len(unique_safety),
        "volumes": {k: dict(v) for k, v in volumes.items()},
        "cell_sentiment": {f"{k[0]}_{k[1]}": v for k, v in cell_sentiment.items()},
    }
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
