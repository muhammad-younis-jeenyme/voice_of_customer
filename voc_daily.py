#!/usr/bin/env python3
"""Pull VoC data from Snowflake and run sentiment/theme analysis."""
import json
import os
import re
import sys
from collections import defaultdict

import snowflake.connector

REPORT_DATE = sys.argv[1] if len(sys.argv) > 1 else "2026-08-02"
SAMPLE_LIMIT = 100

START_TS = f"{REPORT_DATE} 00:00:00"
# end is start of next day


def get_conn():
    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PAT"],
        database=os.environ.get("SNOWFLAKE_DATABASE", "JEENY_PROD"),
        role=os.environ.get("SNOWFLAKE_ROLE"),
    )
    cur = conn.cursor()
    cur.execute(f"USE WAREHOUSE {os.environ.get('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH')}")
    return conn


VOLUME_SQL = """
SELECT
  FEEDBACK_SOURCE,
  persona,
  COUNT(*) AS volume
FROM (
  SELECT
    FEEDBACK_SOURCE,
    CASE
      WHEN USER_TYPE = 'driver' THEN 'driver'
      WHEN USER_TYPE IN ('passenger', 'customer') THEN 'rider'
      ELSE 'other'
    END AS persona
  FROM jeeny_prod.general.feedbackevents
  WHERE TIMESTAMP >= %(start)s
    AND TIMESTAMP < %(start)s::DATE + INTERVAL '1 day'
    AND RATINGS_COMMENTS IS NOT NULL
    AND TRIM(RATINGS_COMMENTS) <> ''
    AND LENGTH(TRIM(RATINGS_COMMENTS)) >= 3
)
WHERE persona IN ('driver', 'rider')
GROUP BY 1, 2
ORDER BY volume DESC
"""

TOTAL_SQL = """
SELECT COUNT(*) AS total_comments
FROM jeeny_prod.general.feedbackevents
WHERE TIMESTAMP >= %(start)s
  AND TIMESTAMP < %(start)s::DATE + INTERVAL '1 day'
  AND RATINGS_COMMENTS IS NOT NULL
  AND TRIM(RATINGS_COMMENTS) <> ''
  AND LENGTH(TRIM(RATINGS_COMMENTS)) >= 3
"""

SAMPLE_SQL = """
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
    AND TIMESTAMP < %(start)s::DATE + INTERVAL '1 day'
    AND RATINGS_COMMENTS IS NOT NULL
    AND TRIM(RATINGS_COMMENTS) <> ''
    AND LENGTH(TRIM(RATINGS_COMMENTS)) >= 3
)
SELECT
  GUID, FEEDBACK_SOURCE, persona, RATINGS_COMMENTS, text_for_analysis,
  OVERALL_SENTIMENT, RATING_STARS, CITY, COUNTRY, TICKET_CATEGORY,
  CATEGORY, URGENCY, TIMESTAMP
FROM base
WHERE persona IN ('driver', 'rider')
  AND rn <= %(limit)s
"""

# Zendesk neutral/ops categories
ZENDESK_NEUTRAL_CATS = {
    "city update", "work permit", "lost item", "lost items", "general inquiry",
    "account update", "document", "registration", "vehicle update",
}

# Zendesk TICKET_CATEGORY -> canonical theme (driver persona)
DRIVER_TICKET_MAP = {
    "i did not get paid the cancellation fees": "Cancellation fees not received / unfair fines",
    "amount has been deducted from my balance": "Payment not credited / withdrawals",
    "funds have not been transferred to my wallet": "Payment not credited / withdrawals",
    "i did not receive credit card payment amount in wallet": "Payment not credited / withdrawals",
    "i did not receive discount amount in wallet": "Payment not credited / withdrawals",
    "passenger did not pay trip fare in full / part of the trip fare": "Passenger didn't pay (cash)",
    "report an issue with the passenger's behaviour": "Passenger conduct",
    "ride value is less than actual": "Earnings & fares (low pay per km, weak offers)",
    "i did not win although the target was achieved": "Earnings & fares (low pay per km, weak offers)",
    "i won but incentive was not received": "Earnings & fares (low pay per km, weak offers)",
    "price was way higher than the estimate": "Fare/charge accuracy",
}

# Zendesk TICKET_CATEGORY -> canonical theme (rider persona)
RIDER_TICKET_MAP = {
    "price was way higher than the estimate": "Overcharging / fare disputes",
    "driver charged me a higher amount": "Overcharging / fare disputes",
    "driver asked to cancel and i was charged with cancellation fees": "Cancellation (unfair fees, driver-initiated)",
    "driver cancelled and i was charged with cancellation fees": "Cancellation (unfair fees, driver-initiated)",
    "driver was not matching the profile in the app": "Wrong driver / car vs app",
    "car was not matching the information in the app": "Wrong driver / car vs app",
    "i cancelled because driver/car did not match the app": "Wrong driver / car vs app",
    "driver was rude/unprofessional": "Driver conduct",
    "driver started/ended the ride before i got in the car": "No-show / wrong pickup",
    "i cancelled because driver did not show up at the pickup point": "No-show / wrong pickup",
    "driver cancelled without reaching pickup point": "No-show / wrong pickup",
    "driver took a long route/ driver caused the charges to increase": "Route manipulation / slow driving",
    "i was charged multiple times for the trip / authorized amount not reversed": "Payment & charging errors",
    "report an issue with the passenger's behaviour": "Driver conduct",
}

CATEGORY_MAP = {
    "trip & route integrity": "Route manipulation / slow driving",
    "safety & security concern": "Driver conduct",
    "driver conduct & professionalism": "Driver conduct",
    "vehicle cleanliness & comfort": "Vehicle smell / cleanliness",
    "payment & billing": "Payment & charging errors",
    "fare & pricing": "Overcharging / fare disputes",
}

# Theme patterns (on text_for_analysis, case-insensitive)
DRIVER_THEMES = {
    "Cancellation fees not received / unfair fines": [
        r"cancel", r"fine", r"penalty", r"compensation", r"waiting fee", r"no.?show fee",
    ],
    "Earnings & fares (low pay per km, weak offers)": [
        r"low (pay|fare|price|earning)", r"earn", r"commission", r"incentive", r"bonus",
        r"offer", r"cheap", r"not worth", r"trip value", r"less than",
    ],
    "Passenger conduct": [
        r"passenger", r"rude", r"misbehav", r"abusive", r"drunk", r"smok", r"pax",
        r"customer.*(bad|rude)", r"rider.*(bad|rude)",
    ],
    "Passenger didn't pay (cash)": [
        r"didn.?t pay", r"not pay", r"no pay", r"refused", r"unpaid", r"cash",
        r"without pay", r"did not pay",
    ],
    "Fare/charge accuracy": [
        r"wrong (fare|charge|amount|price)", r"incorrect", r"deduct", r"balance",
        r"charged wrong", r"fare.*(wrong|error)",
    ],
    "Payment not credited / withdrawals": [
        r"wallet", r"withdraw", r"not credited", r"not received", r"transfer",
        r"payout", r"balance", r"payment.*not", r"money.*not",
    ],
}

RIDER_THEMES = {
    "Overcharging / fare disputes": [
        r"overcharg", r"too (much|expensive|high)", r"charged (more|extra|double)",
        r"fare.*(high|expensive|wrong)", r"price.*(high|wrong)", r"higher than",
        r"extra (charge|amount|money)", r"expensive",
    ],
    "Driver conduct": [
        r"driver.*(rude|bad|misbehav|aggressive|unprofessional)", r"rude",
        r"unprofessional", r"impolite", r"disrespect", r"angry", r"shout", r"yell",
        r"insult", r"so rude",
    ],
    "Wrong driver / car vs app": [
        r"wrong (driver|car|vehicle)", r"different (driver|car|vehicle)",
        r"not the (driver|car)", r"not match", r"not same", r"identity", r"plate",
        r"another (driver|car)", r"someone else", r"profile",
    ],
    "Route manipulation / slow driving": [
        r"long route", r"detour", r"wrong route", r"slow", r"took long",
        r"extra (km|mile|distance)", r"manipulat", r"unnecessary",
    ],
    "No-show / wrong pickup": [
        r"no.?show", r"didn.?t (come|arrive|show)", r"wrong (pickup|location)",
        r"never came", r"did not come", r"not show", r"before i got",
    ],
    "Cancellation (unfair fees, driver-initiated)": [
        r"cancel", r"fee", r"charged.*cancel", r"driver.*cancel",
    ],
    "Payment & charging errors": [
        r"payment", r"charged.*(twice|double|wrong)", r"card", r"wallet",
        r"refund", r"transaction", r"multiple times",
    ],
    "Vehicle smell / cleanliness": [
        r"smell", r"dirty", r"clean", r"hygiene", r"odor", r"stink",
        r"cigarette", r"smoke", r"filthy",
    ],
    "App / technical issues": [
        r"app", r"bug", r"crash", r"update", r"login", r"technical", r"error",
        r"not work", r"glitch", r"gps", r"map",
    ],
}

SAFETY_PATTERNS = {
    "Harassment": [
        r"harass", r"assault", r"held (my )?hand", r"threaten", r"hit me",
        r"stalk", r"inappropriate", r"sexual", r"touch", r"abuse", r"scared",
        r"harassing", r"touching my",
    ],
    "Reckless driving": [
        r"reckless", r"speed(ing)?", r"accident", r"injur", r"child.*injur",
        r"dangerous (drive|driving)", r"crash", r"almost (hit|crash)",
        r"aggressive.*driv", r"driving.*aggressive", r"aggressival",
    ],
    "Identity mismatch": [
        r"wrong (driver|car|vehicle)", r"different (driver|car|vehicle)",
        r"not the (driver|car)", r"not same car", r"someone else", r"fake driver",
        r"not match", r"profile",
    ],
}

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
    "App / technical issues": "📱",
}


def classify_sentiment(row):
    existing = (row.get("OVERALL_SENTIMENT") or row.get("overall_sentiment") or "").strip()
    if existing in ("Positive", "Neutral", "Negative", "Mixed"):
        return existing

    text = (row.get("text_for_analysis") or row.get("TEXT_FOR_ANALYSIS") or "").lower()
    stars = row.get("RATING_STARS") or row.get("rating_stars")
    source = row.get("FEEDBACK_SOURCE") or row.get("feedback_source") or ""
    ticket_cat = (row.get("TICKET_CATEGORY") or row.get("ticket_category") or "").lower()

    # Zendesk default negative unless neutral category
    if source == "Zendesk":
        if any(n in ticket_cat for n in ZENDESK_NEUTRAL_CATS):
            return "Neutral"
        return "Negative"

    pos_words = ["great", "good", "excellent", "thank", "love", "best", "amazing", "perfect", "wonderful", "happy", "nice", "awesome", "fantastic"]
    neg_words = ["bad", "terrible", "worst", "horrible", "awful", "hate", "angry", "frustrat", "disappoint", "unfair", "rude", "scam", "cheat", "never", "refuse", "complaint"]

    pos_count = sum(1 for w in pos_words if w in text)
    neg_count = sum(1 for w in neg_words if w in text)

    if stars is not None:
        try:
            s = float(stars)
            if s <= 2:
                neg_count += 2
            elif s == 3:
                pass
            elif s >= 4:
                pos_count += 2
        except (ValueError, TypeError):
            pass

    if pos_count > 0 and neg_count > 0:
        return "Mixed"
    if neg_count > pos_count:
        return "Negative"
    if pos_count > neg_count:
        return "Positive"
    if neg_count > 0:
        return "Negative"
    if pos_count > 0:
        return "Positive"
    if len(text) < 10:
        return "Unclear"
    return "Neutral"


def match_themes(text, theme_dict):
    text_lower = (text or "").lower()
    matched = []
    for theme, patterns in theme_dict.items():
        for pat in patterns:
            if re.search(pat, text_lower):
                matched.append(theme)
                break
    return matched


def extract_themes_for_row(row, persona):
    themes = []
    ticket_cat = (row.get("TICKET_CATEGORY") or row.get("ticket_category") or "").lower().strip()
    category = (row.get("CATEGORY") or row.get("category") or "").lower().strip()
    text = row.get("text_for_analysis") or row.get("TEXT_FOR_ANALYSIS") or ""
    source = row.get("FEEDBACK_SOURCE") or row.get("feedback_source") or ""

    if source == "Zendesk" and ticket_cat:
        if persona == "driver" and ticket_cat in DRIVER_TICKET_MAP:
            themes.append(DRIVER_TICKET_MAP[ticket_cat])
        elif persona == "rider" and ticket_cat in RIDER_TICKET_MAP:
            themes.append(RIDER_TICKET_MAP[ticket_cat])

    if category and category in CATEGORY_MAP:
        cat_theme = CATEGORY_MAP[category]
        if persona == "driver" and cat_theme in DRIVER_THEMES:
            themes.append(cat_theme)
        elif persona == "rider":
            themes.append(cat_theme)

    if persona == "driver":
        themes.extend(match_themes(text, DRIVER_THEMES))
    else:
        themes.extend(match_themes(text, RIDER_THEMES))

    # dedupe preserving order
    seen = set()
    result = []
    for t in themes:
        if t not in seen:
            seen.add(t)
            result.append(t)
    return result


def normalize_source(src):
    if src in ("Google Store", "Apple Store"):
        return "app_store"
    return src


def main():
    conn = get_conn()
    cur = conn.cursor()
    params = {"start": START_TS, "limit": SAMPLE_LIMIT}

    cur.execute(TOTAL_SQL, params)
    total_comments = cur.fetchone()[0]

    cur.execute(VOLUME_SQL, params)
    volumes = {}
    for src, persona, vol in cur.fetchall():
        ns = normalize_source(src)
        key = (ns, persona)
        volumes[key] = volumes.get(key, 0) + vol

    cur.execute(SAMPLE_SQL, params)
    cols = [d[0].upper() for d in cur.description]
    rows = []
    for r in cur.fetchall():
        row = dict(zip(cols, r))
        row["persona"] = row.get("PERSONA", row.get("persona"))
        row["text_for_analysis"] = row.get("TEXT_FOR_ANALYSIS", row.get("text_for_analysis"))
        rows.append(row)
    conn.close()

    sample_count = len(rows)

    # Sentiment per cell
    cells = defaultdict(lambda: {"analyzed": 0, "negative": 0, "mixed": 0})
    analyzed_rows = []

    for row in rows:
        sentiment = classify_sentiment(row)
        row["sentiment"] = sentiment
        ns = normalize_source(row.get("FEEDBACK_SOURCE") or row.get("feedback_source"))
        persona = row["persona"]
        key = (ns, persona)

        if sentiment == "Unclear":
            continue
        cells[key]["analyzed"] += 1
        if sentiment == "Negative":
            cells[key]["negative"] += 1
        if sentiment == "Mixed":
            cells[key]["mixed"] += 1
        analyzed_rows.append(row)

    def pct_neg(key):
        c = cells[key]
        if c["analyzed"] == 0:
            return None
        pct = round(100 * (c["negative"] + c["mixed"]) / c["analyzed"])
        prefix = "~" if c["analyzed"] < 30 else ""
        return f"{prefix}{pct}%"

  # Theme extraction on negative/mixed by persona
    driver_neg = [r for r in analyzed_rows if r["persona"] == "driver" and r["sentiment"] in ("Negative", "Mixed")]
    rider_neg = [r for r in analyzed_rows if r["persona"] == "rider" and r["sentiment"] in ("Negative", "Mixed")]

    def theme_counts(neg_rows, persona):
        counts = defaultdict(int)
        for r in neg_rows:
            themes = extract_themes_for_row(r, persona)
            for t in themes:
                counts[t] += 1
        denom = len(neg_rows) or 1
        ranked = sorted(counts.items(), key=lambda x: -x[1])
        result = []
        for theme, cnt in ranked[:7]:
            pct = round(100 * cnt / denom)
            result.append({"theme": theme, "pct": pct, "count": cnt})
        return result, denom

    driver_themes, driver_neg_n = theme_counts(driver_neg, "driver")
    rider_themes, rider_neg_n = theme_counts(rider_neg, "rider")

    # Safety escalations
    safety_rows = []
    for r in analyzed_rows:
        if r["sentiment"] not in ("Negative", "Mixed"):
            continue
        text = r.get("text_for_analysis") or r.get("TEXT_FOR_ANALYSIS") or ""
        city = r.get("CITY") or r.get("city") or ""
        for cat, patterns in SAFETY_PATTERNS.items():
            for pat in patterns:
                if re.search(pat, text.lower()):
                    # anonymize: city + generic description, no raw comment
                    summary = re.sub(r"[-_]+", " ", text[:100]).strip()
                    summary = re.sub(r"\s+", " ", summary)
                    if len(text) > 100:
                        summary += "..."
                    safety_rows.append({
                        "category": cat,
                        "summary": summary,
                        "city": city,
                        "guid": r.get("GUID") or r.get("guid"),
                    })
                    break

    # Dedupe by guid+category
    seen = set()
    unique_safety = []
    for s in safety_rows:
        k = (s["guid"], s["category"])
        if k not in seen:
            seen.add(k)
            unique_safety.append(s)

    output = {
        "report_date": REPORT_DATE,
        "total_comments": total_comments,
        "sample_count": sample_count,
        "volumes": {f"{k[0]}|{k[1]}": v for k, v in volumes.items()},
        "cells": {f"{k[0]}|{k[1]}": dict(v) for k, v in cells.items()},
        "pct_neg": {f"{k[0]}|{k[1]}": pct_neg(k) for k in cells},
        "driver_themes": driver_themes,
        "rider_themes": rider_themes,
        "driver_neg_n": driver_neg_n,
        "rider_neg_n": rider_neg_n,
        "safety": unique_safety[:10],
        "safety_count": len(unique_safety),
    }
    print(json.dumps(output, indent=2, default=str))


if __name__ == "__main__":
    main()
