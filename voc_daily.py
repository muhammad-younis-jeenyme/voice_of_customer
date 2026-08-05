#!/usr/bin/env python3
"""Voice of Customer daily digest — Snowflake pull + sentiment/theme analysis."""

import json
import os
import re
from collections import Counter, defaultdict
from datetime import datetime

import snowflake.connector

REPORT_DATE = os.environ.get("VOC_REPORT_DATE", "2026-08-04")
SAMPLE_LIMIT = int(os.environ.get("VOC_SAMPLE_LIMIT", "100"))
START_TS = f"{REPORT_DATE} 00:00:00"

ZENDESK_NEUTRAL_CATS = {
    "city update", "work permit", "lost item", "lost items", "found item",
    "general inquiry", "partnership", "media", "recruitment", "want to update my operating city",
    "phone number", "i lost an item", "passenger lost an item",
}
VALID_SENTIMENTS = {"Positive", "Neutral", "Negative", "Mixed", "Unclear"}

# Zendesk TICKET_CATEGORY → canonical theme (persona-specific keys)
DRIVER_CAT_THEMES = {
    "i did not get paid the cancellation fees": "Cancellation fees not received / unfair fines",
    "amount has been deducted from my balance": "Payment not credited / withdrawals",
    "ride value is less than actual": "Earnings & fares (low pay per km, weak offers)",
    "ride value is higher than actual": "Fare/charge accuracy",
    "report an issue with the passenger's behaviour": "Passenger conduct",
    "passenger did not pay trip fare": "Passenger didn't pay (cash)",
    "funds have not been transferred to my wallet": "Payment not credited / withdrawals",
    "i did not receive credit card payment amount in wallet": "Payment not credited / withdrawals",
    "i did not win although the target was achieved": "Earnings & fares (low pay per km, weak offers)",
    "intercity surcharge was not added": "Earnings & fares (low pay per km, weak offers)",
    "bad rating": "Passenger conduct",
}

RIDER_CAT_THEMES = {
    "price was way higher than the estimate": "Overcharging / fare disputes",
    "driver cancelled and i was charged with cancellation fees": "Cancellation (unfair fees, driver-initiated)",
    "driver asked to cancel and i was charged with cancellation fees": "Cancellation (unfair fees, driver-initiated)",
    "i cancelled because driver did not show up at the pickup point": "No-show / wrong pickup",
    "driver was not matching the profile in the app": "Wrong driver / car vs app",
    "i cancelled because driver/car did not match the app": "Wrong driver / car vs app",
    "i did not receive discount amount in wallet": "Payment & charging errors",
    "i entered a wrong amount on payment screen": "Payment & charging errors",
    "bad rating": "Driver conduct",
    "report a technical issue": "App / technical issues",
}

DRIVER_KW_THEMES = {
    "Cancellation fees not received / unfair fines": [r"cancel", r"fine", r"penalt", r"no.?show fee", r"compensation", r"deduct.*fee", r"الغاء"],
    "Earnings & fares (low pay per km, weak offers)": [r"low (fare|pay|price|earning)", r"earn", r"commission", r"offer", r"per km", r"bonus", r"incentive", r"reward", r"target", r"weak", r"اقل"],
    "Passenger conduct": [r"rude", r"abusive", r"aggressive", r"disrespect", r"insult", r"passenger", r"rider.*(rude|bad)", r"smok", r"drunk|intoxicat", r"سلوك", r"راكب"],
    "Passenger didn't pay (cash)": [r"didn.?t pay", r"not pay", r"unpaid", r"cash", r"refus.*pay", r"without pay", r"لم يدفع", r"ما دفع"],
    "Fare/charge accuracy": [r"fare.*(wrong|incorrect)", r"charge.*(wrong|incorrect)", r"route", r"waiting", r"distance", r"recalculat", r"قيمة"],
    "Payment not credited / withdrawals": [r"wallet", r"withdraw", r"transfer", r"not credit", r"not receiv", r"balance", r"payout", r"deposit", r"محفظ"],
    "App / technical issues": [r"app (crash|bug|freeze|error)", r"technical", r"gps", r"location", r"login"],
}

RIDER_KW_THEMES = {
    "Overcharging / fare disputes": [
        r"overcharg", r"high (fare|price|cost|rate)", r"extra (money|cash|charge)", r"more than", r"quote", r"estimate",
        r"expensive", r"inflat", r"higher than", r"took (more|original)", r"too high",
        r"اكثر من", r"زاد", r"سعر غير", r"المبلغ زاد", r"حرام", r"سحب.*فلوس",
    ],
    "Driver conduct": [
        r"rude", r"abusive", r"aggressive", r"disrespect", r"insult", r"unprofessional",
        r"bad (manner|behavior|attitude|car)", r"impolite", r"yell", r"shout", r"very bad",
        r"قلة ادب", r"سلوك", r"سيء", r"سي جدا", r"عديم", r"صرخ", r"عدوان", r"وسخ", r"مو محترم",
        r"رفض المشوار", r"مخالف",
    ],
    "Wrong driver / car vs app": [
        r"wrong (driver|car|vehicle|plate)", r"different (driver|car|vehicle)", r"not match", r"mismatch",
        r"another (driver|car|person)", r"profile", r"not same car", r"didn.?t come in the same",
        r"مو نفس", r"مختلف", r"شخص تاني", r"غير سعودي", r"باكستاني",
    ],
    "Route manipulation / slow driving": [
        r"long route", r"detour", r"manipulat", r"slow", r"delay", r"extra (km|kilometer|distance)",
        r"went around", r"wrong (entrance|route|way)", r"drives? so much slowly",
        r"اطول مسار", r"يطول الطريق", r"الطريق", r"مسار", r"مطب",
    ],
    "No-show / wrong pickup": [
        r"no.?show", r"didn.?t (come|arrive|show)", r"waited", r"wrong pickup", r"never came",
        r"late arrival", r"delayed pick", r"لم يأت", r"لم يصل", r"ماركبت", r"ما اركب", r"لم اركب",
    ],
    "Cancellation (unfair fees, driver-initiated)": [
        r"cancel", r"cancellation", r"cancelled", r"driver.*cancel", r"unfair.*fee", r"charged.*cancel",
        r"خصم", r"الغاء", r"سوي الغاء",
    ],
    "Payment & charging errors": [
        r"double charg", r"charged twice", r"refund", r"wallet", r"payment.*(error|fail)", r"deduct",
        r"no change", r"not change", r"محفظ", r"رجعلى",
    ],
    "Vehicle smell / cleanliness": [
        r"smell", r"dirty", r"clean", r"hygien", r"odor", r"stink", r"cigarette", r"smoke",
        r"ريح", r"عفن", r"نظاف", r"دخان",
    ],
    "App / technical issues": [r"app (crash|bug|freeze|error)", r"technical", r"gps", r"location", r"login"],
}

SAFETY_PATTERNS = {
    "Harassment": [r"harass", r"assault", r"touch", r"held.*hand", r"threaten", r"stalk", r"explicit", r"sexual", r"whatsapp", r"snapchat", r"flirt", r"inappropriat"],
    "Reckless driving": [r"reckless", r"speed", r"dangerous", r"accident", r"crash", r"near.?miss", r"intoxicat", r"drunk", r"impair", r"sleep.*drive", r"phone.*driv"],
    "Identity mismatch": [r"wrong (driver|car|vehicle)", r"different (driver|car|vehicle)", r"not match", r"mismatch", r"not matching the profile"],
}


def connect():
    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PAT"],
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        database=os.environ.get("SNOWFLAKE_DATABASE", "JEENY_PROD"),
        role=os.environ.get("SNOWFLAKE_ROLE"),
    )
    cur = conn.cursor()
    cur.execute("USE WAREHOUSE COMPUTE_WH")
    return conn, cur


def classify_sentiment(row):
    existing = row.get("OVERALL_SENTIMENT")
    if existing and str(existing).strip() in VALID_SENTIMENTS:
        return str(existing).strip()

    text = (row.get("TEXT_FOR_ANALYSIS") or "").lower()
    source = row.get("FEEDBACK_SOURCE", "")
    stars = row.get("RATING_STARS")
    ticket_cat = (row.get("TICKET_CATEGORY") or "").lower()

    if source == "Zendesk":
        if any(n in ticket_cat for n in ZENDESK_NEUTRAL_CATS):
            return "Neutral"
        return "Negative"

    pos_kw = r"\b(good|great|excellent|thank|thanks|perfect|amazing|nice|best|love|happy|polite|professional|clean|fast|helpful)\b"
    neg_kw = r"\b(bad|terrible|awful|worst|horrible|disgust|angry|frustrat|complain|unfair|cheat|scam|never|hate|poor|disappoint)\b"
    pos = bool(re.search(pos_kw, text))
    neg = bool(re.search(neg_kw, text))

    if stars is not None:
        try:
            s = float(stars)
            if s <= 2:
                neg = True
            elif s >= 4:
                pos = True
            elif s == 3:
                return "Mixed" if pos and neg else "Neutral"
        except (TypeError, ValueError):
            pass

    if pos and neg:
        return "Mixed"
    if neg:
        return "Negative"
    if pos:
        return "Positive"
    return "Unclear" if len(text.strip()) < 10 else "Neutral"


def themes_for_row(row, persona):
    text = " ".join(
        filter(None, [row.get("TEXT_FOR_ANALYSIS"), row.get("RATINGS_COMMENTS")])
    )
    cat = (row.get("TICKET_CATEGORY") or "").lower()
    found = set()

    cat_map = DRIVER_CAT_THEMES if persona == "driver" else RIDER_CAT_THEMES
    kw_map = DRIVER_KW_THEMES if persona == "driver" else RIDER_KW_THEMES

    for key, theme in cat_map.items():
        if key in cat:
            found.add(theme)

    for theme, pats in kw_map.items():
        if any(re.search(p, text, re.I) for p in pats):
            found.add(theme)

    return found


def pct(n, denom):
    if denom == 0:
        return "n/a"
    p = round(100 * n / denom)
    return f"~{p}%"


def main():
    conn, cur = connect()

    cur.execute(
        """
        SELECT COUNT(*) FROM jeeny_prod.general.feedbackevents
        WHERE TIMESTAMP >= %s AND TIMESTAMP < DATEADD(day, 1, %s::DATE)
          AND RATINGS_COMMENTS IS NOT NULL AND TRIM(RATINGS_COMMENTS) <> ''
          AND LENGTH(TRIM(RATINGS_COMMENTS)) >= 3
        """,
        (START_TS, REPORT_DATE),
    )
    total_comments = cur.fetchone()[0]

    cur.execute(
        f"""
        WITH base AS (
          SELECT GUID, FEEDBACK_SOURCE,
            CASE WHEN USER_TYPE = 'driver' THEN 'driver'
                 WHEN USER_TYPE IN ('passenger','customer') THEN 'rider'
                 ELSE 'other' END AS persona,
            RATINGS_COMMENTS,
            COALESCE(NULLIF(TRIM(EN_TRANSLATION),''), RATINGS_COMMENTS) AS text_for_analysis,
            OVERALL_SENTIMENT, RATING_STARS, CITY, COUNTRY, TICKET_CATEGORY,
            CATEGORY, URGENCY, TIMESTAMP,
            ROW_NUMBER() OVER (
              PARTITION BY FEEDBACK_SOURCE,
                CASE WHEN USER_TYPE='driver' THEN 'driver'
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
        """,
        (START_TS, REPORT_DATE),
    )
    cols = [d[0] for d in cur.description]
    rows = [dict(zip(cols, r)) for r in cur.fetchall()]

    cur.execute(
        """
        WITH base AS (
          SELECT FEEDBACK_SOURCE,
            CASE WHEN USER_TYPE='driver' THEN 'driver'
                 WHEN USER_TYPE IN ('passenger','customer') THEN 'rider'
                 ELSE 'other' END AS persona
          FROM jeeny_prod.general.feedbackevents
          WHERE TIMESTAMP >= %s AND TIMESTAMP < DATEADD(day, 1, %s::DATE)
            AND RATINGS_COMMENTS IS NOT NULL AND TRIM(RATINGS_COMMENTS) <> ''
            AND LENGTH(TRIM(RATINGS_COMMENTS)) >= 3
        )
        SELECT FEEDBACK_SOURCE, persona, COUNT(*) FROM base
        WHERE persona IN ('driver','rider') GROUP BY 1,2
        """,
        (START_TS, REPORT_DATE),
    )
    volumes = {(s, p): v for s, p, v in cur.fetchall()}
    conn.close()

    for r in rows:
        r["SENTIMENT"] = classify_sentiment(r)

    sample_count = len(rows)

    cell_stats = defaultdict(lambda: {"total": 0, "neg": 0})
    for r in rows:
        key = (r["FEEDBACK_SOURCE"], r["PERSONA"])
        cell_stats[key]["total"] += 1
        if r["SENTIMENT"] in ("Negative", "Mixed"):
            cell_stats[key]["neg"] += 1

    def cell_neg_pct(source, persona):
        st = cell_stats.get((source, persona), {"total": 0, "neg": 0})
        return pct(st["neg"], st["total"])

    def vol(source, persona):
        return volumes.get((source, persona), 0)

    app_store_driver = vol("Google Store", "driver") + vol("Apple Store", "driver")
    app_store_rider = vol("Google Store", "rider") + vol("Apple Store", "rider")

    def extract_themes(persona):
        persona_rows = [r for r in rows if r["PERSONA"] == persona]
        neg_rows = [r for r in persona_rows if r["SENTIMENT"] in ("Negative", "Mixed")]
        counts = Counter()
        for r in neg_rows:
            for theme in themes_for_row(r, persona):
                counts[theme] += 1
        denom = len(persona_rows) or 1
        return [(t, round(100 * c / denom)) for t, c in counts.most_common()], len(neg_rows)

    driver_themes, _ = extract_themes("driver")
    rider_themes, _ = extract_themes("rider")

    safety_hits = []
    for r in rows:
        if r["SENTIMENT"] not in ("Negative", "Mixed"):
            continue
        text = r.get("TEXT_FOR_ANALYSIS") or r.get("RATINGS_COMMENTS") or ""
        city = r.get("CITY") or "Unknown city"
        for category, patterns in SAFETY_PATTERNS.items():
            if any(re.search(p, text, re.I) for p in patterns):
                safety_hits.append({"category": category, "city": city})
                break

    seen_cat = set()
    safety_examples = []
    for h in safety_hits:
        if h["category"] in seen_cat:
            continue
        seen_cat.add(h["category"])
        city = h["city"]
        templates = {
            "Harassment": f"{city} rider reports harassment or inappropriate contact during or after the trip.",
            "Reckless driving": f"{city} rider flags reckless or impaired driving — described the driver as dangerous.",
            "Identity mismatch": f"{city} rider reports a different vehicle and driver than shown in-app.",
        }
        safety_examples.append({"category": h["category"], "line": templates[h["category"]]})
        if len(safety_examples) >= 3:
            break

    emoji_map = {
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
    safety_emoji = {"Harassment": "🆘", "Reckless driving": "🏎️", "Identity mismatch": "🚙"}

    def theme_lines(themes, top_n=6):
        return "\n".join(
            f"* {emoji_map.get(t, '•')} {t} — **~{p}%**" for t, p in themes[:top_n]
        )

    def theme_pct(themes, name, default=0):
        for t, p in themes:
            if name in t:
                return p
        return default

    day_name = datetime.strptime(REPORT_DATE, "%Y-%m-%d").strftime("%a %d %b %Y")

    p1 = (
        f"1. **Fare integrity for riders** — overcharging / fare disputes are ~{theme_pct(rider_themes, 'Overcharging')}% of the rider sample, "
        f"with no-show friction another ~{theme_pct(rider_themes, 'No-show')}%; audit final-fare recalculation and pickup SLA enforcement."
    )
    p2 = (
        f"2. **Driver payout & cash collection** — unpaid cash fares (~{theme_pct(driver_themes, 'cash')}%), "
        f"wallet non-credits (~{theme_pct(driver_themes, 'Payment not credited')}%), "
        f"and missing cancellation fees (~{theme_pct(driver_themes, 'Cancellation')}%) dominate driver negatives; "
        f"tighten auto-compensation and wallet credit SLAs."
    )
    p3 = (
        f"3. **Identity mismatch enforcement** — ~{theme_pct(rider_themes, 'Wrong driver')}% of rider negatives report wrong driver/car vs app; "
        f"pair with safety flags and tighten verification before trip start."
    )

    safety_block = (
        "\n".join(
            f'* {safety_emoji.get(e["category"], "⚠️")} **{e["category"]}** — {e["line"]}'
            for e in safety_examples
        )
        if safety_examples
        else "* No critical safety escalations flagged in sample."
    )

    digest = f"""## 📊 {day_name}

{total_comments:,} comments · stratified sample ~{sample_count} analyzed (up to 100 per source×user-type cell). Zendesk = support tickets (complaint-only by nature, so its %neg is always very high); ride_rating is the truer sentiment gauge.

|Source|Driver (vol · %neg)|Rider (vol · %neg)|
|  ---  |  ---  |  ---  |
|Zendesk|{vol('Zendesk','driver'):,} · {cell_neg_pct('Zendesk','driver')}|{vol('Zendesk','rider'):,} · {cell_neg_pct('Zendesk','rider')}|
|ride_rating|{vol('ride_rating','driver'):,} · {cell_neg_pct('ride_rating','driver')}|{vol('ride_rating','rider'):,} · {cell_neg_pct('ride_rating','rider')}|
|in_app|{vol('in_app','driver'):,} · {cell_neg_pct('in_app','driver')}|—|
|App stores|{app_store_driver:,} · n/a|{app_store_rider:,} · n/a|


### 🏎️ Drivers — top issues

*(% of driver sample)*

{theme_lines(driver_themes)}

### 🧍 Riders — top issues

*(% of rider sample)*

{theme_lines(rider_themes, top_n=7)}

### ⚠️ Safety escalations (~{len(safety_hits)} flagged)

{safety_block}

### 🎯 Priorities

{p1}
{p2}
{p3}

---
"""

    result = {
        "report_date": REPORT_DATE,
        "day_name": day_name,
        "total_comments": total_comments,
        "sample_count": sample_count,
        "digest": digest,
        "priorities": [
            "Fare integrity for riders",
            "Driver payout & cash collection",
            "Identity mismatch enforcement",
        ],
        "driver_themes": driver_themes[:6],
        "rider_themes": rider_themes[:7],
        "safety_count": len(safety_hits),
    }

    out = f"/tmp/voc_digest_{REPORT_DATE}.json"
    with open(out, "w") as f:
        json.dump(result, f, indent=2)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
