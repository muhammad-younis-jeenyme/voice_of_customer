#!/usr/bin/env python3
"""VoC daily sentiment, theme, and digest generation."""
import json
import re
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

VALID_SENTIMENTS = {"Positive", "Neutral", "Negative", "Mixed", "Unclear"}

ZENDESK_NEUTRAL_CATEGORIES = {
    "city update", "work permit", "lost item", "lost items", "general inquiry",
    "account update", "document", "registration", "vehicle update", "profile update",
    "information", "inquiry", "question", "feedback positive", "compliment",
}

POSITIVE_PATTERNS = re.compile(
    r"\b(excellent|great|good|amazing|wonderful|thank|thanks|appreciate|"
    r"professional|polite|friendly|helpful|best|love|perfect|nice|smooth|"
    r"quick|fast|safe|clean|recommend|happy|satisfied|awesome|fantastic)\b",
    re.I,
)
NEGATIVE_PATTERNS = re.compile(
    r"\b(bad|terrible|awful|worst|horrible|rude|disgusting|fraud|scam|cheat|"
    r"overcharg|steal|stole|rob|complain|unfair|angry|frustrat|disappoint|"
    r"hate|never again|unacceptable|abuse|harass|threat|assault|hit|insult|"
    r"reckless|danger|unsafe|slow|late|cancel|refus|didn.?t pay|no pay|"
    r"wrong (driver|car|route)|smell|dirty|lost|missing|problem|issue|"
    r"poor|pathetic|useless|waste|fine|penalty|fee)\b",
    re.I,
)

DRIVER_THEMES = {
    "Cancellation fees not received / unfair fines": [
        r"cancel.*(fee|fine|penalt|compens|not (received|paid|credit))",
        r"(fee|fine|penalt).*(cancel|not (received|paid|credit))",
        r"unfair.*(fine|fee|penalt|cancel)",
        r"didn.?t (get|receive).*(cancel|fee|fine|compens)",
        r"no (fee|fine|compens).*(cancel|passenger cancel)",
    ],
    "Earnings & fares (low pay per km, weak offers)": [
        r"(low|weak|poor).*(earn|pay|fare|offer|income|commission|rate)",
        r"(earn|pay|fare|offer|income).*(low|weak|poor|less|decreas|drop)",
        r"per km", r"kilometer", r"surge", r"bonus", r"incentive",
        r"not worth", r"minimum fare",
    ],
    "Passenger conduct": [
        r"passenger.*(rude|abusive|aggressive|drunk|smok|fight|argu|disrespect|bad behavior)",
        r"(rude|abusive|aggressive|drunk|smok|fight|argu|disrespect).*(passenger|customer|rider)",
        r"bad (passenger|customer|rider)", r"passenger (problem|issue|conduct)",
    ],
    "Passenger didn't pay (cash)": [
        r"(didn.?t|not|never|refus).*(pay|paid|payment)",
        r"no (pay|payment|cash)",
        r"cash.*(not|didn|refus|problem)",
        r"unpaid", r"run away", r"flee",
    ],
    "Fare/charge accuracy": [
        r"(wrong|incorrect|error).*(fare|charge|amount|bill|invoice)",
        r"(fare|charge|amount|bill).*(wrong|incorrect|error|mismatch)",
        r"double charg", r"overcharg",
    ],
    "Payment not credited / withdrawals": [
        r"(wallet|withdraw|credit|transfer|payout|balance).*(not|didn|delay|stuck|pending|missing)",
        r"(not|didn|delay|stuck|pending|missing).*(wallet|withdraw|credit|transfer|payout|balance)",
        r"payment not (credit|receiv|reflect)",
    ],
}

RIDER_THEMES = {
    "Overcharging / fare disputes": [
        r"overcharg", r"too (much|high|expensive)", r"charged (more|extra|twice|double)",
        r"fare.*(high|wrong|incorrect|dispute|increase)", r"rip ?off", r"expensive",
        r"bill.*(high|wrong|incorrect)", r"unfair.*(fare|price|charge)",
    ],
    "Driver conduct": [
        r"driver.*(rude|abusive|aggressive|unprofessional|disrespect|bad|angry|yell|shout|threat|harass)",
        r"(rude|abusive|aggressive|unprofessional|disrespect|bad|angry|yell|shout|threat|harass).*(driver|captain)",
        r"bad (driver|captain)", r"driver (problem|issue|conduct|behavior)",
    ],
    "Wrong driver / car vs app": [
        r"wrong (driver|car|vehicle|plate|number)",
        r"(driver|car|vehicle|plate).*(mismatch|different|not match|not same)",
        r"different (driver|car|vehicle)", r"not the (driver|car|vehicle)",
        r"fake driver", r"someone else",
    ],
    "Route manipulation / slow driving": [
        r"(long|wrong|detour|extra).*(route|way|path|distance)",
        r"route.*(long|wrong|detour|manipul|extra|deviat)",
        r"slow (driv|driver|ride)", r"took (long|forever|too much time)",
        r"traffic.*(on purpose|intentional)", r"going around",
    ],
    "No-show / wrong pickup": [
        r"no.?show", r"didn.?t (come|arrive|show|pick)",
        r"wrong (pickup|location|place|address)",
        r"waited.*(long|forever|too)", r"never came", r"driver left",
    ],
    "Cancellation (unfair fees, driver-initiated)": [
        r"cancel.*(fee|fine|charg|unfair|driver)",
        r"(fee|fine|charg).*(cancel|cancellation)",
        r"driver.*cancel", r"unfair.*cancel", r"cancelled.*(without|for no)",
    ],
    "Payment & charging errors": [
        r"(payment|card|wallet|charge).*(fail|error|problem|issue|declin|duplicate|twice)",
        r"(fail|error|problem|issue|declin|duplicate).*(payment|card|wallet|charge)",
        r"double (charg|bill|deduct)", r"charged twice",
    ],
    "Vehicle smell / cleanliness": [
        r"(smell|odor|stink|dirty|clean|hygien|filthy|trash|garbage)",
        r"(car|vehicle|seat).*(dirty|smell|clean|hygien)",
    ],
}

ZENDESK_THEME_MAP = {
    "ride value is less than actual": ("rider", "Overcharging / fare disputes"),
    "price was way higher than the estimate": ("rider", "Overcharging / fare disputes"),
    "report an issue with the passenger's behaviour": ("driver", "Passenger conduct"),
    "passenger did not pay trip fare in full / part of the trip fare": ("driver", "Passenger didn't pay (cash)"),
    "i did not get paid the cancellation fees": ("driver", "Cancellation fees not received / unfair fines"),
    "driver cancelled and i was charged with cancellation fees": ("rider", "Cancellation (unfair fees, driver-initiated)"),
    "i cancelled because driver did not show up at the pickup point": ("rider", "No-show / wrong pickup"),
    "amount has been deducted from my balance": ("driver", "Payment not credited / withdrawals"),
    "funds have not been transferred to my wallet": ("driver", "Payment not credited / withdrawals"),
    "i did not receive discount amount in wallet": ("rider", "Payment & charging errors"),
    "report an issue with the driver's behaviour": ("rider", "Driver conduct"),
    "driver took a longer route": ("rider", "Route manipulation / slow driving"),
    "driver was not the same as shown in the app": ("rider", "Wrong driver / car vs app"),
    "car was not the same as shown in the app": ("rider", "Wrong driver / car vs app"),
    "bad rating": None,
}

ARABIC_THEME_HINTS = {
    "Overcharging / fare disputes": [r"غالي", r"زيادة", r"فلوس", r"مبلغ", r"سعر", r"فاتورة", r"محفظ"],
    "Driver conduct": [r"سائق سي", r"أسلوب", r"تعامل", r"وقح", r"غير لائق", r"محترم"],
    "Wrong driver / car vs app": [r"نفس السيارة", r"مختلف", r"مو نفس", r"سيارة مختلف"],
    "Route manipulation / slow driving": [r"طريق", r"طويل", r"مسار", r"تأخير", r"بطي"],
    "Vehicle smell / cleanliness": [r"ريح", r"عفن", r"نظاف", r"مقرف", r"وسخ", r"مبلل"],
    "Cancellation (unfair fees, driver-initiated)": [r"إلغاء", r"الغاء", r"رسوم", r"غرامة"],
    "Passenger didn't pay (cash)": [r"ما دفع", r"لم يدفع", r"بدون دفع"],
    "Passenger conduct": [r"راكب", r"زبون", r"سلوك"],
}

SAFETY_PATTERNS = {
    "Harassment": [
        r"harass", r"assault", r"threat", r"insult", r"held (my )?hand",
        r"touch(ed)? inappropriat", r"stalk", r"repeated (call|message|text)",
        r"after (the )?ride", r"abuse", r"sexual",
    ],
    "Reckless driving": [
        r"reckless", r"dangerous (driv|driver)", r"speed(ing)?", r"accident",
        r"injur", r"child.*injur", r"hit (me|us|a )", r"almost (hit|crash)",
        r"unsafe", r"swerv", r"brake hard",
    ],
    "Identity mismatch": [
        r"wrong (driver|car|vehicle).*(safety|scared|afraid|unsafe|danger)",
        r"(different|fake|not the) (driver|car).*(scared|afraid|unsafe|danger|kid)",
        r"identity", r"not who.*(app|show)",
        r"مو نفس", r"مختلف.*(سائق|سيارة)",
    ],
}


def analysis_text(row):
    return (row.get("TEXT_FOR_ANALYSIS") or row.get("text_for_analysis") or row.get("RATINGS_COMMENTS") or "").strip()


def classify_sentiment(row):
    existing = row.get("OVERALL_SENTIMENT")
    if existing and str(existing).strip() in VALID_SENTIMENTS - {"Unclear"}:
        return str(existing).strip()

    text = analysis_text(row)
    source = row.get("FEEDBACK_SOURCE") or ""
    rating = row.get("RATING_STARS")
    ticket_cat = (row.get("TICKET_CATEGORY") or "").lower()

    if source == "Zendesk":
        neutral = any(n in ticket_cat for n in ZENDESK_NEUTRAL_CATEGORIES)
        if neutral:
            pos = len(POSITIVE_PATTERNS.findall(text))
            neg = len(NEGATIVE_PATTERNS.findall(text))
            if pos > neg and pos > 0:
                return "Positive"
            if neg > pos and neg > 0:
                return "Negative"
            return "Neutral"
        return "Negative"

    pos = len(POSITIVE_PATTERNS.findall(text))
    neg = len(NEGATIVE_PATTERNS.findall(text))

    if pos > 0 and neg > 0:
        sentiment = "Mixed"
    elif neg > pos and neg > 0:
        sentiment = "Negative"
    elif pos > neg and pos > 0:
        sentiment = "Positive"
    elif pos == 0 and neg == 0:
        sentiment = "Unclear"
    else:
        sentiment = "Neutral"

    if rating is not None:
        try:
            r = float(rating)
            if r <= 2 and sentiment in ("Positive", "Neutral", "Unclear"):
                sentiment = "Negative"
            elif r >= 4 and sentiment in ("Negative", "Neutral", "Unclear"):
                sentiment = "Positive"
            elif r == 3 and sentiment == "Unclear":
                sentiment = "Neutral"
        except (TypeError, ValueError):
            pass

    return sentiment


def match_themes(text, theme_map, persona=None, ticket_category=None):
    matched = []
    if ticket_category:
        cat_key = ticket_category.strip().lower()
        mapped = ZENDESK_THEME_MAP.get(cat_key)
        if mapped:
            p, theme = mapped
            if persona is None or p == persona:
                matched.append(theme)
    for label, patterns in theme_map.items():
        for pat in patterns:
            if re.search(pat, text, re.I):
                matched.append(label)
                break
    for label, patterns in ARABIC_THEME_HINTS.items():
        if label in theme_map or (persona == "driver" and label in DRIVER_THEMES) or (persona == "rider" and label in RIDER_THEMES):
            for pat in patterns:
                if re.search(pat, text):
                    matched.append(label)
                    break
    return list(dict.fromkeys(matched))


def pct(n, total, approximate=False):
    if total == 0:
        return "n/a"
    p = round(100 * n / total)
    prefix = "~" if approximate or total < 30 else ""
    return f"{prefix}{p}%"


def normalize_source(src):
    if src in ("Google Store", "Apple Store"):
        return "App stores"
    return src


def main():
    tz = ZoneInfo("Asia/Riyadh")
    report_date = (datetime.now(tz) - timedelta(days=1)).date()
    day_name = report_date.strftime("%a %d %b %Y")
    date_str = str(report_date)

    with open(f"/tmp/voc_sample_{date_str}.json") as f:
        sample = json.load(f)

    with open(f"/tmp/voc_volumes_{date_str}.json") as f:
        vol_data = json.load(f)

    total_comments = vol_data["total"]
    volumes = vol_data["volumes"]

    for r in sample:
        r["sentiment"] = classify_sentiment(r)

    # %neg per cell
    cells = defaultdict(list)
    for r in sample:
        cells[(r["FEEDBACK_SOURCE"], r["PERSONA"])].append(r)

    cell_neg = {}
    for key, rows in cells.items():
        analyzed = [r for r in rows if r["sentiment"] != "Unclear"]
        neg = sum(1 for r in analyzed if r["sentiment"] in ("Negative", "Mixed"))
        cell_neg[key] = (len(analyzed), neg)

    # Build digest table volumes
    vol_map = defaultdict(lambda: {"driver": 0, "rider": 0})
    for src, persona, vol in volumes:
        norm = normalize_source(src)
        vol_map[norm][persona] += vol

    def table_cell(src, persona):
        v = vol_map[src][persona]
        if v == 0:
            return "0 · n/a"
        if src == "App stores":
            return f"{v} · n/a"
        key = None
        for raw in ("Zendesk", "ride_rating", "in_app", "Google Store", "Apple Store"):
            if normalize_source(raw) == src:
                k = (raw, persona)
                if k in cell_neg:
                    key = k
                    break
        if key is None:
            for k in cell_neg:
                if normalize_source(k[0]) == src and k[1] == persona:
                    key = k
                    break
        if key is None:
            return f"{v} · n/a"
        analyzed, neg = cell_neg[key]
        return f"{v} · {pct(neg, analyzed, approximate=analyzed < 30)}"

    # Theme extraction
    driver_neg = [r for r in sample if r["PERSONA"] == "driver" and r["sentiment"] in ("Negative", "Mixed")]
    rider_neg = [r for r in sample if r["PERSONA"] == "rider" and r["sentiment"] in ("Negative", "Mixed")]
    driver_total = sum(1 for r in sample if r["PERSONA"] == "driver")
    rider_total = sum(1 for r in sample if r["PERSONA"] == "rider")

    def theme_counts(rows, theme_map, persona_total, persona):
        counts = Counter()
        for r in rows:
            text = analysis_text(r)
            for theme in match_themes(text, theme_map, persona=persona, ticket_category=r.get("TICKET_CATEGORY")):
                counts[theme] += 1
        ranked = []
        for theme, count in counts.most_common():
            ranked.append((theme, pct(count, persona_total, approximate=persona_total < 30)))
        return ranked

    driver_themes = theme_counts(driver_neg, DRIVER_THEMES, driver_total, "driver")
    rider_themes = theme_counts(rider_neg, RIDER_THEMES, rider_total, "rider")

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

    # Safety flags
    safety_flags = []
    for r in sample:
        if r["sentiment"] not in ("Negative", "Mixed"):
            continue
        text = analysis_text(r)
        for category, patterns in SAFETY_PATTERNS.items():
            for pat in patterns:
                if re.search(pat, text, re.I):
                    summary = text[:120].strip()
                    summary = re.sub(r"\d{7,}", "[redacted]", summary)
                    summary = re.sub(r"\b[A-Z][a-z]+ [A-Z][a-z]+\b", "[name]", summary)
                    safety_flags.append({"category": category, "summary": summary, "guid": r.get("GUID")})
                    break

    # Dedupe safety: one per category, distinct summaries
    unique_safety = []
    seen_cats = set()
    seen_summaries = set()
    for s in safety_flags:
        if s["category"] in seen_cats:
            continue
        key = s["summary"][:60]
        if key in seen_summaries:
            continue
        seen_cats.add(s["category"])
        seen_summaries.add(key)
        unique_safety.append(s)
        if len(unique_safety) >= 3:
            break

    SAFETY_EMOJI = {"Harassment": "🆘", "Reckless driving": "🏎️", "Identity mismatch": "🚙"}

    # Priorities (data-driven)
    priority_specs = []
    if rider_themes:
        t, p = rider_themes[0]
        priority_specs.append(
            (t, p, "rider",
             "Audit final-fare recalculation, estimate vs actual fare gaps, and route-deviation detection.")
        )
    if driver_themes:
        t, p = driver_themes[0]
        priority_specs.append(
            (t, p, "driver",
             "Tighten passenger conduct reporting, cash-payment enforcement, and driver support SLAs.")
        )
    if len(rider_themes) > 1:
        t, p = rider_themes[1]
        priority_specs.append(
            (t, p, "rider",
             "Improve route monitoring, ETA accuracy, and slow-driving / detour alerts.")
        )
    for t, p in driver_themes[1:]:
        if len(priority_specs) >= 3:
            break
        if not any(t == s[0] for s in priority_specs):
            priority_specs.append(
                (t, p, "driver",
                 "Review payout timing, cancellation compensation rules, and wallet transfer reliability.")
            )

    priorities = [
        f"{i+1}. **{t}** — {p} of {who} sample; prominent across ride_rating and Zendesk. {action}"
        for i, (t, p, who, action) in enumerate(priority_specs[:3])
    ]

    # Compose digest
    lines = [
        f"## 📊 {day_name}",
        "",
        f"{total_comments} comments · stratified sample ~{len(sample)} analyzed (up to 100 per source×user-type cell). Zendesk = support tickets (complaint-only by nature, so its %neg is always very high); ride_rating is the truer sentiment gauge.",
        "",
        "|Source|Driver (vol · %neg)|Rider (vol · %neg)|",
        "|  ---  |  ---  |  ---  |",
        f"|Zendesk|{table_cell('Zendesk', 'driver')}|{table_cell('Zendesk', 'rider')}|",
        f"|ride_rating|{table_cell('ride_rating', 'driver')}|{table_cell('ride_rating', 'rider')}|",
        f"|in_app|{table_cell('in_app', 'driver')}|—|",
        f"|App stores|{table_cell('App stores', 'driver')}|{table_cell('App stores', 'rider')}|",
        "",
        "### 🏎️ Drivers — top issues",
        "",
        "*(% of driver sample)*",
        "",
    ]
    for theme, p in driver_themes[:6]:
        emoji = THEME_EMOJI.get(theme, "•")
        lines.append(f"* {emoji} {theme} — **{p}**")

    lines += ["", "### 🧍 Riders — top issues", "", "*(% of rider sample)*", ""]
    for theme, p in rider_themes[:7]:
        emoji = THEME_EMOJI.get(theme, "•")
        lines.append(f"* {emoji} {theme} — **{p}**")

    lines += ["", f"### ⚠️ Safety escalations (~{len(safety_flags)} flagged)", ""]
    if unique_safety:
        for s in unique_safety:
            emoji = SAFETY_EMOJI.get(s["category"], "🆘")
            lines.append(f"* {emoji} **{s['category']}** — {s['summary']}")
    else:
        lines.append("* No critical safety escalations flagged in today's sample.")

    lines += ["", "### 🎯 Priorities", ""]
    for p in priorities[:3]:
        lines.append(p)
    lines += ["", "---", ""]

    digest = "\n".join(lines)

    result = {
        "report_date": str(report_date),
        "day_name": day_name,
        "total_comments": total_comments,
        "sample_size": len(sample),
        "digest": digest,
        "priorities": priorities[:3],
        "driver_themes": driver_themes,
        "rider_themes": rider_themes,
        "safety_count": len(safety_flags),
        "cell_neg": {f"{k[0]}|{k[1]}": v for k, v in cell_neg.items()},
    }
    with open("/tmp/voc_result.json", "w") as f:
        json.dump(result, f, indent=2)
    print(json.dumps({k: result[k] for k in result if k != "digest"}, indent=2))
    print("---DIGEST---")
    print(digest)


if __name__ == "__main__":
    main()
