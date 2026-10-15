#!/usr/bin/env python3
"""Analyze VoC sample: sentiment, themes, safety, priorities, digest."""
import json
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime

VALID_SENTIMENTS = {"Positive", "Neutral", "Negative", "Mixed"}

# Theme keyword patterns (canonical label -> regex fragments)
DRIVER_THEMES = {
    "Passenger conduct": r"rude|abuse|insult|bad behavior|misbehav|disrespect|scream|fight|hit me|spit|drunk passenger|passenger.*(rude|bad|conduct)",
    "Passenger didn't pay (cash)": r"didn.?t pay|not pay|no pay|without pay|refus.*pay|cash.*(not|no|without)|unpaid|did not pay",
    "Earnings & fares (low pay per km, weak offers)": r"low (pay|fare|earning|income|price)|earn.*low|pay.*low|offer.*(low|bad|weak)|commission|bonus|incentive|km.*pay|per km|surge|income",
    "Cancellation fees not received / unfair fines": r"cancel.*(fee|fine|penalty|charge)|fine.*(unfair|wrong)|penalty|deduct|suspended|blocked|ban(ned)?|wrong.*cancel",
    "Fare/charge accuracy": r"wrong (fare|charge|amount|price|fee)|fare.*(wrong|incorrect|error)|charge.*(wrong|incorrect|error)|overcharg.*driver|deduct",
    "Payment not credited / withdrawals": r"withdraw|wallet|not credit|payment.*(not|missing|delay)|balance|transfer|payout|didn.?t receive.*money",
}

RIDER_THEMES = {
    "Overcharging / fare disputes": r"overcharg|extra (charge|fee|amount)|fare.*(high|wrong|double|increase|expensive)|charged.*more|too much|price.*(high|wrong)",
    "Driver conduct": r"rude|abuse|insult|bad (driver|behavior|attitude)|disrespect|scream|yell|unprofessional|aggressive driver",
    "Wrong driver / car vs app": r"wrong (driver|car|vehicle|plate|taxi)|different (car|driver|vehicle)|not the (car|driver)|plate.*(wrong|different)|identity",
    "Route manipulation / slow driving": r"long route|longer route|detour|route.*(long|wrong|manipul)|slow driv|took long|extra km|circled|went around",
    "No-show / wrong pickup": r"no show|didn.?t (come|arrive|show)|wrong pickup|waited.*(long|forever)|never came|late pickup|pickup.*wrong",
    "Cancellation (unfair fees, driver-initiated)": r"cancel.*(fee|charge|unfair)|driver.*cancel|cancelled.*(ride|trip)|cancellation fee",
    "Payment & charging errors": r"payment.*(fail|error|issue|problem)|charged twice|double charg|card.*(declin|fail)|wallet.*(issue|problem)|refund",
    "Vehicle smell / cleanliness": r"smell|dirty|clean|hygien|filthy|stink|odor|unclean",
}

SAFETY_PATTERNS = {
    "Harassment": r"harass|assault|molest|held (my )?hand|threaten|stalk|inappropriate touch|sexual|insulted.*(me|passenger|driver)",
    "Reckless driving": r"reckless|speed(ing)?|dangerous driv|accident|crash|injur|child.*injur|hit (me|us|car)",
    "Identity mismatch": r"wrong (driver|car|vehicle|person).*safety|kidnap|unsafe.*(driver|car)|not the registered|fake driver|different.*(scary|unsafe)",
}

ZENDESK_NEUTRAL_CATS = {
    "city update", "work permit", "lost item", "lost and found", "general inquiry",
    "information", "account update", "document", "registration",
    "want to update my operating city", "updating my personal photo", "personal photo",
    "phone number", "stc pay",
}

TICKET_CATEGORY_THEME = {
    "I did not get paid the cancellation fees": "Cancellation fees not received / unfair fines",
    "Amount has been deducted from my balance": "Cancellation fees not received / unfair fines",
    "Report an issue with the passenger's behaviour": "Passenger conduct",
    "Passenger did not pay trip fare in full / part of the trip fare": "Passenger didn't pay (cash)",
    "Ride value is less than actual": "Earnings & fares (low pay per km, weak offers)",
    "I did not win although the target was achieved": "Earnings & fares (low pay per km, weak offers)",
    "Funds have not been transferred to my wallet": "Payment not credited / withdrawals",
    "I did not receive credit card payment amount in wallet": "Payment not credited / withdrawals",
    "I did not receive discount amount in wallet": "Payment not credited / withdrawals",
    "Bad Rating": "Driver conduct",
    "Price was way higher than the estimate": "Overcharging / fare disputes",
    "Driver overcharged me for the trip/took extra money from me": "Overcharging / fare disputes",
    "Driver took a long route/ Driver caused the charges to increase": "Route manipulation / slow driving",
    "Driver made an unscheduled stop": "Route manipulation / slow driving",
    "Driver was not matching the profile in the app": "Wrong driver / car vs app",
    "I cancelled because Driver/Car did not match the app": "Wrong driver / car vs app",
    "I cancelled because driver did not show up at the pickup point": "No-show / wrong pickup",
    "Driver asked to cancel and I was charged with cancellation fees": "Cancellation (unfair fees, driver-initiated)",
    "Driver started/ended the ride before I got in the car": "Driver conduct",
    "I was charged multiple times for the trip": "Payment & charging errors",
    "Report a Technical Issue": "Payment & charging errors",
    "Report a technical issue": "Payment & charging errors",
}

SAFETY_CATEGORIES = {
    "I had a serious incident": "Harassment",
}


def normalize_source(src: str) -> str:
    if src in ("Google Store", "Apple Store"):
        return "App stores"
    return src


def classify_sentiment(row: dict) -> str:
    existing = row.get("overall_sentiment")
    if existing and str(existing).strip() in VALID_SENTIMENTS:
        return str(existing).strip()

    text = (row.get("text_for_analysis") or "").lower()
    src = row.get("feedback_source", "")
    stars = row.get("rating_stars")

    if src == "Zendesk":
        cat = (row.get("ticket_category") or "").lower()
        if any(n in cat for n in ZENDESK_NEUTRAL_CATS):
            return "Neutral"
        return "Negative"

    pos = len(re.findall(
        r"good|great|excellent|thank|love|best|perfect|amazing|nice|happy|satisfied|5 star|five star",
        text,
    ))
    neg = len(re.findall(
        r"bad|terrible|worst|hate|awful|horrible|disappoint|frustrat|complain|problem|issue|scam|cheat|"
        r"rude|slow|late|wrong|overcharg|didn.?t|not work|poor|unfair|1 star|one star|2 star|two star",
        text,
    ))

    if pos and neg:
        sentiment = "Mixed"
    elif neg > pos:
        sentiment = "Negative"
    elif pos > neg:
        sentiment = "Positive"
    elif stars is not None:
        try:
            s = float(stars)
            if s <= 2:
                sentiment = "Negative"
            elif s == 3:
                sentiment = "Mixed" if neg else "Neutral"
            elif s >= 4:
                sentiment = "Positive"
            else:
                sentiment = "Neutral"
        except (TypeError, ValueError):
            sentiment = "Unclear"
    else:
        sentiment = "Unclear"

    return sentiment


def match_themes(text: str, theme_map: dict) -> set:
    text = text.lower()
    matched = set()
    for label, pattern in theme_map.items():
        if re.search(pattern, text, re.I):
            matched.add(label)
    return matched


def pct(n, d, sample_size):
    if d == 0:
        return "n/a"
    p = round(100 * n / d)
    return f"~{p}%" if sample_size < 30 else f"{p}%"


def pct_neg_label(neg, analyzed, sample_size):
    if analyzed == 0:
        return "n/a"
    p = round(100 * neg / analyzed)
    return f"~{p}%"


def anonymize(text: str, max_len: int = 120) -> str:
    text = re.sub(r"\b\d{8,}\b", "[redacted]", text)
    text = re.sub(r"\b[\w.-]+@[\w.-]+\.\w+\b", "[email]", text)
    text = re.sub(r"\b[A-Z][a-z]+ [A-Z][a-z]+\b", "[person]", text)
    text = text.strip()
    if len(text) > max_len:
        text = text[: max_len - 3].rsplit(" ", 1)[0] + "..."
    return text or "Reported safety concern"


def build_volume_table(volume_rows, sentiments_by_cell):
    vol = defaultdict(lambda: defaultdict(int))
    for r in volume_rows:
        src = normalize_source(r["feedback_source"])
        vol[src][r["persona"]] += r["volume"]

    def cell(src, persona):
        v = vol[src].get(persona, 0)
        s = sentiments_by_cell.get((src, persona), {})
        analyzed = s.get("analyzed", 0)
        neg = s.get("negative", 0)
        sample_n = s.get("sample", 0)
        if src == "App stores" or v == 0:
            pct = "n/a"
        else:
            pct = pct_neg_label(neg, analyzed, sample_n)
        return v, pct

    rows = []
    for src in ["Zendesk", "ride_rating", "in_app", "App stores"]:
        d_v, d_p = cell(src, "driver")
        r_v, r_p = cell(src, "rider")
        if src == "in_app":
            r_v, r_p = "—", "—"
        rows.append((src, d_v, d_p, r_v, r_p))
    return rows, vol


def themes_for_row(row: dict, theme_map: dict) -> set:
    text = row.get("text_for_analysis") or ""
    matched = match_themes(text, theme_map)
    cat = (row.get("ticket_category") or "").strip()
    if cat in TICKET_CATEGORY_THEME:
        matched.add(TICKET_CATEGORY_THEME[cat])
    return matched


def top_themes(rows, persona, theme_map, emoji_map):
    neg_mixed = [r for r in rows if r["persona"] == persona and r["sentiment"] in ("Negative", "Mixed")]
    denom = len([r for r in rows if r["persona"] == persona])
    counts = Counter()
    for r in neg_mixed:
        for t in themes_for_row(r, theme_map):
            counts[t] += 1

    items = []
    for theme, count in counts.most_common(7):
        p = round(100 * count / denom) if denom else 0
        emoji = emoji_map.get(theme, "•")
        items.append({"theme": theme, "pct": p, "emoji": emoji, "count": count})
    return items[:6 if persona == "driver" else 7], denom


EMOJI_MAP = {
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


def safety_scan(rows):
    flagged = []
    seen = set()
    for r in rows:
        if r["sentiment"] not in ("Negative", "Mixed"):
            continue
        text = r.get("text_for_analysis") or ""
        cat = (r.get("ticket_category") or "").strip()
        category = None
        if cat in SAFETY_CATEGORIES:
            category = SAFETY_CATEGORIES[cat]
        else:
            for label, pattern in SAFETY_PATTERNS.items():
                if re.search(pattern, text, re.I):
                    category = label
                    break
        if category and r["guid"] not in seen:
            seen.add(r["guid"])
            summary = anonymize(text) if len(text.strip()) > 5 else f"Support ticket: {cat or category}"
            flagged.append({
                "category": category,
                "summary": summary,
                "persona": r["persona"],
            })
    return flagged


def build_priorities(driver_themes, rider_themes, safety):
    priorities = []

    def add(title, evidence, action):
        priorities.append({"title": title, "evidence": evidence, "action": action})

    if rider_themes:
        t = rider_themes[0]
        if "Overcharging" in t["theme"] or "Route" in t["theme"]:
            add(
                "Fare integrity for riders",
                f"{t['theme']} + related fare issues ≈ {t['pct']}% of rider sample",
                "Audit final-fare recalculation, estimate accuracy, and route-deviation detection.",
            )
        elif "Driver conduct" in t["theme"]:
            add(
                "Driver conduct & rating quality",
                f"Driver conduct / bad ratings ≈ {t['pct']}% of rider sample",
                "Review low-rating follow-up, driver coaching, and repeat-offender enforcement.",
            )
        else:
            add(t["theme"], f"{t['theme']} ≈ {t['pct']}% of rider sample", "Prioritize product fix and support playbook update.")

    if driver_themes:
        t = driver_themes[0]
        if "Cancellation" in t["theme"]:
            add(
                "Driver payout reliability",
                f"{t['theme']} ≈ {t['pct']}% of driver sample",
                "Review cancellation-fee crediting rules and wallet transfer SLAs.",
            )
        elif "Earnings" in t["theme"]:
            add(
                "Driver earnings & offer quality",
                f"{t['theme']} ≈ {t['pct']}% of driver sample",
                "Recalibrate per-km pricing, surge visibility, and incentive targets.",
            )
        else:
            add(t["theme"], f"{t['theme']} ≈ {t['pct']}% of driver sample", "Address top driver pain point with ops + product fix.")

    if len(priorities) < 3:
        for t in rider_themes[1:]:
            if len(priorities) >= 3:
                break
            if "Overcharging" in t["theme"] or "Route" in t["theme"]:
                add(
                    "Fare integrity for riders",
                    f"{t['theme']} ≈ {t['pct']}% of rider sample",
                    "Audit final-fare recalculation and route-deviation detection.",
                )
            elif "Payment" in t["theme"]:
                add(
                    "Payment & wallet reliability",
                    f"{t['theme']} ≈ {t['pct']}% of rider sample",
                    "Fix duplicate charges and wallet credit delays.",
                )

    if len(priorities) < 3 and safety:
        add(
            "Safety escalation response",
            f"{len(safety)} safety-related tickets flagged in sample",
            "Fast-track trust & safety review queue and tighten serious-incident triage.",
        )
    return priorities[:3]


def compose_digest(data, analysis):
    rd = data["report_date"]
    dt = datetime.strptime(rd, "%Y-%m-%d")
    day_name = dt.strftime("%a %d %b %Y")

    lines = [
        f"## 📊 {day_name}",
        "",
        f"{data['total_comments']} comments · stratified sample ~{data['sample_count']} analyzed (up to 100 per source×user-type cell). Zendesk = support tickets (complaint-only by nature, so its %neg is always very high); ride_rating is the truer sentiment gauge.",
        "",
        "|Source|Driver (vol · %neg)|Rider (vol · %neg)|",
        "|  ---  |  ---  |  ---  |",
    ]
    for src, d_v, d_p, r_v, r_p in analysis["volume_table"]:
        if src == "in_app":
            lines.append(f"|{src}|{d_v} · {d_p}|—|")
        elif src == "App stores":
            lines.append(f"|{src}|{d_v} · n/a|{r_v} · n/a|")
        else:
            lines.append(f"|{src}|{d_v} · {d_p}|{r_v} · {r_p}|")

    lines += ["", "### 🏎️ Drivers — top issues", "", "*(% of driver sample)*", ""]
    for t in analysis["driver_themes"]:
        lines.append(f"* {t['emoji']} {t['theme']} — **~{t['pct']}%**")

    lines += ["", "### 🧍 Riders — top issues", "", "*(% of rider sample)*", ""]
    for t in analysis["rider_themes"]:
        lines.append(f"* {t['emoji']} {t['theme']} — **~{t['pct']}%**")

    safety = analysis["safety"]
    lines += ["", f"### ⚠️ Safety escalations (~{len(safety)} flagged)", ""]
    safety_emoji = {"Harassment": "🆘", "Reckless driving": "🏎️", "Identity mismatch": "🚙"}
    for s in safety[:3]:
        em = safety_emoji.get(s["category"], "🆘")
        lines.append(f"* {em} **{s['category']}** — {s['summary']}")
    if not safety:
        lines.append("* No critical safety patterns detected in sample.")

    lines += ["", "### 🎯 Priorities", ""]
    for i, p in enumerate(analysis["priorities"], 1):
        lines.append(f"{i}. **{p['title']}** — {p['evidence']}. {p['action']}")

    lines += ["", "---", ""]
    return "\n".join(lines)


def analyze(data):
    sample = data["sample"]
    for row in sample:
        row["sentiment"] = classify_sentiment(row)

    sentiments_by_cell = defaultdict(lambda: {"negative": 0, "analyzed": 0, "sample": 0})
    for row in sample:
        src = normalize_source(row["feedback_source"])
        key = (src, row["persona"])
        sentiments_by_cell[key]["sample"] += 1
        if row["sentiment"] == "Unclear":
            continue
        sentiments_by_cell[key]["analyzed"] += 1
        if row["sentiment"] in ("Negative", "Mixed"):
            sentiments_by_cell[key]["negative"] += 1

    volume_table, _ = build_volume_table(data["volume"], sentiments_by_cell)
    driver_themes, _ = top_themes(sample, "driver", DRIVER_THEMES, EMOJI_MAP)
    rider_themes, _ = top_themes(sample, "rider", RIDER_THEMES, EMOJI_MAP)
    safety = safety_scan(sample)
    priorities = build_priorities(driver_themes, rider_themes, safety)
    digest = compose_digest(data, {
        "volume_table": volume_table,
        "driver_themes": driver_themes,
        "rider_themes": rider_themes,
        "safety": safety,
        "priorities": priorities,
    })

    return {
        "sentiments_by_cell": {f"{k[0]}|{k[1]}": v for k, v in sentiments_by_cell.items()},
        "driver_themes": driver_themes,
        "rider_themes": rider_themes,
        "safety": safety,
        "priorities": priorities,
        "digest": digest,
    }


def main():
    data = json.load(sys.stdin)
    result = analyze(data)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
