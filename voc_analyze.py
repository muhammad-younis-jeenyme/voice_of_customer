#!/usr/bin/env python3
"""Analyze VoC sample: sentiment, themes, safety, priorities, digest."""
import json
import re
import sys
from collections import Counter, defaultdict

VALID_SENTIMENTS = {"Positive", "Neutral", "Negative", "Mixed"}

ZENDESK_NEUTRAL_CATS = [
    "lost an item", "lost item", "found item", "work permit", "operating city",
    "city update", "technical issue", "report a technical", "stc pay", "account",
    "registration", "document", "general inquiry",
]

ZENDESK_THEME_MAP = {
    "cancellation fee": "Cancellation fees not received / unfair fines",
    "did not get paid the cancellation": "Cancellation fees not received / unfair fines",
    "cancelled because": "Cancellation fees not received / unfair fines",
    "ride value is less": "Earnings & fares (low pay per km, weak offers)",
    "did not win although the target": "Earnings & fares (low pay per km, weak offers)",
    "earning": "Earnings & fares (low pay per km, weak offers)",
    "commission": "Earnings & fares (low pay per km, weak offers)",
    "passenger's behaviour": "Passenger conduct",
    "passenger behaviour": "Passenger conduct",
    "passenger did not pay": "Passenger didn't pay (cash)",
    "did not pay trip fare": "Passenger didn't pay (cash)",
    "deducted from my balance": "Payment not credited / withdrawals",
    "did not receive discount": "Payment not credited / withdrawals",
    "withdraw": "Payment not credited / withdrawals",
    "wallet": "Payment not credited / withdrawals",
    "price was way higher": "Overcharging / fare disputes",
    "overcharged me": "Overcharging / fare disputes",
    "extra money": "Overcharging / fare disputes",
    "driver was rude": "Driver conduct",
    "unprofessional": "Driver conduct",
    "serious incident": "Driver conduct",
    "not matching the profile": "Wrong driver / car vs app",
    "driver did not show up": "No-show / wrong pickup",
    "started/ended the ride before": "No-show / wrong pickup",
    "wrong location": "No-show / wrong pickup",
    "bad rating": None,
}

DRIVER_KEYWORDS = {
    "Cancellation fees not received / unfair fines": [r"cancel", r"fine", r"penalt", r"compensat"],
    "Earnings & fares (low pay per km, weak offers)": [r"earn", r"low pay", r"offer", r"commission", r"income", r"target"],
    "Passenger conduct": [r"passenger.*(rude|bad|behav|disrespect)", r"rude passenger"],
    "Passenger didn't pay (cash)": [r"didn.?t pay", r"no pay", r"unpaid", r"cash", r"محتال"],
    "Fare/charge accuracy": [r"wrong (fare|charge|amount)", r"overcharg", r"incorrect"],
    "Payment not credited / withdrawals": [r"withdraw", r"wallet", r"balance", r"deduct", r"payout"],
}

RIDER_KEYWORDS = {
    "Overcharging / fare disputes": [r"overcharg", r"too (much|expensive|high)", r"higher than", r"extra money", r"rip.?off"],
    "Driver conduct": [r"driver.*(rude|bad|unprofession|angry|harass)", r"rude driver", r"serious incident"],
    "Wrong driver / car vs app": [r"wrong (driver|car)", r"not match", r"mismatch", r"profile"],
    "Route manipulation / slow driving": [r"long route", r"detour", r"slow", r"extra (km|distance)", r"نقاط توقف"],
    "No-show / wrong pickup": [r"no.?show", r"wrong (pickup|location)", r"didn.?t (arrive|come|show)", r"dropped.*wrong"],
    "Cancellation (unfair fees, driver-initiated)": [r"cancel", r"fine", r"penalt", r"driver.*cancel"],
    "Payment & charging errors": [r"payment.*(fail|error)", r"charg.*(twice|double)", r"card.*declin"],
    "Vehicle smell / cleanliness": [r"smell", r"dirty", r"clean", r"hygien", r"odor"],
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
}

SAFETY_PATTERNS = [
    ("Harassment", [r"\bharass", r"\bassault", r"\bheld hand", r"\bthreaten", r"\bstalk",
                    r"\babus", r"\bsexual", r"أسئله خاصه", r"يدق.*بعد.*الرحله"]),
    ("Reckless driving", [r"\breckless", r"\binjur", r"\baccident", r"\bdangerous driv",
                          r"child.*injur", r"stressful.driving", r"chasing"]),
    ("Identity mismatch", [r"not matching the profile", r"wrong driver", r"fake driver",
                           r"imperson", r"safety.*mismatch"]),
]


def g(row: dict, *keys, default=None):
    for k in keys:
        if k in row and row[k] is not None:
            v = row[k]
            if isinstance(v, str) and v.strip().lower() == "none":
                continue
            return v
        lk = k.lower() if isinstance(k, str) else k
        for rk, rv in row.items():
            if rk.lower() == lk.lower() and rv is not None:
                if isinstance(rv, str) and rv.strip().lower() == "none":
                    continue
                return rv
    return default


def clean_text(text: str) -> str:
    if not text:
        return ""
    text = re.sub(r"^[a-z_]+ :: ", "", text.strip(), flags=re.I)
    text = text.replace("-", " ")
    return text


def is_zendesk_neutral(cat: str) -> bool:
    cl = (cat or "").lower()
    return any(n in cl for n in ZENDESK_NEUTRAL_CATS)


def classify_sentiment(row: dict) -> str:
    existing = g(row, "OVERALL_SENTIMENT")
    if existing and str(existing).strip() in VALID_SENTIMENTS:
        return str(existing).strip()

    src = g(row, "FEEDBACK_SOURCE", default="")
    text = clean_text(g(row, "TEXT_FOR_ANALYSIS", "RATINGS_COMMENTS", default="")).lower()
    stars = g(row, "RATING_STARS")
    ticket_cat = g(row, "TICKET_CATEGORY", default="") or ""

    if src == "Zendesk":
        if is_zendesk_neutral(ticket_cat):
            return "Neutral"
        return "Negative"

    if stars is not None:
        try:
            s = float(stars)
            if s <= 2:
                base = "Negative"
            elif s == 3:
                base = "Neutral"
            else:
                base = "Positive"
        except (TypeError, ValueError):
            base = "Neutral"
    else:
        base = "Neutral"

    if not text or len(text.strip()) < 3:
        return base if base != "Neutral" else "Unclear"

    pos = bool(re.search(r"\b(good|great|excellent|amazing|thank|love|best|perfect|happy|wonderful|nice|awesome)\b", text))
    neg = bool(re.search(r"\b(bad|terrible|awful|worst|hate|angry|frustrat|disappoint|horrible|poor|complain|unfair|scam|سيى|محتال)\b", text))
    if pos and neg:
        return "Mixed"
    if neg:
        return "Negative"
    if pos:
        return "Positive"
    return base


def theme_from_ticket(cat: str, persona: str) -> str | None:
    cl = (cat or "").lower()
    for key, theme in ZENDESK_THEME_MAP.items():
        if key in cl:
            if theme:
                return theme
            break
    return None


def match_themes(row: dict) -> set[str]:
    persona = g(row, "PERSONA", default="")
    src = g(row, "FEEDBACK_SOURCE", default="")
    text = clean_text(g(row, "TEXT_FOR_ANALYSIS", "RATINGS_COMMENTS", default="")).lower()
    ticket_cat = g(row, "TICKET_CATEGORY", default="") or ""
    themes = set()

    zt = theme_from_ticket(ticket_cat, persona)
    if zt:
        themes.add(zt)

    kw_map = DRIVER_KEYWORDS if persona == "driver" else RIDER_KEYWORDS
    for theme, patterns in kw_map.items():
        for pat in patterns:
            if re.search(pat, text, re.I):
                themes.add(theme)
                break

    return themes


def pct_label(n: int, denom: int) -> str:
    if denom == 0:
        return "n/a"
    p = round(100 * n / denom)
    return f"~{p}%"


def pct_neg_label(neg: int, analyzed: int) -> str:
    if analyzed == 0:
        return "n/a"
    return f"~{round(100 * neg / analyzed)}%"


def scan_safety(row: dict) -> tuple[str, str] | None:
    text = clean_text(g(row, "TEXT_FOR_ANALYSIS", "RATINGS_COMMENTS", default=""))
    ticket = g(row, "TICKET_CATEGORY", default="") or ""
    combined = f"{ticket} {text}".lower()
    for category, patterns in SAFETY_PATTERNS:
        for pat in patterns:
            if re.search(pat, combined, re.I):
                summary = text.strip() or ticket.strip()
                summary = re.sub(r"^[a-z_]+ :: ", "", summary, flags=re.I)
                summary = re.sub(r"\b\d{7,}\b", "[redacted]", summary)
                summary = re.sub(r"\s+", " ", summary)[:140]
                if len(text) > 140:
                    summary += "…"
                return category, summary
    return None


def normalize_source(src: str) -> str:
    if src in ("Google Store", "Apple Store"):
        return "App stores"
    return src


def build_volume_table(volume_rows: list, analyzed: list) -> dict:
    vol = defaultdict(lambda: {"driver": 0, "rider": 0})
    for r in volume_rows:
        src = normalize_source(r["feedback_source"])
        vol[src][r["persona"]] += r["volume"]

    neg_stats = defaultdict(lambda: {"driver": {"neg": 0, "analyzed": 0},
                                     "rider": {"neg": 0, "analyzed": 0}})
    for row in analyzed:
        src = normalize_source(g(row, "FEEDBACK_SOURCE", default=""))
        persona = g(row, "PERSONA", default="")
        if persona not in ("driver", "rider"):
            continue
        if row["sentiment"] == "Unclear":
            continue
        neg_stats[src][persona]["analyzed"] += 1
        if row["sentiment"] in ("Negative", "Mixed"):
            neg_stats[src][persona]["neg"] += 1

    table = {}
    for src in ["Zendesk", "ride_rating", "in_app", "App stores"]:
        t = vol[src]
        d_s, r_s = neg_stats[src]["driver"], neg_stats[src]["rider"]
        table[src] = {
            "driver_vol": t["driver"],
            "rider_vol": t["rider"],
            "driver_pct": pct_neg_label(d_s["neg"], d_s["analyzed"]) if t["driver"] else "n/a",
            "rider_pct": pct_neg_label(r_s["neg"], r_s["analyzed"]) if t["rider"] else "n/a",
        }
    return table


def theme_counts(rows: list, persona: str) -> list[tuple[str, int, int]]:
    persona_rows = [r for r in rows if g(r, "PERSONA") == persona]
    neg_mixed = [r for r in persona_rows if r["sentiment"] in ("Negative", "Mixed")]
    denom = len(persona_rows)
    counter = Counter()
    for row in neg_mixed:
        for t in match_themes(row):
            counter[t] += 1
    return [(t, c, denom) for t, c in counter.most_common()]


def derive_priorities(driver_themes, rider_themes, safety) -> list[tuple[str, str]]:
    items = []
    actions = {
        "Driver conduct": "Tighten driver conduct escalation and re-training for repeat offenders.",
        "Overcharging / fare disputes": "Audit final-fare recalculation and route-deviation detection.",
        "Cancellation (unfair fees, driver-initiated)": "Review cancellation fee policy and driver-initiated cancel triggers.",
        "Route manipulation / slow driving": "Improve route monitoring and fare adjustment for detours.",
        "Wrong driver / car vs app": "Strengthen vehicle identity verification at ride start.",
        "Passenger conduct": "Improve passenger reporting flow and enforcement for repeat offenders.",
        "Cancellation fees not received / unfair fines": "Audit cancellation compensation payouts and dispute resolution SLAs.",
        "Earnings & fares (low pay per km, weak offers)": "Review fare floors and surge allocation for driver earnings.",
        "Passenger didn't pay (cash)": "Strengthen cash-trip payment verification and recovery process.",
        "No-show / wrong pickup": "Improve pickup ETA accuracy and no-show fee fairness.",
        "Payment not credited / withdrawals": "Fix wallet crediting delays and withdrawal failures.",
    }

    pool = [(t, c, d, "rider") for t, c, d in rider_themes] + [(t, c, d, "driver") for t, c, d in driver_themes]
    pool.sort(key=lambda x: (-x[1], x[3] == "rider"))
    seen = set()
    for theme, count, denom, side in pool:
        if theme in seen:
            continue
        seen.add(theme)
        pct = pct_label(count, denom)
        action = actions.get(theme, "Investigate root cause and assign owner for remediation.")
        items.append((theme, f"{pct} of {side} negatives/mixed across sources. {action}"))
        if len(items) >= 3:
            break

    if safety and len(items) < 3:
        cat, summary = safety[0]
        items.insert(0, (f"Safety — {cat}", f"Flagged in sample ({summary[:90]}…). Escalate to trust & safety review."))
        items = items[:3]

    while len(items) < 3:
        items.append(("Monitor volume trends", "Continue tracking sentiment by source; no additional critical theme emerged."))
    return items[:3]


def compose_digest(data: dict, analysis: dict) -> str:
    day = data["report_day_name"]
    total = data["total_comments"]
    sample_n = data["sample_size"]
    table = analysis["volume_table"]
    driver_themes = analysis["driver_themes"][:6]
    rider_themes = analysis["rider_themes"][:7]
    safety = analysis["safety"]
    priorities = analysis["priorities"]

    lines = [
        f"## 📊 {day}",
        "",
        f"{total} comments · stratified sample ~{sample_n} analyzed (up to 100 per source×user-type cell). Zendesk = support tickets (complaint-only by nature, so its %neg is always very high); ride_rating is the truer sentiment gauge.",
        "",
        "|Source|Driver (vol · %neg)|Rider (vol · %neg)|",
        "|  ---  |  ---  |  ---  |",
    ]

    for src in ["Zendesk", "ride_rating", "in_app", "App stores"]:
        t = table[src]
        d_cell = f"{t['driver_vol']} · {t['driver_pct']}" if t["driver_vol"] else "0 · n/a"
        if src == "in_app":
            r_cell = "—"
        elif t["rider_vol"]:
            r_cell = f"{t['rider_vol']} · {t['rider_pct']}" if src != "App stores" else f"{t['rider_vol']} · n/a"
        else:
            r_cell = "0 · n/a"
        if src == "App stores" and t["driver_vol"]:
            d_cell = f"{t['driver_vol']} · n/a"
        lines.append(f"|{src}|{d_cell}|{r_cell}|")

    lines += ["", "### 🏎️ Drivers — top issues", "", "*(% of driver sample)*", ""]
    for theme, count, denom in driver_themes:
        emoji = THEME_EMOJI.get(theme, "•")
        lines.append(f"* {emoji} {theme} — **{pct_label(count, denom)}**")
    if not driver_themes:
        lines.append("* No significant driver themes in sample.")

    lines += ["", "### 🧍 Riders — top issues", "", "*(% of rider sample)*", ""]
    for theme, count, denom in rider_themes:
        emoji = THEME_EMOJI.get(theme, "•")
        lines.append(f"* {emoji} {theme} — **{pct_label(count, denom)}**")
    if not rider_themes:
        lines.append("* No significant rider themes in sample.")

    lines += ["", f"### ⚠️ Safety escalations (~{len(safety)} flagged)", ""]
    if safety:
        safety_emoji = {"Harassment": "🆘", "Reckless driving": "🏎️", "Identity mismatch": "🚙"}
        for cat, summary in safety[:3]:
            lines.append(f"* {safety_emoji.get(cat, '⚠️')} **{cat}** — {summary}")
    else:
        lines.append("* No critical safety cases flagged in today's negative/mixed sample.")

    lines += ["", "### 🎯 Priorities", ""]
    for i, (title, body) in enumerate(priorities, 1):
        lines.append(f"{i}. **{title}** — {body}")

    lines += ["", "---"]
    return "\n".join(lines)


def analyze(data: dict) -> dict:
    analyzed = []
    for row in data["sample"]:
        analyzed.append({**row, "sentiment": classify_sentiment(row)})

    safety = []
    seen_summaries = set()
    for row in analyzed:
        if row["sentiment"] not in ("Negative", "Mixed"):
            continue
        hit = scan_safety(row)
        if hit and hit[1] not in seen_summaries:
            safety.append(hit)
            seen_summaries.add(hit[1])
        if len(safety) >= 3:
            break

    driver_themes = theme_counts(analyzed, "driver")
    rider_themes = theme_counts(analyzed, "rider")
    volume_table = build_volume_table(data["volume_rows"], analyzed)
    priorities = derive_priorities(driver_themes, rider_themes, safety)
    digest = compose_digest(data, {
        "volume_table": volume_table,
        "driver_themes": driver_themes,
        "rider_themes": rider_themes,
        "safety": safety,
        "priorities": priorities,
    })

    return {
        "volume_table": volume_table,
        "driver_themes": driver_themes,
        "rider_themes": rider_themes,
        "safety": safety,
        "priorities": priorities,
        "digest": digest,
        "analyzed_sample_size": len(analyzed),
    }


if __name__ == "__main__":
    path = sys.argv[1]
    with open(path) as f:
        data = json.load(f)
    result = analyze(data)
    out = sys.argv[2] if len(sys.argv) > 2 else path.replace(".json", "_analysis.json")
    with open(out, "w") as f:
        json.dump(result, f, indent=2)
    digest_path = path.replace(".json", "_digest.md")
    with open(digest_path, "w") as f:
        f.write(result["digest"])
    print(json.dumps({
        "priorities": result["priorities"],
        "safety_count": len(result["safety"]),
        "volume_table": result["volume_table"],
        "driver_themes": result["driver_themes"][:5],
        "rider_themes": result["rider_themes"][:5],
        "digest_path": digest_path,
    }, indent=2))
