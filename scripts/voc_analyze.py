#!/usr/bin/env python3
"""Analyze VoC sample: sentiment, themes, safety, digest markdown."""
import json
import re
import sys
from collections import defaultdict
from datetime import datetime

SAMPLE_PATH = sys.argv[1] if len(sys.argv) > 1 else "/tmp/voc/sample.json"
VOLUME_PATH = sys.argv[2] if len(sys.argv) > 2 else "/tmp/voc/volume.json"
TOTAL_PATH = sys.argv[3] if len(sys.argv) > 3 else "/tmp/voc/total.json"
REPORT_DATE = sys.argv[4] if len(sys.argv) > 4 else "2026-10-08"

VALID_SENTIMENTS = {"Positive", "Neutral", "Negative", "Mixed"}

ZENDESK_NEUTRAL_CATEGORIES = {
    "city update",
    "work permit",
    "lost item",
    "lost & found",
    "general inquiry",
    "account update",
    "document",
    "registration",
    "activation",
}

ARABIC_NEG = re.compile(
    r"سيء|سيئ|رديء|زفت|نصب|احتيال|غش|سرق|سرقة|كذب|غبي|حمار|قذر|وسخ|"
    r"مشكلة|مشاكل|تأخير|متأخر|غالي|زيادة|زياده|ماوصل|ما وصل|رفض|إلغاء|الغاء|"
    r"تعطل|عطل|خربان|نصاب|احتال",
    re.I,
)

DRIVER_THEMES = [
    (
        "Cancellation fees not received / unfair fines",
        "🚫",
        re.compile(
            r"cancel|cancellation|fine|penalty|fee.*not|didn.?t receive|unfair",
            re.I,
        ),
    ),
    (
        "Earnings & fares (low pay per km, weak offers)",
        "💰",
        re.compile(
            r"earn|income|pay|fare|km|kilometer|offer|bonus|commission|low.*pay|"
            r"underpay|trip.*short|distance",
            re.I,
        ),
    ),
    (
        "Passenger conduct",
        "😡",
        re.compile(
            r"passenger|rider|customer|rude|abuse|insult|beha?vio|attitude|"
            r"smok|drunk|fight|argu",
            re.I,
        ),
    ),
    (
        "Passenger didn't pay (cash)",
        "💵",
        re.compile(
            r"didn.?t pay|not pay|cash|unpaid|refus.*pay|without pay|no pay",
            re.I,
        ),
    ),
    (
        "Fare/charge accuracy",
        "📄",
        re.compile(
            r"wrong (fare|amount|charge)|incorrect|miscalculat|overcharg|deduct",
            re.I,
        ),
    ),
    (
        "Payment not credited / withdrawals",
        "🏦",
        re.compile(
            r"withdraw|wallet|credit|payout|transfer|bank|not credited|missing.*money",
            re.I,
        ),
    ),
]

RIDER_THEMES = [
    (
        "Overcharging / fare disputes",
        "💸",
        re.compile(
            r"overcharg|expensive|too much|high fare|fare dispute|charged more|"
            r"double charg|extra charg|bill",
            re.I,
        ),
    ),
    (
        "Driver conduct",
        "😡",
        re.compile(
            r"driver|rude|abuse|insult|beha?vio|attitude|angry|shout|yell|disrespect",
            re.I,
        ),
    ),
    (
        "Wrong driver / car vs app",
        "🚙",
        re.compile(
            r"wrong (driver|car|vehicle)|different (car|driver|plate)|not match|"
            r"identity|another car|fake",
            re.I,
        ),
    ),
    (
        "Route manipulation / slow driving",
        "🗺️",
        re.compile(
            r"route|detour|long way|slow|traffic trick|manipulat|circle|extra km|"
            r"longer route",
            re.I,
        ),
    ),
    (
        "No-show / wrong pickup",
        "📍",
        re.compile(
            r"no.?show|didn.?t (come|arrive|pick)|wrong pickup|wait(ed)? (long|forever)|"
            r"late pickup",
            re.I,
        ),
    ),
    (
        "Cancellation (unfair fees, driver-initiated)",
        "🚫",
        re.compile(
            r"cancel|cancellation|fee|driver cancel|unfair.*cancel",
            re.I,
        ),
    ),
    (
        "Payment & charging errors",
        "🏦",
        re.compile(
            r"payment|charg|card|wallet|refund|deduct|double|failed.*pay",
            re.I,
        ),
    ),
    (
        "Vehicle smell / cleanliness",
        "🧼",
        re.compile(r"smell|dirty|clean|hygien|odor|stink|filthy", re.I),
    ),
]

SAFETY_PATTERNS = [
    ("Harassment", "🆘", re.compile(
        r"harass|assault|molest|threat|insult|held hand|touch|stalk|"
        r"repeated call|after ride|message after",
        re.I,
    )),
    ("Reckless driving", "🏎️", re.compile(
        r"reckless|speed|fast|accident|crash|injur|child|hit|dangerous driv",
        re.I,
    )),
    ("Identity mismatch", "🚙", re.compile(
        r"wrong (driver|car)|different (car|driver|plate)|not match|fake driver|"
        r"another person",
        re.I,
    )),
]

ZENDESK_CATEGORY_THEME = {
    "passenger conduct": ("Passenger conduct", "😡"),
    "passenger's behaviour": ("Passenger conduct", "😡"),
    "passenger behaviour": ("Passenger conduct", "😡"),
    "driver conduct": ("Driver conduct", "😡"),
    "driver was rude": ("Driver conduct", "😡"),
    "fare": ("Fare/charge accuracy", "📄"),
    "payment": ("Payment not credited / withdrawals", "🏦"),
    "cancellation": ("Cancellation fees not received / unfair fines", "🚫"),
    "cancellation fee": ("Cancellation fees not received / unfair fines", "🚫"),
    "did not get paid the cancellation": ("Cancellation fees not received / unfair fines", "🚫"),
    "earnings": ("Earnings & fares (low pay per km, weak offers)", "💰"),
    "ride value is less": ("Earnings & fares (low pay per km, weak offers)", "💰"),
    "overcharge": ("Overcharging / fare disputes", "💸"),
    "price was way higher": ("Overcharging / fare disputes", "💸"),
    "driver overcharged": ("Overcharging / fare disputes", "💸"),
    "route": ("Route manipulation / slow driving", "🗺️"),
    "passenger did not pay": ("Passenger didn't pay (cash)", "💵"),
    "not matching the profile": ("Wrong driver / car vs app", "🚙"),
    "did not show up": ("No-show / wrong pickup", "📍"),
    "charged multiple times": ("Payment & charging errors", "🏦"),
    "deducted without": ("Payment & charging errors", "🏦"),
    "wallet": ("Payment & charging errors", "🏦"),
    "driver cancelled and i was charged": (
        "Cancellation (unfair fees, driver-initiated)",
        "🚫",
    ),
    "driver asked to cancel": ("Cancellation (unfair fees, driver-initiated)", "🚫"),
}


def norm_source(raw: str) -> str:
    if raw in ("Google Store", "Apple Store"):
        return "app_stores"
    return raw


def digest_source_label(src: str) -> str:
    if src == "app_stores":
        return "App stores"
    if src == "in_app":
        return "in_app"
    if src == "ride_rating":
        return "ride_rating"
    if src == "Zendesk":
        return "Zendesk"
    return src


def classify_sentiment(row: dict) -> str:
    existing = row.get("OVERALL_SENTIMENT")
    if existing and str(existing).strip() in VALID_SENTIMENTS:
        return str(existing).strip()

    text = (row.get("TEXT_FOR_ANALYSIS") or row.get("text_for_analysis") or "").strip()
    source = row.get("FEEDBACK_SOURCE") or ""
    stars = row.get("RATING_STARS")
    ticket_cat = (row.get("TICKET_CATEGORY") or "").lower()

    if source == "Zendesk":
        for neutral in ZENDESK_NEUTRAL_CATEGORIES:
            if neutral in ticket_cat:
                return "Neutral"
        return "Negative"

    text_lower = text.lower()
    if len(text) < 5 and ARABIC_NEG.search(text):
        return "Negative"

    neg_words = re.compile(
        r"\b(bad|terrible|awful|worst|horrible|hate|angry|frustrat|complain|"
        r"problem|issue|never again|scam|cheat|refund|disappoint|poor|unfair|"
        r"rude|slow|late|wrong|didn.?t|not good|waste)\b",
        re.I,
    )
    pos_words = re.compile(
        r"\b(good|great|excellent|amazing|thank|love|perfect|best|happy|"
        r"professional|polite|quick|fast|nice|wonderful)\b",
        re.I,
    )
    has_neg = bool(neg_words.search(text_lower) or ARABIC_NEG.search(text))
    has_pos = bool(pos_words.search(text_lower))

    if has_neg and has_pos:
        return "Mixed"
    if has_neg:
        return "Negative"
    if has_pos:
        return "Positive"

    if stars is not None:
        try:
            s = float(stars)
            if s <= 2:
                return "Negative"
            if s == 3:
                return "Neutral"
            if s >= 4:
                return "Positive"
        except (TypeError, ValueError):
            pass

    if len(text) < 8:
        return "Unclear"
    return "Neutral"


def pct_neg(counts: dict) -> str:
    analyzed = counts.get("analyzed", 0)
    neg = counts.get("negative", 0)
    if analyzed == 0:
        return "n/a"
    p = round(100 * neg / analyzed)
    prefix = "~" if analyzed < 30 else "~"
    return f"{prefix}{p}%"


def match_themes(text: str, ticket_cat: str, themes: list, persona: str = "") -> set:
    found = set()
    tl = text.lower()
    tc = (ticket_cat or "").lower()
    theme_labels = {label for label, _, _ in themes}
    for label, _emoji, pat in themes:
        if pat.search(tl):
            found.add(label)
    for key, (label, _) in ZENDESK_CATEGORY_THEME.items():
        if key in tc and (not theme_labels or label in theme_labels):
            found.add(label)
    # persona-specific zendesk category aliases
    if persona == "driver" and "not receiving requests" in tc:
        found.add("Earnings & fares (low pay per km, weak offers)")
    return found


def theme_stats(rows: list, persona: str, themes: list) -> list:
    neg_mix = [r for r in rows if r["persona"] == persona and r["sentiment"] in ("Negative", "Mixed")]
    denom = len(neg_mix)
    if denom == 0:
        return []
    counts = defaultdict(int)
    for r in neg_mix:
        text = r.get("text_for_analysis") or ""
        matched = match_themes(
            text, r.get("ticket_category") or "", themes, persona=persona
        )
        for t in matched:
            counts[t] += 1
    ranked = []
    emoji_map = {label: emo for label, emo, _ in themes}
    for label, cnt in sorted(counts.items(), key=lambda x: -x[1]):
        pct = round(100 * cnt / denom)
        ranked.append((label, emoji_map.get(label, "•"), pct))
    return ranked[:7]


def safety_flags(rows: list) -> list:
    flags = []
    for r in rows:
        if r["sentiment"] not in ("Negative", "Mixed"):
            continue
        text = (r.get("text_for_analysis") or "")[:500]
        for cat, emoji, pat in SAFETY_PATTERNS:
            if pat.search(text):
                summary = anonymize_summary(text)
                flags.append({"category": cat, "emoji": emoji, "summary": summary})
                break
    return flags


def anonymize_summary(text: str) -> str:
    t = re.sub(
        r"driver_behaviour\s*::\s*[\w\-!.]+",
        "driver conduct complaint",
        text,
        flags=re.I,
    )
    t = re.sub(r"\b\d{7,}\b", "[redacted]", t)
    t = re.sub(r"\b[A-Z][a-z]+\s+[A-Z][a-z]+\b", "Customer", t)
    t = re.sub(r"\s+", " ", t).strip()
    # Generic one-liners for highly sensitive content
    if re.search(r"sexual|harass|assault|molest|driver conduct complaint", t, re.I):
        return "Customer reports harassment during trip; requests account action against driver."
    if re.search(r"reckless|accident|injur|crash", t, re.I):
        return "Customer reports unsafe driving during trip."
    if re.search(r"wrong (driver|car)|not match|different car", t, re.I):
        return "Customer reports vehicle/driver did not match app profile."
    if len(t) > 120:
        t = t[:117] + "..."
    return t


def main():
    with open(SAMPLE_PATH) as f:
        sample = json.load(f)
    with open(VOLUME_PATH) as f:
        volume = json.load(f)
    with open(TOTAL_PATH) as f:
        total_info = json.load(f)

    total_comments = total_info["total_comments"]
    report_date = total_info.get("report_date", REPORT_DATE)
    d = datetime.strptime(report_date, "%Y-%m-%d")
    report_day_name = d.strftime("%a %d %b %Y")

    # normalize keys to lowercase for sample
    norm_sample = []
    for row in sample:
        r = {k.lower() if isinstance(k, str) else k: v for k, v in row.items()}
        r["persona"] = r.get("persona") or ""
        r["feedback_source"] = r.get("feedback_source") or ""
        r["text_for_analysis"] = r.get("text_for_analysis") or ""
        r["sentiment"] = classify_sentiment(
            {k.upper(): v for k, v in r.items() if k != "sentiment"}
        )
        norm_sample.append(r)

    # volume table
    vol_map = defaultdict(lambda: {"driver": 0, "rider": 0})
    for v in volume:
        src = norm_source(v["FEEDBACK_SOURCE"])
        persona = v["PERSONA"].lower()
        vol_map[src][persona] = int(v["VOLUME"])

    # sentiment per cell
    cell_counts = defaultdict(lambda: {"analyzed": 0, "negative": 0})
    for r in norm_sample:
        src = norm_source(r["feedback_source"])
        persona = r["persona"]
        if r["sentiment"] == "Unclear":
            continue
        key = (src, persona)
        cell_counts[key]["analyzed"] += 1
        if r["sentiment"] in ("Negative", "Mixed"):
            cell_counts[key]["negative"] += 1

    def cell_display(src_key: str, persona: str):
        vol = vol_map.get(src_key, {}).get(persona, 0)
        cc = cell_counts.get((src_key, persona), {"analyzed": 0, "negative": 0})
        if vol == 0:
            return "0 · n/a"
        if src_key == "app_stores":
            return f"{vol} · n/a"
        return f"{vol} · {pct_neg(cc)}"

    driver_themes = theme_stats(norm_sample, "driver", DRIVER_THEMES)
    rider_themes = theme_stats(norm_sample, "rider", RIDER_THEMES)
    safety = safety_flags(norm_sample)

    sample_count = len(norm_sample)

    # priorities from top themes
    priorities = build_priorities(driver_themes, rider_themes, safety, norm_sample)

    digest = compose_digest(
        report_day_name,
        total_comments,
        sample_count,
        cell_display,
        driver_themes,
        rider_themes,
        safety,
        priorities,
    )

    out = {
        "report_date": report_date,
        "report_day_name": report_day_name,
        "total_comments": total_comments,
        "sample_count": sample_count,
        "priorities": priorities,
        "digest": digest,
        "driver_themes": driver_themes,
        "rider_themes": rider_themes,
        "safety_count": len(safety),
    }
    with open("/tmp/voc/analysis.json", "w") as f:
        json.dump(out, f, indent=2)
    print(json.dumps({k: v for k, v in out.items() if k != "digest"}))


def build_priorities(driver_themes, rider_themes, safety, rows):
    prios = []

    def add_theme(theme_list, audience):
        if not theme_list:
            return
        label, _e, pct = theme_list[0]
        if "conduct" in label.lower():
            action = "Strengthen reporting, coaching, and suspension workflows."
        elif "cancellation" in label.lower():
            action = "Audit fee logic and auto-refund edge cases."
        elif "payment" in label.lower() or "charging" in label.lower():
            action = "Reconcile wallet/card charges and speed up refunds."
        elif "earnings" in label.lower() or "fare" in label.lower():
            action = "Review fare bands, offers, and payout transparency."
        else:
            action = "Targeted product/ops fix with owner assignment."
        prios.append({
            "title": label,
            "text": f"~{pct}% of {audience} negative/mixed mentions — {action}",
        })

    add_theme(rider_themes, "rider")
    add_theme(driver_themes, "driver")
    if safety:
        prios.append({
            "title": "Safety escalation review",
            "text": f"{len(safety)} safety-related flags in sample — fast-track trust & safety triage.",
        })
    elif len(prios) < 3 and len(rider_themes) > 1:
        t = rider_themes[1]
        prios.append({
            "title": t[0],
            "text": f"~{t[2]}% of rider negatives — audit detection and rider refunds.",
        })
    elif len(prios) < 3 and len(driver_themes) > 1:
        t = driver_themes[1]
        prios.append({
            "title": t[0],
            "text": f"~{t[2]}% of driver negatives — clarify policy and in-app messaging.",
        })
    while len(prios) < 3:
        prios.append({
            "title": "Monitor ride_rating sentiment",
            "text": "Continue daily stratified sampling; Zendesk volumes dominate complaint mix.",
        })
    return prios[:3]


def report_date_context(rows):
    sources = set()
    for r in rows:
        if r["sentiment"] in ("Negative", "Mixed") and r["persona"] == "rider":
            sources.add(r["feedback_source"])
    if len(sources) > 2:
        return "multiple sources"
    return ", ".join(sorted(sources)[:2]) or "feedback"


def compose_digest(
    day_name,
    total_comments,
    sample_count,
    cell_display,
    driver_themes,
    rider_themes,
    safety,
    priorities,
):
    lines = [
        f"## 📊 {day_name}",
        "",
        f"{total_comments} comments · stratified sample ~{sample_count} analyzed (up to 100 per source×user-type cell). "
        "Zendesk = support tickets (complaint-only by nature, so its %neg is always very high); "
        "ride_rating is the truer sentiment gauge.",
        "",
        "|Source|Driver (vol · %neg)|Rider (vol · %neg)|",
        "|  ---  |  ---  |  ---  |",
        f"|Zendesk|{cell_display('Zendesk', 'driver')}|{cell_display('Zendesk', 'rider')}|",
        f"|ride_rating|{cell_display('ride_rating', 'driver')}|{cell_display('ride_rating', 'rider')}|",
        f"|in_app|{cell_display('in_app', 'driver')}|—|",
        f"|App stores|{cell_display('app_stores', 'driver')}|{cell_display('app_stores', 'rider')}|",
        "",
        "### 🏎️ Drivers — top issues",
        "",
        "*(% of driver sample)*",
        "",
    ]
    for label, emoji, pct in driver_themes[:6]:
        lines.append(f"* {emoji} {label} — **~{pct}%**")
    if not driver_themes:
        lines.append("* No dominant themes in sample")

    lines += [
        "",
        "### 🧍 Riders — top issues",
        "",
        "*(% of rider sample)*",
        "",
    ]
    for label, emoji, pct in rider_themes[:7]:
        lines.append(f"* {emoji} {label} — **~{pct}%**")
    if not rider_themes:
        lines.append("* No dominant themes in sample")

    lines += [
        "",
        f"### ⚠️ Safety escalations (~{len(safety)} flagged)",
        "",
    ]
    for s in safety[:3]:
        lines.append(f"* {s['emoji']} **{s['category']}** — {s['summary']}")
    if not safety:
        lines.append("* No critical safety patterns flagged in sample")

    lines += [
        "",
        "### 🎯 Priorities",
        "",
    ]
    for i, p in enumerate(priorities, 1):
        lines.append(f"{i}. **{p['title']}** — {p['text']}")

    lines += ["", "---", ""]
    return "\n".join(lines)


if __name__ == "__main__":
    main()
