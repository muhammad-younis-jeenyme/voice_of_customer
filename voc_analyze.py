#!/usr/bin/env python3
"""Analyze VoC sample: sentiment, themes, safety flags, digest composition."""
import json
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime

IN_PATH = sys.argv[1] if len(sys.argv) > 1 else None
OUT_PATH = sys.argv[2] if len(sys.argv) > 2 else None

if not IN_PATH:
    print("Usage: voc_analyze.py <pull_json> [out_json]")
    sys.exit(1)

with open(IN_PATH) as f:
    data = json.load(f)

report_date = data["report_date"]
dt = datetime.strptime(report_date, "%Y-%m-%d")
report_day_name = dt.strftime("%a %d %b %Y")

VALID_SENTIMENTS = {"Positive", "Neutral", "Negative", "Mixed", "Unclear"}

# Zendesk neutral categories
ZENDESK_NEUTRAL_CATS = {
    "city update", "work permit", "lost item", "lost items", "found item",
    "account update", "document", "registration", "general inquiry",
}

# Theme patterns (canonical label -> regex list)
DRIVER_THEMES = {
    "Cancellation fees not received / unfair fines": [
        r"cancel", r"fine", r"penalt", r"fee.*not", r"unfair.*fee", r"compensat",
        r"deduct", r"refund.*cancel",
    ],
    "Earnings & fares (low pay per km, weak offers)": [
        r"earn", r"low pay", r"pay.*low", r"fare.*low", r"offer", r"commission",
        r"income", r"salary", r"per km", r"bonus",
    ],
    "Passenger conduct": [
        r"passenger.*(rude|bad|abuse|conduct|behavior|behaviour|fight|argu)",
        r"rider.*(rude|bad|abuse|conduct|behavior|behaviour|fight|argu)",
        r"customer.*(rude|bad|abuse|conduct)",
    ],
    "Passenger didn't pay (cash)": [
        r"didn.?t pay", r"not pay", r"no pay", r"refus.*pay", r"cash.*not",
        r"without pay", r"unpaid",
    ],
    "Fare/charge accuracy": [
        r"fare.*(wrong|incorrect|error)", r"charge.*(wrong|incorrect|error)",
        r"overcharg", r"extra.*charg", r"bill.*wrong",
    ],
    "Payment not credited / withdrawals": [
        r"withdraw", r"wallet", r"not credit", r"payment.*not", r"payout",
        r"transfer.*fail", r"balance",
    ],
}

RIDER_THEMES = {
    "Overcharging / fare disputes": [
        r"overcharg", r"fare.*(high|wrong|expensive|dispute|increase)",
        r"charged.*more", r"extra.*charg", r"price.*(high|wrong)",
        r"bill.*(high|wrong)", r"too much",
    ],
    "Driver conduct": [
        r"driver.*(rude|bad|abuse|conduct|behavior|behaviour|unprofession|angry|shout)",
        r"unprofession", r"insult", r"harass", r"threaten",
    ],
    "Wrong driver / car vs app": [
        r"wrong driver", r"different driver", r"wrong car", r"different car",
        r"car.*(different|wrong)", r"driver.*(different|wrong)", r"plate.*(different|wrong)",
        r"identity", r"not match",
    ],
    "Route manipulation / slow driving": [
        r"route", r"long way", r"detour", r"slow", r"delay", r"manipulat",
        r"traffic.*(add|extra)", r"went.*wrong way",
    ],
    "No-show / wrong pickup": [
        r"no.?show", r"didn.?t come", r"never came", r"wrong pickup", r"wrong location",
        r"wait.*long", r"late.*pickup",
    ],
    "Cancellation (unfair fees, driver-initiated)": [
        r"cancel", r"cancellation", r"cancel.*fee", r"driver.*cancel",
        r"unfair.*cancel",
    ],
    "Payment & charging errors": [
        r"payment.*(fail|error|problem)", r"charg.*(fail|error|twice|double)",
        r"card.*(fail|declin)", r"wallet",
    ],
    "Vehicle smell / cleanliness": [
        r"smell", r"dirty", r"clean", r"hygien", r"odor", r"odour", r"filthy",
    ],
}

SAFETY_PATTERNS = {
    "Harassment": [r"harass", r"assault", r"held hand", r"threaten", r"stalk", r"insult", r"touch"],
    "Reckless driving": [r"reckless", r"accident", r"injur", r"child.*injur", r"dangerous", r"speed"],
    "Identity mismatch": [r"wrong driver", r"different driver", r"wrong car", r"different car", r"not match", r"identity"],
    "Stalking / repeated contact": [r"stalk", r"repeated call", r"after ride", r"message.*after"],
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


def classify_sentiment(row):
    existing = row.get("overall_sentiment")
    if existing and str(existing).strip() in VALID_SENTIMENTS:
        return str(existing).strip()

    text = (row.get("text_for_analysis") or "").strip()
    source = row.get("feedback_source", "")
    stars = row.get("rating_stars")
    ticket_cat = (row.get("ticket_category") or "").lower()

    # Zendesk default negative unless neutral category
    if source == "Zendesk":
        if any(n in ticket_cat for n in ZENDESK_NEUTRAL_CATS):
            return "Neutral"
        return "Negative"

    text_lower = text.lower()
    neg_words = [
        "bad", "terrible", "awful", "horrible", "worst", "hate", "angry", "frustrat",
        "complain", "unfair", "rude", "disappoint", "poor", "never again", "scam",
        "cheat", "fraud", "disgust", "unacceptable", "problem", "issue", "wrong",
    ]
    pos_words = [
        "good", "great", "excellent", "amazing", "wonderful", "thank", "happy",
        "satisfied", "perfect", "best", "love", "appreciate", "professional", "nice",
    ]
    neg_hits = sum(1 for w in neg_words if w in text_lower)
    pos_hits = sum(1 for w in pos_words if w in text_lower)

    if stars is not None:
        try:
            s = float(stars)
            if s <= 2:
                lean = "Negative"
            elif s >= 4:
                lean = "Positive"
            elif s == 3:
                lean = "Neutral"
            else:
                lean = None
        except (TypeError, ValueError):
            lean = None
    else:
        lean = None

    if neg_hits and pos_hits:
        return "Mixed"
    if neg_hits >= 2 or (neg_hits >= 1 and len(text_lower) < 80):
        return "Negative"
    if pos_hits >= 2:
        return "Positive"
    if lean:
        return lean
    if neg_hits == 1:
        return "Negative"
    if pos_hits == 1:
        return "Positive"
    if len(text_lower) < 10:
        return "Unclear"
    return "Neutral"


# Zendesk ticket category -> theme mapping
ZENDESK_CAT_DRIVER = {
    "i did not get paid the cancellation fees": "Cancellation fees not received / unfair fines",
    "report an issue with the passenger's behaviour": "Passenger conduct",
    "passenger did not pay trip fare in full / part of the trip fare": "Passenger didn't pay (cash)",
    "amount has been deducted from my balance": "Cancellation fees not received / unfair fines",
    "ride value is less than actual": "Earnings & fares (low pay per km, weak offers)",
    "i did not receive credit card payment amount in wallet": "Payment not credited / withdrawals",
    "i did not receive discount amount in wallet": "Payment not credited / withdrawals",
    "i did not win although the target was achieved": "Earnings & fares (low pay per km, weak offers)",
}

ZENDESK_CAT_RIDER = {
    "price was way higher than the estimate": "Overcharging / fare disputes",
    "driver overcharged me for the trip/took extra money from me": "Overcharging / fare disputes",
    "driver took a long route/ driver caused the charges to increase": "Route manipulation / slow driving",
    "driver was not matching the profile in the app": "Wrong driver / car vs app",
    "i cancelled because driver did not show up at the pickup point": "No-show / wrong pickup",
    "bad rating": None,  # fall through to text
    "report an issue with the passenger's behaviour": "Driver conduct",
}

def themes_from_ticket_category(cat, persona):
    if not cat:
        return []
    key = cat.strip().lower()
    mapping = ZENDESK_CAT_DRIVER if persona == "driver" else ZENDESK_CAT_RIDER
    label = mapping.get(key)
    return [label] if label else []


def match_themes(text, theme_dict):
    text_lower = (text or "").lower()
    matched = []
    for label, patterns in theme_dict.items():
        for pat in patterns:
            if re.search(pat, text_lower):
                matched.append(label)
                break
    return matched


def pct(n, denom, tilde=False):
    if denom == 0:
        return "n/a"
    val = round(100 * n / denom)
    prefix = "~" if tilde else ""
    return f"{prefix}{val}%"


def anonymize(text, max_len=120):
    t = text or ""
    # Skip category-code style strings (e.g. "reckless_driving ::")
    if re.match(r"^[a-z_]+\s*::", t.strip().lower()):
        return "Safety incident reported via support ticket"
    t = re.sub(r"\b\d{8,}\b", "[redacted]", t)
    t = re.sub(r"@\w+", "[user]", t)
    t = re.sub(r"https?://\S+", "[link]", t)
    t = t.strip()
    if len(t) > max_len:
        t = t[: max_len - 3] + "..."
    return t


# Normalize volumes
vol = defaultdict(lambda: {"driver": 0, "rider": 0})
for r in data["volume_rows"]:
    src = r["feedback_source"]
    persona = r["persona"]
    v = r["volume"]
    if src in ("Google Store", "Apple Store"):
        vol["App stores"][persona] += v
    else:
        vol[src][persona] += v

# Analyze sample
sample = data["sample_rows"]
for row in sample:
    row["sentiment"] = classify_sentiment(row)

# %neg per source×persona cell
cell_stats = defaultdict(lambda: {"neg": 0, "total": 0})
for row in sample:
    key = (row["feedback_source"], row["persona"])
    cell_stats[key]["total"] += 1
    if row["sentiment"] in ("Negative", "Mixed"):
        cell_stats[key]["neg"] += 1

def cell_pctneg(source, persona):
    key = (source, persona)
    stats = cell_stats.get(key, {"neg": 0, "total": 0})
    if stats["total"] == 0:
        return "n/a"
    tilde = stats["total"] < 30
    return pct(stats["neg"], stats["total"], tilde=tilde)

# Theme extraction per persona
driver_sample = [r for r in sample if r["persona"] == "driver"]
rider_sample = [r for r in sample if r["persona"] == "rider"]

def extract_theme_pcts(rows, theme_dict):
    neg_mixed = [r for r in rows if r["sentiment"] in ("Negative", "Mixed")]
    denom = len(rows)  # denominator = full persona sample per playbook
    counts = Counter()
    for r in neg_mixed:
        text = r.get("text_for_analysis") or r.get("ratings_comments") or ""
        persona = r.get("persona", "rider")
        themes = themes_from_ticket_category(r.get("ticket_category"), persona)
        if not themes:
            themes = match_themes(text, theme_dict)
        for t in set(themes):
            counts[t] += 1
    ranked = []
    for label, count in counts.most_common():
        tilde = denom < 30
        ranked.append({
            "label": label,
            "pct": pct(count, denom, tilde=tilde),
            "count": count,
            "emoji": THEME_EMOJI.get(label, "•"),
        })
    return ranked

driver_themes = extract_theme_pcts(driver_sample, DRIVER_THEMES)
rider_themes = extract_theme_pcts(rider_sample, RIDER_THEMES)

# Safety flags
safety_flags = []
SAFETY_EXCLUDE = [r"not pay", r"didn.?t pay", r"payment", r"wallet", r"fare", r"charg", r"cancel.*fee"]

for r in sample:
    if r["sentiment"] not in ("Negative", "Mixed"):
        continue
    text = r.get("text_for_analysis") or r.get("ratings_comments") or ""
    text_lower = text.lower()
    cat = (r.get("ticket_category") or "").lower()

    if any(re.search(p, text_lower) for p in SAFETY_EXCLUDE):
        continue

    if "serious incident" in cat:
        safety_flags.append({
            "category": "Reckless driving",
            "summary": anonymize(text) if len(text.strip()) > 20 else "Serious incident reported via support ticket",
            "persona": r["persona"],
        })
        continue
    if "harass" in cat:
        safety_flags.append({
            "category": "Harassment",
            "summary": anonymize(text) if len(text.strip()) > 20 else "Harassment reported via support ticket",
            "persona": r["persona"],
        })
        continue
    for category, patterns in SAFETY_PATTERNS.items():
        for pat in patterns:
            if re.search(pat, text_lower):
                safety_flags.append({
                    "category": category,
                    "summary": anonymize(text),
                    "persona": r["persona"],
                })
                break

# Dedupe safety by summary
seen = set()
unique_safety = []
for s in safety_flags:
    key = (s["category"], s["summary"][:60])
    if key not in seen:
        seen.add(key)
        unique_safety.append(s)

# Priorities
def build_priorities(driver_themes, rider_themes, safety_count):
    priorities = []
    # Combine top themes
    all_themes = []
    for t in rider_themes[:3]:
        all_themes.append(("rider", t))
    for t in driver_themes[:3]:
        all_themes.append(("driver", t))
    all_themes.sort(key=lambda x: int(re.sub(r"[^\d]", "", x[1]["pct"]) or "0"), reverse=True)

    actions = {
        "Driver conduct": "Strengthen driver conduct enforcement, deactivation triggers, and post-ride complaint SLAs.",
        "Overcharging / fare disputes": "Audit final-fare recalculation, estimate vs actual fare gaps, and route-deviation detection.",
        "Cancellation fees not received / unfair fines": "Review cancellation compensation rules and automate fee credit for eligible driver cancels.",
        "Passenger conduct": "Improve passenger rating enforcement and in-ride reporting for driver safety.",
        "Earnings & fares (low pay per km, weak offers)": "Review fare floors, surge transparency, and per-km minimum payout.",
        "Wrong driver / car vs app": "Tighten vehicle/driver identity verification at trip start.",
        "Route manipulation / slow driving": "Improve route-deviation alerts and GPS audit tooling.",
        "Passenger didn't pay (cash)": "Strengthen cash-payment confirmation and driver reimbursement flows.",
    }

    used = set()
    for persona, theme in all_themes:
        label = theme["label"]
        if label in used:
            continue
        used.add(label)
        evidence = f"{theme['pct']} of {persona} sample"
        action = actions.get(label, "Investigate root cause and assign owner for remediation.")
        priorities.append({
            "title": label,
            "evidence": evidence,
            "action": action,
        })
        if len(priorities) >= 3:
            break

    if len(priorities) < 3 and safety_count > 0:
        priorities.append({
            "title": "Safety escalations",
            "evidence": f"{safety_count} safety-related flags in today's sample",
            "action": "Prioritize safety review queue and fast-track identity/conduct cases.",
        })
    return priorities[:3]


priorities = build_priorities(driver_themes, rider_themes, len(unique_safety))

# Compose digest table rows
def fmt_cell(source, persona):
    v = vol.get(source, {}).get(persona, 0)
    if v == 0:
        return "0 · n/a"
    if source == "App stores":
        return f"{v} · n/a"
    p = cell_pctneg(source, persona)
    return f"{v} · {p}"

# Build markdown digest
lines = [
    f"## 📊 {report_day_name}",
    "",
    f"{data['total_comments']} comments · stratified sample ~{data['sample_count']} analyzed (up to 100 per source×user-type cell). Zendesk = support tickets (complaint-only by nature, so its %neg is always very high); ride_rating is the truer sentiment gauge.",
    "",
    "|Source|Driver (vol · %neg)|Rider (vol · %neg)|",
    "|  ---  |  ---  |  ---  |",
    f"|Zendesk|{fmt_cell('Zendesk', 'driver')}|{fmt_cell('Zendesk', 'rider')}|",
    f"|ride_rating|{fmt_cell('ride_rating', 'driver')}|{fmt_cell('ride_rating', 'rider')}|",
    f"|in_app|{fmt_cell('in_app', 'driver')}|—|",
    f"|App stores|{fmt_cell('App stores', 'driver')}|{fmt_cell('App stores', 'rider')}|",
    "",
    "### 🏎️ Drivers — top issues",
    "",
    "*(% of driver sample)*",
    "",
]
for t in driver_themes[:6]:
    lines.append(f"* {t['emoji']} {t['label']} — **{t['pct']}**")
if not driver_themes:
    lines.append("* No significant driver themes in sample")

lines += [
    "",
    "### 🧍 Riders — top issues",
    "",
    "*(% of rider sample)*",
    "",
]
for t in rider_themes[:7]:
    lines.append(f"* {t['emoji']} {t['label']} — **{t['pct']}**")
if not rider_themes:
    lines.append("* No significant rider themes in sample")

lines += [
    "",
    f"### ⚠️ Safety escalations (~{len(unique_safety)} flagged)",
    "",
]
safety_emoji = {"Harassment": "🆘", "Reckless driving": "🏎️", "Identity mismatch": "🚙", "Stalking / repeated contact": "🆘"}
for s in unique_safety[:3]:
    em = safety_emoji.get(s["category"], "🆘")
    lines.append(f"* {em} **{s['category']}** — {s['summary']}")
if not unique_safety:
    lines.append("* No safety escalations flagged in sample")

lines += [
    "",
    "### 🎯 Priorities",
    "",
]
for i, p in enumerate(priorities, 1):
    lines.append(f"{i}. **{p['title']}** — {p['evidence']}. {p['action']}")

lines += ["", "---", ""]

digest = "\n".join(lines)

result = {
    "report_date": report_date,
    "report_day_name": report_day_name,
    "total_comments": data["total_comments"],
    "sample_count": data["sample_count"],
    "digest": digest,
    "driver_themes": driver_themes,
    "rider_themes": rider_themes,
    "safety_flags": unique_safety,
    "priorities": priorities,
    "vol": dict(vol),
    "cell_stats": {f"{k[0]}|{k[1]}": v for k, v in cell_stats.items()},
}

out = OUT_PATH or f"/tmp/voc/analysis_{report_date}.json"
with open(out, "w") as f:
    json.dump(result, f, indent=2)

print(json.dumps({
    "report_date": report_date,
    "total_comments": data["total_comments"],
    "sample_count": data["sample_count"],
    "priorities": [p["title"] for p in priorities],
    "safety_count": len(unique_safety),
}, indent=2))
print("\n---DIGEST---\n")
print(digest)
