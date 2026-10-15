"""Compose VoC daily digest markdown from analysis JSON."""
import json
import sys

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


def fmt_cell(row):
    vol = row["volume"]
    pct = row["neg_pct"]
    if vol == 0:
        return "0 · n/a"
    if pct == "n/a":
        return f"{vol} · n/a"
    if not str(pct).startswith("~") and pct not in ("n/a", "0%"):
        pct = f"~{pct}"
    return f"{vol} · {pct}"


def compose(d):
    day = d["report_day_name"]
    total = d["total_comments"]
    sample = d["sample_count"]
    t = d["table_rows"]

    lines = [
        f"## 📊 {day}",
        "",
        f"{total} comments · stratified sample ~{sample} analyzed (up to 100 per source×user-type cell). Zendesk = support tickets (complaint-only by nature, so its %neg is always very high); ride_rating is the truer sentiment gauge.",
        "",
        "|Source|Driver (vol · %neg)|Rider (vol · %neg)|",
        "|  ---  |  ---  |  ---  |",
        f"|Zendesk|{fmt_cell(t['Zendesk_driver'])}|{fmt_cell(t['Zendesk_rider'])}|",
        f"|ride_rating|{fmt_cell(t['ride_rating_driver'])}|{fmt_cell(t['ride_rating_rider'])}|",
        f"|in_app|{fmt_cell(t['in_app_driver'])}|—|",
        f"|App stores|{fmt_cell(t['App stores_driver'])}|{fmt_cell(t['App stores_rider'])}|",
        "",
        "### 🏎️ Drivers — top issues",
        "",
        "*(% of driver sample)*",
        "",
    ]

    for item in d["driver_themes"][:6]:
        emoji = THEME_EMOJI.get(item["theme"], "•")
        pct = item["pct"]
        if not str(pct).startswith("~") and pct not in ("n/a", "0%"):
            pct = f"~{pct}"
        lines.append(f"* {emoji} {item['theme']} — **{pct}**")

    lines.extend([
        "",
        "### 🧍 Riders — top issues",
        "",
        "*(% of rider sample)*",
        "",
    ])

    for item in d["rider_themes"][:7]:
        emoji = THEME_EMOJI.get(item["theme"], "•")
        pct = item["pct"]
        if not str(pct).startswith("~") and pct not in ("n/a", "0%"):
            pct = f"~{pct}"
        lines.append(f"* {emoji} {item['theme']} — **{pct}**")

    safety_count = d["safety_count"]
    lines.extend([
        "",
        f"### ⚠️ Safety escalations (~{safety_count} flagged)",
        "",
    ])

    for ex in d["safety_examples"]:
        emoji = SAFETY_EMOJI.get(ex["category"], "⚠️")
        lines.append(f"* {emoji} **{ex['category']}** — {ex['summary']}")

    if not d["safety_examples"]:
        lines.append("* No critical safety escalations flagged in today's sample.")

    lines.extend([
        "",
        "### 🎯 Priorities",
        "",
    ])

    for i, p in enumerate(d["priorities"][:3], 1):
        action = p["action"].rstrip(".")
        lines.append(f"{i}. **{p['title']}** — {p['evidence']}. {action}.")

    lines.extend(["", "---", ""])
    return "\n".join(lines)


def main():
    d = json.load(sys.stdin)
    print(compose(d))


if __name__ == "__main__":
    main()
