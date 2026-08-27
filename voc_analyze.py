"""Analyze VoC sample: sentiment, themes, safety, priorities."""
import json
import re
import sys
from collections import defaultdict


VALID_SENTIMENTS = {"Positive", "Neutral", "Negative", "Mixed"}

ZENDESK_NEUTRAL_CATEGORIES = {
    "city update", "work permit", "lost item", "lost & found", "lost and found",
    "general inquiry", "account update", "document", "registration",
    "vehicle update", "profile update", "information", "how to",
}

DRIVER_THEMES = {
    "Cancellation fees not received / unfair fines": [
        r"cancel", r"fine", r"penalty", r"fee not", r"didn.t receive.*fee",
        r"unfair.*fine", r"cancellation fee",
    ],
    "Earnings & fares (low pay per km, weak offers)": [
        r"earn", r"low pay", r"low fare", r"per km", r"offer", r"commission",
        r"income", r"salary", r"not enough money", r"cheap fare",
    ],
    "Passenger conduct": [
        r"passenger.*rude", r"bad passenger", r"passenger.*behav",
        r"rude customer", r"customer.*rude", r"passenger.*attitude",
    ],
    "Passenger didn't pay (cash)": [
        r"didn.t pay", r"not pay", r"no pay", r"refuse.*pay", r"cash.*not",
        r"without pay", r"did not pay", r"won.t pay",
    ],
    "Fare/charge accuracy": [
        r"wrong fare", r"fare.*wrong", r"incorrect.*charge", r"charge.*wrong",
        r"meter", r"overcharge.*driver", r"deduct",
    ],
    "Payment not credited / withdrawals": [
        r"withdraw", r"wallet", r"not credited", r"payout", r"payment.*not",
        r"balance", r"transfer", r"cashout",
    ],
}

RIDER_THEMES = {
    "Overcharging / fare disputes": [
        r"overcharg", r"too much", r"high fare", r"expensive", r"fare.*high",
        r"charged more", r"double", r"extra charge", r"unfair.*fare",
    ],
    "Driver conduct": [
        r"rude driver", r"driver.*rude", r"bad driver", r"driver.*behav",
        r"unprofessional", r"driver.*attitude", r"aggressive driver",
    ],
    "Wrong driver / car vs app": [
        r"wrong driver", r"wrong car", r"different car", r"different driver",
        r"not the driver", r"car.*different", r"plate.*wrong", r"identity",
    ],
    "Route manipulation / slow driving": [
        r"long route", r"wrong route", r"detour", r"slow", r"manipulat",
        r"extra km", r"took longer", r"traffic.*fake",
    ],
    "No-show / wrong pickup": [
        r"no show", r"didn.t come", r"wrong pickup", r"wrong location",
        r"never arrived", r"did not arrive", r"waited.*long",
    ],
    "Cancellation (unfair fees, driver-initiated)": [
        r"cancel.*fee", r"cancelled.*me", r"driver.*cancel", r"unfair.*cancel",
        r"cancellation.*charge", r"cancel.*without",
    ],
    "Payment & charging errors": [
        r"payment.*error", r"charged twice", r"double charge", r"card.*charge",
        r"wallet.*error", r"payment.*fail", r"refund",
    ],
    "Vehicle smell / cleanliness": [
        r"smell", r"dirty", r"clean", r"hygiene", r"odor", r"stink",
    ],
}

ZENDESK_CATEGORY_MAP = {
    "driver": {
        "cancellation": "Cancellation fees not received / unfair fines",
        "fine": "Cancellation fees not received / unfair fines",
        "earning": "Earnings & fares (low pay per km, weak offers)",
        "fare": "Earnings & fares (low pay per km, weak offers)",
        "passenger": "Passenger conduct",
        "payment": "Payment not credited / withdrawals",
        "withdraw": "Payment not credited / withdrawals",
        "wallet": "Payment not credited / withdrawals",
    },
    "rider": {
        "overcharge": "Overcharging / fare disputes",
        "fare": "Overcharging / fare disputes",
        "driver": "Driver conduct",
        "route": "Route manipulation / slow driving",
        "cancel": "Cancellation (unfair fees, driver-initiated)",
        "payment": "Payment & charging errors",
        "vehicle": "Vehicle smell / cleanliness",
        "clean": "Vehicle smell / cleanliness",
        "identity": "Wrong driver / car vs app",
        "wrong": "Wrong driver / car vs app",
    },
}

SAFETY_PATTERNS = {
    "Harassment": [
        r"harass", r"assault", r"threaten", r"insult", r"held.*hand",
        r"stalk", r"inappropriate", r"touch", r"abuse",
    ],
    "Reckless driving": [
        r"reckless", r"speed", r"accident", r"injur", r"crash", r"dangerous",
        r"child.*injur",
    ],
    "Identity mismatch": [
        r"wrong driver", r"wrong car", r"different.*car", r"not.*driver",
        r"identity.*mismatch", r"fake driver",
    ],
}


def classify_sentiment(row):
    existing = row.get("OVERALL_SENTIMENT")
    if existing and existing in VALID_SENTIMENTS:
        return existing

    text = (row.get("text_for_analysis") or "").strip()
    source = row.get("FEEDBACK_SOURCE") or ""
    category = (row.get("TICKET_CATEGORY") or "").lower()
    stars = row.get("RATING_STARS")

    if source == "Zendesk":
        if any(n in category for n in ZENDESK_NEUTRAL_CATEGORIES):
            return "Neutral"
        return "Negative"

    if len(text) < 3:
        return "Unclear"

    text_lower = text.lower()
    neg_words = [
        "bad", "terrible", "awful", "horrible", "worst", "hate", "angry",
        "frustrat", "complain", "disappoint", "unfair", "rude", "scam",
        "cheat", "never again", "waste", "poor", "slow", "late",
        # Arabic common negatives (no translation available for in_app)
        "سيئ", "سيئة", "بخس", "غش", "زعل", "مشكلة", "اسوء", "فاشل", "لا استمتع",
    ]
    pos_words = [
        "good", "great", "excellent", "amazing", "love", "thank", "happy",
        "wonderful", "perfect", "best", "awesome", "nice", "friendly",
        "professional", "recommend",
    ]

    neg_score = sum(1 for w in neg_words if w in text_lower)
    pos_score = sum(1 for w in pos_words if w in text_lower)

    if stars is not None:
        try:
            s = float(stars)
            if s <= 2:
                neg_score += 2
            elif s >= 4:
                pos_score += 2
            elif s == 3:
                if neg_score > 0 and pos_score > 0:
                    return "Mixed"
        except (TypeError, ValueError):
            pass

    if neg_score > 0 and pos_score > 0:
        return "Mixed"
    if neg_score > pos_score:
        return "Negative"
    if pos_score > neg_score:
        return "Positive"
    if stars is not None:
        try:
            s = float(stars)
            if s <= 2:
                return "Negative"
            if s >= 4:
                return "Positive"
            return "Neutral"
        except (TypeError, ValueError):
            pass
    return "Neutral"


def match_themes(text, persona, ticket_category, theme_dict):
    matched = set()
    text_upper = (text or "").upper()
    text_lower = (text or "").lower()
    cat_lower = (ticket_category or "").lower()

    for theme, patterns in theme_dict.items():
        for pat in patterns:
            if re.search(pat, text_lower, re.I):
                matched.add(theme)
                break

    cat_map = ZENDESK_CATEGORY_MAP.get(persona, {})
    for key, theme in cat_map.items():
        if key in cat_lower:
            matched.add(theme)

    return matched


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


def _norm_row(row):
    """Normalize Snowflake uppercase keys to lowercase aliases."""
    return {
        "guid": row.get("GUID") or row.get("guid"),
        "feedback_source": row.get("FEEDBACK_SOURCE") or row.get("feedback_source"),
        "persona": row.get("PERSONA") or row.get("persona"),
        "ratings_comments": row.get("RATINGS_COMMENTS") or row.get("ratings_comments"),
        "text_for_analysis": row.get("TEXT_FOR_ANALYSIS") or row.get("text_for_analysis"),
        "overall_sentiment": row.get("OVERALL_SENTIMENT") or row.get("overall_sentiment"),
        "rating_stars": row.get("RATING_STARS") or row.get("rating_stars"),
        "city": row.get("CITY") or row.get("city"),
        "country": row.get("COUNTRY") or row.get("country"),
        "ticket_category": row.get("TICKET_CATEGORY") or row.get("ticket_category"),
        "category": row.get("CATEGORY") or row.get("category"),
        "urgency": row.get("URGENCY") or row.get("urgency"),
        "timestamp": row.get("TIMESTAMP") or row.get("timestamp"),
    }


def analyze(data):
    sample = [_norm_row(r) for r in data["sample"]]
    volumes_raw = data["volumes"]

    for row in sample:
        row["sentiment"] = classify_sentiment({
            "OVERALL_SENTIMENT": row["overall_sentiment"],
            "text_for_analysis": row["text_for_analysis"],
            "FEEDBACK_SOURCE": row["feedback_source"],
            "TICKET_CATEGORY": row["ticket_category"],
            "RATING_STARS": row["rating_stars"],
        })

    # Cell-level sentiment stats
    cell_stats = defaultdict(lambda: {"total": 0, "negative": 0})
    persona_stats = defaultdict(lambda: {"total": 0, "negative": 0, "mixed": 0})

    for row in sample:
        src = normalize_source(row["feedback_source"])
        persona = row["persona"]
        key = (src, persona)
        cell_stats[key]["total"] += 1
        persona_stats[persona]["total"] += 1
        if row["sentiment"] == "Negative":
            cell_stats[key]["negative"] += 1
            persona_stats[persona]["negative"] += 1
        if row["sentiment"] in ("Negative", "Mixed"):
            persona_stats[persona]["mixed"] += 1

    # Volume table (normalized)
    vol_table = defaultdict(lambda: {"driver": 0, "rider": 0})
    for v in volumes_raw:
        src = normalize_source(v["feedback_source"])
        vol_table[src][v["persona"]] = v["volume"]

    # Theme extraction
    driver_neg_mixed = [r for r in sample if r["persona"] == "driver" and r["sentiment"] in ("Negative", "Mixed")]
    rider_neg_mixed = [r for r in sample if r["persona"] == "rider" and r["sentiment"] in ("Negative", "Mixed")]

    driver_theme_counts = defaultdict(int)
    rider_theme_counts = defaultdict(int)

    for row in driver_neg_mixed:
        themes = match_themes(
            row.get("text_for_analysis"), "driver",
            row.get("ticket_category"), DRIVER_THEMES,
        )
        for t in themes:
            driver_theme_counts[t] += 1

    for row in rider_neg_mixed:
        themes = match_themes(
            row.get("text_for_analysis"), "rider",
            row.get("ticket_category"), RIDER_THEMES,
        )
        for t in themes:
            rider_theme_counts[t] += 1

    driver_sample_total = persona_stats["driver"]["total"]
    rider_sample_total = persona_stats["rider"]["total"]

    def theme_list(counts, denominator):
        ranked = sorted(counts.items(), key=lambda x: -x[1])
        result = []
        for theme, count in ranked[:7]:
            approx = denominator < 30
            result.append({
                "theme": theme,
                "count": count,
                "pct": pct(count, denominator, approximate=approx),
            })
        return result

    driver_themes = theme_list(driver_theme_counts, driver_sample_total)
    rider_themes = theme_list(rider_theme_counts, rider_sample_total)

    # Safety escalations
    safety_items = []
    for row in sample:
        if row["sentiment"] not in ("Negative", "Mixed"):
            continue
        text = (row.get("text_for_analysis") or "")[:200]
        text_lower = text.lower()
        for category, patterns in SAFETY_PATTERNS.items():
            for pat in patterns:
                if re.search(pat, text_lower, re.I):
                    city = row.get("city") or "Unknown city"
                    summary = f"{category} report in {city} — comment flagged from {normalize_source(row['feedback_source'])} feedback."
                    if len(text) > 20:
                        snippet = text[:120].strip()
                        if snippet:
                            summary = f"{snippet}… ({city})"
                    safety_items.append({"category": category, "summary": summary})
                    break

    # Dedupe similar safety items
    seen_cats = set()
    unique_safety = []
    for item in safety_items:
        if item["category"] not in seen_cats:
            seen_cats.add(item["category"])
            unique_safety.append(item)
        if len(unique_safety) >= 3:
            break

    # Build cell pct table
    table_rows = {}
    for src_name in ["Zendesk", "ride_rating", "in_app", "App stores"]:
        for persona in ["driver", "rider"]:
            key = (src_name, persona)
            stats = cell_stats.get(key, {"total": 0, "negative": 0})
            vol = vol_table[src_name][persona]
            neg_pct = pct(stats["negative"], stats["total"], approximate=stats["total"] < 30)
            table_rows[f"{src_name}_{persona}"] = {
                "volume": vol,
                "neg_pct": neg_pct if stats["total"] > 0 else ("n/a" if src_name == "App stores" else "n/a"),
            }

    # App stores always n/a for neg
    for persona in ["driver", "rider"]:
        key = ("App stores", persona)
        stats = cell_stats.get(key, {"total": 0, "negative": 0})
        vol = vol_table["App stores"][persona]
        table_rows[f"App stores_{persona}"] = {
            "volume": vol,
            "neg_pct": "n/a",
        }

    # in_app rider typically none
    if vol_table["in_app"]["rider"] == 0:
        table_rows["in_app_rider"] = {"volume": 0, "neg_pct": "n/a"}

    # Priorities from top themes
    priorities = []

    rider_top = rider_themes[0] if rider_themes else None
    driver_top = driver_themes[0] if driver_themes else None
    rider_second = rider_themes[1] if len(rider_themes) > 1 else None
    driver_second = driver_themes[1] if len(driver_themes) > 1 else None

    if rider_top and rider_top["theme"] == "Driver conduct":
        priorities.append({
            "title": "Driver conduct & rating quality",
            "evidence": f"Driver conduct ≈ {rider_top['pct']} of rider sample (ride_rating + Zendesk)",
            "action": "Audit low-rated driver cohorts; tighten post-ride conduct review and rider reporting flows.",
        })
    elif rider_top:
        priorities.append({
            "title": rider_top["theme"],
            "evidence": f"~{rider_top['pct']} of rider sample mentions this theme",
            "action": "Prioritize product and ops fixes for this rider pain point.",
        })

    if driver_top and "Cancellation" in driver_top["theme"]:
        priorities.append({
            "title": "Driver payout reliability / cancellation fees",
            "evidence": f"Cancellation fees + wallet issues ≈ {driver_top['pct']} of driver sample",
            "action": "Audit cancellation-fee crediting and wallet payout SLAs; fix stuck balances.",
        })
    elif driver_top:
        priorities.append({
            "title": driver_top["theme"],
            "evidence": f"~{driver_top['pct']} of driver sample mentions this theme",
            "action": "Review driver payout policies and in-app messaging on passenger conduct.",
        })

    if rider_second and "Cancellation" in rider_second["theme"]:
        priorities.append({
            "title": "Rider cancellation fairness",
            "evidence": f"Unfair cancellation fees ≈ {rider_second['pct']} of rider negatives",
            "action": "Review driver-initiated cancellation patterns and auto-waive fees when driver at fault.",
        })
    elif driver_second and "Payment" in driver_second["theme"]:
        priorities.append({
            "title": "Driver wallet & withdrawal reliability",
            "evidence": f"Payment not credited ≈ {driver_second['pct']} of driver sample",
            "action": "Expedite stuck withdrawals and improve wallet balance transparency.",
        })
    elif rider_second:
        priorities.append({
            "title": rider_second["theme"],
            "evidence": f"~{rider_second['pct']} of rider sample",
            "action": "Strengthen monitoring and escalation for this rider issue cluster.",
        })

    while len(priorities) < 3:
        for t in driver_themes + rider_themes:
            if not any(p["title"].startswith(t["theme"][:20]) for p in priorities):
                priorities.append({
                    "title": t["theme"],
                    "evidence": f"~{t['pct']} of sample",
                    "action": "Review ops escalation and product fixes for this theme.",
                })
                break
        else:
            break

    return {
        "report_date": data["report_date"],
        "report_day_name": data["report_day_name"],
        "total_comments": data["total_comments"],
        "sample_count": len(sample),
        "table_rows": table_rows,
        "driver_themes": driver_themes,
        "rider_themes": rider_themes,
        "safety_count": len(safety_items),
        "safety_examples": unique_safety[:3],
        "priorities": priorities[:3],
        "cell_stats": {f"{k[0]}_{k[1]}": v for k, v in cell_stats.items()},
    }


def main():
    data = json.load(sys.stdin)
    result = analyze(data)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
