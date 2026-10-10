#!/usr/bin/env python3
"""Voice of Customer daily fetch + analyze for canvas digest."""
import json
import os
import re
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import snowflake.connector

SAMPLE_PER_CELL = 100
VALID_SENTIMENTS = {"Positive", "Neutral", "Negative", "Mixed"}

ARABIC_NEG = re.compile(
    r"سيء|سيئ|رديء|فاشل|غش|نصب|سرق|كذب|وقح|زفت|تأخير|متأخر|رفض|إلغاء|الغاء|مشكلة|غلط|خطأ|زعل|غاضب",
    re.I,
)

# Theme patterns: (canonical_label, emoji, persona, patterns)
THEME_RULES = [
    ("Cancellation fees not received / unfair fines", "🚫", "driver", [
        r"cancel", r"cancellation", r"fine", r"penalty", r"fee not", r"unfair.*fine",
        r"إلغاء", r"غرام", r"عقوب",
    ]),
    ("Earnings & fares (low pay per km, weak offers)", "💰", "driver", [
        r"earn", r"low pay", r"per km", r"fare.*low", r"commission", r"offer",
        r"ربح", r"أرباح", r"عمول", r"سعر.*قليل",
    ]),
    ("Passenger conduct", "😡", "driver", [
        r"passenger.*(rude|bad|drunk|smok)", r"rider.*(rude|bad)", r"conduct",
        r"زبون.*(سيء|وقح)", r"راكب",
    ]),
    ("Passenger didn't pay (cash)", "💵", "driver", [
        r"didn.?t pay", r"not pay", r"cash.*(refus|didn)", r"unpaid",
        r"لم يدفع", r"ما دفع",
    ]),
    ("Fare/charge accuracy", "📄", "driver", [
        r"wrong (fare|charge|amount)", r"fare.*(wrong|incorrect)", r"charge.*wrong",
    ]),
    ("Payment not credited / withdrawals", "🏦", "driver", [
        r"withdraw", r"wallet", r"not credited", r"payout", r"transfer",
        r"سحب", r"محفظ",
    ]),
    ("Overcharging / fare disputes", "💸", "rider", [
        r"overcharg", r"too (much|expensive)", r"fare.*(high|wrong|dispute)", r"charged.*more",
        r"غش", r"نصب", r"زيادة.*سعر",
    ]),
    ("Driver conduct", "😡", "rider", [
        r"driver.*(rude|bad|angry|yell|shout|abusive)", r"rude driver", r"conduct",
        r"سائق.*(سيء|وقح|غاضب)",
    ]),
    ("Wrong driver / car vs app", "🚙", "rider", [
        r"wrong (driver|car|vehicle)", r"different (car|driver)", r"plate", r"identity",
        r"سيارة.*غلط", r"سائق.*غير",
    ]),
    ("Route manipulation / slow driving", "🗺️", "rider", [
        r"route", r"detour", r"longer", r"slow", r"manipul", r"gps",
        r"طريق", r"تأخير",
    ]),
    ("No-show / wrong pickup", "📍", "rider", [
        r"no.?show", r"didn.?t (come|arrive|pick)", r"wrong pickup", r"waited",
        r"ما جاء", r"لم يأت",
    ]),
    ("Cancellation (unfair fees, driver-initiated)", "🚫", "rider", [
        r"cancel", r"cancellation", r"driver cancel", r"unfair.*fee",
        r"إلغاء",
    ]),
    ("Payment & charging errors", "🏦", "rider", [
        r"payment", r"charged twice", r"double charge", r"card", r"wallet",
        r"دفع", r"خصم",
    ]),
    ("Vehicle smell / cleanliness", "🧼", "rider", [
        r"smell", r"dirty", r"clean", r"hygien", r"odor",
        r"رائحة", r"نظاف",
    ]),
]

ZENDESK_NEUTRAL_CATS = re.compile(
    r"city update|work permit|lost item|general inquiry|information|account update",
    re.I,
)

SAFETY_PATTERNS = [
    ("Harassment", re.compile(
        r"harass|assault|held hand|threaten|hit|insult|stalk|repeated call|after ride|"
        r"تحرش|اعتداء|تهديد|ضرب",
        re.I,
    )),
    ("Reckless driving", re.compile(
        r"reckless|injury|child injured|accident|speed|dangerous drive|"
        r"حادث|إصاب|سرعة جنون",
        re.I,
    )),
    ("Identity mismatch", re.compile(
        r"wrong driver.*(safety|scared|afraid)|identity|not the (driver|car).*scared|"
        r"سائق.*مختلف.*خوف",
        re.I,
    )),
]


def connect():
    conn = snowflake.connector.connect(
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PAT"],
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ.get("SNOWFLAKE_DATABASE"),
        role=os.environ.get("SNOWFLAKE_ROLE"),
    )
    cur = conn.cursor()
    cur.execute(f"USE WAREHOUSE {os.environ['SNOWFLAKE_WAREHOUSE']}")
    return conn, cur


def normalize_source(raw: str) -> str:
    if raw in ("Google Store", "Apple Store"):
        return "App stores"
    return raw


def classify_sentiment(row: dict) -> str:
    existing = (row.get("OVERALL_SENTIMENT") or "").strip()
    if existing in VALID_SENTIMENTS:
        return existing

    text = (row.get("TEXT_FOR_ANALYSIS") or "").strip()
    stars = row.get("RATING_STARS")
    source = row.get("FEEDBACK_SOURCE") or ""
    ticket_cat = row.get("TICKET_CATEGORY") or ""

    if source == "Zendesk":
        if ZENDESK_NEUTRAL_CATS.search(ticket_cat or ""):
            return "Neutral"
        return "Negative"

    if not text or len(text) < 3:
        return "Unclear"

    tl = text.lower()
    neg_words = (
        "bad", "terrible", "awful", "worst", "hate", "angry", "frustrat", "complain",
        "scam", "cheat", "rude", "never again", "disappoint", "horrible", "refund",
        "unfair", "problem", "issue", "wrong", "late", "slow", "dirty", "smell",
    )
    pos_words = (
        "good", "great", "excellent", "thank", "love", "best", "awesome", "perfect",
        "happy", "nice", "wonderful", "appreciate", "5 star", "recommend",
    )
    neg = sum(1 for w in neg_words if w in tl) + (1 if ARABIC_NEG.search(text) else 0)
    pos = sum(1 for w in pos_words if w in tl)

    if stars is not None:
        try:
            s = float(stars)
            if s <= 2 and neg >= pos:
                return "Negative"
            if s >= 4 and pos >= neg:
                return "Positive"
            if s == 3:
                if neg and pos:
                    return "Mixed"
                return "Neutral"
        except (TypeError, ValueError):
            pass

    if neg and pos:
        return "Mixed"
    if neg:
        return "Negative"
    if pos:
        return "Positive"
    if len(text) < 8:
        return "Unclear"
    return "Neutral"


def match_themes(text: str, persona: str) -> list[str]:
    if not text:
        return []
    labels = []
    for label, _emoji, p, patterns in THEME_RULES:
        if p != persona:
            continue
        for pat in patterns:
            if re.search(pat, text, re.I):
                labels.append(label)
                break
    return labels


def anonymize_summary(text: str, category: str) -> str:
    t = text or ""
    t = re.sub(r"^[a-z_]+ :: ", "", t, flags=re.I)
    t = re.sub(r"[-_]+", " ", t)
    t = re.sub(r"\b\d{7,}\b", "[redacted]", t)
    t = re.sub(r"@[\w.]+", "[user]", t)
    t = re.sub(r"\s+", " ", t).strip()
    if len(t) > 120:
        t = t[:117] + "..."
    if len(t) < 20 or re.match(r"^[\W\d]+$", t):
        t = f"Customer reported {category.lower()} during trip; details withheld."
    return t


def pct_label(n: int, denom: int) -> str:
    if denom == 0:
        return "n/a"
    p = round(100 * n / denom)
    prefix = "~" if denom < 30 else "~"
    return f"{prefix}{p}%"


def main():
    now_riyadh = datetime.now(ZoneInfo("Asia/Riyadh"))
    report_date = now_riyadh.date() - timedelta(days=1)
    start_ts = report_date.isoformat()
    day_name = report_date.strftime("%a %d %b %Y")

    conn, cur = connect()

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
          WHERE TIMESTAMP >= %s
            AND TIMESTAMP < DATEADD(day, 1, %s::DATE)
            AND RATINGS_COMMENTS IS NOT NULL
            AND TRIM(RATINGS_COMMENTS) <> ''
            AND LENGTH(TRIM(RATINGS_COMMENTS)) >= 3
        )
        SELECT FEEDBACK_SOURCE, persona, COUNT(*) AS volume
        FROM base
        WHERE persona IN ('driver', 'rider')
        GROUP BY 1, 2
        ORDER BY volume DESC
        """,
        (start_ts, start_ts),
    )
    volume_rows = cur.fetchall()

    cur.execute(
        """
        SELECT COUNT(*) FROM jeeny_prod.general.feedbackevents
        WHERE TIMESTAMP >= %s
          AND TIMESTAMP < DATEADD(day, 1, %s::DATE)
          AND RATINGS_COMMENTS IS NOT NULL
          AND TRIM(RATINGS_COMMENTS) <> ''
          AND LENGTH(TRIM(RATINGS_COMMENTS)) >= 3
        """,
        (start_ts, start_ts),
    )
    total_comments = cur.fetchone()[0]

    cur.execute(
        f"""
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
          WHERE TIMESTAMP >= %s
            AND TIMESTAMP < DATEADD(day, 1, %s::DATE)
            AND RATINGS_COMMENTS IS NOT NULL
            AND TRIM(RATINGS_COMMENTS) <> ''
            AND LENGTH(TRIM(RATINGS_COMMENTS)) >= 3
        )
        SELECT GUID, FEEDBACK_SOURCE, persona, RATINGS_COMMENTS, text_for_analysis,
               OVERALL_SENTIMENT, RATING_STARS, CITY, COUNTRY, TICKET_CATEGORY,
               CATEGORY, URGENCY, TIMESTAMP
        FROM base
        WHERE persona IN ('driver', 'rider') AND rn <= {SAMPLE_PER_CELL}
        """,
        (start_ts, start_ts),
    )
    cols = [d[0] for d in cur.description]
    sample = [dict(zip(cols, r)) for r in cur.fetchall()]
    cur.close()
    conn.close()

    # Volume table: digest sources
    vol = defaultdict(lambda: {"driver": 0, "rider": 0})
    for src, persona, volume in volume_rows:
        ds = normalize_source(src)
        if persona in ("driver", "rider"):
            vol[ds][persona] += volume

    digest_sources = ["Zendesk", "ride_rating", "in_app", "App stores"]

    analyzed = []
    for row in sample:
        row["TEXT_FOR_ANALYSIS"] = row.pop("TEXT_FOR_ANALYSIS", None) or row.get("TEXT_FOR_ANALYSIS")
        # snowflake returns lowercase keys sometimes
        key_map = {k.upper(): k for k in row}
        def g(name):
            return row.get(key_map.get(name, name), row.get(name.lower()))

        rec = {
            "FEEDBACK_SOURCE": g("FEEDBACK_SOURCE"),
            "persona": g("PERSONA") if g("PERSONA") else row.get("persona"),
            "TEXT_FOR_ANALYSIS": g("TEXT_FOR_ANALYSIS"),
            "OVERALL_SENTIMENT": g("OVERALL_SENTIMENT"),
            "RATING_STARS": g("RATING_STARS"),
            "TICKET_CATEGORY": g("TICKET_CATEGORY"),
            "RATINGS_COMMENTS": g("RATINGS_COMMENTS"),
            "CITY": g("CITY"),
        }
        rec["sentiment"] = classify_sentiment(rec)
        rec["digest_source"] = normalize_source(rec["FEEDBACK_SOURCE"] or "")
        analyzed.append(rec)

    sample_count = len(analyzed)

    # %neg per digest_source x persona
    cell_stats = defaultdict(lambda: {"neg": 0, "analyzed": 0})
    for r in analyzed:
        key = (r["digest_source"], r["persona"])
        if r["sentiment"] in ("Negative", "Mixed"):
            cell_stats[key]["neg"] += 1
        if r["sentiment"] != "Unclear":
            cell_stats[key]["analyzed"] += 1

    def cell_pct(ds, persona):
        st = cell_stats.get((ds, persona), {"neg": 0, "analyzed": 0})
        if st["analyzed"] == 0:
            return "n/a"
        return pct_label(st["neg"], st["analyzed"]).replace("~", "~")

    def vol_cell(ds, persona):
        v = vol.get(ds, {}).get(persona, 0)
        pct = cell_pct(ds, persona)
        if ds == "App stores":
            return f"{v} · n/a"
        if v == 0:
            return "0 · n/a"
        return f"{v} · {pct}"

    # Themes
    theme_counts = {"driver": Counter(), "rider": Counter()}
    theme_emojis = {}
    neg_mix_denom = {"driver": 0, "rider": 0}
    for r in analyzed:
        p = r["persona"]
        if r["sentiment"] not in ("Negative", "Mixed"):
            continue
        neg_mix_denom[p] += 1
        text = (r.get("TEXT_FOR_ANALYSIS") or "") + " " + (r.get("TICKET_CATEGORY") or "")
        for label, emoji, tp, _ in THEME_RULES:
            if tp != p:
                continue
        matched = match_themes(text, p)
        for label in matched:
            theme_counts[p][label] += 1
            for lbl, em, tp, _ in THEME_RULES:
                if lbl == label:
                    theme_emojis[label] = em

    def top_themes(persona, n=6):
        denom = neg_mix_denom[persona] or 1
        items = theme_counts[persona].most_common(n)
        out = []
        for label, cnt in items:
            em = theme_emojis.get(label, "•")
            pct = round(100 * cnt / denom)
            out.append((label, em, pct))
        return out

    # Safety
    SAFETY_GENERIC = {
        "Harassment": "Customer reports verbal harassment or threats during or after the trip.",
        "Reckless driving": "Rider reports reckless or dangerous driving during the trip.",
        "Identity mismatch": "Rider reports vehicle or driver did not match the app profile.",
    }
    safety_hits = []
    for r in analyzed:
        if r["sentiment"] not in ("Negative", "Mixed"):
            continue
        text = r.get("TEXT_FOR_ANALYSIS") or ""
        for cat, pat in SAFETY_PATTERNS:
            if pat.search(text):
                city = r.get("CITY")
                loc = f" ({city})" if city else ""
                safety_hits.append((cat, SAFETY_GENERIC[cat] + loc, r.get("CITY")))
                break

    # Priorities (heuristic from top themes)
    driver_top = top_themes("driver", 3)
    rider_top = top_themes("rider", 3)
    priorities = []
    if rider_top:
        l, _, p = rider_top[0]
        title = l.split(" /")[0]
        action = (
            "Clarify cancellation policy in-app, auto-waive disputed driver-initiated cancels, "
            "and coach captains on accept/complete behavior."
            if "Cancellation" in l
            else "Improve payment retry logic, receipt clarity, and wallet dispute handling."
        )
        priorities.append(
            f"1. **{title}** — ~{p}% of rider negatives/mixed in sample. {action}"
        )
    if driver_top:
        l, _, p = driver_top[0]
        title = l.split("(")[0].strip()
        if "pay" in l.lower() and "passenger" in l.lower():
            action = (
                "Enforce cash-payment confirmation flows and faster support on unpaid-ride reports."
            )
        elif "conduct" in l.lower():
            action = "Strengthen reporting, coaching, and suspension workflows for passenger behavior."
        elif "cancellation" in l.lower():
            action = "Auto-credit eligible cancellation fees and show payout status in the driver wallet."
        else:
            action = "Review fare bands, surge transparency, and payout SLAs."
        priorities.append(
            f"{len(priorities)+1}. **{title}** — ~{p}% of driver negatives/mixed. {action}"
        )
    if safety_hits:
        priorities.append(
            f"{len(priorities)+1}. **Safety escalation review** — {len(safety_hits)} flagged in sample. "
            f"Fast-track trust & safety triage for harassment, reckless driving, and identity mismatch."
        )
    while len(priorities) < 3:
        priorities.append(
            f"{len(priorities)+1}. **Support quality** — Zendesk volumes remain complaint-heavy; "
            f"reduce repeat contacts via proactive status updates on payouts and fare adjustments."
        )

    # Build digest markdown
    lines = [
        f"## 📊 {day_name}",
        "",
        f"{total_comments} comments · stratified sample ~{sample_count} analyzed (up to 100 per source×user-type cell). "
        "Zendesk = support tickets (complaint-only by nature, so its %neg is always very high); "
        "ride_rating is the truer sentiment gauge.",
        "",
        "|Source|Driver (vol · %neg)|Rider (vol · %neg)|",
        "|  ---  |  ---  |  ---  |",
        f"|Zendesk|{vol_cell('Zendesk', 'driver')}|{vol_cell('Zendesk', 'rider')}|",
        f"|ride_rating|{vol_cell('ride_rating', 'driver')}|{vol_cell('ride_rating', 'rider')}|",
        f"|in_app|{vol_cell('in_app', 'driver')}|—|",
        f"|App stores|{vol_cell('App stores', 'driver')}|{vol_cell('App stores', 'rider')}|",
        "",
        "### 🏎️ Drivers — top issues",
        "",
        "*(% of driver sample)*",
        "",
    ]
    for label, em, pct in top_themes("driver", 6):
        lines.append(f"* {em} {label} — **~{pct}%**")
    if not top_themes("driver"):
        lines.append("* No dominant driver themes in sample.")

    lines += ["", "### 🧍 Riders — top issues", "", "*(% of rider sample)*", ""]
    for label, em, pct in top_themes("rider", 7):
        lines.append(f"* {em} {label} — **~{pct}%**")
    if not top_themes("rider"):
        lines.append("* No dominant rider themes in sample.")

    lines += ["", f"### ⚠️ Safety escalations (~{len(safety_hits)} flagged)", ""]
    shown = 0
    for cat, summary, _city in safety_hits[:3]:
        em = "🆘" if "Harass" in cat else "🏎️" if "Reckless" in cat else "🚙"
        lines.append(f"* {em} **{cat}** — {summary}")
        shown += 1
    if shown == 0:
        lines.append("* No safety-keyword escalations in today's sample.")

    lines += ["", "### 🎯 Priorities", ""]
    for i, pr in enumerate(priorities[:3], 1):
        if pr[0].isdigit():
            lines.append(pr)
        else:
            lines.append(f"{i}. {pr}")

    lines.append("")
    lines.append("---")

    digest = "\n".join(lines)

    out = {
        "report_date": str(report_date),
        "day_name": day_name,
        "total_comments": total_comments,
        "sample_count": sample_count,
        "digest": digest,
        "priorities_short": [p.split("—")[0].replace("1. ", "").replace("2. ", "").replace("3. ", "") for p in priorities[:3]],
        "volume": dict(vol),
        "safety_count": len(safety_hits),
    }
    print(json.dumps(out, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
