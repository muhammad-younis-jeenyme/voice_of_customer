#!/usr/bin/env python3
"""Voice of Customer daily digest — Snowflake pull + sentiment/theme analysis."""

import json
import os
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import snowflake.connector

SAMPLE_PER_CELL = 100
REPORT_TZ = ZoneInfo("Asia/Riyadh")

# Zendesk categories treated as neutral/ops (not auto-negative)
ZENDESK_NEUTRAL_CATEGORIES = {
    "city update", "work permit", "lost item", "lost items", "found item",
    "general inquiry", "account update", "document", "registration",
}

# Theme keyword patterns (applied to text_for_analysis, lowercased)
DRIVER_THEMES = {
    "Passenger conduct": [
        r"\b(rude|abusive|aggressive|disrespect|insult|scream|fight|argu)\w*",
        r"\b(passenger|customer|client).{0,30}(rude|bad|terrible|awful)",
        r"\b(behavio|conduct|attitude)\w*",
    ],
    "Passenger didn't pay (cash)": [
        r"\b(didn'?t|not|never|refus\w*|fail\w*)\s+pay",
        r"\b(cash|money).{0,20}(didn'?t|not|refus|without)",
        r"\bnon.?payment|unpaid|stiff",
    ],
    "Earnings & fares (low pay per km, weak offers)": [
        r"\b(low|less|poor|bad|weak|unfair)\s+(pay|fare|earning|income|offer|rate|price)",
        r"\b(earning|fare|pay|income|offer).{0,25}(low|less|poor|bad|weak|unfair)",
        r"\bper\s*km|per\s*kilometer|commission",
        r"\b(offer|trip).{0,20}(far|long|distance|reject)",
    ],
    "Cancellation fees not received / unfair fines": [
        r"\b(cancel\w*|cancellation).{0,30}(fee|fine|penalt|charg|deduct)",
        r"\b(fee|fine|penalt).{0,20}(cancel|unfair|wrong|not\s+receiv)",
        r"\bunfair\s+(fine|fee|penalt)",
    ],
    "Fare/charge accuracy": [
        r"\b(wrong|incorrect|error|mismatch).{0,20}(fare|charge|amount|bill|price)",
        r"\b(fare|charge|amount|bill).{0,20}(wrong|incorrect|error|mismatch)",
    ],
    "Payment not credited / withdrawals": [
        r"\b(withdraw|wallet|credit|payout|transfer|balance).{0,30}(not|fail|delay|stuck|missing)",
        r"\b(not|fail|delay|stuck|missing).{0,20}(withdraw|wallet|credit|payout|transfer|balance)",
    ],
}

RIDER_THEMES = {
    "Overcharging / fare disputes": [
        r"\b(overcharg|over.?charg|extra\s+charg|too\s+much|inflat\w*|rip.?off)",
        r"\b(fare|price|amount|bill|charg\w*).{0,25}(high|expensive|wrong|more|extra|increas)",
        r"\bcharged?\s+(more|extra|double|twice)",
    ],
    "Driver conduct": [
        r"\b(driver|captain).{0,30}(rude|bad|terrible|awful|aggressive|abusive|disrespect|insult)",
        r"\b(rude|abusive|aggressive|disrespect|insult|scream|fight|argu)\w*.{0,30}(driver|captain)",
        r"\b(behavio|conduct|attitude)\w*",
        r"\b(smok\w*|phone|texting|eating).{0,20}(driver|while\s+driv)",
    ],
    "Wrong driver / car vs app": [
        r"\b(wrong|different|another|mismatch).{0,25}(driver|car|vehicle|plate|photo|picture)",
        r"\b(driver|car|vehicle|plate).{0,25}(wrong|different|another|mismatch|not\s+match)",
        r"\bnot\s+(the\s+)?(same|match)",
    ],
    "Route manipulation / slow driving": [
        r"\b(longer|detour|extra\s+km|route|manipulat|circulat|roundabout)",
        r"\b(slow|delay|stuck|traffic).{0,20}(driver|driv|intentional)",
        r"\b(took\s+long|long\s+way|unnecessary)",
    ],
    "No-show / wrong pickup": [
        r"\b(no.?show|didn'?t\s+(come|arrive|show)|never\s+came|waited)",
        r"\b(wrong|incorrect)\s+(pickup|location|address|pin)",
        r"\b(pickup|location).{0,20}(wrong|incorrect)",
    ],
    "Cancellation (unfair fees, driver-initiated)": [
        r"\b(driver|captain).{0,25}(cancel)",
        r"\b(cancel\w*|cancellation).{0,30}(fee|fine|unfair|charg|driver)",
        r"\bunfair\s+(cancel|fee|fine)",
    ],
    "Payment & charging errors": [
        r"\b(payment|pay|card|wallet|charge|debit|credit).{0,30}(fail|error|wrong|double|twice|deduct)",
        r"\b(double|twice).{0,15}(charg|deduct|bill)",
    ],
    "Vehicle smell / cleanliness": [
        r"\b(dirty|smell|odor|odour|filthy|unclean|hygien)",
        r"\b(clean|cleanliness|sanit).{0,20}(bad|poor|terrible)",
    ],
}

SAFETY_PATTERNS = {
    "Harassment": [
        r"\b(harass|assault|molest|touch|grab|held\s+hand|threaten|stalk|inappropriat)",
        r"\b(insult|abuse|verbal).{0,20}(sexual|physical)",
        r"\b(repeated|keep).{0,15}(call|message|contact).{0,20}(after)",
    ],
    "Reckless driving": [
        r"\b(reckless|dangerous|speed|accident|crash|injur|hurt|child)",
        r"\b(driv\w*).{0,20}(fast|dangerous|reckless|crazy)",
    ],
    "Identity mismatch": [
        r"\b(wrong|different|another).{0,20}(driver|car|vehicle).{0,30}(safety|scared|afraid|danger)",
        r"\b(not\s+match|mismatch|fake|impersonat).{0,30}(driver|car|vehicle)",
        r"\b(safety|scared|afraid|danger).{0,30}(driver|car|vehicle|match)",
    ],
}

VALID_SENTIMENTS = {"Positive", "Negative", "Neutral", "Mixed", "Unclear"}


def connect():
    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PAT"],
        role=os.environ.get("SNOWFLAKE_ROLE", "ROLE_PRODUCT"),
        database=os.environ.get("SNOWFLAKE_DATABASE", "JEENY_PROD"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
    )
    cur = conn.cursor()
    cur.execute("USE WAREHOUSE COMPUTE_WH")
    return conn, cur


def normalize_source(src):
    if src in ("Google Store", "Apple Store"):
        return "App stores"
    return src


def classify_sentiment(row):
  """Classify sentiment for a single row."""
  existing = row.get("OVERALL_SENTIMENT")
  if existing and str(existing).strip() in VALID_SENTIMENTS - {"Unclear"}:
      return str(existing).strip()

  text = (row.get("TEXT_FOR_ANALYSIS") or "").lower()
  source = row.get("FEEDBACK_SOURCE", "")
  stars = row.get("RATING_STARS")
  ticket_cat = (row.get("TICKET_CATEGORY") or "").lower()

  # Zendesk default negative unless neutral category
  if source == "Zendesk":
      if any(nc in ticket_cat for nc in ZENDESK_NEUTRAL_CATEGORIES):
          return "Neutral"
      return "Negative"

  if not text or len(text.strip()) < 3:
      return "Unclear"

  pos_words = len(re.findall(
      r"\b(good|great|excellent|amazing|wonderful|thank|love|best|perfect|nice|happy|satisfied|awesome|fantastic)\w*",
      text,
  ))
  neg_words = len(re.findall(
      r"\b(bad|terrible|awful|horrible|worst|hate|angry|frustrat|disappoint|complain|poor|unfair|wrong|never\s+again|disgust)\w*",
      text,
  ))

  if stars is not None:
      try:
          s = float(stars)
          if s <= 2:
              if pos_words > 0 and neg_words > 0:
                  return "Mixed"
              return "Negative"
          if s == 3:
              if neg_words > pos_words:
                  return "Mixed"
              return "Neutral"
          if s >= 4:
              if neg_words > 0:
                  return "Mixed"
              return "Positive"
      except (TypeError, ValueError):
          pass

  if pos_words > 0 and neg_words > 0:
      return "Mixed"
  if neg_words > pos_words:
      return "Negative"
  if pos_words > neg_words:
      return "Positive"
  if neg_words > 0:
      return "Negative"
  return "Neutral"


def match_themes(text, theme_dict):
  """Return set of matched theme labels."""
  if not text:
      return set()
  t = text.lower()
  matched = set()
  for label, patterns in theme_dict.items():
      for pat in patterns:
          if re.search(pat, t, re.I):
              matched.add(label)
              break
  # Zendesk ticket category hints
  return matched


def zendesk_category_theme(ticket_cat, persona):
  """Map Zendesk TICKET_CATEGORY to canonical theme."""
  if not ticket_cat:
      return set()
  tc = ticket_cat.lower()
  themes = set()
  mapping = {
      "overcharg": "Overcharging / fare disputes",
      "fare": "Overcharging / fare disputes",
      "payment": "Payment & charging errors",
      "cancel": "Cancellation (unfair fees, driver-initiated)" if persona == "rider" else "Cancellation fees not received / unfair fines",
      "driver conduct": "Driver conduct",
      "passenger conduct": "Passenger conduct",
      "route": "Route manipulation / slow driving",
      "wrong driver": "Wrong driver / car vs app",
      "vehicle": "Vehicle smell / cleanliness",
      "earning": "Earnings & fares (low pay per km, weak offers)",
      "safety": "Harassment",
      "harass": "Harassment",
  }
  for key, theme in mapping.items():
      if key in tc:
          themes.add(theme)
  return themes


def pct_label(n, denom):
  if denom == 0:
      return "n/a"
  p = round(100 * n / denom)
  prefix = "~" if denom < 30 else ""
  return f"{prefix}{p}%"


def theme_pct(theme_counts, persona_sample_size):
  results = []
  for theme, count in theme_counts.most_common():
      pct = round(100 * count / persona_sample_size) if persona_sample_size else 0
      results.append((theme, pct, count))
  return results


def anonymize_summary(text, category):
  """One-line anonymized safety summary — never expose raw comment text."""
  defaults = {
      "Harassment": "Rider reported inappropriate and harassing driver behavior during trip",
      "Reckless driving": "Rider reported dangerous driving, speeding, and felt unsafe during trip",
      "Identity mismatch": "Rider reported driver or vehicle did not match app booking details",
  }
  return defaults.get(category, f"{category} incident reported during trip")


def run(report_date: str):
  start_ts = f"{report_date} 00:00:00"
  end_date = (datetime.strptime(report_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
  end_ts = f"{end_date} 00:00:00"

  conn, cur = connect()

  volume_sql = """
  WITH base AS (
    SELECT
      FEEDBACK_SOURCE,
      CASE
        WHEN USER_TYPE = 'driver' THEN 'driver'
        WHEN USER_TYPE IN ('passenger', 'customer') THEN 'rider'
        ELSE 'other'
      END AS persona,
      RATINGS_COMMENTS
    FROM jeeny_prod.general.feedbackevents
    WHERE TIMESTAMP >= %s
      AND TIMESTAMP < %s
      AND RATINGS_COMMENTS IS NOT NULL
      AND TRIM(RATINGS_COMMENTS) <> ''
      AND LENGTH(TRIM(RATINGS_COMMENTS)) >= 3
  )
  SELECT FEEDBACK_SOURCE, persona, COUNT(*) AS volume
  FROM base
  WHERE persona IN ('driver', 'rider')
  GROUP BY 1, 2
  ORDER BY volume DESC
  """

  total_sql = """
  SELECT COUNT(*) AS total_comments
  FROM jeeny_prod.general.feedbackevents
  WHERE TIMESTAMP >= %s
    AND TIMESTAMP < %s
    AND RATINGS_COMMENTS IS NOT NULL
    AND TRIM(RATINGS_COMMENTS) <> ''
    AND LENGTH(TRIM(RATINGS_COMMENTS)) >= 3
  """

  sample_sql = f"""
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
      AND TIMESTAMP < %s
      AND RATINGS_COMMENTS IS NOT NULL
      AND TRIM(RATINGS_COMMENTS) <> ''
      AND LENGTH(TRIM(RATINGS_COMMENTS)) >= 3
  )
  SELECT *
  FROM base
  WHERE persona IN ('driver', 'rider')
    AND rn <= {SAMPLE_PER_CELL}
  """

  cur.execute(volume_sql, (start_ts, end_ts))
  volume_rows = cur.fetchall()
  vol_cols = [d[0] for d in cur.description]

  cur.execute(total_sql, (start_ts, end_ts))
  total_comments = cur.fetchone()[0]

  cur.execute(sample_sql, (start_ts, end_ts))
  sample_cols = [d[0] for d in cur.description]
  sample_rows = [dict(zip(sample_cols, r)) for r in cur.fetchall()]
  conn.close()

  # Normalize volume
  vol = defaultdict(lambda: {"driver": 0, "rider": 0})
  for row in volume_rows:
      d = dict(zip(vol_cols, row))
      src = normalize_source(d["FEEDBACK_SOURCE"])
      vol[src][d["PERSONA"]] += d["VOLUME"]

  # Sentiment per cell
  cells = defaultdict(list)
  for row in sample_rows:
      row["TEXT_FOR_ANALYSIS"] = row.pop("TEXT_FOR_ANALYSIS", row.get("TEXT_FOR_ANALYSIS"))
      src = normalize_source(row["FEEDBACK_SOURCE"])
      persona = row["PERSONA"]
      sentiment = classify_sentiment({
          "OVERALL_SENTIMENT": row.get("OVERALL_SENTIMENT"),
          "TEXT_FOR_ANALYSIS": row.get("TEXT_FOR_ANALYSIS"),
          "FEEDBACK_SOURCE": row.get("FEEDBACK_SOURCE"),
          "RATING_STARS": row.get("RATING_STARS"),
          "TICKET_CATEGORY": row.get("TICKET_CATEGORY"),
      })
      row["sentiment"] = sentiment
      cells[(src, persona)].append(row)

  def cell_neg_pct(src, persona):
      rows = cells.get((src, persona), [])
      if not rows:
          return "n/a", 0
      analyzed = [r for r in rows if r["sentiment"] != "Unclear"]
      neg = sum(1 for r in analyzed if r["sentiment"] in ("Negative", "Mixed"))
      return pct_label(neg, len(analyzed)), len(analyzed)

  # Themes from negative/mixed in sample
  driver_theme_counts = Counter()
  rider_theme_counts = Counter()
  driver_sample = [r for r in sample_rows if r["PERSONA"] == "driver"]
  rider_sample = [r for r in sample_rows if r["PERSONA"] == "rider"]

  safety_flags = []

  for row in sample_rows:
      if row["sentiment"] not in ("Negative", "Mixed"):
          continue
      text = row.get("TEXT_FOR_ANALYSIS") or ""
      persona = row["PERSONA"]
      themes = match_themes(text, DRIVER_THEMES if persona == "driver" else RIDER_THEMES)
      if row.get("FEEDBACK_SOURCE") == "Zendesk":
          themes |= zendesk_category_theme(row.get("TICKET_CATEGORY"), persona)
      counter = driver_theme_counts if persona == "driver" else rider_theme_counts
      for th in themes:
          counter[th] += 1

      # Safety scan
      for cat, patterns in SAFETY_PATTERNS.items():
          for pat in patterns:
              if re.search(pat, text, re.I):
                  safety_flags.append({
                      "category": cat,
                      "summary": anonymize_summary(text, cat),
                      "persona": persona,
                  })
                  break

  driver_themes = theme_pct(driver_theme_counts, len(driver_sample))
  rider_themes = theme_pct(rider_theme_counts, len(rider_sample))

  # Keep up to 3 safety examples (allow same category with distinct incidents)
  unique_safety = safety_flags[:3]

  # Priorities
  top_rider = rider_themes[:3]
  top_driver = driver_themes[:3]
  priorities = []
  if top_rider:
      t, p, _ = top_rider[0]
      priorities.append(
          f"1. **{t}** — ~{p}% of rider sample; spans ride_rating and Zendesk. "
          f"Audit fare integrity, driver matching, and support routing for this theme."
      )
  if len(top_rider) > 1:
      t, p, _ = top_rider[1]
      priorities.append(
          f"2. **{t}** — ~{p}% of rider negatives. "
          f"Strengthen detection and rider refund/credit workflows."
      )
  elif top_driver:
      t, p, _ = top_driver[0]
      priorities.append(
          f"2. **{t}** — ~{p}% of driver sample. "
          f"Review policy and payout accuracy for driver-facing fixes."
      )
  if len(top_rider) > 2:
      t, p, _ = top_rider[2]
      priorities.append(
          f"3. **{t}** — ~{p}% of rider sample. "
          f"Prioritize product fixes and ops playbooks for repeat complaints."
      )
  elif top_driver and len(top_driver) > 1:
      t, p, _ = top_driver[1]
      priorities.append(
          f"3. **{t}** — ~{p}% of driver sample. "
          f"Address earnings transparency and cancellation fee policies."
      )
  elif len(unique_safety) > 0:
      priorities.append(
          f"3. **Safety response** — {len(safety_flags)} safety-related flags in sample. "
          f"Escalate harassment and identity-mismatch cases to trust & safety review."
      )

  while len(priorities) < 3:
      priorities.append(
          f"{len(priorities)+1}. **Monitor volume trends** — Continue daily VoC tracking and alert on spikes."
      )

  # Emoji map
  emoji_map = {
      "Cancellation fees not received / unfair fines": "🚫",
      "Earnings & fares (low pay per km, weak offers)": "💰",
      "Passenger conduct": "😡",
      "Driver conduct": "😡",
      "Passenger didn't pay (cash)": "💵",
      "Fare/charge accuracy": "📄",
      "Payment not credited / withdrawals": "🏦",
      "Overcharging / fare disputes": "💸",
      "Wrong driver / car vs app": "🚙",
      "Route manipulation / slow driving": "🗺️",
      "No-show / wrong pickup": "📍",
      "Cancellation (unfair fees, driver-initiated)": "🚫",
      "Payment & charging errors": "🏦",
      "Vehicle smell / cleanliness": "🧼",
      "Harassment": "🆘",
      "Reckless driving": "🏎️",
      "Identity mismatch": "🚙",
  }

  safety_emoji = {"Harassment": "🆘", "Reckless driving": "🏎️", "Identity mismatch": "🚙"}

  day_name = datetime.strptime(report_date, "%Y-%m-%d").strftime("%a %d %b %Y")

  def fmt_vol(src, persona):
      v = vol[src][persona]
      pct, _ = cell_neg_pct(src, persona)
      if src == "App stores" and persona == "driver" and v == 0:
          return "0 · n/a"
      if v == 0:
          return "0 · n/a"
      return f"{v} · {pct}"

  def theme_lines(themes, n=6):
      lines = []
      for theme, pct, _ in themes[:n]:
          em = emoji_map.get(theme, "•")
          lines.append(f"* {em} {theme} — **~{pct}%**")
      return "\n".join(lines) if lines else "* _No dominant themes in sample_"

  safety_lines = ""
  if unique_safety:
      for sf in unique_safety[:3]:
          em = safety_emoji.get(sf["category"], "🆘")
          safety_lines += f"* {em} **{sf['category']}** — {sf['summary']}\n"
  else:
      safety_lines = "* _No safety escalations flagged in sample_\n"

  digest = f"""## 📊 {day_name}

{total_comments} comments · stratified sample ~{len(sample_rows)} analyzed (up to 100 per source×user-type cell). Zendesk = support tickets (complaint-only by nature, so its %neg is always very high); ride_rating is the truer sentiment gauge.

|Source|Driver (vol · %neg)|Rider (vol · %neg)|
|  ---  |  ---  |  ---  |
|Zendesk|{fmt_vol('Zendesk', 'driver')}|{fmt_vol('Zendesk', 'rider')}|
|ride_rating|{fmt_vol('ride_rating', 'driver')}|{fmt_vol('ride_rating', 'rider')}|
|in_app|{fmt_vol('in_app', 'driver')}|—|
|App stores|{fmt_vol('App stores', 'driver')}|{fmt_vol('App stores', 'rider')}|

### 🏎️ Drivers — top issues

*(% of driver sample)*

{theme_lines(driver_themes)}

### 🧍 Riders — top issues

*(% of rider sample)*

{theme_lines(rider_themes, 7)}

### ⚠️ Safety escalations (~{len(safety_flags)} flagged)

{safety_lines}
### 🎯 Priorities

{chr(10).join(priorities)}

---
"""

  result = {
      "report_date": report_date,
      "day_name": day_name,
      "total_comments": total_comments,
      "sample_size": len(sample_rows),
      "digest": digest,
      "priorities": priorities[:3],
      "top_priority": priorities[0].split("**")[1] if priorities else "",
      "safety_count": len(safety_flags),
      "driver_themes": driver_themes[:5],
      "rider_themes": rider_themes[:5],
      "volume": dict(vol),
  }
  return result


if __name__ == "__main__":
  if len(sys.argv) > 1:
      rd = sys.argv[1]
  else:
      today = datetime.now(REPORT_TZ).date()
      rd = (today - timedelta(days=1)).strftime("%Y-%m-%d")
  out = run(rd)
  print(json.dumps(out, indent=2, default=str))
