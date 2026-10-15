#!/usr/bin/env python3
"""Voice of Customer daily digest — Snowflake pull + sentiment/theme analysis."""

import json
import os
import re
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import snowflake.connector

REPORT_TZ = ZoneInfo("Asia/Riyadh")
SAMPLE_PER_CELL = 100
MIN_COMMENT_LEN = 3

VALID_SENTIMENTS = {"Positive", "Neutral", "Negative", "Mixed"}

# Zendesk categories treated as neutral/ops (not complaint)
ZENDESK_NEUTRAL_CATEGORIES = {
    "city update", "work permit", "lost item", "lost & found", "lost and found",
    "general inquiry", "account update", "document", "registration",
}

DRIVER_THEMES = {
    "Passenger conduct": [
        r"\b(rude|abusive|harass|insult|smok|drunk|fight|aggressive|bad behavior|misbehav)\b",
        r"\b(passenger|client|customer).{0,30}(rude|bad|terrible|awful)\b",
        r"سلوك|مسافر.{0,10}(سيء|سيئ|وقح)",
    ],
    "Passenger didn't pay (cash)": [
        r"\b(didn'?t pay|not pay|no pay|refus.*pay|unpaid|without pay)\b",
        r"\b(cash).{0,20}(didn'?t|not|refus|without)\b",
        r"لم يدفع|ما دفع|رفض.{0,10}دفع",
    ],
    "Earnings & fares (low pay per km, weak offers)": [
        r"\b(low|less|poor|bad).{0,15}(pay|earning|fare|income|commission)\b",
        r"\b(earning|pay|fare|income).{0,15}(low|less|poor|bad|decreas)\b",
        r"\b(offer|bonus|incentive|surge).{0,15}(low|weak|bad|no)\b",
        r"أجر|دخل|عمولة.{0,10}(قليل|ضعيف|منخفض)",
    ],
    "Cancellation fees not received / unfair fines": [
        r"\b(cancel.{0,20}(fee|fine|penalt)|fine|penalt|deduct).{0,20}(unfair|wrong|not receiv|didn'?t receiv|no fee)\b",
        r"\b(unfair|wrong).{0,15}(fine|fee|penalt|deduct)\b",
        r"غرام|خصم|إلغاء.{0,10}(ظلم|لم أستلم|ما استلمت)",
    ],
    "Fare/charge accuracy": [
        r"\b(wrong|incorrect|error).{0,15}(fare|charge|amount|price|bill)\b",
        r"\b(fare|charge|amount).{0,15}(wrong|incorrect|error|mismatch)\b",
        r"أجرة.{0,10}(خطأ|غلط|خاطئ)",
    ],
    "Payment not credited / withdrawals": [
        r"\b(withdraw|wallet|balance|credit|payout|transfer).{0,25}(not|didn'?t|delay|stuck|pending|missing)\b",
        r"\b(not|didn'?t|delay|stuck|pending|missing).{0,25}(withdraw|wallet|balance|credit|payout)\b",
        r"سحب|محفظ|رصيد.{0,10}(لم|ما|تأخر|معلق)",
    ],
}

RIDER_THEMES = {
    "Overcharging / fare disputes": [
        r"\b(overcharg|over.?charg|too much|expensive|high fare|fare dispute|charged more|extra charge)\b",
        r"\b(fare|price|amount|bill).{0,20}(high|expensive|wrong|more|extra|increas)\b",
        r"زيادة.{0,10}(أجرة|سعر)|أجرة.{0,10}(عالية|كثير|زيادة)",
    ],
    "Driver conduct": [
        r"\b(rude|abusive|harass|insult|aggressive|bad behavior|unprofessional|disrespect)\b",
        r"\b(driver|captain).{0,30}(rude|bad|terrible|awful|unprofessional)\b",
        r"سائق.{0,10}(سيء|وقح|غير محترم)",
    ],
    "Wrong driver / car vs app": [
        r"\b(wrong|different).{0,15}(driver|car|vehicle|plate|captain)\b",
        r"\b(driver|car|vehicle|plate).{0,15}(wrong|different|mismatch|not match)\b",
        r"سائق.{0,10}(مختلف|غير)|سيارة.{0,10}(مختلف|غير)",
    ],
    "Route manipulation / slow driving": [
        r"\b(long route|detour|longer route|manipulat|slow driv|took longer|unnecessary route)\b",
        r"\b(route|road).{0,20}(long|wrong|detour|manipulat)\b",
        r"طريق.{0,10}(طويل|تطويل|تحايل)",
    ],
    "No-show / wrong pickup": [
        r"\b(no.?show|didn'?t (come|arrive|show)|wrong pickup|wrong location|waited)\b",
        r"\b(pickup|pick.?up).{0,20}(wrong|late|delay|no)\b",
        r"لم يحضر|لم يأت|موقع.{0,10}(خطأ|غلط)",
    ],
    "Cancellation (unfair fees, driver-initiated)": [
        r"\b(cancel|cancellation).{0,25}(fee|fine|unfair|driver|without reason)\b",
        r"\b(driver).{0,20}(cancel|refus)\b",
        r"إلغاء.{0,10}(غرام|رسوم|سائق)",
    ],
    "Payment & charging errors": [
        r"\b(payment|charge|card|wallet|double charg).{0,25}(fail|error|wrong|issue|problem|twice)\b",
        r"\b(fail|error|wrong|issue).{0,25}(payment|charge|card|wallet)\b",
        r"دفع|شحن.{0,10}(خطأ|فشل|مشكل)",
    ],
    "Vehicle smell / cleanliness": [
        r"\b(dirty|smell|odor|stink|unclean|filthy|hygien)\b",
        r"\b(car|vehicle|cab).{0,20}(dirty|smell|clean)\b",
        r"رائحة|قذر|نظافة|وسخ",
    ],
}

SAFETY_PATTERNS = {
    "Harassment": [
        r"\b(harass|assault|molest|threaten|inappropriat.{0,10}touch|held (my )?hand)\b",
        r"تحرش|اعتداء|تهديد|مس.{0,5}بشكل",
    ],
    "Reckless driving": [
        r"\b(reckless|dangerous driv|speeding|child.{0,10}injur|hit (me|us|a ))\b",
        r"قيادة.{0,10}(خطير|متهور)|حادث|إصاب",
    ],
    "Identity mismatch": [
        r"\b(wrong driver|different driver|not (the )?same (driver|car)|impersonat|fake driver)\b",
        r"سائق.{0,10}(مختلف|غير مطابق)|سيارة.{0,10}(مختلف|غير)",
    ],
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


def connect():
    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PAT"],
        database=os.environ.get("SNOWFLAKE_DATABASE", "JEENY_PROD"),
        role=os.environ.get("SNOWFLAKE_ROLE", "ROLE_PRODUCT"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH").strip(),
    )
    cur = conn.cursor()
    cur.execute("USE WAREHOUSE COMPUTE_WH")
    return conn, cur


def classify_sentiment(row):
  """Classify sentiment for a single row."""
  existing = (row.get("OVERALL_SENTIMENT") or "").strip()
  if existing in VALID_SENTIMENTS:
      return existing

  text = (row.get("TEXT_FOR_ANALYSIS") or "").lower()
  source = (row.get("FEEDBACK_SOURCE") or "")
  category = (row.get("TICKET_CATEGORY") or row.get("CATEGORY") or "").lower()
  stars = row.get("RATING_STARS")

  if source == "Zendesk":
      if any(n in category for n in ZENDESK_NEUTRAL_CATEGORIES):
          return "Neutral"
      return "Negative"

  neg_kw = r"\b(bad|terrible|awful|worst|hate|angry|frustrat|complain|problem|issue|poor|disappoint|unfair|scam|fraud|never again|refund)\b"
  pos_kw = r"\b(good|great|excellent|amazing|love|thank|perfect|best|wonderful|happy|satisf)\b"

  has_neg = bool(re.search(neg_kw, text))
  has_pos = bool(re.search(pos_kw, text))

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

  if len(text.strip()) < 10:
      return "Unclear"
  return "Neutral"


def match_themes(text, theme_dict):
  text_l = (text or "").lower()
  matched = []
  for theme, patterns in theme_dict.items():
      for pat in patterns:
          if re.search(pat, text_l, re.IGNORECASE):
              matched.append(theme)
              break
  return matched


def anonymize_safety(text, city=None, category=None):
  t = (text or "").strip()
  # Prefer English summary when text is mostly non-Latin
  non_latin = sum(1 for c in t if ord(c) > 127)
  if non_latin > len(t) * 0.3:
      loc = city or "city"
      defaults = {
          "Harassment": f"Rider reported verbal harassment or inappropriate behavior during trip ({loc})",
          "Reckless driving": f"Reckless or dangerous driving reported with safety concern ({loc})",
          "Identity mismatch": f"Vehicle or driver did not match app profile at pickup ({loc})",
      }
      return defaults.get(category, f"Safety concern reported ({loc})")
  t = re.sub(r"\b\d{8,}\b", "[phone]", t)
  t = re.sub(r"\b[A-Z][a-z]+ [A-Z][a-z]+\b", "[name]", t)
  loc = city or "city"
  if len(t) > 120:
      t = t[:117] + "..."
  return f"Report in {loc}: {t}" if t else f"Safety concern reported in {loc}"


def digest_source(raw):
  if raw in ("Google Store", "Apple Store"):
      return "App stores"
  return raw


def run(report_date: str):
  start_ts = f"{report_date} 00:00:00"
  end_date = (datetime.strptime(report_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")

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
    AND TIMESTAMP < %s
    AND RATINGS_COMMENTS IS NOT NULL
    AND TRIM(RATINGS_COMMENTS) <> ''
    AND LENGTH(TRIM(RATINGS_COMMENTS)) >= %s
)
SELECT FEEDBACK_SOURCE, persona, COUNT(*) AS volume
FROM base
WHERE persona IN ('driver', 'rider')
GROUP BY 1, 2
ORDER BY volume DESC
"""
  cur.execute(volume_sql, (start_ts, f"{end_date} 00:00:00", MIN_COMMENT_LEN))
  volume_rows = [{"FEEDBACK_SOURCE": r[0], "persona": r[1], "volume": r[2]} for r in cur.fetchall()]

  cur.execute(
      """SELECT COUNT(*) FROM jeeny_prod.general.feedbackevents
         WHERE TIMESTAMP >= %s AND TIMESTAMP < %s
         AND RATINGS_COMMENTS IS NOT NULL AND TRIM(RATINGS_COMMENTS) <> ''
         AND LENGTH(TRIM(RATINGS_COMMENTS)) >= %s""",
      (start_ts, f"{end_date} 00:00:00", MIN_COMMENT_LEN),
  )
  total_comments = cur.fetchone()[0]

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
    AND LENGTH(TRIM(RATINGS_COMMENTS)) >= %s
)
SELECT GUID, FEEDBACK_SOURCE, persona, RATINGS_COMMENTS, text_for_analysis,
       OVERALL_SENTIMENT, RATING_STARS, CITY, COUNTRY, TICKET_CATEGORY, CATEGORY, URGENCY, TIMESTAMP
FROM base
WHERE persona IN ('driver', 'rider') AND rn <= {SAMPLE_PER_CELL}
"""
  cur.execute(sample_sql, (start_ts, f"{end_date} 00:00:00", MIN_COMMENT_LEN))
  cols = [d[0] for d in cur.description]
  sample = []
  for row in cur.fetchall():
      d = dict(zip(cols, row))
      # normalize keys to lowercase for internal use
      sample.append({k.lower(): v for k, v in d.items()})
  conn.close()

  # Sentiment per row
  for row in sample:
      row["sentiment"] = classify_sentiment({
          "OVERALL_SENTIMENT": row.get("overall_sentiment"),
          "TEXT_FOR_ANALYSIS": row.get("text_for_analysis"),
          "FEEDBACK_SOURCE": row.get("feedback_source"),
          "TICKET_CATEGORY": row.get("ticket_category"),
          "CATEGORY": row.get("category"),
          "RATING_STARS": row.get("rating_stars"),
      })

  # Volume table for digest
  vol_map = defaultdict(lambda: {"driver": 0, "rider": 0})
  for vr in volume_rows:
      ds = digest_source(vr["FEEDBACK_SOURCE"])
      vol_map[ds][vr["persona"]] += vr["volume"]

  # %neg per cell (source x persona) from sample
  cell_stats = defaultdict(lambda: {"neg": 0, "total": 0})
  for row in sample:
      key = (digest_source(row["feedback_source"]), row["persona"])
      if row["sentiment"] in ("Negative", "Mixed"):
          cell_stats[key]["neg"] += 1
      if row["sentiment"] != "Unclear":
          cell_stats[key]["total"] += 1

  def pct_neg(source, persona):
      key = (source, persona)
      t = cell_stats[key]["total"]
      if t == 0:
          return "n/a"
      pct = round(100 * cell_stats[key]["neg"] / t)
      return f"~{pct}%" if t < 30 else f"~{pct}%"

  def fmt_cell(source, persona):
      vol = vol_map[source][persona]
      if vol == 0:
          return "0 · n/a"
      p = pct_neg(source, persona)
      return f"{vol:,} · {p}"

  # Themes
  driver_sample = [r for r in sample if r["persona"] == "driver"]
  rider_sample = [r for r in sample if r["persona"] == "rider"]
  driver_neg = [r for r in driver_sample if r["sentiment"] in ("Negative", "Mixed")]
  rider_neg = [r for r in rider_sample if r["sentiment"] in ("Negative", "Mixed")]

  def theme_counts(neg_rows, theme_dict, denom):
      counts = defaultdict(int)
      for row in neg_rows:
          themes = match_themes(row.get("text_for_analysis"), theme_dict)
          for t in set(themes):
              counts[t] += 1
      ranked = sorted(counts.items(), key=lambda x: -x[1])
      result = []
      for theme, cnt in ranked[:7]:
          pct = round(100 * cnt / denom) if denom else 0
          result.append({"theme": theme, "pct": pct, "count": cnt})
      return result

  driver_themes = theme_counts(driver_neg, DRIVER_THEMES, len(driver_sample))
  rider_themes = theme_counts(rider_neg, RIDER_THEMES, len(rider_sample))

  # Safety
  safety_examples = []
  safety_count = 0
  for row in sample:
      if row["sentiment"] not in ("Negative", "Mixed"):
          continue
      text = row.get("text_for_analysis") or ""
      for cat, patterns in SAFETY_PATTERNS.items():
          if any(re.search(p, text, re.IGNORECASE) for p in patterns):
              safety_count += 1
              if len(safety_examples) < 3:
                  safety_examples.append({
                      "category": cat,
                      "summary": anonymize_safety(text, row.get("city"), cat),
                  })
              break

  # Priorities (heuristic from top themes)
  priorities = []
  if rider_themes:
      rt = rider_themes[0]
      priorities.append(
          f"1. **{rt['theme']} for riders** — ~{rt['pct']}% of rider sample; strengthen fare transparency and dispute resolution."
      )
  if driver_themes:
      dt = driver_themes[0]
      priorities.append(
          f"2. **{dt['theme']} for drivers** — ~{dt['pct']}% of driver sample; review enforcement and compensation workflows."
      )
  if safety_count:
      priorities.append(
          f"3. **Safety escalation response** — {safety_count} safety-related flags in sample; fast-track harassment and reckless-driving cases."
      )
  elif len(driver_themes) > 1:
      dt2 = driver_themes[1]
      priorities.append(
          f"3. **{dt2['theme']} for drivers** — ~{dt2['pct']}% of driver sample; targeted ops review."
      )
  while len(priorities) < 3 and rider_themes:
      idx = len(priorities)
      if idx < len(rider_themes):
          rt = rider_themes[idx]
          priorities.append(
              f"{idx+1}. **{rt['theme']}** — ~{rt['pct']}% of rider sample; investigate root causes."
          )
      else:
          break

  day_name = datetime.strptime(report_date, "%Y-%m-%d").strftime("%a %d %b %Y")

  def theme_lines(themes):
      lines = []
      for t in themes[:6]:
          emoji = THEME_EMOJI.get(t["theme"], "•")
          lines.append(f"* {emoji} {t['theme']} — **~{t['pct']}%**")
      return "\n".join(lines)

  safety_lines = ""
  if safety_count == 0:
      safety_lines = "* No critical safety flags in today's sample."
  else:
      emoji_map = {"Harassment": "🆘", "Reckless driving": "🏎️", "Identity mismatch": "🚙"}
      for ex in safety_examples:
          safety_lines += f"* {emoji_map.get(ex['category'], '⚠️')} **{ex['category']}** — {ex['summary']}\n"

  digest = f"""## 📊 {day_name}

{total_comments:,} comments · stratified sample ~{len(sample)} analyzed (up to 100 per source×user-type cell). Zendesk = support tickets (complaint-only by nature, so its %neg is always very high); ride_rating is the truer sentiment gauge.

|Source|Driver (vol · %neg)|Rider (vol · %neg)|
|  ---  |  ---  |  ---  |
|Zendesk|{fmt_cell('Zendesk', 'driver')}|{fmt_cell('Zendesk', 'rider')}|
|ride_rating|{fmt_cell('ride_rating', 'driver')}|{fmt_cell('ride_rating', 'rider')}|
|in_app|{fmt_cell('in_app', 'driver')}|—|
|App stores|{fmt_cell('App stores', 'driver')}|{fmt_cell('App stores', 'rider')}|

### 🏎️ Drivers — top issues

*(% of driver sample)*

{theme_lines(driver_themes)}

### 🧍 Riders — top issues

*(% of rider sample)*

{theme_lines(rider_themes)}

### ⚠️ Safety escalations (~{safety_count} flagged)

{safety_lines.strip()}

### 🎯 Priorities

{chr(10).join(priorities)}

---
"""

  result = {
      "report_date": report_date,
      "report_day_name": day_name,
      "total_comments": total_comments,
      "sample_size": len(sample),
      "digest": digest,
      "priorities": priorities[:3],
      "top_priority": priorities[0] if priorities else "No priorities identified",
      "volume_rows": volume_rows,
      "driver_themes": driver_themes,
      "rider_themes": rider_themes,
      "safety_count": safety_count,
  }
  return result


if __name__ == "__main__":
  date = sys.argv[1] if len(sys.argv) > 1 else (
      datetime.now(REPORT_TZ) - timedelta(days=1)
  ).strftime("%Y-%m-%d")
  out = run(date)
  print(json.dumps(out, indent=2, default=str))
