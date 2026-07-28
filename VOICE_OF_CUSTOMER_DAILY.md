# Voice of Customer — Daily Cloud Agent Playbook

> **Purpose:** Run every day as a Cursor Cloud Agent. Pull yesterday's feedback from Snowflake, analyze sentiment on a stratified sample using `EN_TRANSLATION`, publish a daily digest, and update the Slack Voice of Customer canvas.

---

## Configuration

Update these values before the first scheduled run:


| Setting                  | Value                               | Notes                                                      |
| ------------------------ | ----------------------------------- | ---------------------------------------------------------- |
| **Report timezone**      | `Asia/Riyadh` (UTC+3)               | Digest covers **yesterday** in this timezone               |
| **Snowflake table**      | `jeeny_prod.general.feedbackevents` | Primary source                                             |
| **Slack canvas ID**      | F0BDZ51GQCT                         | Set after first run creates the canvas; store in this file |
| **Slack notify channel** | -                                   | `#product-announcements` — change if needed                |
| **Sample size per cell** | `100`                               | Max rows sampled per `feedback_source × persona`           |
| **Min comment length**   | `3`                                 | Exclude noise (`.`, `Q`, single chars)                     |


---



## Daily workflow (execute in order)



### Step 0 — Set the report date

```text
report_date = yesterday in Asia/Riyadh (YYYY-MM-DD)
report_day_name = day name, e.g. Mon 13 Jul 2026
start_ts = report_date 00:00:00
end_ts   = report_date 23:59:59.999
```

---



### Step 1 — Pull volume counts (full population, not sample)

Run on Snowflake MCP (`user-snowflake` / `sql_exec_tool`):

```sql
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
  WHERE TIMESTAMP >= :start_ts
    AND TIMESTAMP <  :end_ts + INTERVAL '1 day'
    AND RATINGS_COMMENTS IS NOT NULL
    AND TRIM(RATINGS_COMMENTS) <> ''
    AND LENGTH(TRIM(RATINGS_COMMENTS)) >= 3
)
SELECT
  FEEDBACK_SOURCE,
  persona,
  COUNT(*) AS volume
FROM base
WHERE persona IN ('driver', 'rider')
GROUP BY 1, 2
ORDER BY volume DESC;
```

Also get **total comments**:

```sql
SELECT COUNT(*) AS total_comments
FROM jeeny_prod.general.feedbackevents
WHERE TIMESTAMP >= :start_ts
  AND TIMESTAMP <  :end_ts + INTERVAL '1 day'
  AND RATINGS_COMMENTS IS NOT NULL
  AND TRIM(RATINGS_COMMENTS) <> ''
  AND LENGTH(TRIM(RATINGS_COMMENTS)) >= 3;
```

**Source normalization for the digest table:**


| Raw `FEEDBACK_SOURCE`         | Digest row                                  |
| ----------------------------- | ------------------------------------------- |
| `Zendesk`                     | Zendesk                                     |
| `ride_rating`                 | ride_rating                                 |
| `in_app`                      | in_app                                      |
| `Google Store`, `Apple Store` | App stores (combine driver + rider volumes) |


**Persona normalization:**


| Raw `USER_TYPE`         | Digest column |
| ----------------------- | ------------- |
| `driver`                | Driver        |
| `passenger`, `customer` | Rider         |


---



### Step 2 — Draw stratified sample (~250 per cell)

For each `(FEEDBACK_SOURCE, persona)` cell with comments, pull up to 250 rows:

```sql
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
  WHERE TIMESTAMP >= :start_ts
    AND TIMESTAMP <  :end_ts + INTERVAL '1 day'
    AND RATINGS_COMMENTS IS NOT NULL
    AND TRIM(RATINGS_COMMENTS) <> ''
    AND LENGTH(TRIM(RATINGS_COMMENTS)) >= 3
)
SELECT *
FROM base
WHERE persona IN ('driver', 'rider')
  AND rn <= 250;
```

Expected sample size: ~975 rows (up to 250 × ~4 active cells). Record actual sample count.

---



### Step 3 — Sentiment analysis (sample only)

**Do NOT translate comments.** Use `text_for_analysis` = `EN_TRANSLATION` when present, else `RATINGS_COMMENTS`.

For each sampled row:

1. **Reuse existing sentiment** when `OVERALL_SENTIMENT` is populated and valid (`Positive`, `Neutral`, `Negative`, `Mixed`).
2. **Classify in-agent** when missing, using `text_for_analysis`:
  - `Positive` — praise, satisfaction, 4–5 star tone
  - `Neutral` — factual, no clear polarity
  - `Negative` — complaint, frustration, 1–2 star tone
  - `Mixed` — both praise and complaint
  - `Unclear` — too short or ambiguous (exclude from %neg unless complaint-bearing)

**Rating cross-check (tie-break only):**


| `RATING_STARS` | Lean            |
| -------------- | --------------- |
| 1–2            | Negative        |
| 3              | Neutral / Mixed |
| 4–5            | Positive        |
| null (Zendesk) | Text only       |


**%neg formula per cell:**

```text
%neg = round(100 * negative_count / analyzed_count_in_cell)
```

Use `~` prefix when sample < 30 (e.g. `~65%`).

---



### Step 4 — Extract themes (negative + mixed only)

From the analyzed sample, cluster complaints into canonical pain points.

**Driver themes (examples):**

- Passenger conduct
- Passenger didn't pay (cash)
- Earnings & fares (low pay per km, weak offers)
- Cancellation fees not received / unfair fines
- Fare/charge accuracy

**Rider themes (examples):**

- Overcharging / fare disputes
- Driver conduct
- Wrong driver / car vs app
- Route manipulation / slow driving
- Cancellation (unfair fees, driver-initiated)
- Payment & charging errors

Rules:

- Merge semantically similar issues into one canonical label
- Rank by % of negative/mixed reviews mentioning each theme
- Show top 5 per persona with approximate share (e.g. `~14%`)
- One review can contribute to multiple themes; count once per theme per review

---



### Step 5 — Flag safety escalations

Scan negative/mixed sample for safety keywords and themes:

- harassment, assault, held hand, threatened, hit, insulted
- reckless driving, injury, child injured
- stalking, repeated calls/messages after ride
- driver/vehicle identity mismatch in safety context

List total count and up to 3 anonymized one-line summaries. Never include `USER_ID`, phone numbers, or full names.

---



### Step 6 — Write priorities (top 3 actionable)

From the day's themes and safety flags, produce 3 numbered priorities:

1. **Theme** — evidence from digest (% of negatives, sources affected)
2. **Recommended action** — specific product/ops fix

Example format:

```text
1. Fare integrity for riders — Overcharging + route-deviation ≈ 1/3 of rider negatives.
   Audit final-fare recalculation and route-deviation detection.
```

---



### Step 7 — Compose the daily digest

Use this **exact** canvas structure (emoji headers + bulleted issues):

```markdown
## 📊 {Day} {DD} {Mon} {YYYY}

{total_comments} comments · stratified sample ~{sample_count} analyzed (up to 100 per source×user-type cell). Zendesk = support tickets (complaint-only by nature, so its %neg is always very high); ride_rating is the truer sentiment gauge.

|Source|Driver (vol · %neg)|Rider (vol · %neg)|
|  ---  |  ---  |  ---  |
|Zendesk|{d_vol} · ~{d_pct}%|{r_vol} · ~{r_pct}%|
|ride_rating|{d_vol} · ~{d_pct}%|{r_vol} · ~{r_pct}%|
|in_app|{d_vol} · ~{d_pct}%|—|
|App stores|{d_vol} · n/a|{r_vol} · n/a|

### 🏎️ Drivers — top issues

*(% of driver sample)*

* 🚫 Cancellation fees not received / unfair fines — **~{pct}%**
* 💰 Earnings & fares (low pay per km, weak offers) — **~{pct}%**
* 😡 Passenger conduct — **~{pct}%**
* 💵 Passenger didn't pay (cash) — **~{pct}%**
* 📄 Fare/charge accuracy — **~{pct}%**
* 🏦 Payment not credited / withdrawals — **~{pct}%**

(Include top 5–6 driver themes; pick emojis from mapping below.)

### 🧍 Riders — top issues

*(% of rider sample)*

* 💸 Overcharging / fare disputes — **~{pct}%**
* 😡 Driver conduct — **~{pct}%**
* 🚙 Wrong driver / car vs app — **~{pct}%**
* 🗺️ Route manipulation / slow driving — **~{pct}%**
* 📍 No-show / wrong pickup — **~{pct}%**
* 🚫 Cancellation (unfair fees, driver-initiated) — **~{pct}%**
* 🏦 Payment & charging errors — **~{pct}%**
* 🧼 Vehicle smell / cleanliness — **~{pct}%**

(Include top 5–7 rider themes; pick emojis from mapping below.)

### ⚠️ Safety escalations (~{count} flagged)

* 🆘 **Harassment** — {anonymized one-liner}
* 🏎️ **Reckless driving** — {anonymized one-liner}
* 🚙 **Identity mismatch** — {anonymized one-liner}

(Show up to 3 examples. Add a 4th bullet only if a distinct critical case exists.)

### 🎯 Priorities

1. **{Action title}** — {evidence from digest}. {Recommended next step}.
2. **{Action title}** — {evidence}. {Recommended next step}.
3. **{Action title}** — {evidence}. {Recommended next step}.

---
```

**Emoji mapping (use consistently):**


| Theme                         | Emoji |
| ----------------------------- | ----- |
| Cancellation fees             | 🚫    |
| Earnings / low fares          | 💰    |
| Passenger conduct             | 😡    |
| Driver conduct                | 😡    |
| Cash non-payment              | 💵    |
| Fare / charge accuracy        | 📄    |
| Wallet / payment not credited | 🏦    |
| Overcharging                  | 💸    |
| Wrong driver / car            | 🚙    |
| Route manipulation            | 🗺️   |
| No-show / wrong pickup        | 📍    |
| Vehicle cleanliness           | 🧼    |
| Harassment / safety           | 🆘    |
| Reckless driving              | 🏎️   |
| Identity mismatch             | 🚙    |


**Formatting rules for Slack Canvas:**

- Date heading: `## 📊 Mon 27 Jul 2026` (include day name + emoji, **not** `slack_date`)
- Section headers: `### 🏎️`, `### 🧍`, `### ⚠️`, `### 🎯` — always with emoji
- Issue lists: bulleted (`*`), one theme per line, emoji prefix, bold percentage
- Sub-labels: `*(% of driver sample)*` / `*(% of rider sample)*` on its own line
- Safety: bullet list with bold category label, em dash, description
- Priorities: numbered list, bold action title, em dash, evidence + recommendation
- End each daily block with `---` separator
- **Never** compress issues into a single inline paragraph
- Use `#` / `##` / `###` only (max 3 levels); no `####`

---



### Step 8 — Update Slack canvas



#### First run (no canvas ID configured)

1. Call `slack_create_canvas`:
  - **title:** `Voice of Customer`
  - **content:** Full digest from Step 7
2. Save returned `canvas_id` and `canvas_url`
3. Update `REPLACE_WITH_CANVAS_ID` in this file's Configuration section
4. Post to notify channel:

```text
:bar_chart: *Voice of Customer — {report_day_name}*
Daily digest published → {canvas_url}
{one_line_summary_of_top_priority}
```

Use `slack_send_message` on channel `C07HCELL00M`.

#### Subsequent runs (canvas ID configured)

1. Call `slack_read_canvas` with the stored `canvas_id` to get fresh `section_id_mapping`
2. Call `slack_update_canvas` with **prepend** on the **first body section** (section immediately after the title) so newest digest appears at top:

```json
{
  "canvas_id": "{canvas_id}",
  "sections": [
    {
      "edit_type": "prepend",
      "section_id": "{first_body_section_id}",
      "content": "{full_digest_markdown_from_step_7}"
    }
  ]
}
```

1. If the canvas grows beyond ~30 daily entries, delete the oldest digest section (use `edit_type: delete` on its date heading section) to keep the canvas manageable
2. Post the same channel notification as first run
3. Always share `canvas_url` in the completion message

**Never fabricate section IDs.** Always read the canvas immediately before updating.

---



## Error handling


| Failure                       | Action                                                                                          |
| ----------------------------- | ----------------------------------------------------------------------------------------------- |
| Snowflake query timeout       | Narrow to single-day filter; run volume + sample queries separately                             |
| Zero comments for a cell      | Show `0 · n/a` in table                                                                         |
| Slack canvas update fails     | Fall back to `slack_create_canvas` with title `Voice of Customer — {date}` and alert in channel |
| Sample too small (<100 total) | Still publish; note low volume in callout                                                       |
| MCP auth error                | Stop and report; do not publish partial digest                                                  |


---



## Automation setup (Cursor Cloud Agent)

Schedule this playbook as a **daily cron automation**:


| Field            | Value                                                                     |
| ---------------- | ------------------------------------------------------------------------- |
| **Trigger**      | Cron — `0 8 * * 1-5` (08:00 AM Sun–Thu, Asia/Riyadh) or `0 8 * `* * daily |
| **Tools**        | Snowflake MCP (`user-snowflake`), Slack MCP (`plugin-slack-slack`)        |
| **Instructions** | `@VOICE_OF_CUSTOMER_DAILY.md` — follow every step in order                |
| **Compute**      | Cloud Agent (required for Snowflake + Slack MCP access)                   |


Adjust schedule if you prefer the digest before 09:00 KSA standups.

---



## Reference — table columns used


| Column                | Use                                                             |
| --------------------- | --------------------------------------------------------------- |
| `TIMESTAMP`           | Daily window filter                                             |
| `USER_TYPE`           | Persona (`driver` / `passenger` / `customer`)                   |
| `FEEDBACK_SOURCE`     | Source dimension                                                |
| `RATINGS_COMMENTS`    | Original comment text                                           |
| `EN_TRANSLATION`      | **Primary text for sentiment analysis** (skip translation step) |
| `OVERALL_SENTIMENT`   | Reuse when populated                                            |
| `RATING_STARS`        | Tie-break for ride_rating / app store                           |
| `TICKET_CATEGORY`     | Zendesk theme hints                                             |
| `CATEGORY`, `URGENCY` | Optional enrichment for priorities                              |
| `CITY`, `COUNTRY`     | Geo context in examples                                         |
| `GUID`                | Row identifier (never expose in digest)                         |


---



## Quality checklist (before publishing)

- [ ] Report date is yesterday (Asia/Riyadh), not today
- [ ] Volume counts use full population; sentiment uses sample only
- [ ] `EN_TRANSLATION` used — no new translation calls
- [ ] Issue sections use emoji headers (🏎️ 🧍 ⚠️ 🎯) with bulleted themes — never inline paragraphs
- [ ] Safety summaries anonymized
- [ ] Canvas updated (prepend) or created on first run
- [ ] Channel notification posted with canvas link
- [ ] Top priority mentioned in Slack message