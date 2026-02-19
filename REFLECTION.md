# Part 7: Reflection & Governance

## Data Engineering Mini Project - PII Detection & Data Quality Pipeline

---

## 1. What Were the Biggest Data Quality Issues?

Working through this dataset revealed five recurring categories of problems that show up constantly in real-world data engineering work.

**Issue 1: Invalid "sentinel" strings in date fields.**
Rows 6 and 10 contained the literal text `invalid_date` stored in a date column. This is arguably the most dangerous kind of data problem because it looks like data - it's not empty, it's not null - but it's completely meaningless. Any downstream system that tries to parse it as a date will crash. The fix was to replace it with `[INVALID_DATE]` and flag those rows for human review, since we can't invent a date we don't have.

**Issue 2: Inconsistent date formats across the same column.**
Three different date formats appeared in `date_of_birth` and `created_date`: `YYYY-MM-DD`, `YYYY/MM/DD`, and `MM/DD/YYYY`. This is a classic sign of data collected from multiple sources or entry forms with no validation. The fix was a multi-format parser that tries each format in turn and normalises everything to ISO 8601 (`YYYY-MM-DD`). Impact: 4 rows needed date normalisation.

**Issue 3: Inconsistent phone number formats.**
Four different phone formats existed in a single column: dashes (`555-123-4567`), parentheses (`(555) 234-5678`), dots (`555.789.0123`), and raw digits (`5557890123`). Any string comparison or lookup on phone numbers would silently fail across formats. The fix was to strip all non-digit characters and reformat to `XXX-XXX-XXXX`. Impact: 3 rows fixed.

**Issue 4: Missing values in critical fields.**
Five fields had at least one missing value: `first_name`, `last_name`, `address`, `income`, and `account_status`. Missing `account_status` is particularly critical - without it, a payment or fraud system can't determine what permissions that customer has. The strategy was to fill with typed placeholders (`[UNKNOWN]`, `0`, `unknown`) rather than deleting rows, since deleting would lose all the other valid data in those rows.

**Issue 5: Casing inconsistency in names and emails.**
`PATRICIA.DAVIS@GMAIL.COM` in uppercase and `wilson` in lowercase both represent real data but would fail case-sensitive lookups and look unprofessional in any customer-facing system. Title case was applied to names and lowercase to emails. This is the kind of subtle issue that causes bugs months after data is ingested - a query like `WHERE email = 'patricia.davis@gmail.com'` would miss `PATRICIA.DAVIS@GMAIL.COM` entirely in a case-sensitive database.

---

## 2. PII Risk Assessment

The dataset contained six categories of high-risk personally identifiable information:

- **Names** (first and last) - directly identify an individual
- **Email addresses** - unique contact point, enables phishing at scale
- **Phone numbers** - enables voice/SMS social engineering attacks
- **Physical addresses** - enables physical harm, mail fraud, and location tracking
- **Dates of birth** - used as verification questions by banks, healthcare providers, and government systems
- **Income** - financial sensitivity; reveals high-value targets for fraud

The most serious threat is not any single field in isolation - it is their *combination*. A dataset with name + email + phone + address + DOB is a complete identity dossier. An attacker with this data can:

- Open lines of credit in a victim's name (identity theft)
- Answer security questions at any institution that uses DOB or address for verification
- Send highly personalised phishing emails that appear legitimate
- Physically locate and approach individuals

In our dataset, 6 out of 10 rows had all five high-risk fields present simultaneously. That is not a small exposure - it is a ready-made fraud toolkit.

---

## 3. Masking Trade-offs

Masking PII is not free. Every field we hide reduces what the data can be used for.

**What we gave up by masking:**
- We can no longer contact customers directly from the masked dataset (emails and phones are hidden)
- We cannot verify identity using the masked data
- We cannot de-duplicate customers by name if the same person appears twice under slightly different spellings

**What we preserved:**
- Income distributions are fully intact for financial analysis
- Account status breakdown (active / inactive / suspended) is available for operational reporting
- Year of birth is preserved in the masked DOB (`1985-**-**`), so age-bracket analysis is still possible
- Account creation dates are untouched, so trend analysis over time works fine

**When masking is worth the trade-off:**
Masking makes sense whenever the *consumer* of the data does not need to identify specific individuals. An analytics team asking "what is the average income of suspended accounts?" needs none of the PII fields at all. A machine learning team building a churn model only needs behavioural and financial signals, not names or addresses.

**When you would NOT mask:**
The team responsible for customer communications (sending emails, SMS notifications) obviously needs real contact details. The fraud investigation team needs real addresses to file reports. The answer is not to give everyone the unmasked data - it is to give each team only the fields they actually need, a principle called *data minimisation*.

---

## 4. Validation Strategy

The validators we built caught the following correctly:

- Empty required fields (names, account status, income)
- Unparseable dates (`invalid_date`)
- Non-standard date formats (`YYYY/MM/DD`, `MM/DD/YYYY`)
- Non-standard phone formats (dots, parentheses, raw digits)
- Email case issues
- Invalid account status values

**What the validators missed or could not fix:**

The validators cannot catch *semantically wrong but syntactically valid* data. For example, Row 5's customer has a date of birth of `2005-12-25`, making them about 20 years old - technically valid, but suspicious for a financial services customer. We flagged this with an age check (under 18), but a 20-year-old with an income of $55,000 could still be legitimate or could be a data entry error. Only a human reviewer can make that call.

Similarly, the validator has no way to know that `[INVALID_DATE]` rows represent genuinely broken data that needs to be sourced again from the original system - it just flags and moves on.

**How to improve the validators:**

Cross-field validation would catch more issues. For instance: if `account_status` is `active` but `income` is `0`, that combination warrants a review flag. If `created_date` is before `date_of_birth`, that is impossible and should be a hard failure. These kinds of rules require understanding the *business logic*, not just the data types.

---

## 5. Production Operations

In a real production environment, this pipeline would not be run manually. It would be scheduled and monitored.

**Scheduling:** For a fintech company ingesting customer data from multiple sources, this pipeline would likely run daily - triggered whenever a new batch of raw data lands in cloud storage (e.g., an S3 bucket). It could also run on-demand when a data migration happens.

**What happens if validation fails:** The pipeline should distinguish between *warnings* and *hard failures*. A few rows with non-standard phone formats are a warning - clean what you can and flag the rest. Missing `account_status` on 50% of rows is a hard failure - stop the pipeline, do not write any output, and alert the data engineering team immediately. Writing bad data to a production database is always worse than writing no data.

**Notification:** Alerts should go to a Slack channel or PagerDuty when the pipeline fails. The execution report generated in Stage 6 provides the information needed to diagnose what went wrong. In a mature system, a data quality dashboard would track metrics like "% rows passing validation" over time, making it easy to spot when a new data source suddenly degrades quality.

**Handling failures:** Every output of this pipeline should be versioned. If a bad batch slips through and corrupts downstream data, engineers need to be able to roll back to the last known-good dataset. Cloud storage with versioning enabled (S3 versioning, GCS object versioning) handles this automatically.

---

## 6. Lessons Learned

**What was surprising:** How much damage a single badly formatted field can do. The `invalid_date` string in two rows is not just a cosmetic problem - it would crash any date arithmetic, break any time-series analysis, and cause silent errors in downstream joins. Real data is messier than any tutorial dataset suggests, and the messiness is rarely random - it reflects the history of different systems, different teams, and different assumptions all colliding.

**What was harder than expected:** Deciding *what to do* with missing values is genuinely hard. There is no universally correct answer. Filling income with `0` is defensible but could skew averages in a model. Deleting the row preserves data integrity but loses valid information in other columns. Every decision involves a trade-off, and that trade-off should be documented - which is exactly what the cleaning log is for.

**What to do differently next time:** Build the validation rules *before* looking at the data, based purely on business requirements. Looking at the data first risks writing validators that fit the data you have rather than the data you need. In production, validation schemas should be version-controlled alongside the pipeline code, so changes to the rules are tracked and auditable.

The broader lesson is that data quality is not a one-time cleaning exercise - it is an ongoing discipline. The pipeline built here is a foundation, not a finish line.
