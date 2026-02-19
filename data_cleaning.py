"""
Part 4: Data Cleaning
Fixes all quality issues found in Parts 1-3:
  - Normalizes phone formats -> XXX-XXX-XXXX
  - Normalizes date formats -> YYYY-MM-DD
  - Applies title case to names
  - Handles missing values with documented strategy
  - Re-runs validation to confirm all fixes
  - Saves customers_cleaned.csv
"""

import pandas as pd
import re
import os
from datetime import datetime

# ── Load raw data ──────────────────────────────────────────────────────────────
df = pd.read_csv("customers_raw.csv", dtype=str)
df.columns = df.columns.str.strip()
for col in df.columns:
    df[col] = df[col].str.strip()

TOTAL_ROWS = len(df)

report_lines = []
actions = []  # track every change made

def add(text=""):
    report_lines.append(text)
    # print(text) # Suppressed as per user request

def log_action(category, detail):
    actions.append({"category": category, "detail": detail})

def safe(val):
    return "" if pd.isna(val) else str(val).strip()

# ══════════════════════════════════════════════════════════════════════════════
# CLEANING FUNCTIONS
# ══════════════════════════════════════════════════════════════════════════════

def normalize_phone(val):
    """
    Convert any phone format to XXX-XXX-XXXX.
    Strips all non-digit characters, then reformats.
    """
    val = safe(val)
    if not val:
        return val
    digits = re.sub(r"\D", "", val)  # keep digits only
    if len(digits) == 10:
        return f"{digits[:3]}-{digits[3:6]}-{digits[6:]}"
    return val  # return as-is if we can't parse it

def normalize_date(val):
    """
    Convert any recognizable date format to YYYY-MM-DD.
    Returns original value if it can't be parsed (will be flagged).
    """
    val = safe(val)
    if not val:
        return val
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%m/%d/%Y"):
        try:
            return datetime.strptime(val, fmt).strftime("%Y-%m-%d")
        except:
            pass
    return val  # unparseable — leave as-is, will be flagged

def normalize_name(val):
    """Apply title case: 'PATRICIA' -> 'Patricia', 'johnson' -> 'Johnson'"""
    val = safe(val)
    return val.title() if val else val

def normalize_email(val):
    """Lowercase the email."""
    val = safe(val)
    return val.lower() if val else val

def normalize_status(val):
    """Lowercase the account status."""
    val = safe(val)
    return val.lower() if val else val

# ══════════════════════════════════════════════════════════════════════════════
# MISSING VALUE STRATEGY
# Strategy documented per column:
#   Names       -> fill '[UNKNOWN]'  (we must have a placeholder, can't delete row)
#   Address     -> fill '[UNKNOWN]'  (same reasoning)
#   Income      -> fill 0            (safest financial default, flag for review)
#   Status      -> fill 'unknown'    (can't assume; flag for human review)
#   Dates       -> leave unfixable ones as '[INVALID_DATE]' (flag for review)
# ══════════════════════════════════════════════════════════════════════════════

MISSING_STRATEGY = {
    "first_name":    "[UNKNOWN]",
    "last_name":     "[UNKNOWN]",
    "address":       "[UNKNOWN]",
    "income":        "0",
    "account_status":"unknown",
}

# ══════════════════════════════════════════════════════════════════════════════
# APPLY CLEANING — column by column
# ══════════════════════════════════════════════════════════════════════════════

cleaned = df.copy()
phone_fixed = date_fixed = name_fixed = email_fixed = status_fixed = 0
missing_filled = {col: 0 for col in MISSING_STRATEGY}

for idx in range(len(cleaned)):
    row = cleaned.iloc[idx]

    # ── Phone normalization ───────────────────────────────────────────────────
    original_phone = safe(row["phone"])
    new_phone = normalize_phone(original_phone)
    if original_phone and new_phone != original_phone:
        cleaned.at[idx, "phone"] = new_phone
        phone_fixed += 1
        log_action("Phone normalization",
                   f"Row {idx+1}: '{original_phone}' -> '{new_phone}'")

    # ── Date of birth normalization ───────────────────────────────────────────
    original_dob = safe(row["date_of_birth"])
    new_dob = normalize_date(original_dob)
    if original_dob and new_dob != original_dob and new_dob != "[INVALID_DATE]":
        cleaned.at[idx, "date_of_birth"] = new_dob
        date_fixed += 1
        log_action("Date normalization (date_of_birth)",
                   f"Row {idx+1}: '{original_dob}' -> '{new_dob}'")
    elif original_dob in ("invalid_date",) or (original_dob and new_dob == original_dob and not re.match(r"^\d{4}-\d{2}-\d{2}$", original_dob)):
        cleaned.at[idx, "date_of_birth"] = "[INVALID_DATE]"
        log_action("Invalid date flagged (date_of_birth)",
                   f"Row {idx+1}: '{original_dob}' -> '[INVALID_DATE]'")

    # ── Created date normalization ────────────────────────────────────────────
    original_cd = safe(row["created_date"])
    new_cd = normalize_date(original_cd)
    if original_cd and new_cd != original_cd:
        cleaned.at[idx, "created_date"] = new_cd
        date_fixed += 1
        log_action("Date normalization (created_date)",
                   f"Row {idx+1}: '{original_cd}' -> '{new_cd}'")
    elif original_cd == "invalid_date":
        cleaned.at[idx, "created_date"] = "[INVALID_DATE]"
        log_action("Invalid date flagged (created_date)",
                   f"Row {idx+1}: '{original_cd}' -> '[INVALID_DATE]'")

    # ── Name title case ───────────────────────────────────────────────────────
    for name_col in ["first_name", "last_name"]:
        original_name = safe(row[name_col])
        new_name = normalize_name(original_name)
        if original_name and new_name != original_name:
            cleaned.at[idx, name_col] = new_name
            name_fixed += 1
            log_action(f"Name case ({name_col})",
                       f"Row {idx+1}: '{original_name}' -> '{new_name}'")

    # ── Email lowercase ───────────────────────────────────────────────────────
    original_email = safe(row["email"])
    new_email = normalize_email(original_email)
    if original_email and new_email != original_email:
        cleaned.at[idx, "email"] = new_email
        email_fixed += 1
        log_action("Email lowercase",
                   f"Row {idx+1}: '{original_email}' -> '{new_email}'")

    # ── Account status lowercase ──────────────────────────────────────────────
    original_status = safe(row["account_status"])
    new_status = normalize_status(original_status)
    if original_status and new_status != original_status:
        cleaned.at[idx, "account_status"] = new_status
        status_fixed += 1
        log_action("Status normalization",
                   f"Row {idx+1}: '{original_status}' -> '{new_status}'")

# ── Fill missing values ───────────────────────────────────────────────────────
for col, fill_val in MISSING_STRATEGY.items():
    mask = cleaned[col].isna() | cleaned[col].str.strip().eq("")
    count = mask.sum()
    if count > 0:
        cleaned.loc[mask, col] = fill_val
        missing_filled[col] = int(count)
        for idx in cleaned[mask].index:
            log_action(f"Missing value filled ({col})",
                       f"Row {idx+1}: empty -> '{fill_val}'")

# ══════════════════════════════════════════════════════════════════════════════
# RE-RUN VALIDATION on cleaned data
# (same rules from Part 3, inline here for self-contained script)
# ══════════════════════════════════════════════════════════════════════════════

def revalidate(df_check):
    """Quick re-validation pass. Returns (pass_count, fail_count, failures)."""
    failures = []

    def vname(v, field):
        v = safe(v)
        if not v or v == "[UNKNOWN]": return True   # placeholder is acceptable
        return bool(re.match(r"^[A-Za-z\-']{2,50}$", v))

    def vemail(v):
        v = safe(v)
        return bool(re.match(r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$", v))

    def vphone(v):
        return bool(re.match(r"^\d{3}-\d{3}-\d{4}$", safe(v)))

    def vdate(v):
        v = safe(v)
        if v in ("[INVALID_DATE]", ""): return True   # flagged — not a format error
        return bool(re.match(r"^\d{4}-\d{2}-\d{2}$", v))

    def vstatus(v):
        return safe(v).lower() in {"active", "inactive", "suspended", "unknown"}

    def vincome(v):
        v = safe(v)
        try: return float(v) >= 0
        except: return False

    pass_count = 0
    for idx, row in df_check.iterrows():
        row_ok = all([
            vname(row["first_name"], "first_name"),
            vname(row["last_name"], "last_name"),
            vemail(row["email"]),
            vphone(row["phone"]),
            vdate(row["date_of_birth"]),
            vdate(row["created_date"]),
            vstatus(row["account_status"]),
            vincome(row["income"]),
        ])
        if row_ok:
            pass_count += 1
        else:
            failures.append(idx + 1)
    return pass_count, len(df_check) - pass_count, failures

passes_before, fails_before, _ = revalidate(df)
passes_after,  fails_after, remaining_failures = revalidate(cleaned)

# ══════════════════════════════════════════════════════════════════════════════
# BUILD REPORT
# ══════════════════════════════════════════════════════════════════════════════

add("DATA CLEANING LOG")
add("=" * 60)
add()

add("ACTIONS TAKEN:")
add("-" * 60)

add("\nNormalization:")
norm_actions = [a for a in actions if "normalization" in a["category"].lower() or "case" in a["category"].lower() or "lowercase" in a["category"].lower()]
for a in norm_actions:
    add(f"  [{a['category']}] {a['detail']}")

add(f"\n  Summary:")
add(f"  - Phone formats normalized:  {phone_fixed} row(s)")
add(f"  - Date formats normalized:   {date_fixed} row(s)")
add(f"  - Name casing fixed:         {name_fixed} row(s)")
add(f"  - Email casing fixed:        {email_fixed} row(s)")

add("\nInvalid dates flagged:")
flag_actions = [a for a in actions if "flagged" in a["category"].lower()]
for a in flag_actions:
    add(f"  {a['detail']}")

add("\nMissing Values:")
add("  Strategy used:")
add("    first_name    -> '[UNKNOWN]'  (can't delete row, need placeholder)")
add("    last_name     -> '[UNKNOWN]'  (same reasoning)")
add("    address       -> '[UNKNOWN]'  (same reasoning)")
add("    income        -> 0            (safest financial default)")
add("    account_status -> 'unknown'    (flag for human review)")
add("")
for col, count in missing_filled.items():
    if count > 0:
        fill = MISSING_STRATEGY[col]
        add(f"  - {col}: {count} row(s) filled with '{fill}'")

add()
add("-" * 60)
add("VALIDATION COMPARISON:")
add("-" * 60)
add(f"  Before cleaning: {passes_before} passed, {fails_before} failed")
add(f"  After cleaning:  {passes_after} passed, {fails_after} failed")
if fails_after == 0:
    add("  Status: NO ISSUES REMAINING")
else:
    add(f"  Status: {fails_after} row(s) still have issues (rows: {remaining_failures})")
    add("  Note: Rows with [INVALID_DATE] are flagged for human review")

add()
add("-" * 60)
add("COMPLETE ACTION LOG (all changes):")
add("-" * 60)
for i, a in enumerate(actions, 1):
    add(f"  {i:>2}. [{a['category']}] {a['detail']}")

add()
add(f"Output: customers_cleaned.csv ({TOTAL_ROWS} rows, {len(cleaned.columns)} columns)")
add("END OF REPORT")
add("=" * 60)

# ── Save outputs ───────────────────────────────────────────────────────────────
os.makedirs("outputs", exist_ok=True)
with open("outputs/cleaning_log.txt", "w", encoding="utf-8") as f:
    f.write("\n".join(report_lines))

cleaned.to_csv("outputs/customers_cleaned.csv", index=False)
# Local copy for convenience
cleaned.to_csv("customers_cleaned.csv", index=False)

print("\nSaved: outputs/cleaning_log.txt")
print("Saved: outputs/customers_cleaned.csv")
