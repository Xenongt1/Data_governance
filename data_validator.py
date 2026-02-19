"""
Part 3: Data Validator
Defines validation rules for each column and runs them against the raw data.
Captures every failure with the row, column, value, and reason.
"""

import pandas as pd
import re
import os
from datetime import datetime

# ── Load Data ──────────────────────────────────────────────────────────────────
df = pd.read_csv("customers_raw.csv", dtype=str)
df.columns = df.columns.str.strip()
for col in df.columns:
    df[col] = df[col].str.strip()

TOTAL_ROWS = len(df)

report_lines = []
def add(text=""):
    report_lines.append(text)
    # print(text) # Suppressed as per user request

# ══════════════════════════════════════════════════════════════════════════════
# VALIDATION RULES
# Each rule is a function that returns (passed: bool, reason: str)
# ══════════════════════════════════════════════════════════════════════════════

def safe(val):
    """Return empty string if NaN, else stripped string."""
    return "" if pd.isna(val) else str(val).strip()

# ── Rule definitions ──────────────────────────────────────────────────────────

def validate_customer_id(val, all_ids):
    val = safe(val)
    if not val:
        return False, "Empty customer_id"
    try:
        n = int(val)
        if n <= 0:
            return False, f"customer_id must be positive, got {n}"
    except:
        return False, f"customer_id must be an integer, got '{val}'"
    if all_ids.count(val) > 1:
        return False, f"Duplicate customer_id: {val}"
    return True, "OK"

def validate_name(val, field):
    val = safe(val)
    if not val:
        return False, f"{field} is empty"
    if len(val) < 2:
        return False, f"{field} too short (min 2 chars): '{val}'"
    if len(val) > 50:
        return False, f"{field} too long (max 50 chars): '{val}'"
    if not re.match(r"^[A-Za-z\-']+$", val):
        return False, f"{field} contains non-alphabetic characters: '{val}'"
    return True, "OK"

def validate_email(val):
    val = safe(val)
    if not val:
        return False, "Email is empty"
    pattern = re.compile(r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$")
    if not pattern.match(val):
        return False, f"Invalid email format: '{val}'"
    return True, "OK"

def validate_phone(val):
    val = safe(val)
    if not val:
        return False, "Phone is empty"
    # Accept any of the formats seen in the data — we flag non-standard ones
    pattern = re.compile(r"^(\(?\d{3}\)?[\s.\-]?\d{3}[\s.\-]?\d{4})$")
    if not pattern.match(val):
        return False, f"Unrecognizable phone format: '{val}'"
    standard = re.compile(r"^\d{3}-\d{3}-\d{4}$")
    if not standard.match(val):
        return False, f"Non-standard phone format (expected XXX-XXX-XXXX): '{val}'"
    return True, "OK"

def validate_date(val, field):
    val = safe(val)
    if not val:
        return False, f"{field} is empty"
    # Try all known formats
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%m/%d/%Y"):
        try:
            datetime.strptime(val, fmt)
            # Valid date but check if it's the standard format
            if fmt != "%Y-%m-%d":
                return False, f"{field} wrong format '{val}' (expected YYYY-MM-DD, got {fmt})"
            return True, "OK"
        except:
            pass
    return False, f"{field} is not a valid date: '{val}'"

def validate_date_of_birth(val):
    passed, reason = validate_date(val, "date_of_birth")
    if not passed:
        return passed, reason
    # Extra check: age must be 18-120
    try:
        dob = datetime.strptime(safe(val), "%Y-%m-%d")
        age = (datetime.now() - dob).days / 365.25
        if age < 18:
            return False, f"Customer appears to be under 18 (age ~{age:.1f}): '{val}'"
        if age > 120:
            return False, f"Date of birth implies impossibly old age ({age:.1f}): '{val}'"
    except:
        pass
    return True, "OK"

def validate_address(val):
    val = safe(val)
    if not val:
        return False, "Address is empty"
    if len(val) < 10:
        return False, f"Address too short (min 10 chars): '{val}'"
    if len(val) > 200:
        return False, f"Address too long (max 200 chars)"
    return True, "OK"

def validate_income(val):
    val = safe(val)
    if not val:
        return False, "Income is empty"
    try:
        n = float(val)
        if n < 0:
            return False, f"Income cannot be negative: {n}"
        if n > 10_000_000:
            return False, f"Income exceeds $10M cap: {n}"
    except:
        return False, f"Income is not a number: '{val}'"
    return True, "OK"

def validate_account_status(val):
    val = safe(val)
    VALID = {"active", "inactive", "suspended"}
    if not val:
        return False, "account_status is empty"
    if val.lower() not in VALID:
        return False, f"Invalid account_status '{val}' (must be: active, inactive, suspended)"
    return True, "OK"

# ══════════════════════════════════════════════════════════════════════════════
# RUN VALIDATION
# ══════════════════════════════════════════════════════════════════════════════

# Collect all customer_ids for uniqueness check
all_ids = df["customer_id"].tolist()

# failures[col] = list of {row, value, reason}
failures = {col: [] for col in df.columns}
row_pass_fail = []  # True/False per row

for idx, row in df.iterrows():
    row_num = idx + 1  # human-readable (1-indexed)
    row_failed = False

    def run_check(col, result):
        passed, reason = result
        if not passed:
            failures[col].append({
                "row": row_num,
                "customer_id": safe(row.get("customer_id")),
                "value": safe(row.get(col)),
                "reason": reason
            })
        return passed

    checks = [
        run_check("customer_id",    validate_customer_id(row["customer_id"], all_ids)),
        run_check("first_name",     validate_name(row["first_name"], "first_name")),
        run_check("last_name",      validate_name(row["last_name"], "last_name")),
        run_check("email",          validate_email(row["email"])),
        run_check("phone",          validate_phone(row["phone"])),
        run_check("date_of_birth",  validate_date_of_birth(row["date_of_birth"])),
        run_check("address",        validate_address(row["address"])),
        run_check("income",         validate_income(row["income"])),
        run_check("account_status", validate_account_status(row["account_status"])),
        run_check("created_date",   validate_date(row["created_date"], "created_date")),
    ]
    row_failed = not all(checks)

    row_pass_fail.append(not row_failed)

passed_rows = sum(row_pass_fail)
failed_rows = TOTAL_ROWS - passed_rows
total_failures = sum(len(v) for v in failures.values())

# ══════════════════════════════════════════════════════════════════════════════
# BUILD REPORT
# ══════════════════════════════════════════════════════════════════════════════

add("VALIDATION RESULTS")
add("=" * 60)
add()
add(f"SUMMARY:")
add(f"  Total rows:    {TOTAL_ROWS}")
add(f"  PASS (all checks): {passed_rows} rows")
add(f"  FAIL (any check):  {failed_rows} rows")
add(f"  Total individual failures: {total_failures}")
add()

add("FAILURES BY COLUMN:")
add("-" * 60)

for col, col_failures in failures.items():
    if not col_failures:
        add(f"\n{col}: OK - No failures")
        continue

    add(f"\n{col}: FAIL - {len(col_failures)} failure(s)")
    for f in col_failures:
        add(f"  - Row {f['row']} (customer_id={f['customer_id']}): {f['reason']}")

add()
add("-" * 60)
add("VALIDATION RULES REFERENCE:")
add("-" * 60)
rules = [
    ("customer_id",    "Unique, positive integer"),
    ("first_name",     "Non-empty, 2-50 chars, letters only"),
    ("last_name",      "Non-empty, 2-50 chars, letters only"),
    ("email",          "Valid format: something@domain.com"),
    ("phone",          "Standard format: XXX-XXX-XXXX"),
    ("date_of_birth",  "Valid date YYYY-MM-DD, age between 18-120"),
    ("address",        "Non-empty, 10-200 chars"),
    ("income",         "Non-negative number, <= $10,000,000"),
    ("account_status", "One of: active, inactive, suspended"),
    ("created_date",   "Valid date YYYY-MM-DD"),
]
for col, rule in rules:
    add(f"  {col:<20} -> {rule}")

add()
add("ROW-BY-ROW PASS/FAIL:")
add("-" * 60)
for idx, passed in enumerate(row_pass_fail):
    cid = safe(df.iloc[idx]["customer_id"])
    status = "PASS" if passed else "FAIL"
    add(f"  Row {idx+1} (customer_id={cid}): {status}")

add()
add("END OF REPORT")
add("=" * 60)

# ── Save ───────────────────────────────────────────────────────────────────────
os.makedirs("outputs", exist_ok=True)
with open("outputs/validation_results.txt", "w", encoding="utf-8") as f:
    f.write("\n".join(report_lines))

print("\nReport saved to outputs/validation_results.txt")
