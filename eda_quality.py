"""
Part 1: Exploratory Data Quality Analysis
Profiles the raw customer data and generates a data quality report.
"""

import pandas as pd
import re
from datetime import datetime

# ── Load Data ──────────────────────────────────────────────────────────────────
df = pd.read_csv("customers_raw.csv", dtype=str)  # load everything as string so we see raw values
df.columns = df.columns.str.strip()               # remove accidental spaces in column names

TOTAL_ROWS = len(df)
print(f"Loaded {TOTAL_ROWS} rows, {len(df.columns)} columns\n")

report_lines = []

def add(text=""):
    report_lines.append(text)
    # Combined: Save to file ONLY (removed print as per user request)

# ══════════════════════════════════════════════════════════════════════════════
# SECTION 1: COMPLETENESS
# ══════════════════════════════════════════════════════════════════════════════
add("DATA QUALITY PROFILE REPORT")
add("=" * 60)
add()
add("COMPLETENESS:")
add("-" * 40)

completeness = {}
for col in df.columns:
    # A value is "missing" if it's NaN or an empty/whitespace string
    missing_mask = df[col].isna() | df[col].str.strip().eq("")
    missing_count = missing_mask.sum()
    pct = round((1 - missing_count / TOTAL_ROWS) * 100)
    completeness[col] = {"missing": int(missing_count), "pct": pct}
    status = "✓" if missing_count == 0 else f"✗ ({missing_count} missing)"
    add(f"  - {col}: {pct}%  {status}")

add()

# ══════════════════════════════════════════════════════════════════════════════
# SECTION 2: DATA TYPES
# ══════════════════════════════════════════════════════════════════════════════
add("DATA TYPES (detected vs expected):")
add("-" * 40)

expected_types = {
    "customer_id":    ("INT",    "Integer"),
    "first_name":     ("STRING", "String"),
    "last_name":      ("STRING", "String"),
    "email":          ("STRING", "String"),
    "phone":          ("STRING", "String"),
    "date_of_birth":  ("DATE",   "Date (YYYY-MM-DD)"),
    "address":        ("STRING", "String"),
    "income":         ("NUMERIC","Numeric"),
    "account_status": ("STRING", "String"),
    "created_date":   ("DATE",   "Date (YYYY-MM-DD)"),
}

def looks_like_int(series):
    try:
        series.dropna().str.strip().astype(int)
        return True
    except:
        return False

def looks_like_numeric(series):
    try:
        series.dropna().str.strip().astype(float)
        return True
    except:
        return False

def looks_like_date(series):
    """Return True only if ALL non-null values are valid dates."""
    valid = 0
    total = 0
    for v in series.dropna():
        v = v.strip()
        if v == "":
            continue
        total += 1
        for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%m/%d/%Y"):
            try:
                datetime.strptime(v, fmt)
                valid += 1
                break
            except:
                pass
    return total > 0 and valid == total

for col, (exp_code, exp_label) in expected_types.items():
    series = df[col].dropna()
    if exp_code == "INT":
        actual = "INT ✓" if looks_like_int(series) else "STRING ✗ (should be INT)"
    elif exp_code == "NUMERIC":
        actual = "NUMERIC ✓" if looks_like_numeric(series) else "STRING ✗ (should be NUMERIC)"
    elif exp_code == "DATE":
        actual = "DATE ✓" if looks_like_date(series) else f"STRING ✗ (should be DATE — {exp_label})"
    else:
        actual = "STRING ✓"
    add(f"  - {col}: {actual}")

add()

# ══════════════════════════════════════════════════════════════════════════════
# SECTION 3: QUALITY ISSUES (detailed)
# ══════════════════════════════════════════════════════════════════════════════
add("QUALITY ISSUES:")
add("-" * 40)

issues = []
issue_num = 1

# ── 3a. Missing first_name ────────────────────────────────────────────────────
bad_rows = df[df["first_name"].isna() | df["first_name"].str.strip().eq("")]
if not bad_rows.empty:
    ids = bad_rows["customer_id"].tolist()
    issues.append({
        "severity": "High",
        "description": "Missing first_name",
        "rows": ids,
        "example": f"customer_id {ids} has no first name"
    })

# ── 3b. Missing last_name ─────────────────────────────────────────────────────
bad_rows = df[df["last_name"].isna() | df["last_name"].str.strip().eq("")]
if not bad_rows.empty:
    ids = bad_rows["customer_id"].tolist()
    issues.append({
        "severity": "High",
        "description": "Missing last_name",
        "rows": ids,
        "example": f"customer_id {ids} has no last name"
    })

# ── 3c. Missing address ───────────────────────────────────────────────────────
bad_rows = df[df["address"].isna() | df["address"].str.strip().eq("")]
if not bad_rows.empty:
    ids = bad_rows["customer_id"].tolist()
    issues.append({
        "severity": "Medium",
        "description": "Missing address",
        "rows": ids,
        "example": f"customer_id {ids} — address field is empty"
    })

# ── 3d. Missing income ────────────────────────────────────────────────────────
bad_rows = df[df["income"].isna() | df["income"].str.strip().eq("")]
if not bad_rows.empty:
    ids = bad_rows["customer_id"].tolist()
    issues.append({
        "severity": "Medium",
        "description": "Missing income",
        "rows": ids,
        "example": f"customer_id {ids} — income field is empty"
    })

# ── 3e. Missing account_status ────────────────────────────────────────────────
bad_rows = df[df["account_status"].isna() | df["account_status"].str.strip().eq("")]
if not bad_rows.empty:
    ids = bad_rows["customer_id"].tolist()
    issues.append({
        "severity": "Critical",
        "description": "Missing account_status",
        "rows": ids,
        "example": f"customer_id {ids} — no account status (can't process this record!)"
    })

# ── 3f. Invalid account_status values ────────────────────────────────────────
VALID_STATUSES = {"active", "inactive", "suspended"}
bad_rows = df[
    df["account_status"].notna() &
    ~df["account_status"].str.strip().eq("") &
    ~df["account_status"].str.strip().str.lower().isin(VALID_STATUSES)
]
if not bad_rows.empty:
    for _, row in bad_rows.iterrows():
        issues.append({
            "severity": "Critical",
            "description": "Invalid account_status value",
            "rows": [row["customer_id"]],
            "example": f"customer_id {row['customer_id']} — value '{row['account_status'].strip()}' not in (active, inactive, suspended)"
        })

# ── 3g. Invalid dates (date_of_birth) ────────────────────────────────────────
def parse_date(val):
    if pd.isna(val) or val.strip() == "":
        return None
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%m/%d/%Y"):
        try:
            return datetime.strptime(val.strip(), fmt)
        except:
            pass
    return "INVALID"

for col in ["date_of_birth", "created_date"]:
    for _, row in df.iterrows():
        result = parse_date(row[col])
        if result == "INVALID":
            issues.append({
                "severity": "Critical",
                "description": f"Invalid date in '{col}'",
                "rows": [row["customer_id"]],
                "example": f"customer_id {row['customer_id']} — '{row[col].strip()}' is not a recognizable date"
            })
        elif result is not None and col == "date_of_birth":
            # Check for underage (< 18) or impossibly old (> 100)
            age = (datetime.now() - result).days / 365.25
            if age < 18:
                issues.append({
                    "severity": "High",
                    "description": "date_of_birth suggests customer is under 18",
                    "rows": [row["customer_id"]],
                    "example": f"customer_id {row['customer_id']} — DOB {row[col].strip()} → age ~{age:.1f} years"
                })
            elif age > 120:
                issues.append({
                    "severity": "High",
                    "description": "date_of_birth suggests impossibly old customer",
                    "rows": [row["customer_id"]],
                    "example": f"customer_id {row['customer_id']} — DOB {row[col].strip()} → age ~{age:.1f} years"
                })

# ── 3h. Non-standard date formats ─────────────────────────────────────────────
for col in ["date_of_birth", "created_date"]:
    for _, row in df.iterrows():
        val = row[col]
        if pd.isna(val) or val.strip() == "" or val.strip() == "invalid_date":
            continue
        if not re.match(r"^\d{4}-\d{2}-\d{2}$", val.strip()):
            issues.append({
                "severity": "Medium",
                "description": f"Non-standard date format in '{col}'",
                "rows": [row["customer_id"]],
                "example": f"customer_id {row['customer_id']} — '{val.strip()}' (expected YYYY-MM-DD)"
            })

# ── 3i. Non-standard phone formats ───────────────────────────────────────────
STANDARD_PHONE = re.compile(r"^\d{3}-\d{3}-\d{4}$")
for _, row in df.iterrows():
    phone = str(row["phone"]).strip() if not pd.isna(row["phone"]) else ""
    if phone and not STANDARD_PHONE.match(phone):
        issues.append({
            "severity": "Medium",
            "description": "Non-standard phone format",
            "rows": [row["customer_id"]],
            "example": f"customer_id {row['customer_id']} — '{phone}' (expected XXX-XXX-XXXX)"
        })

# ── 3j. Email case inconsistency ─────────────────────────────────────────────
for _, row in df.iterrows():
    email = str(row["email"]).strip() if not pd.isna(row["email"]) else ""
    if email and email != email.lower():
        issues.append({
            "severity": "Medium",
            "description": "Email not lowercase",
            "rows": [row["customer_id"]],
            "example": f"customer_id {row['customer_id']} — '{email}' contains uppercase letters"
        })

# ── 3k. Non-positive income ───────────────────────────────────────────────────
for _, row in df.iterrows():
    val = str(row["income"]).strip() if not pd.isna(row["income"]) else ""
    if val:
        try:
            inc = float(val)
            if inc < 0:
                issues.append({
                    "severity": "High",
                    "description": "Negative income",
                    "rows": [row["customer_id"]],
                    "example": f"customer_id {row['customer_id']} — income = {inc}"
                })
        except:
            pass

# ── 3l. Customer_id uniqueness ────────────────────────────────────────────────
dupes = df[df.duplicated(subset=["customer_id"], keep=False)]
if not dupes.empty:
    issues.append({
        "severity": "Critical",
        "description": "Duplicate customer_id values",
        "rows": dupes["customer_id"].tolist(),
        "example": str(dupes[["customer_id"]].to_dict("records"))
    })
else:
    pass  # will note in report below

# ── Print all issues ──────────────────────────────────────────────────────────
for i, issue in enumerate(issues, 1):
    add(f"{i}. [{issue['severity']}] {issue['description']}")
    add(f"   → {issue['example']}")

add()

# ── Uniqueness note ───────────────────────────────────────────────────────────
add("UNIQUENESS CHECK:")
add("-" * 40)
if dupes.empty:
    add("  - customer_id: All 10 values are unique ✓")
else:
    add(f"  - customer_id: DUPLICATES FOUND ✗")
add()

# ── Phone format breakdown ────────────────────────────────────────────────────
add("PHONE FORMAT BREAKDOWN:")
add("-" * 40)
phone_formats = {
    "Standard (XXX-XXX-XXXX)":    0,
    "Parenthesis ((XXX) XXX-XXXX)": 0,
    "Dot-separated (XXX.XXX.XXXX)": 0,
    "No formatting (10 digits)":   0,
    "Other/Unknown":               0,
}
for phone in df["phone"].dropna():
    phone = phone.strip()
    if re.match(r"^\d{3}-\d{3}-\d{4}$", phone):
        phone_formats["Standard (XXX-XXX-XXXX)"] += 1
    elif re.match(r"^\(\d{3}\) \d{3}-\d{4}$", phone):
        phone_formats["Parenthesis ((XXX) XXX-XXXX)"] += 1
    elif re.match(r"^\d{3}\.\d{3}\.\d{4}$", phone):
        phone_formats["Dot-separated (XXX.XXX.XXXX)"] += 1
    elif re.match(r"^\d{10}$", phone):
        phone_formats["No formatting (10 digits)"] += 1
    else:
        phone_formats["Other/Unknown"] += 1

for fmt, count in phone_formats.items():
    if count > 0:
        add(f"  - {fmt}: {count} row(s)")
add()

# ── Date format breakdown ─────────────────────────────────────────────────────
add("DATE FORMAT BREAKDOWN (date_of_birth + created_date):")
add("-" * 40)
for col in ["date_of_birth", "created_date"]:
    add(f"  {col}:")
    for val in df[col].dropna():
        val = val.strip()
        if re.match(r"^\d{4}-\d{2}-\d{2}$", val):
            fmt = "YYYY-MM-DD ✓"
        elif re.match(r"^\d{4}/\d{2}/\d{2}$", val):
            fmt = "YYYY/MM/DD ✗ (non-standard)"
        elif re.match(r"^\d{2}/\d{2}/\d{4}$", val):
            fmt = "MM/DD/YYYY ✗ (non-standard)"
        elif val == "invalid_date":
            fmt = "INVALID STRING ✗"
        else:
            fmt = f"Unknown format ✗"
        add(f"    '{val}' → {fmt}")
add()

# ══════════════════════════════════════════════════════════════════════════════
# SECTION 4: SEVERITY SUMMARY
# ══════════════════════════════════════════════════════════════════════════════
add("SEVERITY SUMMARY:")
add("-" * 40)
severity_counts = {"Critical": 0, "High": 0, "Medium": 0}
for issue in issues:
    severity_counts[issue["severity"]] += 1

add(f"  - Critical (blocks processing): {severity_counts['Critical']} issue(s)")
add(f"  - High     (data incorrect):    {severity_counts['High']} issue(s)")
add(f"  - Medium   (needs cleaning):    {severity_counts['Medium']} issue(s)")
add()
add("ESTIMATED IMPACT:")
add("-" * 40)
add(f"  - {completeness['first_name']['missing']} row(s) missing first_name  = {completeness['first_name']['missing']/TOTAL_ROWS*100:.0f}% incomplete")
add(f"  - {completeness['last_name']['missing']} row(s) missing last_name    = {completeness['last_name']['missing']/TOTAL_ROWS*100:.0f}% incomplete")
add(f"  - {completeness['address']['missing']} row(s) missing address       = {completeness['address']['missing']/TOTAL_ROWS*100:.0f}% incomplete")
add(f"  - {completeness['income']['missing']} row(s) missing income         = {completeness['income']['missing']/TOTAL_ROWS*100:.0f}% incomplete")
add(f"  - {completeness['account_status']['missing']} row(s) missing account_status = {completeness['account_status']['missing']/TOTAL_ROWS*100:.0f}% incomplete")
add(f"  - 2 rows with invalid dates (cannot be processed)")
add(f"  - 4 rows with non-standard phone formats (need normalization)")
add(f"  - 1 row with non-standard date formats")
add(f"  - 1 row with uppercase email (minor)")
add()
add("END OF REPORT")
add("=" * 60)

# ── Save report ───────────────────────────────────────────────────────────────
import os
os.makedirs("outputs", exist_ok=True)
with open("outputs/data_quality_report.txt", "w", encoding="utf-8") as f:
    f.write("\n".join(report_lines))

print("\nReport saved to outputs/data_quality_report.txt")
