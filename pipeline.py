"""
Part 6: End-to-End Pipeline
============================
Orchestrates all steps into a single automated workflow:

  Stage 1: LOAD    - read raw CSV
  Stage 2: CLEAN   - normalize formats, fill missing values
  Stage 3: VALIDATE - check all rules, fail fast on critical errors
  Stage 4: DETECT PII - scan and quantify sensitive data
  Stage 5: MASK    - hide all PII fields
  Stage 6: SAVE    - write cleaned + masked outputs + all reports

Run with:  python pipeline.py
Input:     customers_raw.csv
Outputs:   customers_cleaned.csv, customers_masked.csv, + 4 report files
"""

import pandas as pd
import re
import logging
import os
from datetime import datetime
from pathlib import Path

# ══════════════════════════════════════════════════════════════════════════════
# SETUP
# ══════════════════════════════════════════════════════════════════════════════

# Using relative path for Windows compatibility
OUTPUT_DIR = Path("outputs")
OUTPUT_DIR.mkdir(exist_ok=True)

from logger import setup_logger
log = setup_logger("pipeline")
from data_validator import run_validation

# Track execution for the final report
pipeline_log = []
start_time = datetime.now()

def stage(name):
    """Print a bold stage header and log it."""
    log.info(f"--- STAGE: {name} ---")
    pipeline_log.append({"stage": name, "events": []})

def event(msg, ok=True):
    """Log an event inside the current stage."""
    icon = "[OK]" if ok else "[FAIL]"
    log.info(f"{icon} {msg}")
    pipeline_log[-1]["events"].append({"msg": msg, "ok": ok})

def safe(val):
    return "" if pd.isna(val) else str(val).strip()

# ══════════════════════════════════════════════════════════════════════════════
# STAGE 1: LOAD
# ══════════════════════════════════════════════════════════════════════════════

stage("1: LOAD")

try:
    df = pd.read_csv("customers_raw.csv", dtype=str)
    df.columns = df.columns.str.strip()
    for col in df.columns:
        df[col] = df[col].str.strip()
    TOTAL_ROWS = len(df)
    TOTAL_COLS = len(df.columns)
    event(f"Loaded customers_raw.csv - {TOTAL_ROWS} rows, {TOTAL_COLS} columns")
except FileNotFoundError:
    event("customers_raw.csv not found!", ok=False)
    raise SystemExit("Pipeline aborted: input file missing.")

# ══════════════════════════════════════════════════════════════════════════════
# STAGE 2: CLEAN
# ══════════════════════════════════════════════════════════════════════════════

stage("2: CLEAN")

def normalize_phone(val):
    val = safe(val)
    if not val:
        return val
    digits = re.sub(r"\D", "", val)
    return f"{digits[:3]}-{digits[3:6]}-{digits[6:]}" if len(digits) == 10 else val

def normalize_date(val):
    val = safe(val)
    if not val:
        return val
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%m/%d/%Y"):
        try:
            return datetime.strptime(val, fmt).strftime("%Y-%m-%d")
        except:
            pass
    return "[INVALID_DATE]"

def normalize_name(val):
    val = safe(val)
    return val.title() if val else val

def normalize_email(val):
    val = safe(val)
    return val.lower() if val else val

MISSING_FILL = {
    "first_name":     "[UNKNOWN]",
    "last_name":      "[UNKNOWN]",
    "address":        "[UNKNOWN]",
    "income":         "0",
    "account_status": "unknown",
}

cleaned = df.copy()
phone_fixed = date_fixed = name_fixed = email_fixed = missing_fixed = 0

for idx in range(len(cleaned)):
    row = cleaned.iloc[idx]

    # Phone
    orig = safe(row["phone"])
    new  = normalize_phone(orig)
    if orig and new != orig:
        cleaned.at[idx, "phone"] = new
        phone_fixed += 1

    # Dates
    for dcol in ["date_of_birth", "created_date"]:
        orig = safe(row[dcol])
        new  = normalize_date(orig)
        if orig and new != orig:
            cleaned.at[idx, dcol] = new
            date_fixed += 1

    # Names
    for ncol in ["first_name", "last_name"]:
        orig = safe(row[ncol])
        new  = normalize_name(orig)
        if orig and new != orig:
            cleaned.at[idx, ncol] = new
            name_fixed += 1

    # Email
    orig = safe(row["email"])
    new  = normalize_email(orig)
    if orig and new != orig:
        cleaned.at[idx, "email"] = new
        email_fixed += 1

# Fill missing values
for col, fill in MISSING_FILL.items():
    mask = cleaned[col].isna() | cleaned[col].str.strip().eq("")
    count = int(mask.sum())
    if count:
        cleaned.loc[mask, col] = fill
        missing_fixed += count

event(f"Phone formats normalized:   {phone_fixed} row(s)")
event(f"Date formats normalized:    {date_fixed} row(s)")
event(f"Name casing fixed:          {name_fixed} row(s)")
event(f"Email casing fixed:         {email_fixed} row(s)")
event(f"Missing values filled:      {missing_fixed} field(s)")

# ══════════════════════════════════════════════════════════════════════════════
# STAGE 3: VALIDATE
# ══════════════════════════════════════════════════════════════════════════════

stage("3: VALIDATE")

def vname(v):
    v = safe(v)
    return not v or v == "[UNKNOWN]" or bool(re.match(r"^[A-Za-z\-']{2,50}$", v))

# ── Save temporary cleaned file for GX validation ─────────────────────────────
temp_cleaned_path = "outputs/temp_cleaned.csv"
cleaned.to_csv(temp_cleaned_path, index=False)

# ── Run Great Expectations Validation ─────────────────────────────────────────
gx_success = run_validation(data_path=temp_cleaned_path, output_report="outputs/validation_results.txt")

if gx_success:
    event("All rows passed Great Expectations validation")
else:
    event("Validation failures detected via Great Expectations", ok=False)
    # Note: detailed failures are in outputs/validation_results.txt
    
# ── Hard stop if critical failures remain ─────────────────────────────────────
# GX success being False is enough to stop the pipeline if we consider any failure critical
# or we could parse the GX results for specific columns if needed.
# For simplicity, we'll stop on any GX failure in this stage.
if not gx_success:
    event("CRITICAL validation failures detected - pipeline aborting", ok=False)
    log.error("Pipeline aborted: Great Expectations validation failed.")
    raise SystemExit("Pipeline aborted: critical data quality issues remain.")
else:
    event("No critical validation failures - pipeline continuing")


# ══════════════════════════════════════════════════════════════════════════════
# STAGE 4: DETECT PII
# ══════════════════════════════════════════════════════════════════════════════

stage("4: DETECT PII")

EMAIL_RE   = re.compile(r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$")
PHONE_RE   = re.compile(r"^\d{3}-\d{3}-\d{4}$")
DATE_RE    = re.compile(r"^\d{4}-\d{2}-\d{2}$")
ADDRESS_RE = re.compile(r"^\d+\s+\w+")

pii_counts = {
    "emails":    int(cleaned["email"].apply(lambda v: bool(EMAIL_RE.match(safe(v)))).sum()),
    "phones":    int(cleaned["phone"].apply(lambda v: bool(PHONE_RE.match(safe(v)))).sum()),
    "addresses": int(cleaned["address"].apply(lambda v: bool(ADDRESS_RE.match(safe(v)))).sum()),
    "dates_of_birth": int(cleaned["date_of_birth"].apply(lambda v: bool(DATE_RE.match(safe(v)))).sum()),
    "first_names": int(cleaned["first_name"].apply(lambda v: safe(v) not in ("", "[UNKNOWN]")).sum()),
    "last_names":  int(cleaned["last_name"].apply(lambda v: safe(v) not in ("", "[UNKNOWN]")).sum()),
}

for field, count in pii_counts.items():
    pct = round(count / TOTAL_ROWS * 100)
    event(f"Found PII - {field}: {count}/{TOTAL_ROWS} rows ({pct}%)")

# ══════════════════════════════════════════════════════════════════════════════
# STAGE 5: MASK
# ══════════════════════════════════════════════════════════════════════════════

stage("5: MASK")

def mask_name(v):
    v = safe(v)
    return v if (not v or v == "[UNKNOWN]") else v[0] + "***"

def mask_email(v):
    v = safe(v)
    if not v or "@" not in v:
        return v
    local, domain = v.split("@", 1)
    return local[0] + "***@" + domain

def mask_phone(v):
    v = safe(v)
    parts = v.split("-")
    return f"***-***-{parts[2]}" if len(parts) == 3 else v

def mask_address(v):
    v = safe(v)
    return v if (not v or v == "[UNKNOWN]") else "[MASKED ADDRESS]"

def mask_dob(v):
    v = safe(v)
    if not v or v == "[INVALID_DATE]":
        return v
    parts = v.split("-")
    return f"{parts[0]}-**-**" if len(parts) == 3 else v

masked = cleaned.copy()
masked["first_name"]    = cleaned["first_name"].apply(mask_name)
masked["last_name"]     = cleaned["last_name"].apply(mask_name)
masked["email"]         = cleaned["email"].apply(mask_email)
masked["phone"]         = cleaned["phone"].apply(mask_phone)
masked["address"]       = cleaned["address"].apply(mask_address)
masked["date_of_birth"] = cleaned["date_of_birth"].apply(mask_dob)

event("Names masked       (first_name, last_name)")
event("Emails masked      (local part hidden, domain kept)")
event("Phones masked      (last 4 digits kept)")
event("Addresses masked   (fully replaced)")
event("Dates of birth masked (year kept, month/day hidden)")
event("income, account_status, created_date - NOT masked (business data)")

# ══════════════════════════════════════════════════════════════════════════════
# STAGE 6: SAVE
# ══════════════════════════════════════════════════════════════════════════════

stage("6: SAVE")

# Save data files
cleaned.to_csv(OUTPUT_DIR / "customers_cleaned.csv", index=False)
event("Saved customers_cleaned.csv")

masked.to_csv(OUTPUT_DIR / "customers_masked.csv", index=False)
event("Saved customers_masked.csv")

# ── Generate pipeline execution report ────────────────────────────────────────
end_time = datetime.now()
duration = (end_time - start_time).total_seconds()

report = []
def rpt(text=""):
    report.append(text)

rpt("PIPELINE EXECUTION REPORT")
rpt("=" * 60)
rpt(f"Timestamp: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
rpt(f"Duration:  {duration:.2f} seconds")
rpt()

for entry in pipeline_log:
    rpt(f"Stage {entry['stage']}:")
    for ev in entry["events"]:
        icon = "[OK]" if ev["ok"] else "[FAIL]"
        rpt(f"  {icon} {ev['msg']}")
    rpt()

rpt("-" * 60)
rpt("SUMMARY:")
rpt("-" * 60)
rpt(f"  Input:    customers_raw.csv ({TOTAL_ROWS} rows, messy)")
rpt(f"  Output:   customers_cleaned.csv + customers_masked.csv ({TOTAL_ROWS} rows, clean)")
rpt()
rpt("  Cleaning:")
rpt(f"    - {phone_fixed} phone(s) normalized")
rpt(f"    - {date_fixed} date(s) normalized")
rpt(f"    - {name_fixed} name(s) title-cased")
rpt(f"    - {email_fixed} email(s) lowercased")
rpt(f"    - {missing_fixed} missing value(s) filled")
rpt()
rpt("  Validation:")
rpt(f"    - Status: {'PASS' if gx_success else 'FAIL'} (Great Expectations)")
rpt()
rpt("  PII Detection:")
for field, count in pii_counts.items():
    rpt(f"    - {count} {field} detected and flagged")
rpt()
rpt("  PII Masking:")
rpt("    - first_name, last_name, email, phone, address, date_of_birth -> masked")
rpt("    - income, account_status, created_date, customer_id -> preserved")
rpt()
rpt("  Quality:  PASS")
rpt("  PII Risk: MITIGATED")
rpt()
rpt("  Files saved:")
rpt("    - customers_cleaned.csv")
rpt("    - customers_masked.csv")
rpt("    - pipeline_execution_report.txt")
rpt()
rpt(f"Status: SUCCESS")
rpt("=" * 60)

report_text = "\n".join(report)

with open(OUTPUT_DIR / "pipeline_execution_report.txt", "w", encoding="utf-8") as f:
    f.write(report_text)

event("Saved pipeline_execution_report.txt")

# Final success message to log
log.info(f"PIPELINE COMPLETE - Duration: {duration:.2f}s")
log.info(f"All outputs saved to: {OUTPUT_DIR}")
