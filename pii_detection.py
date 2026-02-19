"""
Part 2: PII Detection
Scans the raw customer data for personally identifiable information (PII),
quantifies exposure risk, and generates a PII detection report.
"""

import pandas as pd
import re
import os

# ── Load Data ──────────────────────────────────────────────────────────────────
df = pd.read_csv("customers_raw.csv", dtype=str)
df.columns = df.columns.str.strip()
for col in df.columns:
    df[col] = df[col].str.strip()

TOTAL_ROWS = len(df)

report_lines = []
def add(text=""):
    report_lines.append(text)
    # print(text)  # Suppressed as per user request

# ══════════════════════════════════════════════════════════════════════════════
# REGEX PATTERNS — these are the "detectors"
# ══════════════════════════════════════════════════════════════════════════════

PATTERNS = {
    "email": re.compile(
        r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$"
    ),
    "phone": re.compile(
        r"^(\(?\d{3}\)?[\s.\-]?\d{3}[\s.\-]?\d{4})$"
    ),
    "date": re.compile(
        r"^\d{4}[-/]\d{2}[-/]\d{2}$|^\d{2}/\d{2}/\d{4}$"
    ),
    "address": re.compile(
        r"^\d+\s+\w+.*"   # starts with a number then street name
    ),
    "name": re.compile(
        r"^[A-Za-z]{2,50}$"
    ),
}

# ══════════════════════════════════════════════════════════════════════════════
# PII COLUMN CLASSIFICATION
# ══════════════════════════════════════════════════════════════════════════════

PII_COLUMNS = {
    "first_name":    {"risk": "HIGH",   "category": "Direct Identifier",   "why": "Identifies the individual by name"},
    "last_name":     {"risk": "HIGH",   "category": "Direct Identifier",   "why": "Identifies the individual by name"},
    "email":         {"risk": "HIGH",   "category": "Contact Information",  "why": "Uniquely links to a person, enables phishing"},
    "phone":         {"risk": "HIGH",   "category": "Contact Information",  "why": "Direct contact vector, enables social engineering"},
    "date_of_birth": {"risk": "HIGH",   "category": "Sensitive Personal",  "why": "Used for identity verification + fraud"},
    "address":       {"risk": "HIGH",   "category": "Sensitive Personal",  "why": "Physical location, enables stalking or mail fraud"},
    "income":        {"risk": "MEDIUM", "category": "Financial Sensitivity","why": "Reveals wealth, enables targeted fraud"},
    "customer_id":   {"risk": "LOW",    "category": "Internal Identifier",  "why": "Alone it's harmless, but links all other PII together"},
}

# ══════════════════════════════════════════════════════════════════════════════
# DETECTION FUNCTIONS
# ══════════════════════════════════════════════════════════════════════════════

def detect_emails(series):
    """Find rows where the value looks like a valid email."""
    found = []
    for idx, val in series.items():
        if pd.notna(val) and PATTERNS["email"].match(str(val)):
            found.append({"row": idx + 1, "value": val})
    return found

def detect_phones(series):
    """Find rows where the value looks like a phone number."""
    found = []
    for idx, val in series.items():
        if pd.notna(val) and PATTERNS["phone"].match(str(val)):
            found.append({"row": idx + 1, "value": val})
    return found

def detect_addresses(series):
    """Find rows where the value looks like a street address."""
    found = []
    for idx, val in series.items():
        if pd.notna(val) and val.strip() != "" and PATTERNS["address"].match(str(val)):
            found.append({"row": idx + 1, "value": val})
    return found

def detect_dates_of_birth(series):
    """Find rows with a parseable date of birth."""
    found = []
    for idx, val in series.items():
        if pd.notna(val) and PATTERNS["date"].match(str(val).strip()):
            found.append({"row": idx + 1, "value": val})
    return found

def detect_names(series):
    """Find rows with a non-empty name."""
    found = []
    for idx, val in series.items():
        if pd.notna(val) and val.strip() != "" and PATTERNS["name"].match(str(val).strip()):
            found.append({"row": idx + 1, "value": val})
    return found

# ── Run detectors ─────────────────────────────────────────────────────────────
detected = {
    "first_name":    detect_names(df["first_name"]),
    "last_name":     detect_names(df["last_name"]),
    "email":         detect_emails(df["email"]),
    "phone":         detect_phones(df["phone"]),
    "address":       detect_addresses(df["address"]),
    "date_of_birth": detect_dates_of_birth(df["date_of_birth"]),
}

# ── Rows with COMBINED PII (most dangerous) ───────────────────────────────────
# A row is "fully exposed" if it has name + email + phone + address + DOB
fully_exposed = []
for idx, row in df.iterrows():
    def safe(v): return str(v).strip() if pd.notna(v) else ""
    fn = safe(row.get("first_name")); ln = safe(row.get("last_name"))
    has_name    = bool(fn) and bool(ln)
    has_email   = bool(safe(row.get("email"))) and bool(PATTERNS["email"].match(safe(row.get("email"))))
    has_phone   = bool(safe(row.get("phone"))) and bool(PATTERNS["phone"].match(safe(row.get("phone"))))
    has_address = bool(safe(row.get("address"))) and bool(PATTERNS["address"].match(safe(row.get("address"))))
    has_dob     = bool(safe(row.get("date_of_birth"))) and bool(PATTERNS["date"].match(safe(row.get("date_of_birth"))))

    pii_count = sum([has_name, has_email, has_phone, has_address, has_dob])
    fully_exposed.append({
        "customer_id": row["customer_id"],
        "pii_fields": pii_count,
        "fully_exposed": pii_count == 5
    })

fully_exposed_count = sum(1 for r in fully_exposed if r["fully_exposed"])
partial_exposure_count = sum(1 for r in fully_exposed if 0 < r["pii_fields"] < 5)

# ══════════════════════════════════════════════════════════════════════════════
# BUILD REPORT
# ══════════════════════════════════════════════════════════════════════════════

add("PII DETECTION REPORT")
add("=" * 60)
add()

# ── Risk Assessment ───────────────────────────────────────────────────────────
add("RISK ASSESSMENT:")
add("-" * 40)
add("  HIGH RISK columns (direct or sensitive PII):")
for col, info in PII_COLUMNS.items():
    if info["risk"] == "HIGH":
        add(f"    - {col}: [{info['category']}] {info['why']}")
add()
add("  MEDIUM RISK columns (financial sensitivity):")
for col, info in PII_COLUMNS.items():
    if info["risk"] == "MEDIUM":
        add(f"    - {col}: [{info['category']}] {info['why']}")
add()
add("  LOW RISK columns (no standalone PII):")
for col, info in PII_COLUMNS.items():
    if info["risk"] == "LOW":
        add(f"    - {col}: [{info['category']}] {info['why']}")
add()

# ── Detection Counts ──────────────────────────────────────────────────────────
add("DETECTED PII (by column):")
add("-" * 40)
for col, results in detected.items():
    pct = round(len(results) / TOTAL_ROWS * 100)
    add(f"  - {col}: {len(results)}/{TOTAL_ROWS} rows ({pct}%) contain PII")
add()

# ── Sample PII values ─────────────────────────────────────────────────────────
add("SAMPLE DETECTED VALUES (first 3 per column):")
add("-" * 40)
for col, results in detected.items():
    samples = [r["value"] for r in results[:3]]
    add(f"  {col}: {samples}")
add()

# ── Regex Pattern Breakdown ───────────────────────────────────────────────────
add("REGEX PATTERNS USED FOR DETECTION:")
add("-" * 40)
pattern_descriptions = {
    "email":   r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$   -> something@domain.com",
    "phone":   r"^(\(?\d{3}\)?[\s.\-]?\d{3}[\s.\-]?\d{4})$             -> (555) 123-4567 or 555.123.4567",
    "date":    r"^\d{4}[-/]\d{2}[-/]\d{2}$|^\d{2}/\d{2}/\d{4}$        -> YYYY-MM-DD or MM/DD/YYYY",
    "address": r"^\d+\s+\w+.*                                            -> starts with house number",
    "name":    r"^[A-Za-z]{2,50}$                                        -> letters only, 2-50 chars",
}
for ptype, desc in pattern_descriptions.items():
    add(f"  {ptype}: {desc}")
add()

# ── Exposure Risk ─────────────────────────────────────────────────────────────
add("EXPOSURE RISK ANALYSIS:")
add("-" * 40)
add(f"  - Fully exposed rows (all 5 PII fields present): {fully_exposed_count}/{TOTAL_ROWS}")
add(f"  - Partially exposed rows (some PII present):     {partial_exposure_count}/{TOTAL_ROWS}")
add()
add("  If this dataset were breached, attackers could:")
add("    FAIL PHISH customers          -> they have full email addresses")
add("    FAIL SPOOF identities         -> they have names + DOB + address combined")
add("    FAIL SOCIAL ENGINEER          -> they have phone numbers to call/text victims")
add("    FAIL COMMIT financial fraud   -> income data reveals high-value targets")
add("    FAIL PHYSICAL threats         -> home addresses are exposed")
add("    FAIL BYPASS security Qs       -> DOB is commonly used in identity verification")
add()

# ── Per-row exposure breakdown ────────────────────────────────────────────────
add("PER-ROW EXPOSURE BREAKDOWN:")
add("-" * 40)
add(f"  {'customer_id':<15} {'PII Fields':>12} {'Risk Level':>12}")
add(f"  {'-'*15} {'-'*12} {'-'*12}")
for r in fully_exposed:
    level = "CRITICAL" if r["pii_fields"] == 5 else ("HIGH" if r["pii_fields"] >= 3 else "LOW")
    add(f"  {r['customer_id']:<15} {r['pii_fields']:>12} {level:>12}")
add()

# ── Mitigation ────────────────────────────────────────────────────────────────
add("MITIGATION RECOMMENDATIONS:")
add("-" * 40)
add("  1. MASK all PII before sharing with analytics teams (Part 5)")
add("  2. ENCRYPT the dataset at rest (AES-256)")
add("  3. RESTRICT access - only authorized roles should see raw PII")
add("  4. AUDIT LOGS - track who accesses this data and when")
add("  5. DATA MINIMIZATION - only collect PII fields you actually need")
add("  6. GDPR/CCPA compliance requires customers to consent to data storage")
add()
add("END OF REPORT")
add("=" * 60)

# ── Save ───────────────────────────────────────────────────────────────────────
os.makedirs("outputs", exist_ok=True)
with open("outputs/pii_detection_report.txt", "w", encoding="utf-8") as f:
    f.write("\n".join(report_lines))

print("\nReport saved to outputs/pii_detection_report.txt")
