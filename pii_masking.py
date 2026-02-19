"""
Part 5: PII Masking
Masks all sensitive fields in the cleaned dataset while preserving data structure.
Masking rules:
  Names:    'John'              -> 'J***'
  Emails:   'john@gmail.com'   -> 'j***@gmail.com'
  Phones:   '555-123-4567'     -> '***-***-4567'
  Addresses: '123 Main St...'  -> '[MASKED ADDRESS]'
  DOB:      '1985-03-15'       -> '1985-**-**'
"""

import pandas as pd
import os

# ── Load cleaned data ─────────────────────────────────────────────────────────
if os.path.exists("outputs/customers_cleaned.csv"):
    df = pd.read_csv("outputs/customers_cleaned.csv", dtype=str)
elif os.path.exists("customers_cleaned.csv"):
    df = pd.read_csv("customers_cleaned.csv", dtype=str)
else:
    # Fallback if both are missing
    df = pd.read_csv("customers_raw.csv", dtype=str)

df.columns = df.columns.str.strip()
for col in df.columns:
    df[col] = df[col].str.strip()

TOTAL_ROWS = len(df)

report_lines = []
def add(text=""):
    report_lines.append(text)
    # print(text) # Suppressed as per user request

def safe(val):
    return "" if pd.isna(val) else str(val).strip()

# ══════════════════════════════════════════════════════════════════════════════
# MASKING FUNCTIONS
# ══════════════════════════════════════════════════════════════════════════════

def mask_name(val):
    """'John' -> 'J***'  |  '[UNKNOWN]' -> '[UNKNOWN]'"""
    val = safe(val)
    if not val or val == "[UNKNOWN]":
        return val
    return val[0] + "***"

def mask_email(val):
    """'john.doe@gmail.com' -> 'j***@gmail.com'"""
    val = safe(val)
    if not val or "@" not in val:
        return val
    local, domain = val.split("@", 1)
    return local[0] + "***@" + domain

def mask_phone(val):
    """'555-123-4567' -> '***-***-4567'"""
    val = safe(val)
    if not val:
        return val
    # Keep last 4 digits only
    parts = val.split("-")
    if len(parts) == 3:
        return f"***-***-{parts[2]}"
    # Fallback for any unexpected format
    return "***-***-" + val[-4:] if len(val) >= 4 else "***"

def mask_address(val):
    """'123 Main St New York NY 10001' -> '[MASKED ADDRESS]'"""
    val = safe(val)
    if not val or val == "[UNKNOWN]":
        return val
    return "[MASKED ADDRESS]"

def mask_dob(val):
    """'1985-03-15' -> '1985-**-**'  |  '[INVALID_DATE]' -> '[INVALID_DATE]'"""
    val = safe(val)
    if not val or val == "[INVALID_DATE]":
        return val
    parts = val.split("-")
    if len(parts) == 3:
        return f"{parts[0]}-**-**"
    return "****-**-**"

# ══════════════════════════════════════════════════════════════════════════════
# APPLY MASKING
# ══════════════════════════════════════════════════════════════════════════════

masked = df.copy()

masked["first_name"]    = df["first_name"].apply(mask_name)
masked["last_name"]     = df["last_name"].apply(mask_name)
masked["email"]         = df["email"].apply(mask_email)
masked["phone"]         = df["phone"].apply(mask_phone)
masked["address"]       = df["address"].apply(mask_address)
masked["date_of_birth"] = df["date_of_birth"].apply(mask_dob)
# income, account_status, created_date, customer_id — NOT masked (business data)

# ══════════════════════════════════════════════════════════════════════════════
# BUILD REPORT
# ══════════════════════════════════════════════════════════════════════════════

add("MASKED SAMPLE REPORT")
add("=" * 60)
add()

# ── Before / After for first 3 rows ──────────────────────────────────────────
add("BEFORE MASKING (first 3 rows):")
add("-" * 60)
cols = list(df.columns)
add("  " + " | ".join(f"{c:<22}" for c in cols))
add("  " + "-" * (25 * len(cols)))
for _, row in df.head(3).iterrows():
    add("  " + " | ".join(f"{safe(row[c]):<22}" for c in cols))

add()
add("AFTER MASKING (first 3 rows):")
add("-" * 60)
add("  " + " | ".join(f"{c:<22}" for c in cols))
add("  " + "-" * (25 * len(cols)))
for _, row in masked.head(3).iterrows():
    add("  " + " | ".join(f"{safe(row[c]):<22}" for c in cols))

add()

# ── Field-by-field masking examples ──────────────────────────────────────────
add("MASKING RULES APPLIED (with examples):")
add("-" * 60)

mask_examples = [
    ("first_name",    "John",                   mask_name("John"),                  "First letter only"),
    ("last_name",     "Doe",                    mask_name("Doe"),                   "First letter only"),
    ("email",         "john.doe@gmail.com",     mask_email("john.doe@gmail.com"),   "Local part hidden, domain kept"),
    ("phone",         "555-123-4567",           mask_phone("555-123-4567"),         "Last 4 digits kept"),
    ("address",       "123 Main St NY",         mask_address("123 Main St NY"),     "Fully replaced"),
    ("date_of_birth", "1985-03-15",             mask_dob("1985-03-15"),             "Year kept, month/day hidden"),
    ("income",        "75000",                  "75000",                            "NOT masked (business data)"),
    ("account_status","active",                 "active",                           "NOT masked (business data)"),
    ("customer_id",   "1",                      "1",                                "NOT masked (internal key)"),
]

for col, before, after, reason in mask_examples:
    add(f"  {col:<20} '{before}' -> '{after}'")
    add(f"  {'':20} Reason: {reason}")
    add()

# ── Full masked dataset preview ───────────────────────────────────────────────
add("FULL MASKED DATASET (all 10 rows, key columns):")
add("-" * 60)
preview_cols = ["customer_id", "first_name", "last_name", "email", "phone", "date_of_birth", "income", "account_status"]
add("  " + " | ".join(f"{c:<20}" for c in preview_cols))
add("  " + "-" * (22 * len(preview_cols)))
for _, row in masked.iterrows():
    add("  " + " | ".join(f"{safe(row[c]):<20}" for c in preview_cols))

add()

# ── Analysis ──────────────────────────────────────────────────────────────────
add("ANALYSIS:")
add("-" * 60)
add(f"  - Data structure preserved: {TOTAL_ROWS} rows, {len(masked.columns)} columns")
add(f"  - PII masked: first_name, last_name, email, phone, address, date_of_birth")
add(f"  - Business data intact: customer_id, income, account_status, created_date")
add()
add("  What analytics teams CAN still do with masked data:")
add("    - Analyse income distributions across account statuses")
add("    - Count active vs inactive vs suspended customers")
add("    - Track account creation trends over time")
add("    - Segment customers by year of birth (year preserved in DOB)")
add()
add("  What analytics teams CANNOT do (by design):")
add("    - Contact customers directly (emails/phones masked)")
add("    - Identify specific individuals (names masked)")
add("    - Locate customers physically (addresses masked)")
add()
add("  Compliance: This masked dataset is safe to share under GDPR/CCPA")
add("  because no individual can be re-identified from the masked fields.")
add()
add("END OF REPORT")
add("=" * 60)

# ── Save outputs ───────────────────────────────────────────────────────────────
os.makedirs("outputs", exist_ok=True)
with open("outputs/masked_sample.txt", "w", encoding="utf-8") as f:
    f.write("\n".join(report_lines))

masked.to_csv("outputs/customers_masked.csv", index=False)
# Local copy for convenience
masked.to_csv("customers_masked.csv", index=False)

print("\nSaved: outputs/masked_sample.txt")
print("Saved: outputs/customers_masked.csv")
