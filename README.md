# Data Governance Pipeline: PII Detection & Data Quality

A robust, end-to-end data engineering pipeline designed to ingest, clean, validate, and protect sensitive customer data. This project demonstrates best practices in data governance, security compliance (GDPR/CCPA), and data quality management.

## Project Overview

This pipeline transforms messy, high-risk raw data into an analytics-ready, protected dataset. It addresses common data quality issues such as inconsistent formatting and missing values while implementing sophisticated PII (Personally Identifiable Information) detection and masking strategies.

### Key Features
- **Exploratory Data Quality Analysis:** Comprehensive profiling of raw datasets.
- **Automated PII Detection:** Regex-based scanning for sensitive data (Emails, Phones, SSNs, Addresses).
- **Strict Data Validation:** Rule-based validation to ensure schema and business logic integrity.
- **Intelligent Data Cleaning:** Multi-format normalization and missing value imputation.
- **Privacy-Preserving Masking:** Partial masking of PII to maintain analytical utility while ensuring compliance.
- **Orchestrated Execution:** Single-command execution of the entire data lifecycle.

---

## Project Structure

```text
Data_governance/
├── customers_raw.csv           # Input: Messy raw data
├── eda_quality.py              # Part 1: Exploratory Analysis logic
├── pii_detection.py            # Part 2: PII Scanner
├── data_validator.py           # Part 3: Validation engine
├── data_cleaning.py            # Part 4: Normalization & Cleaning
├── pii_masking.py              # Part 5: Data Protection/Masking
├── pipeline.py                 # Part 6: Master Orchestrator
├── REFLECTION.md               # Part 7: Governance insights & lessons
├── outputs/                    # Generated reports and data files
│   ├── data_quality_report.txt
│   ├── pii_detection_report.txt
│   ├── validation_results.txt
│   ├── cleaning_log.txt
│   ├── masked_sample.txt
│   ├── pipeline_execution_report.txt
│   ├── customers_cleaned.csv
│   └── customers_masked.csv
└── README.md                   # Project documentation (you are here)
```

---

## Pipeline Stages

The `pipeline.py` script orchestrates the following stages:

1.  **LOAD:** Reads the raw CSV and performs initial structural checks.
2.  **CLEAN:** Normalizes phone numbers (XXX-XXX-XXXX), dates (YYYY-MM-DD), and standardizes casing.
3.  **VALIDATE:** Applies strict rules to every field. Categorizes failures as warnings or critical errors.
4.  **DETECT PII:** Scans for exposure risk and quantifies the amount of sensitive data present.
5.  **MASK:** Applies multi-layer masking (e.g., `j***@gmail.com`) to protect individual identities.
6.  **SAVE:** Persists cleaned/masked data and generates a unified execution report.

---

## Getting Started

### Prerequisites
- Python 3.8+
- Pandas

### Installation
```bash
git clone https://github.com/Xenongt1/Data_governance.git
cd Data_governance
pip install pandas
```

### Running the Pipeline
To run the entire data governance process and generate all reports:
```bash
python pipeline.py
```

---

## Data Quality & Governance Insights

As detailed in `REFLECTION.md`, this project highlights critical real-world challenges:
- **Sentinel Strings:** Handling text like `invalid_date` in numeric/date columns.
- **Exposure Risk:** How the combination of name + DOB + address creates a "ready-made fraud toolkit."
- **Data Minimization:** Balancing the need for analytical utility with the necessity of privacy.

## License
This project is part of a data engineering mini-project.
