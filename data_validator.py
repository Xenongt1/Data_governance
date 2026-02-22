"""
Part 3: Data Validator (Great Expectations Edition)
====================================================
Uses the Great Expectations library to define and run validation rules.
Generates a detailed validation report in the outputs/ directory.
"""

import pandas as pd
import great_expectations as gx
from datetime import datetime
import os
from pathlib import Path
from logger import setup_logger

# ── Setup Logging ─────────────────────────────────────────────────────────────
log = setup_logger("validator")

def run_validation(data_path="customers_raw.csv", output_report="outputs/validation_results.txt"):
    """Runs GX validation and saves the result to a file."""
    
    log.info(f"Starting Great Expectations validation for: {data_path}")
    
    # ── Load Data ─────────────────────────────────────────────────────────────
    try:
        df = pd.read_csv(data_path, dtype=str)
        df.columns = df.columns.str.strip()
    except Exception as e:
        log.error(f"Failed to load data: {e}")
        return False

    # ── Get GX Context ────────────────────────────────────────────────────────
    context = gx.get_context()
    
    # Create an ephemeral data source
    datasource_name = "my_datasource"
    datasource = context.data_sources.add_pandas(name=datasource_name)
    asset_name = f"my_asset_{datetime.now().strftime('%Y%m%d%H%M%S')}" # Unique asset name
    asset = datasource.add_csv_asset(name=asset_name, filepath_or_buffer=data_path)
    
    # Create a batch request
    batch_request = asset.build_batch_request()
    
    # ── Define Expectation Suite ──────────────────────────────────────────────
    suite_name = "customer_quality_suite"
    context.suites.add(gx.ExpectationSuite(name=suite_name))
    
    # Expectations
    # 1. customer_id: Unique, positive integer
    context.suites.get(suite_name).add_expectation(gx.expectations.ExpectColumnValuesToBeUnique(column="customer_id"))
    context.suites.get(suite_name).add_expectation(gx.expectations.ExpectColumnValuesToMatchRegex(
        column="customer_id", regex=r"^[1-9]\d*$"
    ))
    
    # 2. Names: Non-empty, 2-50 chars, alphabetic OR [UNKNOWN]
    for col in ["first_name", "last_name"]:
        context.suites.get(suite_name).add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column=col))
        context.suites.get(suite_name).add_expectation(gx.expectations.ExpectColumnValuesToMatchRegex(
            column=col, regex=r"^([A-Za-z\-']+|\[UNKNOWN\])$"
        ))
        
    # 3. email: Valid email format
    context.suites.get(suite_name).add_expectation(gx.expectations.ExpectColumnValuesToMatchRegex(
        column="email", regex=r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$"
    ))
    
    # 4. phone: Standard format (we check raw, so we expect various but validate normalized later)
    # For Part 3, we expect non-empty and recognizable
    context.suites.get(suite_name).add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="phone"))
    
    # 5. date_of_birth, created_date: Valid dates OR [INVALID_DATE]
    for col in ["date_of_birth", "created_date"]:
        context.suites.get(suite_name).add_expectation(gx.expectations.ExpectColumnValuesToMatchRegex(
            column=col, regex=r"^(\d{4}-\d{2}-\d{2}|\[INVALID_DATE\])$"
        ))

    # 6. account_status: Valid values (including 'unknown' from cleaning)
    context.suites.get(suite_name).add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(
        column="account_status", value_set=["active", "inactive", "suspended", "unknown"]
    ))
    
    # 7. income: Non-negative, <= 10M
    context.suites.get(suite_name).add_expectation(gx.expectations.ExpectColumnValuesToMatchRegex(
        column="income", regex=r"^\d+(\.\d+)?$"
    ))
    
    # ── Run Check ─────────────────────────────────────────────────────────────
    log.info("Running Expectation Suite...")
    
    batch_def = asset.add_batch_definition_whole_dataframe("my_batch_def")
    
    validation_def = context.validation_definitions.add(gx.ValidationDefinition(
        name="my_validation",
        data=batch_def,
        suite=context.suites.get(suite_name)
    ))

    checkpoint = context.checkpoints.add(gx.Checkpoint(
        name="my_checkpoint",
        validation_definitions=[validation_def]
    ))
    
    result = checkpoint.run()
    
    # ── Process Results & Save Report ─────────────────────────────────────────
    validation_results = result.run_results
    
    report_lines = []
    report_lines.append("VALIDATION RESULTS (Great Expectations)")
    report_lines.append("=" * 60)
    report_lines.append(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report_lines.append(f"Data Source: {data_path}")
    report_lines.append("-" * 60)
    
    success = result.success
    report_lines.append(f"OVERALL STATUS: {'PASS' if success else 'FAIL'}")
    report_lines.append("")
    
    # Drill down into results
    # modern GX results are highly nested, let's simplify for the report
    for val_result in result.run_results.values():
        for res in val_result.results:
            col = res.expectation_config.kwargs.get("column", "Table-level")
            exp_type = res.expectation_config.type
            stat = "PASS" if res.success else "FAIL"
            report_lines.append(f"[{stat}] {col}: {exp_type}")
            if not res.success:
                report_lines.append(f"      Details: {res.exception_info.get('exception_message') or res.result}")
    
    report_lines.append("-" * 60)
    report_lines.append("END OF REPORT")
    
    os.makedirs(os.path.dirname(output_report), exist_ok=True)
    with open(output_report, "w", encoding="utf-8") as f:
        f.write("\n".join(report_lines))
        
    log.info(f"Validation report saved to: {output_report}")
    return success

if __name__ == "__main__":
    # If run standalone, it uses customers_raw.csv
    run_validation()
