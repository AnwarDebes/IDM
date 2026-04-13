"""
Data validation script for the epidemiological data warehouse.
Validates raw Tycho data against reference data and reports quality statistics.
"""

import csv
import os
import sys


def load_csv(path: str) -> list[dict]:
    with open(path, "r") as f:
        return list(csv.DictReader(f))


def validate(data_path: str, ref_dir: str = None):
    if ref_dir is None:
        ref_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "reference")

    print(f"Validating: {data_path}")
    print("=" * 60)

    # Load data
    rows = load_csv(data_path)
    total = len(rows)
    print(f"Total rows: {total:,}")

    if total == 0:
        print("ERROR: Empty dataset")
        return False

    # Load reference state codes
    regions = load_csv(os.path.join(ref_dir, "us_regions.csv"))
    valid_states = {r["state_code"] for r in regions}

    # Load reference disease names
    diseases_ref = load_csv(os.path.join(ref_dir, "disease_metadata.csv"))
    valid_diseases = {d["disease_name"] for d in diseases_ref}

    # Validate columns
    expected_cols = {"epi_week", "state", "loc", "loc_type", "disease", "cases", "incidence_per_100000"}
    actual_cols = set(rows[0].keys())
    missing_cols = expected_cols - actual_cols
    if missing_cols:
        print(f"ERROR: Missing columns: {missing_cols}")
        return False
    print(f"Columns: OK ({', '.join(sorted(actual_cols))})")

    # Validation counters
    issues = {
        "null_cases": 0,
        "negative_cases": 0,
        "invalid_state": 0,
        "invalid_disease": 0,
        "invalid_epi_week": 0,
        "null_disease": 0,
    }

    diseases_found = set()
    states_found = set()
    years_found = set()

    for row in rows:
        # Check disease
        disease = row.get("disease", "").strip()
        if not disease:
            issues["null_disease"] += 1
        elif disease not in valid_diseases:
            issues["invalid_disease"] += 1
        else:
            diseases_found.add(disease)

        # Check state
        state = row.get("state", "").strip()
        if state not in valid_states:
            issues["invalid_state"] += 1
        else:
            states_found.add(state)

        # Check cases
        cases_str = row.get("cases", "").strip()
        if not cases_str:
            issues["null_cases"] += 1
        else:
            try:
                cases = int(cases_str)
                if cases < 0:
                    issues["negative_cases"] += 1
            except ValueError:
                issues["null_cases"] += 1

        # Check epi_week
        ew_str = row.get("epi_week", "").strip()
        if not ew_str:
            issues["invalid_epi_week"] += 1
        else:
            try:
                ew = int(ew_str)
                year = ew // 100
                week = ew % 100
                if week < 1 or week > 53 or year < 1800 or year > 2100:
                    issues["invalid_epi_week"] += 1
                else:
                    years_found.add(year)
            except ValueError:
                issues["invalid_epi_week"] += 1

    # Report
    total_issues = sum(issues.values())
    quality_pct = round((1 - total_issues / total) * 100, 2) if total > 0 else 0

    print(f"\nDiseases found ({len(diseases_found)}): {sorted(diseases_found)}")
    print(f"States found: {len(states_found)}")
    print(f"Year range: {min(years_found)} - {max(years_found)}")
    print(f"\nIssues:")
    for k, v in issues.items():
        status = "OK" if v == 0 else f"WARN ({v:,})"
        print(f"  {k}: {status}")
    print(f"\nData quality score: {quality_pct}%")

    passed = quality_pct >= 95
    print(f"\nResult: {'PASSED' if passed else 'FAILED'}")
    return passed


if __name__ == "__main__":
    if len(sys.argv) > 1:
        data_path = sys.argv[1]
    else:
        data_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "data", "raw", "tycho_level1.csv"
        )

    ref_dir = None
    if len(sys.argv) > 2:
        ref_dir = sys.argv[2]

    success = validate(data_path, ref_dir)
    sys.exit(0 if success else 1)
