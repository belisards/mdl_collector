#!/usr/bin/env python3
"""
Schema Discovery Script

Analyzes existing datasets.csv files to generate schema definitions.
Identifies duplicate columns (.1, .2 suffixes) and traces them back to likely sources.
"""

import pandas as pd
import re
from pathlib import Path
from collections import defaultdict

WB_CSV = "data/world_bank/datasets.csv"
UNHCR_CSV = "data/unhcr/datasets.csv"


def analyze_csv(csv_path):
    """Analyze a CSV file and extract schema information."""
    print(f"\n{'='*80}")
    print(f"Analyzing: {csv_path}")
    print(f"{'='*80}")

    df = pd.read_csv(csv_path, nrows=100)  # Sample for type inference

    columns = df.columns.tolist()
    print(f"\nTotal columns: {len(columns)}")

    duplicate_groups = find_duplicate_groups(columns)

    if duplicate_groups:
        print(f"\nDuplicate column groups found: {len(duplicate_groups)}")
        for base, variants in sorted(duplicate_groups.items()):
            print(f"\n  {base}:")
            for col in variants:
                non_null = df[col].notna().sum()
                print(f"    - {col:60s} ({non_null} non-null values)")

    print(f"\n{'-'*80}")
    print("Column Data Types:")
    print(f"{'-'*80}")

    type_counts = defaultdict(list)
    for col in columns:
        dtype = infer_type(df[col])
        type_counts[dtype].append(col)

    for dtype, cols in sorted(type_counts.items()):
        print(f"\n{dtype} ({len(cols)} columns):")
        for col in sorted(cols)[:10]:  # Show first 10 of each type
            print(f"  - {col}")
        if len(cols) > 10:
            print(f"  ... and {len(cols) - 10} more")

    return columns, duplicate_groups, df


def find_duplicate_groups(columns):
    """Find groups of columns that are duplicates (base + .1, .2, etc.)."""
    duplicate_groups = defaultdict(list)

    for col in columns:
        match = re.match(r'^(.+?)(?:\.(\d+))?$', col)
        if match:
            base, suffix = match.groups()
            if suffix:  # Has .N suffix
                duplicate_groups[base].append(col)

    result = {}
    for base, suffixed in duplicate_groups.items():
        if base in columns:
            result[base] = [base] + sorted(suffixed, key=lambda x: int(x.split('.')[-1]))
        else:
            result[base] = sorted(suffixed, key=lambda x: int(x.split('.')[-1]))

    return result


def infer_type(series):
    """Infer the most appropriate type for a column."""
    if series.isna().all():
        return 'object'

    non_null = series.dropna()

    if len(non_null) == 0:
        return 'object'

    if pd.api.types.is_numeric_dtype(series):
        return 'numeric'

    if pd.api.types.is_bool_dtype(series):
        return 'bool'

    return 'object'


def generate_schema_dict(columns, df, duplicate_groups):
    """Generate Python dictionary code for schema definition."""
    lines = []
    lines.append("{")

    processed = set()

    for col in sorted(columns):
        if col in processed:
            continue

        dtype = infer_type(df[col])
        py_type = 'str' if dtype == 'object' else dtype

        base_match = re.match(r'^(.+?)\.(\d+)$', col)
        if base_match and base_match.group(1) in duplicate_groups:
            continue

        lines.append(f"    '{col}': '{py_type}',")
        processed.add(col)

    lines.append("}")

    return "\n".join(lines)


def suggest_consolidation_rules(duplicate_groups, df):
    """Suggest rules for consolidating duplicate columns."""
    print(f"\n{'='*80}")
    print("CONSOLIDATION SUGGESTIONS")
    print(f"{'='*80}")

    for base, variants in sorted(duplicate_groups.items()):
        print(f"\n{base}:")

        if 'version_statement' in base:
            print(f"  Suggestion: Likely from study_desc and doc_desc")
            print(f"    {variants[0]} → study.{base}")
            if len(variants) > 1:
                print(f"    {variants[1]} → doc.{base}")

        elif base == 'notes':
            print(f"  Suggestion: Multiple sources (info, method)")
            for i, var in enumerate(variants):
                if i == 0:
                    print(f"    {var} → info.notes")
                elif i == 1:
                    print(f"    {var} → method.notes")
                else:
                    print(f"    {var} → keep as notes.{i}")

        elif base in ['idno', 'title', 'prod_date', 'producers']:
            print(f"  Suggestion: Likely alternate/duplicate metadata")
            for i, var in enumerate(variants):
                if i == 0 and var == base:
                    print(f"    {var} → keep as-is (primary)")
                else:
                    print(f"    {var} → alternate_{base}")

        else:
            print(f"  Suggestion: Manual review needed")
            for var in variants:
                non_null = df[var].notna().sum()
                print(f"    {var} ({non_null} values)")


def main():
    """Main analysis function."""
    print("Schema Discovery Tool")
    print("=" * 80)

    wb_columns, wb_dupes, wb_df = analyze_csv(WB_CSV)

    unhcr_columns, unhcr_dupes, unhcr_df = analyze_csv(UNHCR_CSV)

    suggest_consolidation_rules(wb_dupes, wb_df)
    suggest_consolidation_rules(unhcr_dupes, unhcr_df)

    print(f"\n{'='*80}")
    print("SCHEMA TEMPLATES")
    print(f"{'='*80}")

    print("\n# World Bank Schema")
    print("WORLD_BANK_SCHEMA = " + generate_schema_dict(wb_columns, wb_df, wb_dupes))

    print("\n# UNHCR Schema")
    print("UNHCR_SCHEMA = " + generate_schema_dict(unhcr_columns, unhcr_df, unhcr_dupes))

    print(f"\n{'='*80}")
    print("Next Steps:")
    print("  1. Review consolidation suggestions above")
    print("  2. Copy schema templates to src/schemas/column_mappings.py")
    print("  3. Implement prefix mapping rules")
    print("  4. Create migration script with consolidation rules")
    print(f"{'='*80}\n")


if __name__ == "__main__":
    main()
