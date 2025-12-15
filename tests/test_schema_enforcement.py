"""
Tests for schema enforcement and column mapping.
"""

import pandas as pd
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from schemas.column_mappings import apply_prefix_mapping, enforce_schema, get_schema_for_source


def test_prefix_mapping():
    """Test that prefix mapping prevents collisions."""
    print("Testing prefix mapping...")

    df = pd.DataFrame({
        'id': [1, 2],
        'study_desc.version_statement.version': ['1.0', '2.0'],
        'doc_desc.version_statement.version': ['1.1', '2.1'],
        'study_info.notes': ['note1', 'note2'],
        'method.notes': ['mnote1', 'mnote2'],
        'data_collection.sampling_procedure': ['random', 'stratified'],
    })

    result = apply_prefix_mapping(df)

    expected_columns = [
        'id',
        'study.version_statement.version',
        'doc.version_statement.version',
        'info.notes',
        'method.notes',
        'method.sampling_procedure',
    ]

    assert set(result.columns) == set(expected_columns), f"Expected {expected_columns}, got {list(result.columns)}"

    assert len(result.columns) == len(expected_columns), "Duplicate columns detected!"

    print("✓ Prefix mapping test passed")


def test_schema_enforcement():
    """Test that schema enforcement adds missing columns and drops extras."""
    print("Testing schema enforcement...")

    schema = {
        'id': 'Int64',
        'title': 'str',
        'abstract': 'str',
    }

    df = pd.DataFrame({
        'id': [1, 2],
        'title': ['A', 'B'],
        'extra_column': ['X', 'Y'],
    })

    result = enforce_schema(df, schema)

    assert set(result.columns) == set(schema.keys()), f"Expected {list(schema.keys())}, got {list(result.columns)}"

    assert 'abstract' in result.columns, "Missing column not added"
    assert 'extra_column' not in result.columns, "Extra column not dropped"

    assert result['abstract'].isna().all(), "Missing column should be NaN"

    print("✓ Schema enforcement test passed")


def test_no_schema_drift_on_concat():
    """Test that concatenating old and new data doesn't create .1 columns."""
    print("Testing concat without schema drift...")

    schema = get_schema_for_source('worldbank')

    old_df = pd.DataFrame({
        'id': [1],
        'title': ['Old'],
    })
    old_df = enforce_schema(old_df, schema)

    new_df = pd.DataFrame({
        'id': [2],
        'title': ['New'],
        'extra_field': ['Extra'],
    })
    new_df = enforce_schema(new_df, schema)

    combined = pd.concat([old_df, new_df], ignore_index=True)

    duplicate_cols = [col for col in combined.columns if '.1' in col or '.2' in col]

    assert len(duplicate_cols) == 0, f"Found duplicate columns: {duplicate_cols}"
    assert len(combined) == 2, "Row count should be 2"

    print("✓ Concat test passed (no schema drift)")


def test_get_schema_for_source():
    """Test getting schema for different sources."""
    print("Testing get_schema_for_source...")

    wb_schema = get_schema_for_source('worldbank')
    assert 'id' in wb_schema
    assert 'study.version_statement.version' in wb_schema
    assert 'doc.version_statement.version' in wb_schema

    unhcr_schema = get_schema_for_source('unhcr')
    assert 'id' in unhcr_schema
    assert 'title' in unhcr_schema

    try:
        get_schema_for_source('invalid')
        assert False, "Should have raised ValueError"
    except ValueError:
        pass

    print("✓ get_schema_for_source test passed")


def run_all_tests():
    """Run all tests."""
    print("=" * 80)
    print("Running schema enforcement tests")
    print("=" * 80)

    try:
        test_prefix_mapping()
        test_schema_enforcement()
        test_no_schema_drift_on_concat()
        test_get_schema_for_source()

        print("\n" + "=" * 80)
        print("✓ All tests passed!")
        print("=" * 80)
        return True

    except AssertionError as e:
        print(f"\n✗ Test failed: {e}")
        return False
    except Exception as e:
        print(f"\n✗ Error running tests: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)
