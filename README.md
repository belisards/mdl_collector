# UNHCR & World Bank microdata library collector

This repository automates data collection from the [UNHCR](https://microdata.unhcr.org/) and the [World Bank](https://microdata.worldbank.org) microdata libraries.

It uses a Python script and Github Action to run weekly. 

Created by the [Joint Data Center on Forced Displacement](https://www.jointdatacenter.org/).

# Development

Dependencies are managed with [uv](https://docs.astral.sh/uv/).

1. Install uv (for example `curl -LsSf https://astral.sh/uv/install.sh | sh`).
2. Sync dependencies: `uv sync --locked`.
3. Run the scraper: `uv run python src/main.py`.

# Quick access

Right-click and "Save link as" to download the files.

- [UNHCR microdata library](data/unhcr/datasets.csv)
  
- [WorldBank microdata library](data/world_bank/datasets.csv)

# Data 

Each microdata library has its own subfolder. The records are saved in the following format:

- `metadata.csv`: list of all datasets available

- `datasets.csv`: all information about the datasets in both microdata libraries

# Schema Management

## Column Naming Convention

To prevent schema drift and maintain data integrity, this project uses **fixed schemas** with smart prefix mapping:

### Prefix Mappings

API responses contain nested JSON that gets flattened. To avoid column name collisions, we use these prefix conventions:

- `study_desc.*` → `study.*` (study-level metadata)
- `doc_desc.*` → `doc.*` (documentation-level metadata)
- `study_info.*` → `info.*` (study information fields)
- `method.*` → `method.*` (methodology fields)
- `data_collection.*` → `method.*` (data collection details)

### Examples

- `study.version_statement.version` - Study description version
- `doc.version_statement.version` - Documentation version
- `info.notes` - General study notes
- `method.notes` - Methodology notes
- `method.sampling_procedure` - Sampling methodology

This ensures that fields from different sources remain distinct and queryable.

## Schema Enforcement

The project enforces fixed schemas for both data sources to ensure:

1. **No schema drift** - Column set remains stable across updates
2. **True incremental updates** - Only new rows added, no full rewrites
3. **Predictable queries** - Column names never change unexpectedly
4. **Clean git diffs** - Only new data appears in commits, not entire files

Schema definitions are in `src/schemas/column_mappings.py`.

## Migration History

**December 2025**: Performed one-time migration to consolidate duplicate columns (`.1`, `.2` suffixes) into properly prefixed columns. Original files backed up as `*.backup`.

- World Bank: 111 → 110 columns (6,709 rows preserved)
- UNHCR: 96 → 70 columns (1,027 rows preserved)

## Adding New Fields to Schema

If the API introduces new fields that should be tracked:

1. Edit `src/schemas/column_mappings.py`
2. Add the field to the appropriate schema dict (`WORLD_BANK_SCHEMA` or `UNHCR_SCHEMA`)
3. Re-run the scraper - new field will be populated in existing rows with NaN

Fields not in the schema are automatically dropped during collection.

# Code

Project structure:

```
src/
├── orchestrators/       # Data collection workflows
│   ├── fetch_datasets.py  # Main dataset fetching logic
│   └── list_metadata.py   # Metadata list collection
├── sources/            # API clients
│   ├── unhcr.py          # UNHCR API interface
│   └── worldbank.py      # World Bank API interface
├── schemas/            # Schema management
│   └── column_mappings.py # Fixed schemas and enforcement
├── migrations/         # One-time data migrations
│   └── consolidate_duplicates.py
└── main.py            # Entry point

scripts/
└── discover_schema.py  # Schema discovery helper

tests/
└── test_schema_enforcement.py  # Schema validation tests
```

## Testing

Run schema enforcement tests:

```bash
uv run python tests/test_schema_enforcement.py
```

## Development Notes

- Uses [uv](https://docs.astral.sh/uv/) for dependency management
- GitHub Actions runs daily at midnight UTC
- Incremental updates: only fetches new datasets not already in CSV
- Schema enforcement prevents column proliferation
