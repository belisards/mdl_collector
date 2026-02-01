# Suggested Improvements for MDL Collector

> Analysis and recommendations for enhancing the UNHCR & World Bank microdata library collector.

---

## Table of Contents

1. [Code Quality & Architecture](#1-code-quality--architecture)
2. [Error Handling & Resilience](#2-error-handling--resilience)
3. [Performance Optimizations](#3-performance-optimizations)
4. [Testing & CI/CD](#4-testing--cicd)
5. [Data Quality & Validation](#5-data-quality--validation)
6. [Documentation](#6-documentation)
7. [Feature Enhancements](#7-feature-enhancements)
8. [Security](#8-security)

---

## 1. Code Quality & Architecture

### 1.1 Project Metadata Mismatch

**Issue:** `pyproject.toml` still references `unhcr-scraper` and placeholder author info.

```toml
# Current
name = "unhcr-scraper"
authors = [{ name = "Your Name", email = "you@example.com" }]

# Suggested
name = "mdl-collector"
authors = [{ name = "Joint Data Center", email = "contact@jointdatacenter.org" }]
```

### 1.2 Configuration Management

**Issue:** URLs, paths, and constants are scattered across multiple files.

**Suggestion:** Create a centralized `config.py` or use environment variables:

```python
# src/config.py
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    # API Endpoints
    WB_METADATA_URL: str = "https://microdata.worldbank.org/index.php/api/catalog/list_idno/survey"
    WB_DATASET_URL: str = "https://microdata.worldbank.org/index.php/metadata/export/{}"
    UNHCR_METADATA_URL: str = "https://microdata.unhcr.org/index.php/api/catalog/search"
    
    # Processing
    MAX_WORKERS: int = 20
    REQUEST_TIMEOUT: int = 30
    RETRY_ATTEMPTS: int = 3
    
    class Config:
        env_file = ".env"

settings = Settings()
```

### 1.3 Dependency Injection

**Issue:** Hard-coded dependencies make testing difficult.

**Suggestion:** Use dependency injection for API clients:

```python
# src/sources/base.py
from abc import ABC, abstractmethod

class DataSource(ABC):
    @abstractmethod
    def fetch_metadata_list(self) -> pd.DataFrame:
        pass
    
    @abstractmethod
    def fetch_dataset(self, id: int) -> dict:
        pass

# Allows easy mocking in tests
```

### 1.4 Type Hints

**Issue:** Inconsistent type hints throughout the codebase.

**Suggestion:** Add comprehensive type hints:

```python
# Before
def fetch_dataset(id):
    ...

# After
def fetch_dataset(id: int) -> dict[str, Any]:
    ...
```

---

## 2. Error Handling & Resilience

### 2.1 Retry Logic with Exponential Backoff

**Issue:** No retry mechanism for failed API requests.

**Suggestion:** Implement retry with `tenacity`:

```python
from tenacity import retry, stop_after_attempt, wait_exponential

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    reraise=True
)
def fetch_dataset(id: int) -> dict:
    response = requests.get(DATASET_EXPORT_URL.format(id), timeout=30)
    response.raise_for_status()
    return response.json()
```

### 2.2 Rate Limiting

**Issue:** No rate limiting could trigger API blocks.

**Suggestion:** Add rate limiting:

```python
from ratelimit import limits, sleep_and_retry

@sleep_and_retry
@limits(calls=10, period=1)  # 10 requests per second
def fetch_dataset(id: int) -> dict:
    ...
```

### 2.3 Graceful Degradation

**Issue:** Single source failure stops entire pipeline.

**Suggestion:** Make sources independent with proper isolation:

```python
def run():
    results = {}
    for source_name, source_func in [("worldbank", run_worldbank), ("unhcr", run_unhcr)]:
        try:
            results[source_name] = source_func()
        except Exception as e:
            logger.error(f"{source_name} failed: {e}")
            results[source_name] = None
    return results
```

### 2.4 Checkpoint/Resume Support

**Issue:** If the job fails mid-run, progress is lost.

**Suggestion:** Implement checkpointing:

```python
def process_meta_with_checkpoints(input_file, output_file, fetch_function, checkpoint_file):
    # Load checkpoint
    processed_ids = set()
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file) as f:
            processed_ids = set(json.load(f))
    
    # Process remaining
    for id in tqdm(new_ids):
        if id in processed_ids:
            continue
        # ... fetch and save
        processed_ids.add(id)
        
        # Save checkpoint every N records
        if len(processed_ids) % 100 == 0:
            with open(checkpoint_file, 'w') as f:
                json.dump(list(processed_ids), f)
```

---

## 3. Performance Optimizations

### 3.1 Async HTTP Requests

**Issue:** Using `ThreadPoolExecutor` with synchronous `requests` is suboptimal.

**Suggestion:** Migrate to `aiohttp` or `httpx` async:

```python
import httpx
import asyncio

async def fetch_datasets_async(ids: list[int]) -> list[dict]:
    async with httpx.AsyncClient() as client:
        tasks = [fetch_single(client, id) for id in ids]
        return await asyncio.gather(*tasks, return_exceptions=True)

async def fetch_single(client: httpx.AsyncClient, id: int) -> dict:
    response = await client.get(DATASET_EXPORT_URL.format(id))
    response.raise_for_status()
    return response.json()
```

### 3.2 Connection Pooling

**Issue:** New connection for each request.

**Suggestion:** Use a session with connection pooling:

```python
# Create session once
session = requests.Session()
adapter = requests.adapters.HTTPAdapter(
    pool_connections=20,
    pool_maxsize=20,
    max_retries=3
)
session.mount('https://', adapter)
```

### 3.3 Incremental Updates Optimization

**Issue:** Currently loads entire CSV to check for existing IDs.

**Suggestion:** Use SQLite for faster lookups on large datasets:

```python
import sqlite3

def get_existing_ids(db_path: str) -> set[int]:
    conn = sqlite3.connect(db_path)
    cursor = conn.execute("SELECT id FROM datasets")
    return set(row[0] for row in cursor.fetchall())
```

### 3.4 Memory Efficiency

**Issue:** Loading 200k+ rows into memory.

**Suggestion:** Use chunked processing for large files:

```python
def process_large_csv(input_file: str, chunksize: int = 10000):
    for chunk in pd.read_csv(input_file, chunksize=chunksize):
        yield from chunk['id'].tolist()
```

---

## 4. Testing & CI/CD

### 4.1 Add Unit Tests

**Issue:** No test suite exists.

**Suggestion:** Create a comprehensive test suite:

```python
# tests/test_sources.py
import pytest
from unittest.mock import patch, Mock

def test_worldbank_fetch_metadata():
    mock_response = Mock()
    mock_response.json.return_value = {"records": [{"id": 1, "title": "Test"}]}
    mock_response.raise_for_status = Mock()
    
    with patch('requests.get', return_value=mock_response):
        df = worldbank.fetch_metadata_list()
        assert len(df) == 1
        assert df.iloc[0]['id'] == 1

def test_schema_enforcement():
    df = pd.DataFrame({'id': [1], 'unknown_col': ['x']})
    result = enforce_schema(df, {'id': 'Int64', 'title': 'str'})
    assert 'unknown_col' not in result.columns
    assert 'title' in result.columns
```

### 4.2 Update GitHub Actions Workflow

**Issue:** Workflow uses outdated actions and Python version.

```yaml
# .github/workflows/scrape.yml
name: Collect Microdata

on:
  workflow_dispatch:
  schedule:
    - cron: '0 0 * * 0'

jobs:
  scrape:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      
      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'
      
      - name: Install uv
        uses: astral-sh/setup-uv@v4
      
      - name: Install dependencies
        run: uv sync --locked
      
      - name: Run tests
        run: uv run pytest tests/ -v
      
      - name: Run collector
        run: uv run python src/main.py
      
      - name: Commit changes
        uses: stefanzweifel/git-auto-commit-action@v5
        with:
          commit_message: "data: update microdata archives"
          file_pattern: 'data/**/*.csv'
```

### 4.3 Add Pre-commit Hooks

```yaml
# .pre-commit-config.yaml
repos:
  - repo: https://github.com/astral-sh/ruff-pre-commit
    rev: v0.3.0
    hooks:
      - id: ruff
        args: [--fix]
      - id: ruff-format

  - repo: https://github.com/pre-commit/mirrors-mypy
    rev: v1.8.0
    hooks:
      - id: mypy
```

---

## 5. Data Quality & Validation

### 5.1 Schema Validation

**Issue:** No runtime validation of API responses.

**Suggestion:** Use Pydantic models for validation:

```python
from pydantic import BaseModel, Field
from typing import Optional

class DatasetMetadata(BaseModel):
    id: int
    title: str
    abstract: Optional[str] = None
    nation: Optional[list[dict]] = None
    
    class Config:
        extra = "allow"  # Allow unknown fields

def fetch_dataset(id: int) -> DatasetMetadata:
    response = requests.get(...)
    return DatasetMetadata.model_validate(response.json())
```

### 5.2 Data Integrity Checks

**Suggestion:** Add validation after each run:

```python
def validate_output(df: pd.DataFrame, source: str) -> list[str]:
    issues = []
    
    # Check for duplicates
    if df['id'].duplicated().any():
        issues.append(f"Duplicate IDs found in {source}")
    
    # Check for required fields
    if df['title'].isna().sum() > df.shape[0] * 0.1:
        issues.append(f"More than 10% missing titles in {source}")
    
    # Check row count didn't decrease significantly
    # (could indicate API issue)
    
    return issues
```

### 5.3 Change Detection & Alerting

**Suggestion:** Track meaningful changes beyond view counts:

```python
def detect_changes(old_df: pd.DataFrame, new_df: pd.DataFrame) -> dict:
    # Exclude volatile columns
    stable_cols = [c for c in old_df.columns if c not in ['total_views', 'total_downloads']]
    
    return {
        'new_records': len(new_df) - len(old_df),
        'modified_records': count_modified(old_df, new_df, stable_cols),
        'deleted_records': len(set(old_df['id']) - set(new_df['id']))
    }
```

---

## 6. Documentation

### 6.1 API Documentation

**Suggestion:** Document the NADA API endpoints being used:

```markdown
## API Reference

### World Bank Microdata Library

| Endpoint | Description |
|----------|-------------|
| `GET /api/catalog/list_idno/survey` | List all survey IDs |
| `GET /metadata/export/{id}` | Export full metadata for dataset |

### UNHCR Microdata Library

| Endpoint | Description |
|----------|-------------|
| `GET /api/catalog/search?ps=9999999` | Search all datasets |
| `GET /metadata/export/{id}/json` | Export metadata as JSON |
```

### 6.2 Architecture Diagram

**Suggestion:** Add a visual overview:

```
┌─────────────────────────────────────────────────────────────┐
│                     GitHub Actions                          │
│                    (Weekly Schedule)                        │
└─────────────────────┬───────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────┐
│                      main.py                                │
│  ┌─────────────────┐     ┌─────────────────┐               │
│  │  list_metadata  │────▶│  fetch_datasets │               │
│  └─────────────────┘     └─────────────────┘               │
└─────────────────────┬───────────────┬───────────────────────┘
                      │               │
          ┌───────────┴───┐     ┌─────┴───────────┐
          ▼               ▼     ▼                 ▼
    ┌──────────┐    ┌──────────┐ ┌──────────┐ ┌──────────┐
    │  UNHCR   │    │World Bank│ │  UNHCR   │ │World Bank│
    │   API    │    │   API    │ │   API    │ │   API    │
    └──────────┘    └──────────┘ └──────────┘ └──────────┘
          │               │           │             │
          ▼               ▼           ▼             ▼
    ┌─────────────────────────────────────────────────────┐
    │                  data/*.csv                         │
    │         (Git-versioned output)                      │
    └─────────────────────────────────────────────────────┘
```

### 6.3 Contributing Guide

**Suggestion:** Add `CONTRIBUTING.md` with:
- Development setup instructions
- Code style guidelines
- Pull request process
- How to add new data sources

---

## 7. Feature Enhancements

### 7.1 Add More Data Sources

The NADA system is used by many organizations. Consider adding:

- **ILO Microdata**: https://www.ilo.org/surveyLib/
- **IPUMS**: https://www.ipums.org/
- **FAO Microdata**: https://microdata.fao.org/

```python
# src/sources/ilo.py
class ILOSource(DataSource):
    BASE_URL = "https://www.ilo.org/surveyLib/index.php/api/catalog"
    ...
```

### 7.2 Data Export Formats

**Suggestion:** Support multiple output formats:

```python
def export_data(df: pd.DataFrame, output_path: str, format: str = "csv"):
    exporters = {
        "csv": lambda: df.to_csv(output_path, index=False),
        "parquet": lambda: df.to_parquet(output_path, index=False),
        "json": lambda: df.to_json(output_path, orient="records", indent=2),
    }
    exporters[format]()
```

### 7.3 CLI Interface

**Suggestion:** Add a proper CLI using `click` or `typer`:

```python
# src/cli.py
import typer

app = typer.Typer()

@app.command()
def collect(
    source: str = typer.Option("all", help="Source to collect: worldbank, unhcr, all"),
    output_format: str = typer.Option("csv", help="Output format: csv, parquet, json"),
    incremental: bool = typer.Option(True, help="Only fetch new records"),
):
    """Collect microdata library metadata."""
    ...

@app.command()
def validate(source: str = "all"):
    """Validate collected data integrity."""
    ...

if __name__ == "__main__":
    app()
```

### 7.4 Notification System

**Suggestion:** Send alerts on significant changes:

```python
def notify_changes(changes: dict):
    if changes['new_records'] > 100:
        send_slack_notification(f"🆕 {changes['new_records']} new datasets added!")
    if changes['deleted_records'] > 0:
        send_slack_notification(f"⚠️ {changes['deleted_records']} datasets removed!")
```

### 7.5 Historical Analysis

**Suggestion:** Track changes over time with a separate history table:

```python
def record_history(df: pd.DataFrame, source: str):
    history_file = f"data/{source}/history.jsonl"
    snapshot = {
        "timestamp": datetime.utcnow().isoformat(),
        "total_records": len(df),
        "sources_by_country": df['nation'].value_counts().to_dict(),
    }
    with open(history_file, 'a') as f:
        f.write(json.dumps(snapshot) + '\n')
```

---

## 8. Security

### 8.1 Secrets Management

**Issue:** No sensitive data currently, but good practice for future.

**Suggestion:** Use GitHub Secrets for any API keys:

```yaml
# .github/workflows/scrape.yml
env:
  API_KEY: ${{ secrets.NADA_API_KEY }}
```

### 8.2 Input Validation

**Issue:** Dataset IDs come from external APIs.

**Suggestion:** Validate inputs:

```python
def fetch_dataset(id: int) -> dict:
    if not isinstance(id, int) or id < 0:
        raise ValueError(f"Invalid dataset ID: {id}")
    ...
```

### 8.3 Dependency Security

**Suggestion:** Add automated dependency scanning:

```yaml
# .github/dependabot.yml
version: 2
updates:
  - package-ecosystem: "pip"
    directory: "/"
    schedule:
      interval: "weekly"
```

---

## Priority Implementation Order

1. **High Impact, Low Effort:**
   - Fix `pyproject.toml` metadata
   - Add retry logic with exponential backoff
   - Update GitHub Actions to v4/v5
   - Add basic unit tests

2. **High Impact, Medium Effort:**
   - Implement async HTTP requests
   - Add configuration management
   - Create CLI interface

3. **Medium Impact, Medium Effort:**
   - Add Pydantic validation
   - Implement checkpointing
   - Add more data sources

4. **Long-term:**
   - SQLite for large dataset management
   - Historical analysis features
   - Notification system

---

## Conclusion

This project has a solid foundation with clean separation of concerns and good use of schema enforcement. The main areas for improvement are around resilience (retry logic, rate limiting), performance (async requests), and developer experience (testing, CLI, documentation).

The incremental update feature is particularly well-implemented and shows good foresight for handling growing datasets.

---

*Generated on 2026-02-01 by code analysis*
