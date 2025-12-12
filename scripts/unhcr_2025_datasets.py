#!/usr/bin/env python
"""
Generate a country → URL list of UNHCR datasets registered in a given year.

Reads the harvested `data/unhcr/metadata.csv` file, filters by the `created`
year, and writes a plain-text report grouped by continent, then country, with
per-country counts and source URLs.
"""
from __future__ import annotations

import argparse
from pathlib import Path
import sys

import pandas as pd
import json

ALIASES = {
    "bosnia-herzegovina": "bosnia and herzegovina",
    "czech republic": "czechia",
    "dem. rep.": "congo (democratic republic of the)",
}


def split_countries(raw: str | float) -> list[str]:
    """Split a country field into individual country names."""
    if pd.isna(raw):
        return []
    text = str(raw).replace("...and", ",")
    for token in [" and ", ";"]:
        text = text.replace(token, ",")
    pieces = [part.strip() for part in text.split(",")]
    cleaned = []
    for p in pieces:
        low = p.lower()
        if not p:
            continue
        if low.endswith("more") or " more" in low:
            continue
        cleaned.append(p)
    return cleaned


def build_dataset_links(
    input_path: Path, year: int, continent_map: dict[str, str] | None
) -> pd.DataFrame:
    """Filter metadata for the requested year and emit country/url pairs."""
    if not input_path.exists():
        raise FileNotFoundError(f"Input metadata not found at {input_path}")

    df = pd.read_csv(input_path)

    for required in ("created", "nation", "url"):
        if required not in df.columns:
            raise ValueError(f"Column '{required}' missing from {input_path}")

    created = pd.to_datetime(df["created"], errors="coerce")
    filtered = df[created.dt.year == year].copy()

    filtered = filtered[["nation", "url"]].dropna(subset=["url"]).copy()

    records: list[dict[str, str]] = []
    for _, row in filtered.iterrows():
        url = str(row["url"]).strip()
        for country in split_countries(row["nation"]):
            continent = None
            if continent_map:
                continent = continent_map.get(country)
            records.append(
                {
                    "country": country,
                    "continent": continent or "Unknown",
                    "url": url,
                }
            )

    return (
        pd.DataFrame(records)
        .drop_duplicates()
        .sort_values(["country", "url"])
        .reset_index(drop=True)
    )


def load_continent_map(path: Path) -> dict[str, str]:
    if not path or not path.exists():
        return {}
    data = path.read_text()
    try:
        parsed = json.loads(data)
    except json.JSONDecodeError:
        try:
            import yaml  # type: ignore
        except ImportError:
            return {}
        parsed = yaml.safe_load(data) or {}
    return {str(k): str(v) for k, v in parsed.items()}


def load_country_ids(path: Path) -> dict[str, str]:
    if not path or not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except json.JSONDecodeError:
        return {}


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Generate a list of UNHCR datasets registered in a given year, "
            "with their country and source URL."
        )
    )
    parser.add_argument(
        "--year",
        type=int,
        default=2025,
        help="Registration year to filter on (default: 2025).",
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=Path("data/unhcr/metadata.csv"),
        help="Path to UNHCR metadata CSV (default: data/unhcr/metadata.csv).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output text path (default: data/unhcr/datasets_<year>_urls.txt).",
    )
    parser.add_argument(
        "--continent-map",
        type=Path,
        default=Path("data/unhcr/country_continents.yml"),
        help="YAML mapping of country -> continent (optional).",
    )
    parser.add_argument(
        "--country-id-map",
        type=Path,
        default=Path("data/unhcr/country_id_mapping.json"),
        help="JSON mapping of country_id -> country_name from catalog page (optional).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or [])
    if args.output is None:
        args.output = Path(f"data/unhcr/datasets_{args.year}_urls.txt")
    try:
        continent_map = load_continent_map(args.continent_map)
        country_id_map = load_country_ids(args.country_id_map)
        records = build_dataset_links(args.input, args.year, continent_map)
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    id_lookup = {v.lower(): k for k, v in country_id_map.items()}

    total_countries = records["country"].nunique()
    # Each record corresponds to a country-url pair; URLs can repeat across countries.
    total_records = len(records)
    total_unique_urls = records["url"].nunique()

    lines = []
    for continent, continent_group in records.groupby("continent"):
        lines.append(f"{continent}")
        for country, group in continent_group.groupby("country"):
            urls = sorted(group["url"].dropna().unique())
            key = country.lower()
            if key in ALIASES:
                key = ALIASES[key]
            country_id = id_lookup.get(key)
            search_url = None
            if country_id:
                search_url = (
                    "https://microdata.unhcr.org/index.php/catalog/"
                    f"?page=1&country%5B%5D={country_id}&sort_by=year&sort_order=desc&ps=15"
                )
            suffix = f" [id: {country_id}]" if country_id else " [id: ?]"
            lines.append(f"  {country} ({len(urls)}){suffix}")
            if search_url:
                lines.append(f"    search: {search_url}")
            lines.extend(f"    {url}" for url in urls)
        lines.append("")

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")

    print(
        f"Wrote {total_records} country-url records "
        f"({total_unique_urls} unique URLs) across "
        f"{total_countries} countries to {args.output}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
