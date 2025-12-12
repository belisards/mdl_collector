#!/usr/bin/env python
"""
Fetch the UNHCR catalog page and extract country ID -> name mapping.

Looks for <input> elements with both a `value` attribute (the ID) and
`data-title` (the country name). Writes results as JSON by default.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import requests
from bs4 import BeautifulSoup

DEFAULT_URL = (
    "https://microdata.unhcr.org/index.php/catalog/"
    "?page=1&sort_by=year&sort_order=desc&ps=15"
)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
}


def fetch_mapping(url: str = DEFAULT_URL) -> dict[str, str]:
    resp = requests.get(url, headers=HEADERS, timeout=20)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    mapping: dict[str, str] = {}
    for inp in soup.find_all("input", attrs={"name": "country[]"}):
        value = inp.get("value")
        title = inp.get("data-title")
        if value and title:
            mapping[str(value)] = str(title)
    return mapping


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract country ID -> name mapping from UNHCR catalog page."
    )
    parser.add_argument(
        "--url",
        default=DEFAULT_URL,
        help="Catalog URL to fetch (default: first page sorted by year desc).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/unhcr/country_id_mapping.json"),
        help="Where to write the JSON mapping (default: data/unhcr/country_id_mapping.json).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or [])
    mapping = fetch_mapping(args.url)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(mapping, indent=2, sort_keys=True))
    print(f"Wrote {len(mapping)} entries to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
