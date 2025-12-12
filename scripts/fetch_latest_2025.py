#!/usr/bin/env python
"""
Fetch the most recently added UNHCR studies and keep only those from a target year.

Uses the public "latest" API endpoint and writes the filtered results to JSON.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List

import requests

API_URL = "https://microdata.unhcr.org/index.php/api/catalog/latest"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
}


def fetch_latest(limit: int) -> List[Dict[str, Any]]:
    resp = requests.get(API_URL, params={"limit": limit}, headers=HEADERS, timeout=20)
    resp.raise_for_status()
    payload = resp.json()
    if not isinstance(payload, dict) or "result" not in payload:
        raise ValueError("Unexpected response format; expected dict with 'result'.")
    data = payload["result"]
    if not isinstance(data, list):
        raise ValueError("Unexpected 'result' format; expected a list.")
    return data


def filter_by_year(studies: List[Dict[str, Any]], year: int) -> List[Dict[str, Any]]:
    keep = []
    for s in studies:
        created = s.get("created")
        if not created:
            continue
        # created appears as 'Nov-13-2025' so check for suffix or year substring.
        if str(year) in str(created):
            keep.append(s)
    return keep


def parse_args(argv: List[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch latest UNHCR studies and keep only those from the target year."
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=500,
        help="Number of latest studies to fetch (default: 500).",
    )
    parser.add_argument(
        "--year",
        type=int,
        default=2025,
        help="Year to filter on (default: 2025).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/unhcr/latest_2025.json"),
        help="Output JSON path (default: data/unhcr/latest_2025.json).",
    )
    return parser.parse_args(argv)


def main(argv: List[str] | None = None) -> int:
    args = parse_args(argv or [])
    studies = fetch_latest(args.limit)
    filtered = filter_by_year(studies, args.year)

    if args.output is None:
        args.output = Path(f"data/unhcr/latest_{args.year}.json")
    args.output.parent.mkdir(parents=True, exist_ok=True)

    args.output.write_text(json.dumps(filtered, indent=2))
    print(f"Fetched {len(studies)} studies, kept {len(filtered)} for year {args.year}")
    print(f"Wrote {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
