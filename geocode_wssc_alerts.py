#!/usr/bin/env python3
"""Geocode WSSC alert addresses into latitude/longitude columns.

Usage:
    python geocode_wssc_alerts.py \
        --input output/wssc_alerts.csv \
        --output output/wssc_alerts_geocoded.csv
"""

from __future__ import annotations

import argparse
import json
import os
import re
import time
from pathlib import Path
from typing import Dict, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import pandas as pd


AT_ADDRESS_PATTERN = re.compile(
    r"water main at\s+(.+?)(?:\.\s+Customers|\.\s+Repairs|\.\s+Temporary|\.|\n)",
    flags=re.IGNORECASE | re.DOTALL,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Geocode WSSC addresses from CSV.")
    parser.add_argument("--input", default="output/wssc_alerts.csv", help="Input CSV path")
    parser.add_argument(
        "--output",
        default="output/wssc_alerts_geocoded.csv",
        help="Output CSV path with geocoding columns",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=1.2,
        help="Seconds between geocoder requests (mainly relevant for Nominatim).",
    )
    parser.add_argument(
        "--provider",
        choices=["nominatim", "opencage", "googlemaps"],
        default="nominatim",
        help="Geocoding provider to use.",
    )
    parser.add_argument(
        "--opencage-api-key",
        default="",
        help="OpenCage API key. If omitted, uses OPENCAGE_API_KEY env var.",
    )
    parser.add_argument(
        "--google-maps-api-key",
        default="",
        help="Google Maps Geocoding API key. If omitted, uses GOOGLE_MAPS_API_KEY env var.",
    )
    parser.add_argument(
        "--append-location",
        default="Maryland, USA",
        help="Text appended to each address to improve geocoding relevance.",
    )
    parser.add_argument(
        "--force-regeocode",
        action="store_true",
        help="Ignore previous output cache and geocode every address again.",
    )
    return parser.parse_args()


def clean_text(value: object) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    text = str(value).replace("\xa0", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text


def fallback_address_from_full_text(full_text: str) -> str:
    if not full_text:
        return ""
    match = AT_ADDRESS_PATTERN.search(full_text)
    if not match:
        return ""
    return clean_text(match.group(1))


def choose_address(address: str, full_text: str) -> str:
    cleaned = clean_text(address)
    if cleaned:
        return cleaned
    return fallback_address_from_full_text(clean_text(full_text))


def parse_float_or_none(value: object) -> Optional[float]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def build_existing_geocode_cache(
    existing_df: pd.DataFrame,
    append_location: str,
) -> Dict[str, Tuple[Optional[float], Optional[float], str, str]]:
    cache: Dict[str, Tuple[Optional[float], Optional[float], str, str]] = {}

    for _, row in existing_df.iterrows():
        query = clean_text(row.get("geocode_query", ""))
        if not query:
            address = choose_address(row.get("address", ""), row.get("full_text", ""))
            if address:
                query = f"{address}, {append_location}" if append_location else address

        if not query:
            continue

        lat = parse_float_or_none(row.get("latitude"))
        lon = parse_float_or_none(row.get("longitude"))
        if lat is None or lon is None:
            continue

        status = clean_text(row.get("geocode_status", "")) or "ok"
        cache[query] = (lat, lon, status, query)

    return cache


def geocode_nominatim(query: str, user_agent: str) -> Tuple[Optional[float], Optional[float], str]:
    params = urlencode({"q": query, "format": "jsonv2", "limit": 1})
    url = f"https://nominatim.openstreetmap.org/search?{params}"
    req = Request(url, headers={"User-Agent": user_agent, "Accept": "application/json"})

    try:
        with urlopen(req, timeout=20) as response:
            payload = response.read().decode("utf-8")
        results = json.loads(payload)
        if not results:
            return None, None, "not_found"

        first = results[0]
        lat = float(first["lat"])
        lon = float(first["lon"])
        return lat, lon, "ok"
    except HTTPError as exc:
        return None, None, f"error:HTTP{exc.code}"
    except URLError as exc:
        return None, None, f"error:{type(exc).__name__}"
    except (TimeoutError, ValueError, json.JSONDecodeError) as exc:
        return None, None, f"error:{type(exc).__name__}"


def geocode_opencage(query: str, api_key: str) -> Tuple[Optional[float], Optional[float], str]:
    params = urlencode({"q": query, "key": api_key, "limit": 1, "no_annotations": 1})
    url = f"https://api.opencagedata.com/geocode/v1/json?{params}"
    req = Request(url, headers={"Accept": "application/json"})

    try:
        with urlopen(req, timeout=20) as response:
            payload = response.read().decode("utf-8")
        data = json.loads(payload)
    except HTTPError as exc:
        return None, None, f"error:HTTP{exc.code}"
    except URLError as exc:
        return None, None, f"error:{type(exc).__name__}"
    except (TimeoutError, ValueError, json.JSONDecodeError) as exc:
        return None, None, f"error:{type(exc).__name__}"

    api_status = data.get("status", {})
    status_code = api_status.get("code")
    if status_code == 200 and data.get("results"):
        geometry = data["results"][0].get("geometry", {})
        lat = geometry.get("lat")
        lon = geometry.get("lng")
        if isinstance(lat, (int, float)) and isinstance(lon, (int, float)):
            return float(lat), float(lon), "ok"
        return None, None, "error:missing_location"

    if status_code == 200 and not data.get("results"):
        return None, None, "not_found"

    if status_code is None:
        return None, None, "error:UNKNOWN"

    return None, None, f"error:HTTP{status_code}"


def geocode_google_maps(query: str, api_key: str) -> Tuple[Optional[float], Optional[float], str]:
    params = urlencode({"address": query, "key": api_key})
    url = f"https://maps.googleapis.com/maps/api/geocode/json?{params}"
    req = Request(url, headers={"Accept": "application/json"})

    try:
        with urlopen(req, timeout=20) as response:
            payload = response.read().decode("utf-8")
        data = json.loads(payload)
    except HTTPError as exc:
        return None, None, f"error:HTTP{exc.code}"
    except URLError as exc:
        return None, None, f"error:{type(exc).__name__}"
    except (TimeoutError, ValueError, json.JSONDecodeError) as exc:
        return None, None, f"error:{type(exc).__name__}"

    api_status = str(data.get("status", "")).upper()
    if api_status == "OK" and data.get("results"):
        location = data["results"][0].get("geometry", {}).get("location", {})
        lat = location.get("lat")
        lon = location.get("lng")
        if isinstance(lat, (int, float)) and isinstance(lon, (int, float)):
            return float(lat), float(lon), "ok"
        return None, None, "error:missing_location"

    if api_status == "ZERO_RESULTS":
        return None, None, "not_found"

    if not api_status:
        return None, None, "error:UNKNOWN"

    return None, None, f"error:{api_status}"


def geocode_addresses(
    df: pd.DataFrame,
    append_location: str,
    delay: float,
    provider: str,
    opencage_api_key: str,
    google_maps_api_key: str,
    existing_cache: Optional[Dict[str, Tuple[Optional[float], Optional[float], str, str]]] = None,
) -> pd.DataFrame:
    user_agent = "data269-wssc-alerts-geocoder/1.0"

    cache: Dict[str, Tuple[Optional[float], Optional[float], str, str]] = dict(existing_cache or {})
    latitudes = []
    longitudes = []
    statuses = []
    queries = []

    for _, row in df.iterrows():
        address = choose_address(row.get("address", ""), row.get("full_text", ""))

        if not address:
            latitudes.append(None)
            longitudes.append(None)
            statuses.append("missing_address")
            queries.append("")
            continue

        query = f"{address}, {append_location}" if append_location else address

        if query in cache:
            lat, lon, status, cached_query = cache[query]
            latitudes.append(lat)
            longitudes.append(lon)
            statuses.append(status)
            queries.append(cached_query)
            continue

        if provider == "opencage":
            lat, lon, status = geocode_opencage(query, api_key=opencage_api_key)
        elif provider == "googlemaps":
            lat, lon, status = geocode_google_maps(query, api_key=google_maps_api_key)
        else:
            lat, lon, status = geocode_nominatim(query, user_agent=user_agent)
        result = (lat, lon, status, query)

        if provider == "nominatim":
            time.sleep(max(delay, 1.0))
        elif delay > 0:
            time.sleep(delay)

        cache[query] = result
        latitudes.append(result[0])
        longitudes.append(result[1])
        statuses.append(result[2])
        queries.append(result[3])

    out = df.copy()
    out["geocode_query"] = queries
    out["latitude"] = latitudes
    out["longitude"] = longitudes
    out["geocode_status"] = statuses
    return out


def main() -> None:
    args = parse_args()
    df = pd.read_csv(args.input)

    opencage_api_key = args.opencage_api_key.strip() or os.environ.get("OPENCAGE_API_KEY", "").strip()
    google_maps_api_key = args.google_maps_api_key.strip() or os.environ.get("GOOGLE_MAPS_API_KEY", "").strip()
    if args.provider == "opencage" and not opencage_api_key:
        raise ValueError(
            "OpenCage provider selected but no API key found. "
            "Set --opencage-api-key or OPENCAGE_API_KEY."
        )
    if args.provider == "googlemaps" and not google_maps_api_key:
        raise ValueError(
            "Google Maps provider selected but no API key found. "
            "Set --google-maps-api-key or GOOGLE_MAPS_API_KEY."
        )

    required_columns = {"address", "full_text"}
    missing = required_columns - set(df.columns)
    if missing:
        missing_str = ", ".join(sorted(missing))
        raise ValueError(f"Input CSV missing required columns: {missing_str}")

    output_path = Path(args.output)
    existing_cache: Dict[str, Tuple[Optional[float], Optional[float], str, str]] = {}
    if output_path.exists() and not args.force_regeocode:
        existing_df = pd.read_csv(output_path)
        existing_cache = build_existing_geocode_cache(existing_df, append_location=args.append_location)
        print(f"Loaded {len(existing_cache)} cached geocoded addresses from {output_path}")
    elif args.force_regeocode:
        print("force-regeocode enabled; skipping previous output cache")

    geocoded = geocode_addresses(
        df,
        append_location=args.append_location,
        delay=args.delay,
        provider=args.provider,
        opencage_api_key=opencage_api_key,
        google_maps_api_key=google_maps_api_key,
        existing_cache=existing_cache,
    )
    geocoded.to_csv(args.output, index=False)

    status_counts = geocoded["geocode_status"].value_counts(dropna=False).to_dict()
    print(f"Wrote geocoded CSV: {args.output}")
    print(f"Geocode status counts: {status_counts}")


if __name__ == "__main__":
    main()
