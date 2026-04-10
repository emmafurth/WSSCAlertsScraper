#!/usr/bin/env python3
"""Extract WSSC addresses and pipe diameters from ``full_text``.

Usage:
    python extract_addresses.py --input output/wssc_alerts.csv \
        --output output/wssc_alerts_addresses_updated.csv

Options:
    --input   Path to the source CSV file. Defaults to ``output/wssc_alerts.csv``.
    --output  Path for the updated CSV file. Defaults to
              ``output/wssc_alerts_addresses_updated.csv``.

The input CSV must contain a ``full_text`` column. The script updates or fills
the ``pipe_diameter`` and ``address`` columns and writes the result to the
output file.
"""

from __future__ import annotations

import argparse
import re

import pandas as pd

ADDRESS_PATTERNS = [
    re.compile(
        r"(?P<diameter>\d{1,3})-inch\swater\smain\s(?:at|on|near)\s(?P<address>.+?)(?:\.\s|$)",
        flags=re.IGNORECASE,
    ),
    re.compile(
        r"water\smain\s(?:at|on|near)\s(?P<address>.+?)(?:\.\s|$)",
        flags=re.IGNORECASE,
    ),
    re.compile(
        r"fire\shydrant\svalve\s(?:at|on|near)\s(?P<address>.+?)(?:\.\s|$)",
        flags=re.IGNORECASE,
    ),
]

TRAILING_NOISE_PATTERNS = [
    re.compile(r",?\s*impact(?:ing|ed)?\b.*$", flags=re.IGNORECASE),
    re.compile(r",?\s*affected\b.*$", flags=re.IGNORECASE),
    re.compile(r"\s+nearby\s+customers?\b.*$", flags=re.IGNORECASE),
    re.compile(r"\s+customers?\b.*$", flags=re.IGNORECASE),
    re.compile(
        r"\s+from\s+(?:monday|tuesday|wednesday|thursday|friday|saturday|sunday|\d).*$",
        flags=re.IGNORECASE,
    ),
]

TITLE_SPLIT_PATTERN = re.compile(r"\s*[-–]\s*")
DIAMETER_PATTERN = re.compile(r"(?P<diameter>\d{1,3})\s*-\s*inch\s+water\s+main\b", flags=re.IGNORECASE)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract addresses from the full_text column and store them in address."
    )
    parser.add_argument("--input", default="output/wssc_alerts.csv", help="Input CSV path")
    parser.add_argument(
        "--output",
        default="output/wssc_alerts_addresses_updated.csv",
        help="Output CSV path",
    )
    return parser.parse_args()


def normalize_text(value: object) -> str:
    if not isinstance(value, str):
        return ""
    text = value.replace("\xa0", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text


def town_from_title(title: object) -> str | None:
    text = normalize_text(title)
    if not text:
        return None

    parts = TITLE_SPLIT_PATTERN.split(text, maxsplit=1)
    if len(parts) != 2:
        return None

    town = parts[1].strip(" -–")
    if not town or any(char.isdigit() for char in town):
        return None
    return town


def extract_address(full_text: object) -> str | None:
    text = normalize_text(full_text)
    if not text:
        return None

    for pattern in ADDRESS_PATTERNS:
        match = pattern.search(text)
        if match:
            return match.group("address").strip()
    return None


def extract_diameter(full_text: object) -> int | None:
    text = normalize_text(full_text)
    if not text:
        return None

    match = DIAMETER_PATTERN.search(text)
    if not match:
        return None
    return int(match.group("diameter"))


def clean_address(address: object, title: object) -> str | None:
    text = normalize_text(address)
    if not text:
        return None

    text = re.sub(r"^(?:North|South|East|West)\s*[–-]\s*(?=\d)", "", text, flags=re.IGNORECASE)
    text = re.sub(r"^the\s+intersection\s+of\s+", "", text, flags=re.IGNORECASE)
    for pattern in TRAILING_NOISE_PATTERNS:
        text = pattern.sub("", text).strip()

    # Normalize terminal patterns like "123 Main St in Bethesda" -> "123 Main St, Bethesda".
    text = re.sub(
        r"\s+(?:in|at|near)\s+([A-Za-z][A-Za-z/ '\-]+)$",
        r", \1",
        text,
        flags=re.IGNORECASE,
    )

    text = text.strip(" ,.;:-")
    town = town_from_title(title)
    if town:
        suffix_pattern = re.compile(
            rf"\s+(?:in|at|near)\s+{re.escape(town)}$",
            flags=re.IGNORECASE,
        )
        if suffix_pattern.search(text):
            text = suffix_pattern.sub(f", {town}", text)
        elif not re.search(rf"(?:^|,\s*){re.escape(town)}$", text, flags=re.IGNORECASE):
            if not re.search(r",\s*[A-Za-z][A-Za-z/ '\-]+$", text):
                text = f"{text}, {town}"

    text = re.sub(r"\s+,", ",", text)
    text = re.sub(r",\s*", ", ", text)
    text = re.sub(r"\s+", " ", text).strip(" ,.;:-")
    return text or None


def build_address(row: pd.Series) -> str | None:
    extracted = extract_address(row.get("full_text"))
    candidate = extracted or normalize_text(row.get("address"))
    return clean_address(candidate, row.get("title"))


def build_diameter(row: pd.Series) -> int | float | None:
    extracted = extract_diameter(row.get("full_text"))
    if extracted is not None:
        return extracted

    existing = row.get("pipe_diameter")
    if pd.isna(existing):
        return None

    try:
        return int(float(existing))
    except (TypeError, ValueError):
        return None


def process_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    if "full_text" not in df.columns:
        raise ValueError("Input dataframe must contain a full_text column")

    result = df.copy()
    result["pipe_diameter"] = result.apply(build_diameter, axis=1)
    result["address"] = result.apply(build_address, axis=1)
    return result


def main() -> None:
    args = parse_args()
    df = pd.read_csv(args.input)

    df = process_dataframe(df)
    df.to_csv(args.output, index=False)

    diameter_matched = df["pipe_diameter"].notna().sum()
    matched = df["address"].notna().sum()
    unmatched = df["address"].isna().sum()
    print(f"Wrote {args.output}")
    print(f"  diameter matched: {diameter_matched}")
    print(f"  matched:   {matched}")
    print(f"  unmatched: {unmatched}")


if __name__ == "__main__":
    main()
