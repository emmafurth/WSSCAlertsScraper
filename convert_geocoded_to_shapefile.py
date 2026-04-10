#!/usr/bin/env python3
"""Convert the latest geocoded WSSC CSV into a point shapefile.

By default, the script looks for the most recent geocoded CSV in the local
``outputs`` folder, including timestamped archive copies, and writes a shapefile
back into ``outputs``.

Usage:
    python convert_geocoded_to_shapefile.py
"""

from __future__ import annotations

import argparse
import csv
import re
from pathlib import Path

try:
    import shapefile  # type: ignore[reportMissingImports]  # pyshp
except ImportError as exc:
    raise SystemExit("Missing dependency 'pyshp'. Install with: pip install pyshp") from exc


OUTPUT_DIR = Path(__file__).resolve().parent / "outputs"
DEFAULT_OUTPUT = OUTPUT_DIR / "wssc_alerts_geocoded.shp"
SHAPEFILE_SIDECAR_EXTENSIONS = {".shp", ".shx", ".dbf", ".prj", ".cpg", ".qpj", ".sbn", ".sbx"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Convert geocoded WSSC CSV rows into a point shapefile.")
    parser.add_argument(
        "--input",
        default="",
        help="Optional geocoded CSV path. If omitted, the newest geocoded CSV in outputs/ is used.",
    )
    parser.add_argument(
        "--output",
        default=str(DEFAULT_OUTPUT),
        help="Output .shp path. Defaults to outputs/wssc_alerts_geocoded.shp.",
    )
    return parser.parse_args()


def normalize_text(value: object) -> str:
    if value is None:
        return ""
    text = str(value).replace("\xa0", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text


def find_latest_geocoded_csv() -> Path:
    candidate_paths = [OUTPUT_DIR / "wssc_alerts_geocoded.csv"]
    candidate_paths.extend((OUTPUT_DIR / "archive").glob("wssc_alerts_geocoded_*.csv"))
    existing_paths = [path for path in candidate_paths if path.exists()]

    if not existing_paths:
        raise FileNotFoundError(f"No geocoded CSV files found in {OUTPUT_DIR}")

    return max(existing_paths, key=lambda path: path.stat().st_mtime_ns)


def resolve_output_path(output: str) -> Path:
    output_path = Path(output).expanduser()
    if output_path.is_dir() or str(output).endswith("/"):
        return output_path / "wssc_alerts_geocoded.shp"
    if output_path.suffix.lower() != ".shp":
        output_path = output_path.with_suffix(".shp")
    return output_path


def delete_existing_shapefile_components(output_path: Path) -> None:
    for extension in SHAPEFILE_SIDECAR_EXTENSIONS:
        component = output_path.with_suffix(extension)
        if component.exists():
            component.unlink()


def csv_to_shapefile(csv_path: Path, output_path: Path) -> None:
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    delete_existing_shapefile_components(output_path)

    with csv_path.open("r", newline="", encoding="utf-8-sig") as in_file:
        reader = csv.DictReader(in_file)
        if not reader.fieldnames:
            raise ValueError("CSV has no header row")

        required_columns = {"title", "date", "pipe_diameter", "geocode_query", "full_text", "latitude", "longitude"}
        missing = required_columns - set(reader.fieldnames)
        if missing:
            missing_str = ", ".join(sorted(missing))
            raise ValueError(f"Input CSV missing required columns: {missing_str}")

        writer = shapefile.Writer(str(output_path), shapeType=shapefile.POINT)
        writer.autoBalance = 1

        # Shapefile DBF field names are limited to 10 characters.
        # The names below keep the requested content while staying within that limit.
        writer.field("title", "C", size=254)
        writer.field("date", "C", size=50)
        writer.field("pipe_diam", "N", size=18, decimal=0)
        writer.field("address", "C", size=254)
        writer.field("full_text", "C", size=254)

        row_count = 0
        skipped_count = 0

        for row in reader:
            try:
                lat = float(normalize_text(row.get("latitude")))
                lon = float(normalize_text(row.get("longitude")))
            except ValueError:
                skipped_count += 1
                continue

            pipe_diameter_text = normalize_text(row.get("pipe_diameter"))
            try:
                pipe_diameter_value = int(float(pipe_diameter_text)) if pipe_diameter_text else None
            except ValueError:
                pipe_diameter_value = None

            writer.point(lon, lat)
            writer.record(
                normalize_text(row.get("title"))[:254],
                normalize_text(row.get("date"))[:50],
                pipe_diameter_value,
                normalize_text(row.get("geocode_query"))[:254],
                normalize_text(row.get("full_text"))[:254],
            )
            row_count += 1

        writer.close()

    output_path.with_suffix(".prj").write_text(
        'GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563]],'
        'PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433]]',
        encoding="utf-8",
    )

    print(f"Wrote shapefile: {output_path}")
    print(f"Points written: {row_count}")
    if skipped_count:
        print(f"Rows skipped (invalid/missing coords): {skipped_count}")
    print("Note: Shapefile DBF field names are limited to 10 characters, so pipe_diameter is stored as pipe_diam.")


def main() -> None:
    args = parse_args()
    csv_path = Path(args.input).expanduser().resolve() if args.input else find_latest_geocoded_csv()
    output_path = resolve_output_path(args.output)
    csv_to_shapefile(csv_path, output_path)


if __name__ == "__main__":
    main()