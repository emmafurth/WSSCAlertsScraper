#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON_BIN="${PYTHON_BIN:-python}"

START_YEAR="${START_YEAR:-2022}"
END_YEAR="${END_YEAR:-2026}"
HEADLESS=false
OUTPUT_DIR="${OUTPUT_DIR:-${SCRIPT_DIR}/outputs}"
SLEEP_SECONDS="${SLEEP_SECONDS:-3.0}"
GEOCODER_PROVIDER="${GEOCODER_PROVIDER:-nominatim}"
GEOCODER_DELAY="${GEOCODER_DELAY:-1.2}"
APPEND_LOCATION="${APPEND_LOCATION:-Maryland, USA}"

usage() {
  cat <<EOF
Usage: $(basename "$0") [options]

Runs the full WSSC pipeline in sequence:
  1) scrape_alert_links.py
  2) scrape_alert_text.py
  3) extract_addresses.py
  4) geocode_wssc_alerts.py
  5) convert_geocoded_to_shapefile.py

Options:
  --start-year YEAR         First year for link scraping (default: ${START_YEAR})
  --end-year YEAR           Last year for link scraping (default: ${END_YEAR})
  --headless                Run Selenium Chrome in headless mode
  --output-dir PATH         Output folder (default: ${OUTPUT_DIR})
  --sleep-seconds FLOAT     Delay between alert-page requests (default: ${SLEEP_SECONDS})
  --provider NAME           Geocoder provider: nominatim|opencage|googlemaps (default: ${GEOCODER_PROVIDER})
  --geocode-delay FLOAT     Delay between geocoder requests (default: ${GEOCODER_DELAY})
  --append-location TEXT    Location suffix for geocode queries (default: "${APPEND_LOCATION}")
  -h, --help                Show this help text

Notes:
  - API keys are read by geocode_wssc_alerts.py from CLI/env:
      OPENCAGE_API_KEY
      GOOGLE_MAPS_API_KEY
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --start-year)
      START_YEAR="$2"
      shift 2
      ;;
    --end-year)
      END_YEAR="$2"
      shift 2
      ;;
    --headless)
      HEADLESS=true
      shift
      ;;
    --output-dir)
      OUTPUT_DIR="$2"
      shift 2
      ;;
    --sleep-seconds)
      SLEEP_SECONDS="$2"
      shift 2
      ;;
    --provider)
      GEOCODER_PROVIDER="$2"
      shift 2
      ;;
    --geocode-delay)
      GEOCODER_DELAY="$2"
      shift 2
      ;;
    --append-location)
      APPEND_LOCATION="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

mkdir -p "$OUTPUT_DIR" "$OUTPUT_DIR/archive"

echo "[1/4] Scraping alert links..."
LINK_ARGS=(
  "${SCRIPT_DIR}/scrape_alert_links.py"
  --start-year "$START_YEAR"
  --end-year "$END_YEAR"
  --output-dir "$OUTPUT_DIR"
)
if [[ "$HEADLESS" == true ]]; then
  LINK_ARGS+=(--headless)
fi
"$PYTHON_BIN" "${LINK_ARGS[@]}"

echo "[2/4] Scraping alert text..."
"$PYTHON_BIN" "${SCRIPT_DIR}/scrape_alert_text.py" \
  --output-dir "$OUTPUT_DIR" \
  --sleep-seconds "$SLEEP_SECONDS"

echo "[3/4] Extracting clean addresses..."
"$PYTHON_BIN" "${SCRIPT_DIR}/extract_addresses.py" \
  --input "$OUTPUT_DIR/wssc_alerts.csv" \
  --output "$OUTPUT_DIR/wssc_alerts_addresses_updated.csv"

echo "[4/4] Geocoding addresses..."
"$PYTHON_BIN" "${SCRIPT_DIR}/geocode_wssc_alerts.py" \
  --input "$OUTPUT_DIR/wssc_alerts_addresses_updated.csv" \
  --output "$OUTPUT_DIR/wssc_alerts_geocoded.csv" \
  --provider "$GEOCODER_PROVIDER" \
  --delay "$GEOCODER_DELAY" \
  --append-location "$APPEND_LOCATION"

echo "[5/5] Converting geocoded CSV to shapefile..."
"$PYTHON_BIN" "${SCRIPT_DIR}/convert_geocoded_to_shapefile.py" \
  --output "$OUTPUT_DIR/wssc_alerts_geocoded.shp"

echo "Pipeline complete."
echo "Outputs:"
echo "  - $OUTPUT_DIR/article_links.txt"
echo "  - $OUTPUT_DIR/wssc_alerts.csv"
echo "  - $OUTPUT_DIR/wssc_alerts_addresses_updated.csv"
echo "  - $OUTPUT_DIR/wssc_alerts_geocoded.csv"
echo "  - $OUTPUT_DIR/wssc_alerts_geocoded.shp"