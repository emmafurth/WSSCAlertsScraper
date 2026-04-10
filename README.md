# WSSC Alerts Scraper

This folder contains a full pipeline to collect WSSC water main repair alerts, extract text from each alert, clean the address fields, and geocode the cleaned addresses.

## Scripts

- `scrape_alert_links.py` collects alert links from the WSSC newsroom page.
- `scrape_alert_text.py` downloads each alert page and extracts the page text into a CSV.
- `extract_addresses.py` cleans and normalizes the `address` and `pipe_diameter` fields from the scraped CSV.
- `geocode_wssc_alerts.py` geocodes cleaned addresses into latitude and longitude.
- `convert_geocoded_to_shapefile.py` converts the latest geocoded CSV into a point shapefile.
- `run_all.sh` runs the full pipeline in sequence.

## Requirements

Install the Python packages used by the scripts:

```bash
pip install pandas requests beautifulsoup4 selenium
```

You also need a working Chrome browser and a compatible ChromeDriver setup for the Selenium script.

## Output Layout

Each script writes two copies of its output:

- A stable file in `outputs/` that gets overwritten on each run.
- A timestamped archive copy in `outputs/archive/`.

The link scraper writes:

- `outputs/article_links.txt`
- `outputs/archive/article_links_<timestamp>.txt`

The text scraper writes:

- `outputs/wssc_alerts.csv`
- `outputs/archive/wssc_alerts_<timestamp>.csv`

The geocoder writes:

- `outputs/wssc_alerts_geocoded.csv`

The shapefile converter writes:

- `outputs/wssc_alerts_geocoded.shp`
- companion shapefile sidecar files in `outputs/`

## Running The Scripts

Run the scripts from this folder.

### 1. Scrape alert links

```bash
python scrape_alert_links.py
```

Optional flags:

- `--start-year` sets the first year to scrape, inclusive.
- `--end-year` sets the last year to scrape, inclusive.
- `--headless` runs Chrome in headless mode.
- `--output-dir` changes the output folder.

Example:

```bash
python scrape_alert_links.py --start-year 2022 --end-year 2026 --headless
```

### 2. Scrape alert text

By default, this script uses the newest timestamped `article_links_*.txt` file in `outputs/`. If no timestamped file exists, it falls back to `outputs/article_links.txt`.

```bash
python scrape_alert_text.py
```

Optional flags:

- `--links-file` uses a specific article links file.
- `--sleep-seconds` changes the delay between page requests.
- `--extract-addresses` runs the address extraction step before saving the CSV.
- `--output-dir` changes the output folder.

Examples:

```bash
python scrape_alert_text.py --links-file outputs/article_links.txt
```

```bash
python scrape_alert_text.py --extract-addresses
```

### 3. Extract addresses separately

If you want to run the cleaning step as a separate script, use:

```bash
python extract_addresses.py --input outputs/wssc_alerts.csv --output outputs/wssc_alerts_addresses_updated.csv
```

Optional flags:

- `--input` sets the source CSV.
- `--output` sets the cleaned CSV path.

### 4. Geocode cleaned addresses

```bash
python geocode_wssc_alerts.py --input outputs/wssc_alerts_addresses_updated.csv --output outputs/wssc_alerts_geocoded.csv
```

Optional flags:

- `--provider` chooses `nominatim`, `opencage`, or `googlemaps`.
- `--delay` controls request delay (for Nominatim, minimum 1 second is enforced).
- `--append-location` appends location text to each query.
- `--opencage-api-key` sets the OpenCage key (or use `OPENCAGE_API_KEY`).
- `--google-maps-api-key` sets the Google key (or use `GOOGLE_MAPS_API_KEY`).

Examples:

```bash
python geocode_wssc_alerts.py --input outputs/wssc_alerts_addresses_updated.csv --output outputs/wssc_alerts_geocoded.csv --provider googlemaps --google-maps-api-key YOUR_KEY
```

```bash
python geocode_wssc_alerts.py --input outputs/wssc_alerts_addresses_updated.csv --output outputs/wssc_alerts_geocoded.csv --provider opencage
```

### 5. Run everything with one command

Make the script executable once:

```bash
chmod +x run_all.sh
```

Run the full pipeline:

```bash
./run_all.sh
```

Useful options:

- `--headless`
- `--start-year 2022 --end-year 2026`
- `--provider nominatim|opencage|googlemaps`
- `--sleep-seconds 3.0`
- `--geocode-delay 1.2`
- `--append-location "Maryland, USA"`

Example:

```bash
./run_all.sh --headless --provider googlemaps
```

### 6. Convert the latest geocoded CSV to a shapefile

```bash
python convert_geocoded_to_shapefile.py
```

Optional flags:

- `--input` uses a specific geocoded CSV instead of auto-detecting the newest one.
- `--output` changes the output `.shp` path or directory.

Notes:

- The script writes the shapefile into `outputs/` and does not create archive copies.
- Shapefile DBF field names are limited to 10 characters, so `pipe_diameter` is stored as `pipe_diam`.
- `full_text` is stored in the shapefile attribute table and may be truncated if it exceeds the DBF field length limit.

## Typical Workflow

1. Run `scrape_alert_links.py` to gather alert URLs.
2. Run `scrape_alert_text.py` to scrape alert text into `outputs/wssc_alerts.csv`.
3. Run `extract_addresses.py` to produce `outputs/wssc_alerts_addresses_updated.csv`.
4. Run `geocode_wssc_alerts.py` to produce `outputs/wssc_alerts_geocoded.csv`.
5. Use `run_all.sh` to execute the full pipeline, including the shapefile export.

## Notes

- `scrape_alert_text.py` can work on raw scraped output or on a dataframe that already passed through the address extraction logic.
- The archive folder is safe to keep as a run history; the stable files in `outputs/` always contain the latest run.