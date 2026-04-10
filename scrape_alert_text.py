from __future__ import annotations

import argparse
import datetime
import random
import re
import time
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup

import extract_addresses


def get_current_datetime_as_intstring() -> str:
    return datetime.datetime.now().strftime("%m%d%Y%H%M%S")


def fetch_with_retry(url: str, headers: dict[str, str], max_retries: int = 3):
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            return response
        except requests.RequestException:
            if attempt == max_retries - 1:
                print(f"Failed to fetch {url} after {max_retries} attempts")
                return None

            wait_time = (2 ** attempt) + random.uniform(0, 1)
            print(f"Attempt {attempt + 1} failed, waiting {wait_time:.2f}s before retry")
            time.sleep(wait_time)


def scrape_wssc_alert_page(url: str, headers: dict[str, str]):
    response = fetch_with_retry(url=url, headers=headers)

    if response is None:
        return None

    soup = BeautifulSoup(response.content, "html.parser")

    title_node = soup.select_one("h1")
    date_node = soup.select_one("time")

    title = title_node.text.strip() if title_node is not None else None
    date = date_node.text.strip() if date_node is not None else None
    full_text = "\n".join(
        [item.get_text() for item in soup.select(".node__content .field--type-text-long p")]
    )

    alert_re = re.compile(
        r"(?P<diameter>\d{1,3})-inch water main (at|on) (?P<address>[\w,\s\.-]+?)\.\s(Customers|Temporary)",
        re.IGNORECASE,
    )
    re_search_results = alert_re.search(full_text)

    data = {
        "title": title,
        "date": date,
        "pipe_diameter": re_search_results.group("diameter") if re_search_results is not None else None,
        "address": re_search_results.group("address") if re_search_results is not None else None,
        "full_text": full_text,
    }

    return data


def find_latest_article_links_file(output_dir: Path) -> Path:
    candidate_files = list(output_dir.glob("article_links*.txt"))
    if not candidate_files:
        raise FileNotFoundError(f"No article_links_*.txt files found in {output_dir}")

    timestamped_files = []
    stable_file = None

    for path in candidate_files:
        match = re.search(r"article_links_(\d+)\.txt$", path.name)
        if match is None:
            if path.name == "article_links.txt":
                stable_file = path
            continue
        timestamped_files.append((int(match.group(1)), path.name, path))

    if timestamped_files:
        return max(timestamped_files)[2]

    if stable_file is not None:
        return stable_file

    raise FileNotFoundError(f"No timestamped article_links_*.txt files found in {output_dir}")


def load_article_links(links_file: Path) -> list[str]:
    with links_file.open("r", encoding="utf-8") as file_handle:
        return [line.strip() for line in file_handle if line.strip()]


def scrape_all_alert_text(
    links_file: Path,
    output_dir: Path,
    sleep_seconds: float = 3.0,
    extract_address_fields: bool = False,
) -> tuple[pd.DataFrame, Path]:
    article_links_list = load_article_links(links_file)

    data = []
    headers = {
        "USER-AGENT": "wssc_alerts scraper (efurth@montgomerycollege.edu)",
    }

    for link in article_links_list:
        row = scrape_wssc_alert_page(link, headers)
        if isinstance(row, dict):
            data.append(row)
        time.sleep(sleep_seconds)

    df = pd.DataFrame(data)
    if extract_address_fields:
        df = extract_addresses.process_dataframe(df)

    output_dir.mkdir(parents=True, exist_ok=True)
    archive_dir = output_dir / "archive"
    archive_dir.mkdir(parents=True, exist_ok=True)

    stable_path = output_dir / "wssc_alerts.csv"
    archive_path = archive_dir / f"wssc_alerts_{get_current_datetime_as_intstring()}.csv"

    for output_path in (stable_path, archive_path):
        df.to_csv(output_path, index=False)

    return df, stable_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape WSSC alert text from article links.")
    parser.add_argument(
        "--links-file",
        type=Path,
        default=None,
        help="Specific article links text file to use.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path(__file__).resolve().parent / "outputs",
        help="Directory that stores article link files and the generated CSV.",
    )
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=3.0,
        help="Seconds to wait between article requests.",
    )
    parser.add_argument(
        "--extract-addresses",
        action="store_true",
        help="Run the address extraction logic before saving the CSV.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    links_file = args.links_file if args.links_file is not None else find_latest_article_links_file(args.output_dir)

    if not links_file.is_absolute():
        links_file = (Path.cwd() / links_file).resolve()

    df, output_path = scrape_all_alert_text(
        links_file=links_file,
        output_dir=args.output_dir,
        sleep_seconds=args.sleep_seconds,
        extract_address_fields=args.extract_addresses,
    )
    print(f"Loaded links from {links_file}")
    print(f"Scraped {len(df)} rows and saved to {output_path}")


if __name__ == "__main__":
    main()