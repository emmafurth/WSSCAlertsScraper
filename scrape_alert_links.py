from __future__ import annotations

import argparse
import datetime
from pathlib import Path

from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.select import Select
from selenium.webdriver.support.ui import WebDriverWait


def get_current_datetime_as_intstring() -> str:
    return datetime.datetime.now().strftime("%m%d%Y%H%M%S")


def build_driver(headless: bool = False) -> webdriver.Chrome:
    options = webdriver.ChromeOptions()
    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--window-size=1920,1080")
    return webdriver.Chrome(options=options)


def get_article_links_with_selenium(start_year: int = 2022, end_year: int = 2026, headless: bool = False) -> list[str]:
    article_links_list: list[str] = []

    start_url = "https://www.wsscwater.com/newsroom"
    print(start_url)

    driver = build_driver(headless=headless)
    try:
        driver.delete_all_cookies()
        driver.get(start_url)
        driver.implicitly_wait(0.5)

        wait = WebDriverWait(driver, timeout=2, poll_frequency=0.2)

        alert_filter = driver.find_element(by=By.XPATH, value="//label[@for='edit-type-253']")
        alert_filter.click()

        wait.until(
            EC.text_to_be_present_in_element_attribute(
                (By.CSS_SELECTOR, ".pager__item--next a"),
                "href",
                "type%5B253%5D=253",
            )
        )

        for year in range(start_year, end_year + 1):
            print(f"Current year: {year}")

            select_year_element = driver.find_element(by=By.ID, value="edit-year")
            select_year = Select(select_year_element)
            print(f"Selected year: {select_year.all_selected_options[0].text}")
            select_year.select_by_value(f"{year}")

            try:
                wait.until(
                    EC.text_to_be_present_in_element_attribute(
                        (By.CSS_SELECTOR, ".pager__item--next a"),
                        "href",
                        f"year={year}",
                    )
                )
            except TimeoutException:
                select_year_element = driver.find_element(by=By.ID, value="edit-year")
                select_year = Select(select_year_element)
                print(f"Selected year: {select_year.all_selected_options[0].text}")
                select_year.select_by_value(f"{year}")
                wait.until(
                    EC.text_to_be_present_in_element_attribute(
                        (By.CSS_SELECTOR, ".pager__item--next a"),
                        "href",
                        f"year={year}",
                    )
                )

            current_page_num = 0

            while True:
                current_page_num += 1
                alerts = driver.find_elements(by=By.CSS_SELECTOR, value="h3 a")
                repair_alerts = filter(lambda element: element.text.startswith("Emergency Water Main Repair"), alerts)

                for alert in repair_alerts:
                    article_links_list.append(alert.get_attribute("href"))

                try:
                    next_link = driver.find_element(by=By.CSS_SELECTOR, value=".pager__item--next a")
                    next_link.click()

                    try:
                        wait.until(
                            EC.text_to_be_present_in_element(
                                (By.CSS_SELECTOR, "li.pager__item.is-active a"),
                                f"{current_page_num + 1}",
                            )
                        )
                    except TimeoutException:
                        next_link = driver.find_element(by=By.CSS_SELECTOR, value=".pager__item--next a")
                        next_link.click()
                        wait.until(
                            EC.text_to_be_present_in_element(
                                (By.CSS_SELECTOR, "li.pager__item.is-active a"),
                                f"{current_page_num + 1}",
                            )
                        )
                    continue

                except NoSuchElementException:
                    break

        return article_links_list

    finally:
        driver.quit()


def write_links_to_file(links: list[str], output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    archive_dir = output_dir / "archive"
    archive_dir.mkdir(parents=True, exist_ok=True)

    stable_path = output_dir / "article_links.txt"
    archive_path = archive_dir / f"article_links_{get_current_datetime_as_intstring()}.txt"

    for output_path in (stable_path, archive_path):
        with output_path.open("w", encoding="utf-8") as file_handle:
            for link in links:
                file_handle.write(f"{link}\n")

    return stable_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape WSSC emergency repair alert links from the newsroom page.")
    parser.add_argument("--start-year", type=int, default=2022, help="First year to scrape, inclusive.")
    parser.add_argument("--end-year", type=int, default=2026, help="Last year to scrape, inclusive.")
    parser.add_argument("--headless", action="store_true", help="Run Chrome in headless mode.")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path(__file__).resolve().parent / "outputs",
        help="Directory for the saved article links file.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    links = get_article_links_with_selenium(
        start_year=args.start_year,
        end_year=args.end_year,
        headless=args.headless,
    )
    output_path = write_links_to_file(links, args.output_dir)
    print(f"Saved {len(links)} links to {output_path}")


if __name__ == "__main__":
    main()