import os
import pathlib
from os import path
from pathlib import Path
from typing import List

from scheduled_scripts.scrape_wise.market_reports import MarketReportsScraper, MarketReportLinks, \
    MissingMarketReportMonthAndYear
from datetime import date


def test_get_date_of_latest_daily_report_in_db():
    scraper = MarketReportsScraper()
    result = scraper._get_date_of_latest_daily_report_in_db()
    assert isinstance(result, date)


def test_scrape_market_reports_for_month_and_year():
    scraper = MarketReportsScraper()
    result = scraper.scrape_all_market_report_links_for_month_and_year("08", "2023")
    assert isinstance(result, list)
    assert len(result) > 0
    first_result: MarketReportLinks = result[0]
    assert first_result['url'].startswith("https://wiseequities.com")


def test_build_list_of_market_reports_missing_month_and_year():
    scraper = MarketReportsScraper()
    result = scraper.build_list_of_missing_market_reports_month_and_year()
    assert isinstance(result, list)
    assert len(result) > 0
    first_result: MissingMarketReportMonthAndYear = result[0]
    assert isinstance(first_result, MissingMarketReportMonthAndYear)


def test_download_all_missing_market_reports():
    scraper: MarketReportsScraper = MarketReportsScraper()
    result = scraper.scrape_all_market_report_links_for_month_and_year("08", "2023")
    missing_market_reports = scraper.build_list_of_missing_market_reports_full_dates()
    all_downloaded_market_reports = scraper.download_all_missing_market_reports(all_market_report_links=result,
                                                                                all_missing_market_report_dates=missing_market_reports)
    assert len(all_downloaded_market_reports) > 0
    first_result = all_downloaded_market_reports[0]
    assert isinstance(first_result, Path)


def test_parse_all_missing_market_report_data():
    test_market_report_data: List[path] = [
        Path(os.path.join(pathlib.Path(__file__).parent.resolve(), "market_report_2023-08-14.pdf"))
    ]
    scraper: MarketReportsScraper = MarketReportsScraper()
    scraper.parse_all_missing_market_report_data(test_market_report_data)
