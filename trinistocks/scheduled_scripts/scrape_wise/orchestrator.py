from pathlib import Path
from typing import List

from scheduled_scripts.scrape_wise.market_reports import MarketReportsScraper, MissingMarketReportMonthAndYear, MarketReportLinks
from datetime import date


def scrape_and_parse_all_missing_reports() -> bool:
    scraper: MarketReportsScraper = MarketReportsScraper()
    missing_market_reports_month_and_year: List[
        MissingMarketReportMonthAndYear] = scraper.build_list_of_missing_market_reports_month_and_year()
    all_missing_market_report_links: List[MarketReportLinks] = []
    for missing_month_and_year in missing_market_reports_month_and_year:
        all_market_report_links_for_month_and_year: List[
            MarketReportLinks] = scraper.scrape_all_market_report_links_for_month_and_year(
            missing_month_and_year['month'], missing_month_and_year['year'])
        all_missing_market_report_links.extend(all_market_report_links_for_month_and_year)
    all_missing_market_reports_full_dates: List[date] = scraper.build_list_of_missing_market_reports_full_dates()
    downloaded_reports: List[Path] = scraper.download_all_missing_market_reports(all_missing_market_report_links,
                                                                                 all_missing_market_reports_full_dates)
    result: bool = scraper.parse_all_missing_market_report_data(downloaded_reports)
    return result
