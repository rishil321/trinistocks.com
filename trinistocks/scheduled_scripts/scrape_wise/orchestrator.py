import argparse
import logging
import sys
from pathlib import Path
from typing import List

from pid.decorator import pidfile

from scheduled_scripts.scrape_wise.market_reports import MarketReportsScraper, MissingMarketReportMonthAndYear, \
    MarketReportLinks
from datetime import date

logger = logging.getLogger(__name__)


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


def set_up_arguments(args):
    # first check the arguments given to this script
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-d",
        "--daily_market_reports",
        action="store_true",
        default=False
    )
    return parser.parse_args(args)


@pidfile()
def main(args) -> int:
    cli_arguments: argparse.Namespace = set_up_arguments(args)
    logger.info("Now starting WISE scraper.")
    if cli_arguments.daily_market_reports:
        logger.info("Now scraping daily market reports.")
        scrape_and_parse_all_missing_reports()
    return 0


if __name__ == "__main__":
    main(sys.argv[1:])
