#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""This is the main module used for scraping data off the Trinidad and Tobago Stock Exchange website.

:raises requests.exceptions.HTTPError: If https://www.stockex.co.tt/ is inaccessible/slow
:return: 0
:rtype: Integer
"""

import argparse

# region IMPORTS
# Put all your imports here, one per line.
# However multiple imports from the same lib are allowed on a line.
# Imports from Python standard libraries
import logging
import multiprocessing
import os
import sys
from datetime import datetime
from logging.config import dictConfig
from multiprocessing.pool import AsyncResult
from typing import Tuple

from dateutil.relativedelta import relativedelta
from pid.decorator import pidfile
from typing_extensions import Self

from scheduled_scripts.scrape_ttse.daily_summary_data import DailySummaryDataScraper
from scheduled_scripts.scrape_ttse.dividends import DividendScraper
from scheduled_scripts.scrape_ttse.listed_equities import ListedEquitiesScraper
from scheduled_scripts.scrape_ttse.newsroom_data import NewsroomDataScraper
from scheduled_scripts.scrape_ttse.technical_analysis_data import TechnicalAnalysisDataScraper
from scheduled_scripts.scraping_engine import ScrapingEngine
from scheduled_scripts import custom_logging, logging_configs

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# Imports from the cheese factory

# Imports from the local filesystem

dictConfig(logging_configs.LOGGING_CONFIG)
logger = logging.getLogger(__name__)
# endregion IMPORTS

# region CONSTANTS
# Put your constants here. These should be named in CAPS

# The timeout to set for multiprocessing tasks (in seconds)
# Define a start date to use for the full updates
TTSE_RECORDS_START_DATE = "2017-01-01"


# endregion CONSTANTS


# Put your class definitions here. These should use the CapWords convention.
class Scraper:
    def __init__(self: Self):
        self.scraping_engine = ScrapingEngine()
        pass

    def __del__(self: Self):
        pass


# region FUNCTION DEFINITIONS
# Put your function definitions here. These should be lowercase, separated by underscores.
def setup_dates_according_to_cli_arguments(cli_arguments: argparse.Namespace) -> Tuple[str, str]:
    if cli_arguments.days_from:
        start_date: str = (datetime.now() + relativedelta(days=-cli_arguments.days_from)).strftime("%Y-%m-%d")
    elif cli_arguments.full_history:
        start_date: str = TTSE_RECORDS_START_DATE
    else:
        # this should never happen, but assign a default start date
        start_date: str = (datetime.now() + relativedelta(days=-1)).strftime("%Y-%m-%d")
    end_date: str = datetime.now().strftime("%Y-%m-%d")
    return start_date, end_date


@pidfile()
def main(cli_arguments: argparse.Namespace) -> int:
    """The main steps in coordinating the scraping"""
    try:
        logger.info("Now starting TTSE scraper.")
        if cli_arguments.intradaily_data:
            daily_summary_data_scraper: DailySummaryDataScraper = DailySummaryDataScraper()
            result: int = daily_summary_data_scraper.update_daily_trade_data_for_today()
            return result
        # else this is a larger update
        start_date, end_date = setup_dates_according_to_cli_arguments(cli_arguments)
        if cli_arguments.news:
            newsroom_scraper: NewsroomDataScraper = NewsroomDataScraper()
            result: int = newsroom_scraper.scrape_newsroom_data(start_date, end_date)
            return result
        elif cli_arguments.listed_equities:
            listed_equities_scraper = ListedEquitiesScraper()
            listed_equities_scraper.scrape_listed_equity_data()
            listed_equities_scraper.update_num_equities_in_sectors()
            return 0
        elif cli_arguments.daily_summary_data:
            with multiprocessing.Pool(os.cpu_count()) as multipool:
                daily_summary_data_scraper: DailySummaryDataScraper = DailySummaryDataScraper()
                dates_to_fetch_sublists: list[
                    list[str]] = daily_summary_data_scraper.build_lists_of_missing_dates_for_each_subprocess(
                    start_date)
                # now call the individual workers to fetch these dates
                async_results: list[AsyncResult] = []
                for core_date_list in dates_to_fetch_sublists:
                    async_results.append(
                        multipool.apply_async(
                            daily_summary_data_scraper.scrape_equity_summary_data_in_subprocess, (core_date_list,)
                        )
                    )
                # wait until all workers finish fetching data before continuing
                for async_result in async_results:
                    logger.info(f"One process of scrape_equity_summary_data exited with code {async_result.get()}")
            return 0
        elif cli_arguments.dividends:
            dividend_scraper: DividendScraper = DividendScraper()
            result: int = dividend_scraper.scrape_dividend_data()
            return result
        elif cli_arguments.technical_analysis_data:
            technical_analysis_data_scraper: TechnicalAnalysisDataScraper = TechnicalAnalysisDataScraper()
            result: int = technical_analysis_data_scraper.update_technical_analysis_data()
            return result
    except Exception as exc:
        logger.exception(f"Error in script {os.path.basename(__file__)}", exc_info=exc)
        custom_logging.flush_smtp_logger()
        return -1


# endregion FUNCTION DEFINITIONS
def set_up_arguments():
    # first check the arguments given to this script
    parser = argparse.ArgumentParser()
    # check what time we need to parse data from
    days_from_group = parser.add_mutually_exclusive_group(required=True)
    days_from_group.add_argument(
        "-f",
        "--full_history",
        help=f"Record all data available on the TTSE (From {TTSE_RECORDS_START_DATE} to now)",
        action="store_true",
        default=False,
        required=False,
    )
    days_from_group.add_argument(
        "-d",
        "--days_from",
        help="Number of days to go back and scrape data from.",
        action="store",
        type=int,
    )
    type_of_data_to_scrape_group = parser.add_mutually_exclusive_group(required=True)
    type_of_data_to_scrape_group.add_argument(
        "-id",
        "--intradaily_data",
        help="Only update data that would have changed immediately during today's trading",
        action="store_true",
    )
    # add the different types of scrapers
    type_of_data_to_scrape_group.add_argument(
        "--listed_equities",
        help="Scrape the data about all equities listed",
        action="store_true",
    )
    type_of_data_to_scrape_group.add_argument(
        "--news",
        help="Scrape the newsroom data",
        action="store_true",
    )
    type_of_data_to_scrape_group.add_argument(
        "--daily_summary_data",
        help="Scrape the daily summary data for equities",
        action="store_true",
    )
    type_of_data_to_scrape_group.add_argument(
        "--dividends",
        help="Scrape the dividend data for stocks",
        action="store_true",
    )
    type_of_data_to_scrape_group.add_argument(
        "--technical_analysis_data",
        help="Scrape the data that is used for technical analyses",
        action="store_true",
    )
    cli_arguments = parser.parse_args()
    return cli_arguments


# If this script is being run from the command-line, then run the main() function
if __name__ == "__main__":
    args: argparse.Namespace = set_up_arguments()
    main(args)
