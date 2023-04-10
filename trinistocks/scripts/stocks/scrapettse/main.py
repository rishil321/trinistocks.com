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

from dateutil.relativedelta import relativedelta
from pid.decorator import pidfile
from typing_extensions import Self

from scripts.stocks.scrapettse.daily_summary_data import DailySummaryDataScraper
from scripts.stocks.scrapettse.dividends import DividendScraper
from scripts.stocks.scrapettse.listed_equities import ListedEquitiesScraper
from scripts.stocks.scrapettse.newsroom_data import NewsroomDataScraper
from scripts.stocks.scrapettse.technical_analysis_data import TechnicalAnalysisDataScraper
from scripts.stocks.scraping_engine import ScrapingEngine
from ... import custom_logging

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# Imports from the cheese factory

# Imports from the local filesystem
from .. import logging_configs

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


@pidfile()
def main(args):
    """The main steps in coordinating the scraping"""
    try:
        # run all functions within a multiprocessing pool
        with multiprocessing.Pool(os.cpu_count()) as multipool:
            logger.debug("Now starting TTSE scraper.")
            newsroom_data_scraper = NewsroomDataScraper()
            # check if this is the intradaily update (run inside the trading day)
            if args.intradaily_update:
                logger.debug("Intradaily scraper called.")
                daily_summary_data_scraper = DailySummaryDataScraper()
                daily_trade_update_result = multipool.apply_async(
                    daily_summary_data_scraper.update_daily_trade_data_for_today, ()
                )
                start_date = (datetime.now() + relativedelta(days=-1)).strftime("%Y-%m-%d")
                end_date = datetime.now().strftime("%Y-%m-%d")
                scrape_all_newsroom_data_result = multipool.apply_async(
                    newsroom_data_scraper.scrape_newsroom_data, (start_date, end_date)
                )
                logger.debug(f"update_daily_trades exited with code {daily_trade_update_result.get()}")
                logger.debug(f"scrape_all_newsroom_data exited with code {scrape_all_newsroom_data_result.get()}")
            else:
                if args.end_of_day_update:
                    logger.debug("End of day scraper called.")
                    start_date = (datetime.now() + relativedelta(days=-1)).strftime("%Y-%m-%d")
                elif args.full_history:
                    logger.debug("Full history scraper called.")
                    start_date = TTSE_RECORDS_START_DATE
                elif args.catchup:
                    logger.debug("Catchup scraper called.")
                    start_date = (datetime.now() + relativedelta(months=-1)).strftime("%Y-%m-%d")
                    end_date = datetime.now().strftime("%Y-%m-%d")
                    scrape_all_newsroom_data_result = multipool.apply_async(
                        newsroom_data_scraper.scrape_newsroom_data, (start_date, end_date)
                    )
                    logger.debug(f"scrape_all_newsroom_data exited with code {scrape_all_newsroom_data_result.get()}")
                listed_equities_scraper = ListedEquitiesScraper()
                scrape_listed_equity_data_result = multipool.apply_async(
                    listed_equities_scraper.scrape_listed_equity_data, ()
                )
                check_num_equities_in_sector_result = multipool.apply_async(
                    listed_equities_scraper.update_num_equities_in_sectors, ()
                )
                dividend_scraper = DividendScraper()
                scrape_dividend_data_result = multipool.apply_async(dividend_scraper.scrape_dividend_data, ())
                # block on the next function to wait until the dates are ready
                daily_summary_data_scraper = DailySummaryDataScraper()
                dates_to_fetch_sublists = multipool.apply(
                    daily_summary_data_scraper.update_equity_summary_data, (start_date,)
                )
                logger.debug(f"scrape_listed_equity_data exited with code {scrape_listed_equity_data_result.get()}")
                logger.debug(
                    f"check_num_equities_in_sector exited with code {check_num_equities_in_sector_result.get()}"
                )
                logger.debug(f"scrape_dividend_data exited with code {scrape_dividend_data_result.get()}")
                # now call the individual workers to fetch these dates
                async_results = []
                for core_date_list in dates_to_fetch_sublists:
                    async_results.append(
                        multipool.apply_async(
                            daily_summary_data_scraper.scrape_equity_summary_data_in_subprocess, (core_date_list,)
                        )
                    )
                # wait until all workers finish fetching data before continuing
                for result in async_results:
                    logger.debug(f"One process of scrape_equity_summary_data exited with code {result.get()}")
                # update the technical analysis stock data
                technical_analysis_data_scraper = TechnicalAnalysisDataScraper()
                update_technical_analysis_result = multipool.apply_async(
                    technical_analysis_data_scraper.update_technical_analysis_data, ()
                )
                logger.debug(
                    f"update_technical_analysis_data exited with code {update_technical_analysis_result.get()}"
                )
            multipool.close()
            multipool.join()
            logger.debug(os.path.basename(__file__) + " was completed.")
            # q_listener.stop()
            return 0
    except Exception as exc:
        logger.exception(f"Error in script {os.path.basename(__file__)}", exc_info=exc)
        custom_logging.flush_smtp_logger()


# endregion FUNCTION DEFINITIONS
def set_up_arguments():
    # first check the arguments given to this script
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-f",
        "--full_history",
        help="Record all data from 2010 to now",
        action="store_true",
    )
    parser.add_argument(
        "-id",
        "--intradaily_update",
        help="Only update data that would have changed immediately during today's trading",
        action="store_true",
    )
    parser.add_argument(
        "-eod",
        "--end_of_day_update",
        help="Update all data that would have changed as a result of today's trading",
        action="store_true",
    )
    parser.add_argument(
        "-c",
        "--catchup",
        help="If we missed any data, run this to scrape missed days (up to 1 month ago)",
        action="store_true",
    )
    args = parser.parse_args()
    return args


# If this script is being run from the command-line, then run the main() function
if __name__ == "__main__":
    args = set_up_arguments()
    main(args)
