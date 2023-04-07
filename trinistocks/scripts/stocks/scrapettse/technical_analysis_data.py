import logging
import re
import time
from logging.config import dictConfig

import pandas as pd
import sqlalchemy
from bs4 import BeautifulSoup, Tag
from numpy import inf
from sqlalchemy import MetaData, Table, insert
from typing_extensions import Self

from scripts.stocks.scraping_engine import ScrapingEngine
from .. import logging_configs
from ..crosslisted_symbols import USD_STOCK_SYMBOLS
from ... import custom_logging
from ...database_ops import DatabaseConnect, _read_listed_symbols_from_db

dictConfig(logging_configs.LOGGING_CONFIG)
logger = logging.getLogger(__name__)


class TechnicalAnalysisDataScraper:
    def __init__(self: Self):
        self.scraping_engine = ScrapingEngine()
        pass

    def __del__(self: Self):
        pass


    def update_technical_analysis_data(self: Self) -> int:
        """
        Calculate/scrape the data needed for the technical_analysis_summary table
        """
        try:
            logger.debug(
                "Now using pandas to fetch latest technical analysis data from https://www.stockex.co.tt/manage-stock/"
            )
            # now go to the url for each symbol that we have listed, and collect the data we need
            # set up a list of dicts to hold our data
            all_technical_data = []
            for symbol in _read_listed_symbols_from_db():
                try:
                    dataframe_list = self._scrape_stock_summary_page_data(symbol)
                    self.calculate_technical_data_for_symbol(all_technical_data, dataframe_list, symbol)
                except (KeyError, IndexError) as err:
                    logger.warning(str(err))
            self._write_technical_analysis_data_to_db(all_technical_data)
            return 0
        except Exception as exc:
            logger.error("Could not complete technical analysis summary data update.", exc_info=exc)
            custom_logging.flush_smtp_logger()
            return -1

    def _write_technical_analysis_data_to_db(self, all_technical_data):
        # now insert the data into the db
        execute_completed_successfully = False
        execute_failed_times = 0
        logger.debug("Now trying to insert data into database.")
        with DatabaseConnect as db_connect:
            technical_analysis_summary_table = Table(
                "technical_analysis_summary",
                MetaData(),
                autoload=True,
                autoload_with=db_connect.dbengine,
            )
            while not execute_completed_successfully and execute_failed_times < 5:
                try:
                    technical_analysis_summary_insert_stmt = insert(technical_analysis_summary_table).values(
                        all_technical_data
                    )
                    technical_analysis_summary_upsert_stmt = (
                        technical_analysis_summary_insert_stmt.on_duplicate_key_update(
                            {x.name: x for x in technical_analysis_summary_insert_stmt.inserted}
                        )
                    )
                    result = db_connect.dbcon.execute(technical_analysis_summary_upsert_stmt)
                    execute_completed_successfully = True
                except sqlalchemy.exc.OperationalError as operr:
                    logger.warning(str(operr))
                    time.sleep(1)
                    execute_failed_times += 1
                logger.debug("Successfully scraped and wrote to db technical summary data.")
                logger.debug(
                    "Number of rows affected in the technical analysis summary table was " + str(result.rowcount)
                )

    def calculate_technical_data_for_symbol(self, all_technical_data, dataframe_list, symbol):
        # table 0 contains the data we need
        technical_analysis_table = dataframe_list[0]
        # create a dict to hold the data that we are interested in
        stock_technical_data = dict(symbol=symbol)
        # fill all the nan values with 0s
        technical_analysis_table.fillna(0, inplace=True)
        # get the values that we are interested in from the table
        stock_technical_data["last_close_price"] = float(technical_analysis_table["Closing Price"][0].replace("$", ""))
        stock_technical_data["high_52w"] = float(technical_analysis_table["Change"][4].replace("$", ""))
        # leaving out the 52w low because it is not correct from the ttse site
        # stock_technical_data['low_52w'] = float(
        #    technical_analysis_table['Change%'][4].replace('$', ''))
        stock_technical_data["wtd"] = float(technical_analysis_table["Opening Price"][6].replace("%", ""))
        stock_technical_data["mtd"] = float(technical_analysis_table["Closing Price"][6].replace("%", ""))
        stock_technical_data["ytd"] = float(technical_analysis_table["Change%"][6].replace("%", ""))
        # calculate our other required values
        # first calculate the SMAs
        # calculate sma20
        with DatabaseConnect() as db_connect:
            closing_quotes_last_20d_df = pd.io.sql.read_sql(
                f"SELECT close_price FROM daily_stock_summary WHERE symbol='{symbol}' order by date desc limit 20;",
                db_connect.dbengine,
            )
            sma20_df = closing_quotes_last_20d_df.rolling(window=20).mean()
            # get the last row value
            stock_technical_data["sma_20"] = sma20_df["close_price"].iloc[-1]
            # calculate sma200
            closing_quotes_last200d_df = pd.io.sql.read_sql(
                f"SELECT close_price FROM daily_stock_summary WHERE symbol='{symbol}' order by date desc limit 200;",
                db_connect.dbengine,
            )
            sma200_df = closing_quotes_last200d_df.rolling(window=200).mean()
            # get the last row value
            stock_technical_data["sma_200"] = sma200_df["close_price"].iloc[-1]
            # calculate beta
            # first get the closing prices and change dollars for this stock for the last year
            stock_change_df = pd.io.sql.read_sql(
                f"SELECT close_price,change_dollars FROM daily_stock_summary WHERE symbol='{symbol}' order by date desc limit 365;",
                db_connect.dbengine,
            )
            # using apply function to create a new column for the stock percent change
            stock_change_df["change_percent"] = (stock_change_df["change_dollars"] * 100) / stock_change_df[
                "close_price"
            ]
            # get the market percentage change
            market_change_df = pd.io.sql.read_sql(
                f"SELECT change_percent FROM historical_indices_info WHERE index_name='Composite Totals' order by date desc limit 365;",
                db_connect.dbengine,
            )
            # now calculate the beta
            stock_change_df["beta"] = (
                stock_change_df["change_percent"].rolling(window=365).cov(other=market_change_df["change_percent"])
            ) / market_change_df["change_percent"].rolling(window=365).var()
            # store the beta
            stock_technical_data["beta"] = stock_change_df["beta"].iloc[-1]
            # now calculate the adtv
            volume_traded_df = pd.io.sql.read_sql(
                f"SELECT volume_traded FROM daily_stock_summary WHERE symbol='{symbol}' order by date desc limit 30;",
                db_connect.dbengine,
            )
            adtv_df = volume_traded_df.rolling(window=30).mean()
            stock_technical_data["adtv"] = adtv_df["volume_traded"].iloc[-1]
            # calculate the 52w low
            stock_technical_data["low_52w"] = stock_change_df["close_price"].min()
            # replace nan/na/inf in dict with None
            for key in stock_technical_data:
                if pd.isna(stock_technical_data[key]):
                    stock_technical_data[key] = None
                if stock_technical_data[key] in [inf, -inf]:
                    stock_technical_data[key] = None
            # add our dict for this stock to our large list
            all_technical_data.append(stock_technical_data)

    def _scrape_stock_summary_page_data(self, symbol):
        stock_summary_page_url = f"https://www.stockex.co.tt/manage-stock/{symbol}"
        logger.debug(f"Navigating to {stock_summary_page_url} to fetch technical summary data.")
        stock_summary_page_data = self.scraping_engine.get_url_and_return_html(url=stock_summary_page_url)
        # get a list of tables from the URL
        dataframe_list = pd.read_html(stock_summary_page_data)
        return dataframe_list