import logging
import re
import time
from datetime import datetime
from logging.config import dictConfig

import numpy as np
import pandas as pd
import sqlalchemy
from bs4 import BeautifulSoup, Tag
from sqlalchemy import MetaData, Table, insert
from typing_extensions import Self

from scripts.stocks.scraping_engine import ScrapingEngine
from .. import logging_configs
from ..crosslisted_symbols import USD_STOCK_SYMBOLS
from ... import custom_logging
from ...database_ops import DatabaseConnect, _read_listed_symbols_from_db

dictConfig(logging_configs.LOGGING_CONFIG)
logger = logging.getLogger(__name__)


class DividendScraper:
    def __init__(self: Self):
        self.scraping_engine = ScrapingEngine()
        pass

    def __del__(self: Self):
        pass

    def scrape_dividend_data(self: Self):
        """Use the requests and pandas libs to browse through
        https://www.stockex.co.tt/manage-stock/<symbol> for each listed security
        """
        try:
            logger.debug("Now trying to scrape dividend data")
            all_listed_symbols, result = _read_listed_symbols_from_db()
            # now get get the tables listing dividend data for each symbol
            for symbol in all_listed_symbols:
                logger.debug(f"Now attempting to fetch dividend data for {symbol}")
                try:
                    dividend_table = self._scrape_dividend_data_for_symbol(symbol)
                    self._write_dividend_data_for_symbol_to_db(dividend_table, result, symbol)
                except Exception as e:
                    logger.error(f"Unable to scrape dividend data for {symbol}", exc_info=e)
            return 0
        except Exception:
            logger.exception("Error encountered while scraping dividend data.")
        finally:
            custom_logging.flush_smtp_logger()

    def _write_dividend_data_for_symbol_to_db(self, dividend_table, result, symbol):
        # now write the dataframe to the db
        with DatabaseConnect() as db_connection:
            logger.debug(f"Now writing dividend data for {symbol} to db.")
            historical_dividend_info_table = Table(
                "historical_dividend_info",
                MetaData(),
                autoload=True,
                autoload_with=db_connection.dbengine,
            )
            # if we had any errors, the values will be written as their defaults (0 or null)
            # write the data to the db
            execute_completed_successfully = False
            execute_failed_times = 0
            while not execute_completed_successfully and execute_failed_times < 5:
                try:
                    insert_stmt = insert(historical_dividend_info_table).values(dividend_table.to_dict("records"))
                    upsert_stmt = insert_stmt.on_duplicate_key_update({x.name: x for x in insert_stmt.inserted})
                    result = db_connection.dbcon.execute(upsert_stmt)
                    execute_completed_successfully = True
                except sqlalchemy.exc.OperationalError as operr:
                    logger.warning(str(operr))
                    time.sleep(2)
                    execute_failed_times += 1
            logger.debug("Number of rows affected in the historical_dividend_info table was " + str(result.rowcount))

    def _scrape_dividend_data_for_symbol(self, symbol) -> pd.DataFrame:
        # Construct the full URL using the symbol
        equity_dividend_url = f"https://www.stockex.co.tt/manage-stock/{symbol}"
        logger.debug("Navigating to " + equity_dividend_url)
        equity_dividend_page = self.scraping_engine.get_url_and_return_html(url=equity_dividend_url)
        # get the dataframes from the page
        dataframe_list = pd.read_html(equity_dividend_page)
        dividend_table = dataframe_list[1]
        # check if dividend data is present
        if len(dividend_table.index) > 1:
            logger.debug(f"Dividend data present for {symbol}")
            # remove the columns we don't need
            dividend_table.drop(["Payment Type", "Ex-Dividend Date", "Payment Date"], axis=1, inplace=True)
            # set the column names
            dividend_table.rename(
                {
                    "Record Date": "record_date",
                    "Dividend Amount": "dividend_amount",
                    "Currency": "currency",
                },
                axis=1,
                inplace=True,
            )
            # get the record date into the appropriate format
            dividend_table["record_date"] = dividend_table["record_date"].map(
                lambda x: datetime.strptime(x, "%d %b %Y"),
                na_action="ignore",
            )
            # get the dividend amount into the appropriate format
            if symbol == "GMLP":
                # compute the actual dividend value per share, based on the percentage and par value ($50)
                dividend_table["dividend_amount"] = (50 / 100) * pd.to_numeric(
                    dividend_table["dividend_amount"].str.replace("%", ""),
                    errors="coerce",
                )
            else:
                dividend_table["dividend_amount"] = pd.to_numeric(
                    dividend_table["dividend_amount"].str.replace("$", ""),
                    errors="coerce",
                )
            # add a series for the symbol
            dividend_table["symbol"] = pd.Series(symbol, index=dividend_table.index)
            logger.debug("Successfully fetched dividend data for " + symbol)
            # check if currency is missing from column
            if dividend_table["currency"].isnull().values.any():
                logger.warning(
                    f"Currency seems to be missing from the dividend table for {symbol}. We will autofill with TTD, but this may be incorrect."
                )
                dividend_table["currency"].fillna("TTD", inplace=True)
            # dividend table replace nan with None
            dividend_table = dividend_table.replace({np.nan: None})
            return dividend_table
        else:
            logger.debug(f"No dividend data found for {symbol}. Skipping.")
            return pd.DataFrame()