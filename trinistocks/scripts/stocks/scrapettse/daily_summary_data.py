import logging
import multiprocessing
import os
import re
import time
from datetime import datetime, timedelta
from logging.config import dictConfig
from typing import List, Tuple

import numpy as np
import pandas as pd
import sqlalchemy
from bs4 import BeautifulSoup, Tag
from sqlalchemy import MetaData, Table, insert, select
from typing_extensions import Self

from scripts.stocks.scraping_engine import ScrapingEngine
from .. import logging_configs
from ..crosslisted_symbols import USD_STOCK_SYMBOLS
from ... import custom_logging
from ...database_ops import DatabaseConnect, _read_listed_symbols_from_db

dictConfig(logging_configs.LOGGING_CONFIG)
logger = logging.getLogger(__name__)


class DailySummaryDataScraper:
    def __init__(self: Self):
        self.scraping_engine = ScrapingEngine()
        pass

    def __del__(self: Self):
        pass



    def scrape_equity_summary_data_in_subprocess(
        self,
        dates_to_fetch,
    ):
        """
        In a new process, use the requests, beautifulsoup and pandas libs to scrape data from
        https://www.stockex.co.tt/market-quote/
        for the list of dates passed to this function.
        Gather the data into a dict, and write that dict to the DB
        """
        # declare a string to identify this PID
        pid_string = " in PID: " + str(os.getpid())
        try:
            daily_stock_data_keys, market_summary_data_keys = self._setup_field_names_for_daily_summary_data_tables()
            # now fetch the data at each url(each market trading date)
            for index, fetch_date in enumerate(dates_to_fetch):
                try:
                    equity_summary_page = self._scrape_equity_summary_data_for_date(
                        dates_to_fetch, fetch_date, index, pid_string
                    )
                    # get a list of tables from the URL
                    dataframe_list: List[pd.DataFrame] = pd.read_html(equity_summary_page)
                    # if this is a valid trading day, extract the values we need from the tables
                    if not len(dataframe_list[00].index) > 4:
                        logger.warning(f"This date is not a valid trading date: {fetch_date} {pid_string}")
                        continue
                    # else
                    logger.debug("This is a valid trading day.")
                    # get a date object suitable for the db
                    fetch_date_db: datetime = datetime.strptime(fetch_date, "%Y-%m-%d")
                    (
                        market_indices_table,
                        ordinary_shares_table,
                        preference_shares_table,
                        second_tier_shares_table,
                        sme_shares_table,
                        mutual_funds_shares_table,
                        usd_equity_shares_table,
                    ) = self._parse_data_from_equity_summary_tables(
                        dataframe_list, market_summary_data_keys, fetch_date_db
                    )
                    all_daily_stock_data: List = self._parse_all_daily_stock_data(
                        daily_stock_data_keys,
                        fetch_date,
                        fetch_date_db,
                        mutual_funds_shares_table,
                        ordinary_shares_table,
                        preference_shares_table,
                        second_tier_shares_table,
                        sme_shares_table,
                        usd_equity_shares_table,
                    )
                    self._write_daily_stock_data_to_db(
                        all_daily_stock_data, fetch_date, market_indices_table, pid_string
                    )
                except KeyError as key_error:
                    logger.warning(f"Could not find a required key on date {fetch_date} {pid_string};{key_error}")
                except IndexError as index_error:
                    logger.warning(f"Could not locate index in a list. {fetch_date} {pid_string};{index_error}")
            return 0
        except Exception:
            logger.exception(
                f"Could not complete historical_indices_summary and daily_stock_summary update. {pid_string}"
            )
            custom_logging.flush_smtp_logger()

    def _write_daily_stock_data_to_db(self, all_daily_stock_data, fetch_date, market_indices_table, pid_string):
        # now insert the data into the db
        with DatabaseConnect() as db_connect:
            logger.debug("Successfully connected to database" + pid_string)
            # Reflect the tables already created in our db
            historical_indices_info_table = Table(
                "historical_indices_info",
                MetaData(),
                autoload=True,
                autoload_with=db_connect.dbengine,
            )
            daily_stock_summary_table = Table(
                "daily_stock_summary",
                MetaData(),
                autoload=True,
                autoload_with=db_connect.dbengine,
            )
            execute_completed_successfully = False
            execute_failed_times = 0
            while not execute_completed_successfully and execute_failed_times < 5:
                try:
                    insert_stmt = insert(historical_indices_info_table).values(market_indices_table.to_dict("records"))
                    upsert_stmt = insert_stmt.on_duplicate_key_update({x.name: x for x in insert_stmt.inserted})
                    result = db_connect.dbcon.execute(upsert_stmt)
                    execute_completed_successfully = True
                    logger.debug(
                        "Successfully scraped and wrote to db market indices data for " + fetch_date + pid_string
                    )
                    logger.debug(
                        "Number of rows affected in the historical_indices_summary table was "
                        + str(result.rowcount)
                        + pid_string
                    )
                    insert_stmt = insert(daily_stock_summary_table).values(all_daily_stock_data)
                    upsert_stmt = insert_stmt.on_duplicate_key_update({x.name: x for x in insert_stmt.inserted})
                    result = db_connect.dbcon.execute(upsert_stmt)
                    execute_completed_successfully = True
                    logger.debug(
                        "Successfully scraped and wrote to db daily equity/shares data for " + fetch_date + pid_string
                    )
                    logger.debug(
                        "Number of rows affected in the daily_stock_summary table was "
                        + str(result.rowcount)
                        + pid_string
                    )
                except sqlalchemy.exc.OperationalError as operr:
                    logger.warning(str(operr))
                    time.sleep(2)
                    execute_failed_times += 1
        return db_connect

    def _parse_all_daily_stock_data(
        self,
        daily_stock_data_keys,
        fetch_date,
        fetch_date_db,
        mutual_funds_shares_table,
        ordinary_shares_table,
        preference_shares_table,
        second_tier_shares_table,
        sme_shares_table,
        usd_equity_shares_table,
    ):
        # now lets try to wrangle the daily data for stocks
        all_daily_stock_data = []
        for shares_table in [
            ordinary_shares_table,
            preference_shares_table,
            second_tier_shares_table,
            mutual_funds_shares_table,
            sme_shares_table,
            usd_equity_shares_table,
        ]:
            if not shares_table.empty:
                # remove the first row from each table since it's a header row
                shares_table.drop(shares_table.index[0])
                # remove the column with the up and down symbols
                shares_table.drop(shares_table.columns[0], axis=1, inplace=True)
                # set the names of columns
                shares_table.columns = daily_stock_data_keys
                # remove the unneeded characters from the symbols
                # note that these characters come after a space
                shares_table["symbol"] = shares_table["symbol"].str.split(" ", 1).str.get(0)
                # replace the last sale date with a boolean
                # if the last sale date is the current date being queried, return 1, else return 0
                shares_table["was_traded_today"] = shares_table["was_traded_today"].map(
                    lambda x: 1
                    if (datetime.strptime(x, "%d-%m-%Y") == datetime.strptime(fetch_date, "%Y-%m-%d"))
                    else 0,
                    na_action="ignore",
                )
                # set the datatype of the columns
                shares_table["open_price"] = pd.to_numeric(shares_table["open_price"], errors="coerce")
                shares_table["high"] = pd.to_numeric(shares_table["high"], errors="coerce")
                shares_table["low"] = pd.to_numeric(shares_table["low"], errors="coerce")
                shares_table["os_bid"] = pd.to_numeric(shares_table["os_bid"], errors="coerce")
                shares_table["os_bid_vol"] = pd.to_numeric(shares_table["os_bid_vol"], errors="coerce")
                shares_table["os_offer"] = pd.to_numeric(shares_table["os_offer"], errors="coerce")
                shares_table["os_offer_vol"] = pd.to_numeric(shares_table["os_offer_vol"], errors="coerce")
                shares_table["last_sale_price"] = pd.to_numeric(shares_table["last_sale_price"], errors="coerce")
                shares_table["volume_traded"] = pd.to_numeric(shares_table["volume_traded"], errors="coerce")
                shares_table["close_price"] = pd.to_numeric(shares_table["close_price"], errors="coerce")
                shares_table["change_dollars"] = pd.to_numeric(shares_table["change_dollars"], errors="coerce")
                # if the high and low columns are 0, replace them with the open price
                shares_table["high"] = shares_table.apply(
                    lambda x: x.open_price if (pd.isna(x.high)) else x.high,
                    axis=1,
                )
                shares_table["low"] = shares_table.apply(
                    lambda x: x.open_price if (pd.isna(x.low)) else x.low,
                    axis=1,
                )
                # replace certain column null values with 0
                shares_table["change_dollars"].fillna(0, inplace=True)
                shares_table["volume_traded"].fillna(0, inplace=True)
                shares_table["os_bid_vol"].fillna(0, inplace=True)
                shares_table["os_offer_vol"].fillna(0, inplace=True)
                # create a series for the value traded
                value_traded_series = pd.Series(0, index=shares_table.index).astype(float)
                # set the name of the series
                value_traded_series.rename("value_traded")
                # add the series to the dateframe
                shares_table = shares_table.assign(value_traded=value_traded_series)
                # calculate the value traded for today
                shares_table["value_traded"] = shares_table.apply(lambda x: x.volume_traded * x.last_sale_price, axis=1)
                # create a series containing the date
                date_series = pd.Series(fetch_date_db, index=shares_table.index)
                # set the name of the series
                date_series.rename("date")
                # add the series to the dateframe
                shares_table = shares_table.assign(date=date_series)
                # replace the nan with None
                shares_table = shares_table.replace({np.nan: None})
                # add all values to the large list
                all_daily_stock_data += shares_table.to_dict("records")
        return all_daily_stock_data

    def _parse_data_from_equity_summary_tables(
        self: Self, dataframe_list: List[pd.DataFrame], market_summary_data_keys: List[str], fetch_date_db: datetime
    ) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame,]:
        # get the tables holding useful data
        market_indices_table = dataframe_list[0]
        ordinary_shares_table = dataframe_list[1]
        preference_shares_table = dataframe_list[2]
        second_tier_shares_table = dataframe_list[3]
        sme_shares_table = dataframe_list[4]
        mutual_funds_shares_table = dataframe_list[5]
        usd_equity_shares_table = dataframe_list[6]
        # extract the values required from the tables
        # first extract the data from the market indices table
        # remove the column with the up and down symbols
        market_indices_table.drop(market_indices_table.columns[0], axis=1, inplace=True)
        # set the names of columns
        market_indices_table.columns = market_summary_data_keys
        # remove all the '-' in the dataframe
        market_indices_table.replace("–", None, inplace=True)
        # set the datatype of the columns
        market_indices_table["index_name"] = market_indices_table["index_name"].astype(str).str.title()
        market_indices_table["index_value"] = pd.to_numeric(market_indices_table["index_value"], errors="coerce")
        market_indices_table["index_change"] = pd.to_numeric(market_indices_table["index_change"], errors="coerce")
        market_indices_table["change_percent"] = pd.to_numeric(market_indices_table["change_percent"], errors="coerce")
        market_indices_table["volume_traded"] = pd.to_numeric(market_indices_table["volume_traded"], errors="coerce")
        market_indices_table["value_traded"] = pd.to_numeric(market_indices_table["value_traded"], errors="coerce")
        market_indices_table["num_trades"] = pd.to_numeric(market_indices_table["num_trades"], errors="coerce")
        # add a series containing the date
        market_indices_table["date"] = pd.Series(fetch_date_db, index=market_indices_table.index)
        market_indices_table.fillna(0, inplace=True)
        return (
            market_indices_table,
            ordinary_shares_table,
            preference_shares_table,
            second_tier_shares_table,
            sme_shares_table,
            mutual_funds_shares_table,
            usd_equity_shares_table,
        )

    def _scrape_equity_summary_data_for_date(self, dates_to_fetch, fetch_date, index, pid_string):
        logger.debug(f"Now loading page {str(index)} of {str(len(dates_to_fetch))} {pid_string}")
        # for each date, we need to navigate to this summary page for that day
        url_summary_page = f"https://www.stockex.co.tt/market-quote/?TradeDate={fetch_date}"
        logger.debug(f"Navigating to {url_summary_page} {pid_string}")
        equity_summary_page = self.scraping_engine.get_url_and_return_html(url=url_summary_page)
        return equity_summary_page

    def _setup_field_names_for_daily_summary_data_tables(self):
        # set up the field names for the tables ( we will set the date column after)
        market_summary_data_keys = [
            "index_name",
            "index_value",
            "index_change",
            "change_percent",
            "volume_traded",
            "value_traded",
            "num_trades",
        ]
        daily_stock_data_keys = [
            "symbol",
            "open_price",
            "high",
            "low",
            "os_bid",
            "os_bid_vol",
            "os_offer",
            "os_offer_vol",
            "last_sale_price",
            "was_traded_today",
            "volume_traded",
            "close_price",
            "change_dollars",
        ]
        return daily_stock_data_keys, market_summary_data_keys

    def update_equity_summary_data(self: Self, start_date: str):
        """
        Create the list of dates that we need to scrape data from https://www.stockex.co.tt/market-quote/
        for, based on the start_date specified and the dates already in the historical_indices_info table
        """
        logger.debug("Now updating daily market summary data.")
        try:
            dates_already_recorded: List = self._read_dates_already_recorded_from_db()
            dates_to_fetch_sublists = self._build_list_of_dates_to_scrape_for_each_subprocess(
                dates_already_recorded, start_date
            )
            return dates_to_fetch_sublists
        except Exception as ex:
            logger.exception(
                f"We ran into a problem while trying to build the list of dates to scrape market summary data for. "
                f"Here's what we know: {str(ex)}"
            )
            custom_logging.flush_smtp_logger()

    def _build_list_of_dates_to_scrape_for_each_subprocess(self, dates_already_recorded, start_date):
        # We want to gather data on all trading days since the start date, so we create a list
        # of all dates that we need to gather still
        dates_to_fetch = []
        fetch_date = datetime.strptime(start_date, "%Y-%m-%d")
        logger.debug("Getting all dates that are not already fetched and are not weekends.")
        # TODO: Extend holidays library for Trinidad and Tobago
        # Get all dates until yesterday
        while fetch_date < datetime.now():
            # if we do not have info on this date already and this is a weekday (stock markets close on weekends)
            if (fetch_date.date() not in dates_already_recorded) and (fetch_date.weekday() < 5):
                # add this date to be fetched
                dates_to_fetch.append(fetch_date.strftime("%Y-%m-%d"))
            # increment the date by one day
            fetch_date += timedelta(days=1)
        # now split our dates_to_fetch list into sublists to multithread
        logger.debug("List of dates to fetch built. Now splitting list by core.")
        num_cores = multiprocessing.cpu_count()
        logger.debug("This machine has " + str(num_cores) + " logical CPU cores.")
        list_length = len(dates_to_fetch)
        dates_to_fetch_sublists = [
            dates_to_fetch[i * list_length // num_cores : (i + 1) * list_length // num_cores] for i in range(num_cores)
        ]
        logger.debug("Lists split successfully.")
        return dates_to_fetch_sublists

    def _read_dates_already_recorded_from_db(self):
        with DatabaseConnect() as db_connect:
            # Reflect the tables already created in our db
            logger.debug("Reading existing data from tables in database...")
            historical_indices_info_table = Table(
                "historical_indices_info",
                MetaData(),
                autoload=True,
                autoload_with=db_connect.dbengine,
            )
            # Now get the dates that we already have recorded (from the historical indices table)
            logger.debug("Creating list of dates to fetch.")
            dates_already_recorded = []
            select_stmt = select([historical_indices_info_table.c.date])
            result = db_connect.dbcon.execute(select_stmt)
            for row in result:
                # We only have a single element in each row tuple, which is the date
                dates_already_recorded.append(row[0])
        return dates_already_recorded

    def update_daily_trade_data_for_today(self: Self) -> int:
        """
        Open the Chrome browser and browse through
        https://stockex.co.tt/controller.php?action=view_quote which shows trading for the last day
        Gather the data into a dict, and write that dict to the DB
        :returns: 0 if successful
        :raises Exception if any issues are encountered
        """
        try:
            today_date = datetime.now().strftime("%Y-%m-%d")
            logger.debug(f"Now using pandas to fetch daily shares data for today ({today_date})")
            daily_stock_data_keys, market_summary_data_keys = self._setup_field_names_for_daily_summary_data_tables()
            daily_trade_data_for_today = self._scrape_daily_trade_data_for_today(today_date)
            # set up a list to store the data to be written to db
            all_daily_stock_data = []
            # get a list of tables from the URL
            dataframe_list = pd.read_html(daily_trade_data_for_today)
            # if this is a valid trading day, and the summary data for today has been published,
            # extract the values we need from the tables
            if len(dataframe_list[00].index) == 8:  # 8
                all_daily_stock_data = self._parse_daily_trading_data_for_today(
                    all_daily_stock_data, daily_stock_data_keys, dataframe_list, today_date
                )
            else:
                # if no summary data has been published yet for today, try to use the marquee on the main page to source data
                logger.debug("No summary data found for today (yet?). Trying to get marquee data from main page.")
                listed_symbols = _read_listed_symbols_from_db()
                main_page = self._scrape_data_from_main_page()
                all_daily_stock_data = self._parse_stock_data_from_main_page(listed_symbols, main_page)
            if not all_daily_stock_data:
                logger.debug("No data found for today. Nothing to write in db.")
                return 0
            # else if we have any data to insert, then push the data into the db
            self._write_daily_stock_data_for_today_to_db(all_daily_stock_data)
            return 0
        except Exception:
            logger.exception("Could not load daily data for today!")
            custom_logging.flush_smtp_logger()

    def _write_daily_stock_data_for_today_to_db(self, all_daily_stock_data):
        with DatabaseConnect as db_connect:
            # load the daily summary table
            daily_stock_summary_table = Table(
                "daily_stock_summary",
                MetaData(),
                autoload=True,
                autoload_with=db_connect.dbengine,
            )
            execute_completed_successfully = False
            execute_failed_times = 0
            while not execute_completed_successfully and execute_failed_times < 5:
                try:
                    insert_stmt = insert(daily_stock_summary_table).values(all_daily_stock_data)
                    upsert_stmt = insert_stmt.on_duplicate_key_update({x.name: x for x in insert_stmt.inserted})
                    result = db_connect.dbcon.execute(upsert_stmt)
                    execute_completed_successfully = True
                    logger.debug("Successfully scraped and wrote to db daily equity/shares data for daily trades.")
                    logger.debug("Number of rows affected in the daily_stock_summary table was " + str(result.rowcount))
                except sqlalchemy.exc.OperationalError as operr:
                    logger.warning(str(operr))
                    time.sleep(2)
                    execute_failed_times += 1

    def _parse_stock_data_from_main_page(self, listed_symbols, main_page) -> List:
        all_daily_stock_data = []
        # parse the text if we were able to load the marquee data
        page_soup = BeautifulSoup(main_page, "lxml")
        # find the elementor-text-editor blocks on the page, which can help us figure out if the market is open
        elementor_text_editor_blocks: List[BeautifulSoup] = page_soup.findAll(
            class_="elementor-text-editor elementor-clearfix"
        )
        now_datetime: datetime = datetime.strptime(elementor_text_editor_blocks[1].text.strip(), '%d %b %Y %H:%M %p')
        market_status: str = elementor_text_editor_blocks[0].text.strip().lower()
        if not market_status == 'open':
            raise RuntimeError("Market is not open right now.")
        marquee = page_soup.find("marquee", id=["tickerTape"])
        if not marquee:
            logger.warning(f"Could not find marquee on today's page.")
        else:
            logger.debug("Found marquee for today. Now parsing data.")
            try:
                # use string operations to try to make sense of the marquee
                marquee_text = marquee.text
                # marquee_text = " Trade Data for 26 Apr 2021 @ 10:06 AM:  AGL  Vol 272  $24.25 (-0.15)  |  CIF  Vol 1,000  $25.05 (0.04)  |  FCI  Vol 875  $6.74 (0.00)  |  FIRST  Vol 3,500  $46.50 (0.10)  |  GHL  Vol 9  $25.61 (0.01)  |  GML  Vol 500  $3.01 (0.00)  |  MASSY  Vol 2,650  $64.00 (0.00)  |  NEL  Vol 1,000  $2.99 (0.00)  |  NGL  Vol 9,700  $13.50 (-0.36)  |  RFHL  Vol 3,093  $132.36 (-0.01)  |  SBTT  Vol 3,657  $54.65 (0.00)  |  WCO  Vol 1,800  $32.98 (0.02)  | "
                marquee_text_date = marquee_text.split(": ")[0]
                marquee_text_symbol_data = marquee_text.split(": ")[1]
                # check if the marquee date is today
                marquee_text_date = marquee_text_date.split("for ")[1].split(" @")[0]
                if datetime.strptime(marquee_text_date, "%d %b %Y").date() == datetime.today().date():
                    # if the marquee is showing data for today, then try to parse and store it
                    logger.info("Marquee is for today. Continuing.")
                    per_symbol_data = marquee_text_symbol_data.split(" | ")
                    for symbol_data in per_symbol_data:
                        # try to store symbol data for each symbol in marquee
                        stock_data = {}
                        symbol_data_chunks = symbol_data.split(" ")
                        if len(symbol_data_chunks) == 9:
                            stock_data["symbol"] = symbol_data_chunks[1]
                            if stock_data["symbol"] in listed_symbols:
                                stock_data["date"] = datetime.today()
                                stock_data["volume_traded"] = int(symbol_data_chunks[4].replace(",", ""))
                                stock_data["last_sale_price"] = float(symbol_data_chunks[6].replace("$", ""))
                                stock_data["open_price"] = stock_data["last_sale_price"]
                                stock_data["close_price"] = stock_data["last_sale_price"]
                                stock_data["high"] = stock_data["last_sale_price"]
                                stock_data["low"] = stock_data["last_sale_price"]
                                stock_data["change_dollars"] = float(
                                    (symbol_data_chunks[7].replace("(", "").replace(")", ""))
                                )
                                stock_data["was_traded_today"] = 1
                                stock_data["value_traded"] = stock_data["volume_traded"] * stock_data["last_sale_price"]
                                logger.debug(f"Marquee data looks good for {stock_data['symbol']}. Adding to db list.")
                                # add dict data to list to be written to db
                                # only if the vol is >0
                                if stock_data["volume_traded"] > 0:
                                    all_daily_stock_data.append(stock_data)
                    return all_daily_stock_data
                else:
                    logger.warning("Marquee is for another date. Ignoring.")
                    return []
            except Exception as exc:
                logger.error(
                    "Problem while parsing data for marquee. Data possibly in invalid format?",
                    exc_info=exc,
                )
                return []

    def _scrape_data_from_main_page(self):
        url_main_page = f"https://www.stockex.co.tt/"
        logger.debug("Navigating to " + url_main_page)
        main_page = self.scraping_engine.get_url_and_return_html(url=url_main_page)
        return main_page

    def _parse_daily_trading_data_for_today(
        self, all_daily_stock_data, daily_stock_data_keys, dataframe_list, today_date
    ):
        # get the tables holding useful data
        market_indices_table = dataframe_list[0]
        ordinary_shares_table = dataframe_list[1]
        preference_shares_table = dataframe_list[2]
        second_tier_shares_table = dataframe_list[3]
        sme_shares_table = dataframe_list[4]
        mutual_funds_shares_table = dataframe_list[5]
        usd_equity_shares_table = dataframe_list[6]
        # extract the values required from the tables
        # lets try to wrangle the daily data for stocks
        for shares_table in [
            ordinary_shares_table,
            preference_shares_table,
            second_tier_shares_table,
            mutual_funds_shares_table,
            sme_shares_table,
            usd_equity_shares_table,
        ]:
            # remove the first row from each table since its a header row
            shares_table.drop(shares_table.index[0])
            if not shares_table.empty:
                # remove the column with the up and down symbols
                shares_table.drop(shares_table.columns[0], axis=1, inplace=True)
                # set the names of columns
                shares_table.columns = daily_stock_data_keys
                # remove the unneeded characters from the symbols
                # note that these characters come after a space
                shares_table["symbol"] = shares_table["symbol"].str.split(" ", 1).str.get(0)
                # replace the last sale date with a boolean
                # if the last sale date is the current date being queried, return 1, else return 0
                shares_table["was_traded_today"] = shares_table["was_traded_today"].map(
                    lambda x: 1
                    if (datetime.strptime(x, "%d-%m-%Y") == datetime.strptime(today_date, "%Y-%m-%d"))
                    else 0,
                    na_action="ignore",
                )
                # set the datatype of the columns
                shares_table["open_price"] = pd.to_numeric(shares_table["open_price"], errors="coerce")
                shares_table["high"] = pd.to_numeric(shares_table["high"], errors="coerce")
                shares_table["low"] = pd.to_numeric(shares_table["low"], errors="coerce")
                shares_table["os_bid"] = pd.to_numeric(shares_table["os_bid"], errors="coerce")
                shares_table["os_bid_vol"] = pd.to_numeric(shares_table["os_bid_vol"], errors="coerce")
                shares_table["os_offer"] = pd.to_numeric(shares_table["os_offer"], errors="coerce")
                shares_table["os_offer_vol"] = pd.to_numeric(shares_table["os_offer_vol"], errors="coerce")
                shares_table["last_sale_price"] = pd.to_numeric(shares_table["last_sale_price"], errors="coerce")
                shares_table["volume_traded"] = pd.to_numeric(shares_table["volume_traded"], errors="coerce")
                shares_table["close_price"] = pd.to_numeric(shares_table["close_price"], errors="coerce")
                shares_table["change_dollars"] = pd.to_numeric(shares_table["change_dollars"], errors="coerce")
                # if the high and low columns are 0, replace them with the open price
                shares_table["high"] = shares_table.apply(
                    lambda x: x.open_price if (pd.isna(x.high)) else x.high, axis=1
                )
                shares_table["low"] = shares_table.apply(lambda x: x.open_price if (pd.isna(x.low)) else x.low, axis=1)
                # replace certain column null values with 0
                shares_table["change_dollars"].fillna(0, inplace=True)
                shares_table["volume_traded"].fillna(0, inplace=True)
                shares_table["os_bid_vol"].fillna(0, inplace=True)
                shares_table["os_offer_vol"].fillna(0, inplace=True)
                # create a series for the value traded
                value_traded_series = pd.Series(0, index=shares_table.index).astype(float)
                # set the name of the series
                value_traded_series.rename("value_traded")
                # add the series to the dateframe
                shares_table = shares_table.assign(value_traded=value_traded_series)
                # calculate the value traded for today
                shares_table["value_traded"] = shares_table.apply(lambda x: x.volume_traded * x.last_sale_price, axis=1)
                # create a series containing the date
                date_series = pd.Series(
                    datetime.strptime(today_date, "%Y-%m-%d"),
                    index=shares_table.index,
                )
                # set the name of the series
                date_series.rename("date")
                # add the series to the dateframe
                shares_table = shares_table.assign(date=date_series)
                # replace the nan with None
                shares_table = shares_table.replace({np.nan: None})
                # add all values to the large list
                all_daily_stock_data += shares_table.to_dict("records")
        return all_daily_stock_data

    def _scrape_daily_trade_data_for_today(self, today_date):
        daily_trade_data_today_page = f"https://www.stockex.co.tt/market-quote/?TradeDate={today_date}"
        logger.debug("Navigating to " + daily_trade_data_today_page)
        daily_trade_data_for_today = self.scraping_engine.get_url_and_return_html(url=daily_trade_data_today_page)
        return daily_trade_data_for_today