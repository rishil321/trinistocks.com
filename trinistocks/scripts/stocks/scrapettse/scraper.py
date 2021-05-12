#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""This is the main module used for scraping data off the Trinidad and Tobago Stock Exchange website.

:raises requests.exceptions.HTTPError: If https://www.stockex.co.tt/ is inaccessible/slow
:return: 0
:rtype: Integer
"""

# region IMPORTS
# Put all your imports here, one per line.
# However multiple imports from the same lib are allowed on a line.
# Imports from Python standard libraries
import concurrent.futures
from ..crosslisted_symbols import USD_STOCK_SYMBOLS
from ...database_ops import DatabaseConnect
from ... import custom_logging
from bs4.element import Tag
from bs4 import BeautifulSoup
import numpy as np
import pandas as pd
import sqlalchemy.exc
from sqlalchemy.dialects.mysql import insert
from sqlalchemy import create_engine, Table, select, MetaData, text, and_
import requests

from pid import PidFile
import logging
import os
import sys
from datetime import datetime, timedelta
from numpy import inf
from dateutil.parser import parse
from dateutil.relativedelta import relativedelta
import argparse
import multiprocessing
import time
import tempfile
import re

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# Imports from the cheese factory

# Imports from the local filesystem

# endregion IMPORTS

# region CONSTANTS
# Put your constants here. These should be named in CAPS

# The timeout to set for multiprocessing tasks (in seconds)
MULTIPROCESSING_TIMEOUT = 60 * 60
WEBPAGE_LOAD_TIMEOUT_SECS = 60
# Define a start date to use for the full updates
TTSE_RECORDS_START_DATE = "2017-01-01"
LOGGERNAME = "scraper"

# endregion CONSTANTS

# Put your class definitions here. These should use the CapWords convention.

# region FUNCTION DEFINITIONS
# Put your function definitions here. These should be lowercase, separated by underscores.


def scrape_listed_equity_data():
    """Use the requests and pandas libs to fetch the current listed equities at
    https://www.stockex.co.tt/listed-securities/?IdInstrumentType=1&IdSegment=&IdSector=
    and scrape the useful output into a list of dictionaries to write to the db
    """
    try:
        logger = logging.getLogger(LOGGERNAME)
        logger.debug("Now scraping listing data from all listed equities.")
        # This list of dicts will contain all data to be written to the db
        all_listed_equity_data = []
        listed_stocks_summary_url = "https://www.stockex.co.tt/listed-securities/?IdInstrumentType=1&IdSegment=&IdSector="
        logger.debug("Navigating to " + listed_stocks_summary_url)
        listed_stocks_summary_page = requests.get(
            listed_stocks_summary_url, timeout=WEBPAGE_LOAD_TIMEOUT_SECS
        )
        if listed_stocks_summary_page.status_code != 200:
            raise requests.exceptions.HTTPError(
                "Could not load URL. " + listed_stocks_summary_url
            )
        else:
            logger.debug("Successfully loaded webpage.")
        # get a list of tables from the URL
        dataframe_list = pd.read_html(listed_stocks_summary_page.text)
        # store the series that lists all the current stock symbols
        listed_stock_symbols = dataframe_list[0]["Symbol"]
        # remove the suspended char from the symbols
        listed_stock_symbols = listed_stock_symbols.str.replace(r"\(S\)", "")
        # Go to the main summary page for each symbol
        for symbol in listed_stock_symbols:
            try:
                per_stock_url = f"https://www.stockex.co.tt/manage-stock/{symbol}/"
                logger.debug("Navigating to " + per_stock_url)
                equity_page = requests.get(
                    per_stock_url, timeout=WEBPAGE_LOAD_TIMEOUT_SECS
                )
                if equity_page.status_code != 200:
                    raise requests.exceptions.HTTPError(
                        "Could not load URL. " + per_stock_url
                    )
                else:
                    logger.debug("Successfully loaded webpage.")
                # set up a dict to store the data for this equity
                equity_data = dict(symbol=symbol)
                # use beautifulsoup to get the securityname, sector, status, financial year end, website
                per_stock_page_soup = BeautifulSoup(equity_page.text, "lxml")
                equity_data["security_name"] = (
                    per_stock_page_soup.find(text="Security:")
                    .find_parent("h2")
                    .find_next("h2")
                    .text.title()
                )
                # apply some custom formatting to our names
                if equity_data["security_name"] == "Agostini S Limited":
                    equity_data["security_name"] = "Agostini's Limited"
                elif equity_data["security_name"] == "Ansa Mcal Limited":
                    equity_data["security_name"] = "ANSA McAL Limited"
                elif equity_data["security_name"] == "Ansa Merchant Bank Limited":
                    equity_data["security_name"] = "ANSA Merchant Bank Limited"
                elif equity_data["security_name"] == "Cinemaone Limited":
                    equity_data["security_name"] = "CinemaOne Limited"
                elif equity_data["security_name"] == "Clico Investment Fund":
                    equity_data["security_name"] = "CLICO Investment Fund"
                elif (
                    equity_data["security_name"]
                    == "Firstcaribbean International Bank Limited"
                ):
                    equity_data[
                        "security_name"
                    ] = "CIBC FirstCaribbean International Bank Limited"
                elif equity_data["security_name"] == "Gracekennedy Limited":
                    equity_data["security_name"] = "GraceKennedy Limited"
                elif equity_data["security_name"] == "Jmmb Group Limited":
                    equity_data["security_name"] = "JMMB Group Limited"
                elif (
                    equity_data["security_name"] == "Mpc Caribbean Clean Energy Limited"
                ):
                    equity_data["security_name"] = "MPC Caribbean Clean Energy Limited"
                elif equity_data["security_name"] == "Ncb Financial Group Limited":
                    equity_data["security_name"] = "NCB Financial Group Limited"
                elif equity_data["security_name"] == "Trinidad And Tobago Ngl Limited":
                    equity_data["security_name"] = "Trinidad And Tobago NGL Limited"
                equity_sector = (
                    per_stock_page_soup.find(text="Sector:")
                    .find_parent("h2")
                    .find_next("h2")
                    .text.title()
                )
                if equity_sector != "Status:":
                    equity_data["sector"] = equity_sector
                else:
                    equity_data["sector"] = None
                if equity_data["sector"] == "Manufacturing Ii":
                    equity_data["sector"] = "Manufacturing II"
                equity_data["status"] = (
                    per_stock_page_soup.find(text="Status:")
                    .find_parent("h2")
                    .find_next("h2")
                    .text.title()
                )
                equity_data["financial_year_end"] = (
                    per_stock_page_soup.find(text="Financial Year End:")
                    .find_parent("h2")
                    .find_next("h2")
                    .text
                )
                website_url = (
                    per_stock_page_soup.find(text="Website:")
                    .find_parent("h2")
                    .find_next("h2")
                    .text
                )
                if website_url != "Issuers":
                    equity_data["website_url"] = website_url
                else:
                    equity_data["website_url"] = None
                # store the currency that the stock is listed in
                if equity_data["symbol"] in USD_STOCK_SYMBOLS:
                    equity_data["currency"] = "USD"
                else:
                    equity_data["currency"] = "TTD"
                # get a list of tables from the URL
                dataframe_list = pd.read_html(equity_page.text)
                # use pandas to get the issued share capital and market cap
                equity_data["issued_share_capital"] = int(
                    float(dataframe_list[0]["Opening Price"][8])
                )
                equity_data["market_capitalization"] = float(
                    re.sub("[ |$|,]", "", dataframe_list[0]["Closing Price"][8])
                )
                # Now we have all the important information for this equity
                # So we can add the dictionary object to our global list
                # But first we check that this symbol has not been added already
                symbol_already_added = next(
                    (
                        item
                        for item in all_listed_equity_data
                        if item["symbol"] == symbol
                    ),
                    False,
                )
                if not symbol_already_added:
                    all_listed_equity_data.append(equity_data)
                # else don't add a duplicate equity
                logger.debug(
                    "Successfully added basic listing data for: "
                    + equity_data["security_name"]
                )
            except Exception as exc:
                logger.warning(
                    f"Could not load page for equity:{symbol}. Here's what we know: {str(exc)}"
                )
        # set up a dataframe with all our data
        all_listed_equity_data_df = pd.DataFrame(all_listed_equity_data)
        # now find the symbol ids? used for the news page for each symbol
        logger.debug("Now trying to fetch symbol ids for news")
        news_url = "https://www.stockex.co.tt/news/"
        logger.debug(f"Navigating to {news_url}")
        equity_page = requests.get(news_url, timeout=WEBPAGE_LOAD_TIMEOUT_SECS)
        if equity_page.status_code != 200:
            raise requests.exceptions.HTTPError("Could not load URL. " + news_url)
        logger.debug("Successfully loaded webpage.")
        # get all the options for the dropdown select, since these contain the ids
        news_page_soup = BeautifulSoup(equity_page.text, "lxml")
        all_symbol_mappings = news_page_soup.find(id="symbol")
        # now parse the soup and get the symbols and their ids
        symbols = []
        symbol_ids = []
        for mapping in all_symbol_mappings:
            if isinstance(mapping, Tag):
                symbol = mapping.contents[0].split()[0]
                symbol_id = mapping.attrs["value"]
                if symbol and symbol_id:
                    symbols.append(symbol)
                    symbol_ids.append(symbol_id)
        # now set up a dataframe
        symbol_id_df = pd.DataFrame(
            list(zip(symbols, symbol_ids)), columns=["symbol", "symbol_id"]
        )
        # merge the two dataframes
        all_listed_equity_data_df = pd.merge(
            all_listed_equity_data_df, symbol_id_df, on="symbol", how="left"
        )
        # Now write the data to the database
        with DatabaseConnect() as db_obj:
            listed_equities_table = Table(
                "listed_equities",
                MetaData(),
                autoload=True,
                autoload_with=db_obj.dbengine,
            )
            logger.debug("Inserting scraped data into listed_equities table")
            listed_equities_insert_stmt = insert(listed_equities_table).values(
                all_listed_equity_data_df.to_dict("records")
            )
            listed_equities_upsert_stmt = (
                listed_equities_insert_stmt.on_duplicate_key_update(
                    {x.name: x for x in listed_equities_insert_stmt.inserted}
                )
            )
            result = db_obj.dbcon.execute(listed_equities_upsert_stmt)
            logger.debug(
                "Database update successful. Number of rows affected was "
                + str(result.rowcount)
            )
        return 0
    except Exception as exc:
        logger.exception(
            f"Problem encountered while updating listed equities. Here's what we know: {str(exc)}"
        )
        custom_logging.flush_smtp_logger()


def check_num_equities_in_sector():
    db_connection = None
    try:
        logger = logging.getLogger(LOGGERNAME)
        logger.debug("Now computing number of equities in each sector.")
        db_connection = DatabaseConnect()
        # set up the tables from the db
        listed_equities_per_sector_table = Table(
            "listed_equities_per_sector",
            MetaData(),
            autoload=True,
            autoload_with=db_connection.dbengine,
        )
        # read the listedequities table into a dataframe
        listed_equities_df = pd.io.sql.read_sql(
            "SELECT sector FROM listed_equities;", db_connection.dbengine
        )
        # create a copy of the dataframe and drop the duplicates to get all sectors
        unique_listed_equities_df = listed_equities_df.copy().drop_duplicates()
        # get the number of times the sector occurs in the df
        listed_equities_sector_counts_df = listed_equities_df["sector"].value_counts(
            dropna=False
        )
        # map the counts to the unique df
        unique_listed_equities_df["num_listed"] = unique_listed_equities_df[
            "sector"
        ].map(listed_equities_sector_counts_df)
        # get the rows that are not na
        unique_listed_equities_df = unique_listed_equities_df[
            unique_listed_equities_df["num_listed"].notna()
        ]
        # update the table in the db
        listed_equities_per_sector_insert_stmt = insert(
            listed_equities_per_sector_table
        ).values(unique_listed_equities_df.to_dict("records"))
        listed_equities_per_sector_upsert_stmt = (
            listed_equities_per_sector_insert_stmt.on_duplicate_key_update(
                {x.name: x for x in listed_equities_per_sector_insert_stmt.inserted}
            )
        )
        result = db_connection.dbcon.execute(listed_equities_per_sector_upsert_stmt)
        logger.debug(
            "Database update successful. Number of rows affected was "
            + str(result.rowcount)
        )
        return 0
    except Exception as exc:
        logger.exception(
            "Problem encountered while calculating number of equities in each sector."
            + str(exc)
        )
        custom_logging.flush_smtp_logger()
    finally:
        if db_connection is not None:
            db_connection.close()


def scrape_historical_indices_data():
    """Use the requests and pandas libs to fetch data for all indices at
    https://www.stockex.co.tt/indices/
    and scrape the useful output into a list of dictionaries to write to the db
    """
    db_connection = None
    try:
        logger = logging.getLogger(LOGGERNAME)
        logger.debug("Now scraping historical data for all indices.")
        # This list of dicts will contain all data to be written to the db
        all_indices_data = []
        # create a list of all index ids and names to be scraped
        all_ttse_indices = [
            dict(name="All T&T Index", id=4),
            dict(name="Composite Index", id=5),
            dict(name="Cross-Listed Index", id=6),
            dict(name="SME Index", id=15),
        ]
        for ttse_index in all_ttse_indices:
            index_url = f"https://www.stockex.co.tt/indices/?indexId={ttse_index['id']}"
            logger.debug("Navigating to " + index_url)
            index_page = requests.get(index_url, timeout=WEBPAGE_LOAD_TIMEOUT_SECS)
            if index_page.status_code != 200:
                raise requests.exceptions.HTTPError(f"Could not load URL: {index_page}")
            else:
                logger.debug("Successfully loaded webpage.")
            # get a list of tables from the URL
            dataframe_list = pd.read_html(index_page.text)
            # get the table that holds the historical index values
            historical_index_values_df = dataframe_list[1]
            # rename the columns
            historical_index_values_df = historical_index_values_df.rename(
                columns={
                    "Trade Date": "date",
                    "Value": "index_value",
                    "Change ($)": "index_change",
                    "Change (%)": "change_percent",
                    "Volume Traded": "volume_traded",
                }
            )
            # convert the date column
            historical_index_values_df["date"] = pd.to_datetime(
                historical_index_values_df["date"], format="%d %b %Y"
            )
            # add a series for the index name
            historical_index_values_df["index_name"] = pd.Series(
                data=ttse_index["name"], index=historical_index_values_df.index
            )
            # convert the dataframe to a list of dicts and add to the large list
            all_indices_data += historical_index_values_df.to_dict("records")
        # Now write the data to the database
        db_connection = DatabaseConnect()
        historical_indices_table = Table(
            "historical_indices_info",
            MetaData(),
            autoload=True,
            autoload_with=db_connection.dbengine,
        )
        logger.debug("Inserting scraped data into historical_indices table")
        insert_stmt = insert(historical_indices_table).values(all_indices_data)
        upsert_stmt = insert_stmt.on_duplicate_key_update(
            {x.name: x for x in insert_stmt.inserted}
        )
        result = db_connection.dbcon.execute(upsert_stmt)
        logger.debug(
            "Database update successful. Number of rows affected was "
            + str(result.rowcount)
        )
        return 0
    except Exception as exc:
        logger.exception(
            f"Problem encountered while updating listed equities. Here's what we know: {str(exc)}"
        )
        custom_logging.flush_smtp_logger()
    finally:
        if db_connection is not None:
            db_connection.close()


def scrape_dividend_data():
    """Use the requests and pandas libs to browse through
    https://www.stockex.co.tt/manage-stock/<symbol> for each listed security
    """
    try:
        logger = logging.getLogger(LOGGERNAME)
        logger.debug("Now trying to scrape dividend data")
        # First read all symbols from the listed_equities table
        all_listed_symbols = []
        with DatabaseConnect() as db_connection:
            listed_equities_table = Table(
                "listed_equities",
                MetaData(),
                autoload=True,
                autoload_with=db_connection.dbengine,
            )
            selectstmt = select([listed_equities_table.c.symbol])
            result = db_connection.dbcon.execute(selectstmt)
            for row in result:
                all_listed_symbols.append(row[0])
            # now get get the tables listing dividend data for each symbol
            for symbol in all_listed_symbols:
                logger.debug(f"Now attempting to fetch dividend data for {symbol}")
                try:
                    # Construct the full URL using the symbol
                    equity_dividend_url = (
                        f"https://www.stockex.co.tt/manage-stock/{symbol}"
                    )
                    logger.debug("Navigating to " + equity_dividend_url)
                    equity_dividend_page = requests.get(
                        equity_dividend_url, timeout=WEBPAGE_LOAD_TIMEOUT_SECS
                    )
                    if equity_dividend_page.status_code != 200:
                        raise requests.exceptions.HTTPError(
                            "Could not load URL. " + equity_dividend_page
                        )
                    else:
                        logger.debug("Successfully loaded webpage.")
                    # get the dataframes from the page
                    dataframe_list = pd.read_html(equity_dividend_page.text)
                    dividend_table = dataframe_list[1]
                    # check if dividend data is present
                    if len(dividend_table.index) > 1:
                        logger.debug(f"Dividend data present for {symbol}")
                        # remove the columns we don't need
                        dividend_table.drop(
                            dividend_table.columns[[0, 2]], axis=1, inplace=True
                        )
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
                        dividend_table["record_date"] = dividend_table[
                            "record_date"
                        ].map(
                            lambda x: datetime.strptime(x, "%d %b %Y"),
                            na_action="ignore",
                        )
                        # get the dividend amount into the appropriate format
                        if symbol == "GMLP":
                            # compute the actual dividend value per share, based on the percentage and par value ($50)
                            dividend_table["dividend_amount"] = (
                                50 / 100
                            ) * pd.to_numeric(
                                dividend_table["dividend_amount"].str.replace("%", ""),
                                errors="coerce",
                            )
                        else:
                            dividend_table["dividend_amount"] = pd.to_numeric(
                                dividend_table["dividend_amount"].str.replace("$", ""),
                                errors="coerce",
                            )
                        # add a series for the symbol
                        dividend_table["symbol"] = pd.Series(
                            symbol, index=dividend_table.index
                        )
                        logger.debug("Successfully fetched dividend data for " + symbol)
                        # check if currency is missing from column
                        if dividend_table["currency"].isnull().values.any():
                            logger.warning(
                                f"Currency seems to be missing from the dividend table for {symbol}. We will autofill with TTD, but this may be incorrect."
                            )
                            dividend_table["currency"].fillna("TTD", inplace=True)
                        # dividend table replace nan with None
                        dividend_table = dividend_table.replace({np.nan: None})
                        # now write the dataframe to the db
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
                        while (
                            not execute_completed_successfully
                            and execute_failed_times < 5
                        ):
                            try:
                                insert_stmt = insert(
                                    historical_dividend_info_table
                                ).values(dividend_table.to_dict("records"))
                                upsert_stmt = insert_stmt.on_duplicate_key_update(
                                    {x.name: x for x in insert_stmt.inserted}
                                )
                                result = db_connection.dbcon.execute(upsert_stmt)
                                execute_completed_successfully = True
                            except sqlalchemy.exc.OperationalError as operr:
                                logger.warning(str(operr))
                                time.sleep(2)
                                execute_failed_times += 1
                        logger.debug(
                            "Number of rows affected in the historical_dividend_info table was "
                            + str(result.rowcount)
                        )
                    else:
                        logger.debug(f"No dividend data found for {symbol}. Skipping.")
                except Exception as e:
                    logger.error(
                        f"Unable to scrape dividend data for {symbol}", exc_info=e
                    )
        return 0
    except Exception:
        logger.exception("Error encountered while scraping dividend data.")
        custom_logging.flush_smtp_logger()


def scrape_equity_summary_data(
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
    db_connect = None
    try:
        logger = logging.getLogger(LOGGERNAME)
        logger.debug(
            "Now opening using pandas to fetch daily summary data" + pid_string
        )
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
        # set up the database connection to write data to the db
        db_connect = DatabaseConnect()
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
        # now fetch the data at each url(each market trading date)
        for index, fetch_date in enumerate(dates_to_fetch):
            try:
                logger.debug(
                    f"Now loading page {str(index)} of {str(len(dates_to_fetch))} {pid_string}"
                )
                # get a date object suitable for the db
                fetch_date_db = datetime.strptime(fetch_date, "%Y-%m-%d")
                # for each date, we need to navigate to this summary page for that day
                url_summary_page = (
                    f"https://www.stockex.co.tt/market-quote/?TradeDate={fetch_date}"
                )
                logger.debug(f"Navigating to {url_summary_page} {pid_string}")
                http_get_req = requests.get(
                    url_summary_page, timeout=WEBPAGE_LOAD_TIMEOUT_SECS
                )
                if http_get_req.status_code != 200:
                    raise requests.exceptions.HTTPError(
                        "Could not load URL. " + url_summary_page + pid_string
                    )
                else:
                    logger.debug("Successfully loaded webpage.")
                # get a list of tables from the URL
                dataframe_list = pd.read_html(http_get_req.text)
                # if this is a valid trading day, extract the values we need from the tables
                if len(dataframe_list[00].index) > 4:
                    logger.debug("This is a valid trading day.")
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
                    market_indices_table.drop(
                        market_indices_table.columns[0], axis=1, inplace=True
                    )
                    # set the names of columns
                    market_indices_table.columns = market_summary_data_keys
                    # remove all the '-' in the dataframe
                    market_indices_table.replace("–", None, inplace=True)
                    # set the datatype of the columns
                    market_indices_table["index_name"] = (
                        market_indices_table["index_name"].astype(str).str.title()
                    )
                    market_indices_table["index_value"] = pd.to_numeric(
                        market_indices_table["index_value"], errors="coerce"
                    )
                    market_indices_table["index_change"] = pd.to_numeric(
                        market_indices_table["index_change"], errors="coerce"
                    )
                    market_indices_table["change_percent"] = pd.to_numeric(
                        market_indices_table["change_percent"], errors="coerce"
                    )
                    market_indices_table["volume_traded"] = pd.to_numeric(
                        market_indices_table["volume_traded"], errors="coerce"
                    )
                    market_indices_table["value_traded"] = pd.to_numeric(
                        market_indices_table["value_traded"], errors="coerce"
                    )
                    market_indices_table["num_trades"] = pd.to_numeric(
                        market_indices_table["num_trades"], errors="coerce"
                    )
                    # add a series containing the date
                    market_indices_table["date"] = pd.Series(
                        fetch_date_db, index=market_indices_table.index
                    )
                    market_indices_table.fillna(0, inplace=True)
                    # now write the dataframe to the db
                    logger.debug(
                        "Finished wrangling market indices data. Now writing to db."
                    )
                    # if we had any errors, the values will be written as their defaults (0 or null)
                    # wrote the data to the db
                    execute_completed_successfully = False
                    execute_failed_times = 0
                    while (
                        not execute_completed_successfully and execute_failed_times < 5
                    ):
                        try:
                            insert_stmt = insert(historical_indices_info_table).values(
                                market_indices_table.to_dict("records")
                            )
                            upsert_stmt = insert_stmt.on_duplicate_key_update(
                                {x.name: x for x in insert_stmt.inserted}
                            )
                            result = db_connect.dbcon.execute(upsert_stmt)
                            execute_completed_successfully = True
                            logger.debug(
                                "Successfully scraped and wrote to db market indices data for "
                                + fetch_date
                                + pid_string
                            )
                            logger.debug(
                                "Number of rows affected in the historical_indices_summary table was "
                                + str(result.rowcount)
                                + pid_string
                            )
                        except sqlalchemy.exc.OperationalError as operr:
                            logger.warning(str(operr))
                            time.sleep(2)
                            execute_failed_times += 1
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
                            # remove the first row from each table since its a header row
                            shares_table.drop(shares_table.index[0])
                            # remove the column with the up and down symbols
                            shares_table.drop(
                                shares_table.columns[0], axis=1, inplace=True
                            )
                            # set the names of columns
                            shares_table.columns = daily_stock_data_keys
                            # remove the unneeded characters from the symbols
                            # note that these characters come after a space
                            shares_table["symbol"] = (
                                shares_table["symbol"].str.split(" ", 1).str.get(0)
                            )
                            # replace the last sale date with a boolean
                            # if the last sale date is the current date being queried, return 1, else return 0
                            shares_table["was_traded_today"] = shares_table[
                                "was_traded_today"
                            ].map(
                                lambda x: 1
                                if (
                                    datetime.strptime(x, "%d-%m-%Y")
                                    == datetime.strptime(fetch_date, "%Y-%m-%d")
                                )
                                else 0,
                                na_action="ignore",
                            )
                            # set the datatype of the columns
                            shares_table["open_price"] = pd.to_numeric(
                                shares_table["open_price"], errors="coerce"
                            )
                            shares_table["high"] = pd.to_numeric(
                                shares_table["high"], errors="coerce"
                            )
                            shares_table["low"] = pd.to_numeric(
                                shares_table["low"], errors="coerce"
                            )
                            shares_table["os_bid"] = pd.to_numeric(
                                shares_table["os_bid"], errors="coerce"
                            )
                            shares_table["os_bid_vol"] = pd.to_numeric(
                                shares_table["os_bid_vol"], errors="coerce"
                            )
                            shares_table["os_offer"] = pd.to_numeric(
                                shares_table["os_offer"], errors="coerce"
                            )
                            shares_table["os_offer_vol"] = pd.to_numeric(
                                shares_table["os_offer_vol"], errors="coerce"
                            )
                            shares_table["last_sale_price"] = pd.to_numeric(
                                shares_table["last_sale_price"], errors="coerce"
                            )
                            shares_table["volume_traded"] = pd.to_numeric(
                                shares_table["volume_traded"], errors="coerce"
                            )
                            shares_table["close_price"] = pd.to_numeric(
                                shares_table["close_price"], errors="coerce"
                            )
                            shares_table["change_dollars"] = pd.to_numeric(
                                shares_table["change_dollars"], errors="coerce"
                            )
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
                            value_traded_series = pd.Series(
                                0, index=shares_table.index
                            ).astype(float)
                            # set the name of the series
                            value_traded_series.rename("value_traded")
                            # add the series to the dateframe
                            shares_table = shares_table.assign(
                                value_traded=value_traded_series
                            )
                            # calculate the value traded for today
                            shares_table["value_traded"] = shares_table.apply(
                                lambda x: x.volume_traded * x.last_sale_price, axis=1
                            )
                            # create a series containing the date
                            date_series = pd.Series(
                                fetch_date_db, index=shares_table.index
                            )
                            # set the name of the series
                            date_series.rename("date")
                            # add the series to the dateframe
                            shares_table = shares_table.assign(date=date_series)
                            # replace the nan with None
                            shares_table = shares_table.replace({np.nan: None})
                            # add all values to the large list
                            all_daily_stock_data += shares_table.to_dict("records")
                    # now insert the data into the db
                    execute_completed_successfully = False
                    execute_failed_times = 0
                    while (
                        not execute_completed_successfully and execute_failed_times < 5
                    ):
                        try:
                            insert_stmt = insert(daily_stock_summary_table).values(
                                all_daily_stock_data
                            )
                            upsert_stmt = insert_stmt.on_duplicate_key_update(
                                {x.name: x for x in insert_stmt.inserted}
                            )
                            result = db_connect.dbcon.execute(upsert_stmt)
                            execute_completed_successfully = True
                            logger.debug(
                                "Successfully scraped and wrote to db daily equity/shares data for "
                                + fetch_date
                                + pid_string
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
                else:
                    logger.warning(
                        "This date is not a valid trading date: "
                        + fetch_date
                        + pid_string
                    )
            except KeyError as keyerr:
                logger.warning(
                    "Could not find a required key on date "
                    + fetch_date
                    + pid_string
                    + str(keyerr)
                )
            except IndexError as idxerr:
                logger.warning(
                    "Could not locate index in a list. "
                    + fetch_date
                    + pid_string
                    + str(idxerr)
                )
            except requests.exceptions.Timeout as timeerr:
                logger.error(
                    "Could not load URL in time. Maybe website is down? "
                    + fetch_date
                    + pid_string,
                    exc_info=timeerr,
                )
            except requests.exceptions.HTTPError as httperr:
                logger.error("HTTP Error!", exc_info=httperr)
        return 0
    except Exception:
        logger.exception(
            "Could not complete historical_indices_summary and daily_stock_summary update."
            + pid_string
        )
        custom_logging.flush_smtp_logger()
    finally:
        # Always close the database connection
        if db_connect is not None:
            db_connect.close()


def update_equity_summary_data(start_date):
    """
    Create the list of dates that we need to scrape data from https://www.stockex.co.tt/market-quote/
    for, based on the start_date specified and the dates already in the historical_indices_info table
    """
    logger = logging.getLogger(LOGGERNAME)
    logger.debug("Now updating daily market summary data.")
    db_connect = None
    try:
        db_connect = DatabaseConnect()
        logger.debug("Successfully connected to database.")
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
        # We want to gather data on all trading days since the start date, so we create a list
        # of all dates that we need to gather still
        dates_to_fetch = []
        fetch_date = datetime.strptime(start_date, "%Y-%m-%d")
        logger.debug(
            "Getting all dates that are not already fetched and are not weekends."
        )
        # TODO: Extend holidays library for Trinidad and Tobago
        # Get all dates until yesterday
        while fetch_date < datetime.now():
            # if we do not have info on this date already and this is a weekday (stock markets close on weekends)
            if (fetch_date.date() not in dates_already_recorded) and (
                fetch_date.weekday() < 5
            ):
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
            dates_to_fetch[
                i * list_length // num_cores : (i + 1) * list_length // num_cores
            ]
            for i in range(num_cores)
        ]
        logger.debug("Lists split successfully.")
        return dates_to_fetch_sublists
    except Exception as ex:
        logger.exception(
            f"We ran into a problem while trying to build the list of dates to scrape market summary data for. Here's what we know: {str(ex)}"
        )
        custom_logging.flush_smtp_logger()


def update_daily_trades():
    """
    Open the Chrome browser and browse through
    https://stockex.co.tt/controller.php?action=view_quote which shows trading for the last day
    Gather the data into a dict, and write that dict to the DB
    :returns: 0 if successful
    :raises Exception if any issues are encountered
    """
    db_connect = None
    try:
        logger = logging.getLogger(LOGGERNAME)
        today_date = datetime.now().strftime("%Y-%m-%d")
        db_connect = DatabaseConnect()
        logger.debug("Successfully connected to database")
        logger.debug(
            f"Now opening using pandas to fetch daily shares data for today ({today_date})"
        )
        daily_shares_data_keys = [
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
        # load the daily summary table
        daily_stock_summary_table = Table(
            "daily_stock_summary",
            MetaData(),
            autoload=True,
            autoload_with=db_connect.dbengine,
        )
        urlsummarypage = (
            f"https://www.stockex.co.tt/market-quote/?TradeDate={today_date}"
        )
        logger.debug("Navigating to " + urlsummarypage)
        http_get_req = requests.get(urlsummarypage, timeout=WEBPAGE_LOAD_TIMEOUT_SECS)
        if http_get_req.status_code != 200:
            raise requests.exceptions.HTTPError(
                "Could not load URL to update latest daily data " + urlsummarypage
            )
        else:
            logger.debug("Successfully loaded webpage.")
        # set up a list to store the data to be written to db
        all_daily_stock_data = []
        # get a list of tables from the URL
        dataframe_list = pd.read_html(http_get_req.text)
        # if this is a valid trading day, and the summary data for today has been published,
        # extract the values we need from the tables
        if len(dataframe_list[00].index) == 8:  # 8
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
                    shares_table.columns = daily_shares_data_keys
                    # remove the unneeded characters from the symbols
                    # note that these characters come after a space
                    shares_table["symbol"] = (
                        shares_table["symbol"].str.split(" ", 1).str.get(0)
                    )
                    # replace the last sale date with a boolean
                    # if the last sale date is the current date being queried, return 1, else return 0
                    shares_table["was_traded_today"] = shares_table[
                        "was_traded_today"
                    ].map(
                        lambda x: 1
                        if (
                            datetime.strptime(x, "%d-%m-%Y")
                            == datetime.strptime(today_date, "%Y-%m-%d")
                        )
                        else 0,
                        na_action="ignore",
                    )
                    # set the datatype of the columns
                    shares_table["open_price"] = pd.to_numeric(
                        shares_table["open_price"], errors="coerce"
                    )
                    shares_table["high"] = pd.to_numeric(
                        shares_table["high"], errors="coerce"
                    )
                    shares_table["low"] = pd.to_numeric(
                        shares_table["low"], errors="coerce"
                    )
                    shares_table["os_bid"] = pd.to_numeric(
                        shares_table["os_bid"], errors="coerce"
                    )
                    shares_table["os_bid_vol"] = pd.to_numeric(
                        shares_table["os_bid_vol"], errors="coerce"
                    )
                    shares_table["os_offer"] = pd.to_numeric(
                        shares_table["os_offer"], errors="coerce"
                    )
                    shares_table["os_offer_vol"] = pd.to_numeric(
                        shares_table["os_offer_vol"], errors="coerce"
                    )
                    shares_table["last_sale_price"] = pd.to_numeric(
                        shares_table["last_sale_price"], errors="coerce"
                    )
                    shares_table["volume_traded"] = pd.to_numeric(
                        shares_table["volume_traded"], errors="coerce"
                    )
                    shares_table["close_price"] = pd.to_numeric(
                        shares_table["close_price"], errors="coerce"
                    )
                    shares_table["change_dollars"] = pd.to_numeric(
                        shares_table["change_dollars"], errors="coerce"
                    )
                    # if the high and low columns are 0, replace them with the open price
                    shares_table["high"] = shares_table.apply(
                        lambda x: x.open_price if (pd.isna(x.high)) else x.high, axis=1
                    )
                    shares_table["low"] = shares_table.apply(
                        lambda x: x.open_price if (pd.isna(x.low)) else x.low, axis=1
                    )
                    # replace certain column null values with 0
                    shares_table["change_dollars"].fillna(0, inplace=True)
                    shares_table["volume_traded"].fillna(0, inplace=True)
                    shares_table["os_bid_vol"].fillna(0, inplace=True)
                    shares_table["os_offer_vol"].fillna(0, inplace=True)
                    # create a series for the value traded
                    value_traded_series = pd.Series(0, index=shares_table.index).astype(
                        float
                    )
                    # set the name of the series
                    value_traded_series.rename("value_traded")
                    # add the series to the dateframe
                    shares_table = shares_table.assign(value_traded=value_traded_series)
                    # calculate the value traded for today
                    shares_table["value_traded"] = shares_table.apply(
                        lambda x: x.volume_traded * x.last_sale_price, axis=1
                    )
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
        else:
            # if no summary data has been published yet for today, try to use the marquee on the main page to source data
            logger.debug(
                "No summary data found for today (yet?). Trying to get marquee data from main page."
            )
            # first get a list of all stock symbols in db
            listed_equities_table = Table(
                "listed_equities",
                MetaData(),
                autoload=True,
                autoload_with=db_connect.dbengine,
            )
            listed_symbols = []
            select_stmt = select([listed_equities_table.c.symbol])
            result = db_connect.dbcon.execute(select_stmt)
            for row in result:
                # We only have a single element in each row tuple, which is the date
                listed_symbols.append(row[0])
            url_main_page = f"https://www.stockex.co.tt/"
            logger.debug("Navigating to " + url_main_page)
            http_get_req = requests.get(
                url_main_page, timeout=WEBPAGE_LOAD_TIMEOUT_SECS
            )
            if http_get_req.status_code != 200:
                raise requests.exceptions.HTTPError(
                    "Could not load URL to update latest daily data " + url_main_page
                )
            else:
                logger.debug("Successfully loaded webpage.")
            # parse the text if we were able to load the marquee data
            page_soup = BeautifulSoup(http_get_req.text, "lxml")
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
                    marquee_text_date = marquee_text_date.split("for ")[1].split(" @")[
                        0
                    ]
                    if (
                        datetime.strptime(marquee_text_date, "%d %b %Y").date()
                        == datetime.today().date()
                    ):
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
                                    stock_data["volume_traded"] = int(
                                        symbol_data_chunks[4].replace(",", "")
                                    )
                                    stock_data["last_sale_price"] = float(
                                        symbol_data_chunks[6].replace("$", "")
                                    )
                                    stock_data["open_price"] = stock_data[
                                        "last_sale_price"
                                    ]
                                    stock_data["close_price"] = stock_data[
                                        "last_sale_price"
                                    ]
                                    stock_data["high"] = stock_data["last_sale_price"]
                                    stock_data["low"] = stock_data["last_sale_price"]
                                    stock_data["change_dollars"] = float(
                                        (
                                            symbol_data_chunks[7]
                                            .replace("(", "")
                                            .replace(")", "")
                                        )
                                    )
                                    stock_data["was_traded_today"] = 1
                                    stock_data["value_traded"] = (
                                        stock_data["volume_traded"]
                                        * stock_data["last_sale_price"]
                                    )
                                    logger.debug(
                                        f"Marquee data looks good for {stock_data['symbol']}. Adding to db list."
                                    )
                                    # add dict data to list to be written to db
                                    # only if the vol is >0
                                    if stock_data["volume_traded"] > 0:
                                        all_daily_stock_data.append(stock_data)
                    else:
                        logger.warning("Marquee is for another date. Ignoring.")
                except Exception as exc:
                    logger.error(
                        "Problem while parsing data for marquee. Data possibly in invalid format?",
                        exc_info=exc,
                    )
        if all_daily_stock_data:
            # if we have any data to insert, then push the data into the db
            execute_completed_successfully = False
            execute_failed_times = 0
            while not execute_completed_successfully and execute_failed_times < 5:
                try:
                    insert_stmt = insert(daily_stock_summary_table).values(
                        all_daily_stock_data
                    )
                    upsert_stmt = insert_stmt.on_duplicate_key_update(
                        {x.name: x for x in insert_stmt.inserted}
                    )
                    result = db_connect.dbcon.execute(upsert_stmt)
                    execute_completed_successfully = True
                    logger.debug(
                        "Successfully scraped and wrote to db daily equity/shares data for daily trades."
                    )
                    logger.debug(
                        "Number of rows affected in the daily_stock_summary table was "
                        + str(result.rowcount)
                    )
                except sqlalchemy.exc.OperationalError as operr:
                    logger.warning(str(operr))
                    time.sleep(2)
                    execute_failed_times += 1
        return 0
    except Exception:
        logger.exception("Could not load daily data for today!")
        custom_logging.flush_smtp_logger()
    finally:
        # Always close the database connection
        if db_connect is not None:
            db_connect.close()


def update_technical_analysis_data():
    """
    Calculate/scrape the data needed for the technical_analysis_summary table
    """
    db_connect = None
    logger = logging.getLogger(LOGGERNAME)
    try:
        db_connect = DatabaseConnect()
        logger.debug("Successfully connected to database")
        logger.debug(
            "Now using pandas to fetch latest technical analysis data from https://www.stockex.co.tt/manage-stock/"
        )
        # load the tables that we require
        listed_equities_table = Table(
            "listed_equities",
            MetaData(),
            autoload=True,
            autoload_with=db_connect.dbengine,
        )
        technical_analysis_summary_table = Table(
            "technical_analysis_summary",
            MetaData(),
            autoload=True,
            autoload_with=db_connect.dbengine,
        )
        # get a list of stored symbols
        selectstmt = select([listed_equities_table.c.symbol])
        result = db_connect.dbcon.execute(selectstmt)
        all_symbols = [r[0] for r in result]
        # now go to the url for each symbol that we have listed, and collect the data we need
        # set up a list of dicts to hold our data
        all_technical_data = []
        for symbol in all_symbols:
            try:
                stock_summary_page = f"https://www.stockex.co.tt/manage-stock/{symbol}"
                logger.debug(
                    "Navigating to "
                    + stock_summary_page
                    + " to fetch technical summary data."
                )
                http_get_req = requests.get(
                    stock_summary_page, timeout=WEBPAGE_LOAD_TIMEOUT_SECS
                )
                if http_get_req.status_code != 200:
                    raise requests.exceptions.HTTPError(
                        "Could not load URL " + stock_summary_page
                    )
                else:
                    logger.debug("Successfully loaded webpage.")
                # get a list of tables from the URL
                dataframe_list = pd.read_html(http_get_req.text)
                # table 2 contains the data we need
                technical_analysis_table = dataframe_list[0]
                # create a dict to hold the data that we are interested in
                stock_technical_data = dict(symbol=symbol)
                # fill all the nan values with 0s
                technical_analysis_table.fillna(0, inplace=True)
                # get the values that we are interested in from the table
                stock_technical_data["last_close_price"] = float(
                    technical_analysis_table["Closing Price"][0].replace("$", "")
                )
                stock_technical_data["high_52w"] = float(
                    technical_analysis_table["Change"][4].replace("$", "")
                )
                # leaving out the 52w low because it is not correct from the ttse site
                # stock_technical_data['low_52w'] = float(
                #    technical_analysis_table['Change%'][4].replace('$', ''))
                stock_technical_data["wtd"] = float(
                    technical_analysis_table["Opening Price"][6].replace("%", "")
                )
                stock_technical_data["mtd"] = float(
                    technical_analysis_table["Closing Price"][6].replace("%", "")
                )
                stock_technical_data["ytd"] = float(
                    technical_analysis_table["Change%"][6].replace("%", "")
                )
                # calculate our other required values
                # first calculate the SMAs
                # calculate sma20
                closing_quotes_last_20d_df = pd.io.sql.read_sql(
                    f"SELECT date,close_price FROM daily_stock_summary WHERE symbol='{symbol}' order by date desc limit 20;",
                    db_connect.dbengine,
                )
                sma20_df = closing_quotes_last_20d_df.rolling(window=20).mean()
                # get the last row value
                stock_technical_data["sma_20"] = sma20_df["close_price"].iloc[-1]
                # calculate sma200
                closing_quotes_last200d_df = pd.io.sql.read_sql(
                    f"SELECT date,close_price FROM daily_stock_summary WHERE symbol='{symbol}' order by date desc limit 200;",
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
                stock_change_df["change_percent"] = (
                    stock_change_df["change_dollars"] * 100
                ) / stock_change_df["close_price"]
                # get the market percentage change
                market_change_df = pd.io.sql.read_sql(
                    f"SELECT change_percent FROM historical_indices_info WHERE index_name='Composite Totals' order by date desc limit 365;",
                    db_connect.dbengine,
                )
                # now calculate the beta
                stock_change_df["beta"] = (
                    stock_change_df["change_percent"]
                    .rolling(window=365)
                    .cov(other=market_change_df["change_percent"])
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
            except KeyError as keyerr:
                logger.warning("Could not find a required key " + str(keyerr))
            except IndexError as idxerr:
                logger.warning("Could not locate index in a list. " + str(idxerr))
        # now insert the data into the db
        execute_completed_successfully = False
        execute_failed_times = 0
        logger.debug("Now trying to insert data into database.")
        while not execute_completed_successfully and execute_failed_times < 5:
            try:
                technical_analysis_summary_insert_stmt = insert(
                    technical_analysis_summary_table
                ).values(all_technical_data)
                technical_analysis_summary_upsert_stmt = (
                    technical_analysis_summary_insert_stmt.on_duplicate_key_update(
                        {
                            x.name: x
                            for x in technical_analysis_summary_insert_stmt.inserted
                        }
                    )
                )
                result = db_connect.dbcon.execute(
                    technical_analysis_summary_upsert_stmt
                )
                execute_completed_successfully = True
            except sqlalchemy.exc.OperationalError as operr:
                logger.warning(str(operr))
                time.sleep(1)
                execute_failed_times += 1
            logger.debug("Successfully scraped and wrote to db technical summary data.")
            logger.debug(
                "Number of rows affected in the technical analysis summary table was "
                + str(result.rowcount)
            )
        return 0
    except requests.exceptions.Timeout as timeerr:
        logger.error(
            "Could not load URL in time. Maybe website is down? " + str(timeerr)
        )
    except requests.exceptions.HTTPError as httperr:
        logger.error(str(httperr))
    except Exception:
        logger.exception("Could not complete technical analysis summary data update.")
        custom_logging.flush_smtp_logger()
    finally:
        # Always close the database connection
        if db_connect is not None:
            db_connect.close()


def parse_news_data_per_stock(symbols_to_fetch_for, start_date, end_date):
    """In a single thread, take a subset of symbols and fetch the news data for each symbol"""
    news_data = []
    logger = logging.getLogger(LOGGERNAME)
    for symbol in symbols_to_fetch_for:
        logger.debug(f"Now attempting to fetch news data for {symbol}")
        try:
            # loop through each page of news until we reach pages that have no news
            page_num = 1
            while True:
                # Construct the full URL using the symbol
                news_url = f"https://www.stockex.co.tt/news/?symbol={symbol['symbol_id']}&category=&date={start_date}&date_to={end_date}&search&page={page_num}#search_c"
                logger.debug("Navigating to " + news_url)
                news_page = requests.get(news_url, timeout=WEBPAGE_LOAD_TIMEOUT_SECS)
                if news_page.status_code != 200:
                    raise requests.exceptions.HTTPError(
                        "Could not load URL. " + news_page
                    )
                else:
                    logger.debug("Successfully loaded webpage.")
                # get the dataframes from the page
                per_stock_page_soup = BeautifulSoup(news_page.text, "lxml")
                news_articles = per_stock_page_soup.findAll("div", class_=["news_item"])
                if not news_articles:
                    # if we have an empty list of news articles, stop incrementing the list, since we have reached the end
                    logger.debug(
                        f"Finished fetching news articles for {symbol['symbol']}"
                    )
                    break
                # else process the list
                for article in news_articles:
                    try:
                        link = article.contents[1].attrs["href"]
                        # load the link to get the main article page
                        logger.debug("Now clicking news link. Navigating to " + link)
                        news_page = requests.get(
                            link, timeout=WEBPAGE_LOAD_TIMEOUT_SECS
                        )
                        if news_page.status_code != 200:
                            raise requests.exceptions.HTTPError(
                                "Could not load URL. " + link
                            )
                        else:
                            logger.debug("Successfully loaded webpage.")
                        # find the elements we want on the page
                        per_stock_page_soup = BeautifulSoup(news_page.text, "lxml")
                        # try to get the category
                        category_type_soup = per_stock_page_soup.select(
                            "div.elementor-text-editor.elementor-clearfix"
                        )
                        possible_categories = [
                            "Annual Report",
                            "Articles",
                            "Audited Financial Statements",
                            "Quarterly Financial Statements",
                        ]
                        category = None
                        for possible_category in category_type_soup:
                            if possible_category.string is not None and (
                                possible_category.string.strip() in possible_categories
                            ):
                                category = possible_category.string.strip()
                        if not category:
                            logger.warning("Could not parse category for this URL.")
                        # try to get the date
                        date = None
                        date_soup = per_stock_page_soup.select(
                            "h2.elementor-heading-title.elementor-size-default"
                        )
                        for possible_date in date_soup:
                            if possible_date.string is not None:
                                try:
                                    date = datetime.strptime(
                                        possible_date.string.strip(), "%d/%m/%Y"
                                    )
                                except ValueError as exc:
                                    pass
                        if not date:
                            raise RuntimeError(
                                f"We were not able to find the date for {article}"
                            )
                        # try to get title
                        title = None
                        title_soup = per_stock_page_soup.select(
                            "h1.elementor-heading-title.elementor-size-xl"
                        )
                        for possible_title in title_soup:
                            if possible_title.string is not None:
                                if len(possible_title.string.strip().split("–")) == 2:
                                    title = (
                                        possible_title.string.strip()
                                        .split("–")[1]
                                        .strip()
                                    )
                                else:
                                    title = possible_title.string.strip()
                        if not title:
                            raise RuntimeError(
                                f"We were not able to find the title for {article}"
                            )
                        # try to get full pdf link
                        link = None
                        link_soup = per_stock_page_soup.select(
                            "div.elementor-text-editor.elementor-clearfix"
                        )
                        for possible_link in link_soup:
                            if (
                                len(possible_link.contents) > 1
                                and "href" in possible_link.contents[1].attrs
                                and possible_link.contents[1].attrs["href"].strip()
                            ):
                                link = possible_link.contents[1].attrs["href"].strip()
                        if not link:
                            raise RuntimeError(
                                "We were not able to find the link for {article}"
                            )
                        # now append the data to our list
                        news_data.append(
                            {
                                "symbol": symbol["symbol"],
                                "category": category,
                                "date": date,
                                "title": title,
                                "link": link,
                            }
                        )
                    except Exception as exc:
                        logger.warning(
                            f"Could not parse article from {article}", exc_info=exc
                        )
                # increment the page num and restart the loop
                page_num += 1
        except Exception:
            logger.warning(
                f"We ran into a problem while checking news for {symbol['symbol']}"
            )
    # return all the news data for this thread
    return news_data


def scrape_newsroom_data(start_date, end_date):
    """Use the requests and pandas libs to fetch the current listed equities at
    https://www.stockex.co.tt/listed-securities/?IdInstrumentType=1&IdSegment=&IdSector=
    and scrape the useful output into a list of dictionaries to write to the db
    """
    logger = logging.getLogger(LOGGERNAME)
    logger.debug(f"Now trying to scrape newsroom date from {start_date} to {end_date}")
    try:
        all_listed_symbols = []
        with DatabaseConnect() as db_connection:
            listed_equities_table = Table(
                "listed_equities",
                MetaData(),
                autoload=True,
                autoload_with=db_connection.dbengine,
            )
            selectstmt = select(
                [listed_equities_table.c.symbol, listed_equities_table.c.symbol_id]
            )
            result = db_connection.dbcon.execute(selectstmt)
            for row in result:
                all_listed_symbols.append({"symbol": row[0], "symbol_id": row[1]})
        # set up a variable to store all data to be written to the db table
        all_news_data = []
        # set up some threads to speed up the process
        num_threads = 5
        # split the complete list of symbols into sublists for the threads
        per_thread_symbols = [
            all_listed_symbols[i::num_threads] for i in range(num_threads)
        ]
        # submit the work to the thread workers
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=num_threads, thread_name_prefix="fetch_news_data"
        ) as executor:
            future_to_news_fetch = {
                executor.submit(
                    parse_news_data_per_stock, symbols, start_date, end_date
                ): symbols
                for symbols in per_thread_symbols
            }
            logger.debug(f"Newsroom pages now being fetched by worker threads.")
            for future in concurrent.futures.as_completed(future_to_news_fetch):
                per_thread_symbols = future_to_news_fetch[future]
                try:
                    news_data = future.result()
                except Exception:
                    logger.exception(
                        f"Ran into an issue with this set of symbols: {per_thread_symbols}"
                    )
                else:
                    logger.debug(
                        "Successfully got data for symbols. Adding to master list."
                    )
                    all_news_data += news_data
        if not all_news_data:
            # if we could not parse any news data for today
            logger.warning(
                "No news data could be parsed for today. Possibly no news released today?"
            )
        else:
            with DatabaseConnect() as db_connection:
                # now write the list of dicts to the database
                stock_news_table = Table(
                    "stock_news_data",
                    MetaData(),
                    autoload=True,
                    autoload_with=db_connection.dbengine,
                )
                logger.debug("Inserting scraped news data into stock_news table")
                insert_stmt = insert(stock_news_table).values(all_news_data)
                upsert_stmt = insert_stmt.on_duplicate_key_update(
                    {x.name: x for x in insert_stmt.inserted}
                )
                result = db_connection.dbcon.execute(upsert_stmt)
                logger.debug(
                    "Database update successful. Number of rows affected was "
                    + str(result.rowcount)
                )
                return 0
    except Exception:
        logger.exception("Ran into an issue while trying to fetch news data.")
        custom_logging.flush_smtp_logger()


def main(args):
    """The main steps in coordinating the scraping"""
    try:
        # Set up logging for this module
        q_listener, q, logger = custom_logging.setup_logging(
            logdirparent=str(os.path.dirname(os.path.realpath(__file__))),
            loggername=LOGGERNAME,
            stdoutlogginglevel=logging.DEBUG,
            smtploggingenabled=True,
            smtplogginglevel=logging.ERROR,
            smtpmailhost="localhost",
            smtpfromaddr="server1@trinistats.com",
            smtptoaddr=["latchmepersad@gmail.com"],
            smtpsubj="Automated report from Python script: "
            + os.path.basename(__file__),
        )
        # Set up a pidfile to ensure that only one instance of this script runs at a time
        with PidFile(piddir=tempfile.gettempdir()):
            # run all functions within a multiprocessing pool
            with multiprocessing.Pool(
                os.cpu_count(), custom_logging.logging_worker_init, [q]
            ) as multipool:
                logger.debug("Now starting TTSE scraper.")
                # check if this is the intradaily update (run inside the trading day)
                if args.intradaily_update:
                    logger.debug("Intradaily scraper called.")
                    daily_trade_update_result = multipool.apply_async(
                        update_daily_trades, ()
                    )
                    start_date = (datetime.now() + relativedelta(days=-1)).strftime(
                        "%Y-%m-%d"
                    )
                    end_date = datetime.now().strftime("%Y-%m-%d")
                    scrape_all_newsroom_data_result = multipool.apply_async(
                        scrape_newsroom_data, (start_date, end_date)
                    )
                    logger.debug(
                        f"update_daily_trades exited with code {daily_trade_update_result.get()}"
                    )
                    logger.debug(
                        f"scrape_all_newsroom_data exited with code {scrape_all_newsroom_data_result.get()}"
                    )
                else:
                    if args.end_of_day_update:
                        logger.debug("End of day scraper called.")
                        start_date = (datetime.now() + relativedelta(days=-1)).strftime(
                            "%Y-%m-%d"
                        )
                    elif args.full_history:
                        logger.debug("Full history scraper called.")
                        start_date = TTSE_RECORDS_START_DATE
                    elif args.catchup:
                        logger.debug("Catchup scraper called.")
                        start_date = (
                            datetime.now() + relativedelta(months=-1)
                        ).strftime("%Y-%m-%d")
                        end_date = datetime.now().strftime("%Y-%m-%d")
                        scrape_all_newsroom_data_result = multipool.apply_async(
                            scrape_newsroom_data, (start_date, end_date)
                        )
                        logger.debug(
                            f"scrape_all_newsroom_data exited with code {scrape_all_newsroom_data_result.get()}"
                        )
                    scrape_listed_equity_data_result = multipool.apply_async(
                        scrape_listed_equity_data, ()
                    )
                    check_num_equities_in_sector_result = multipool.apply_async(
                        check_num_equities_in_sector, ()
                    )
                    scrape_dividend_data_result = multipool.apply_async(
                        scrape_dividend_data, ()
                    )
                    # block on the next function to wait until the dates are ready
                    dates_to_fetch_sublists = multipool.apply(
                        update_equity_summary_data, (start_date,)
                    )
                    logger.debug(
                        f"scrape_listed_equity_data exited with code {scrape_listed_equity_data_result.get()}"
                    )
                    logger.debug(
                        f"check_num_equities_in_sector exited with code {check_num_equities_in_sector_result.get()}"
                    )
                    logger.debug(
                        f"scrape_dividend_data exited with code {scrape_dividend_data_result.get()}"
                    )
                    # now call the individual workers to fetch these dates
                    async_results = []
                    for core_date_list in dates_to_fetch_sublists:
                        async_results.append(
                            multipool.apply_async(
                                scrape_equity_summary_data, (core_date_list,)
                            )
                        )
                    # wait until all workers finish fetching data before continuing
                    for result in async_results:
                        logger.debug(
                            f"One process of scrape_equity_summary_data exited with code {result.get()}"
                        )
                    # update the technical analysis stock data
                    update_technical_analysis_result = multipool.apply_async(
                        update_technical_analysis_data, ()
                    )
                    logger.debug(
                        f"update_technical_analysis_data exited with code {update_technical_analysis_result.get()}"
                    )
                multipool.close()
                multipool.join()
                logger.debug(os.path.basename(__file__) + " was completed.")
                q_listener.stop()
                return 0
    except Exception as exc:
        logger.exception(f"Error in script {os.path.basename(__file__)}", exc_info=exc)
        custom_logging.flush_smtp_logger()


# endregion FUNCTION DEFINITIONS


# If this script is being run from the command-line, then run the main() function
if __name__ == "__main__":
    # first check the arguements given to this script
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
        help="If we missed any data, run this to scrape missed days",
        action="store_true",
    )
    args = parser.parse_args()
    main(args)
