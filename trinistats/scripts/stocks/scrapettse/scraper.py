#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""This is the main module used for scraping data off the Trinidad and Tobago Stock Exchange website.

:raises requests.exceptions.HTTPError: If https://www.stockex.co.tt/ is inaccessible/slow
:return: 0
:rtype: Integer
"""

# Put all your imports here, one per line.
# However multiple imports from the same lib are allowed on a line.
# Imports from Python standard libraries
import logging
import os
import sys
from datetime import datetime, timedelta
from dateutil.parser import parse
from dateutil.relativedelta import relativedelta
import argparse
import multiprocessing
import time
import tempfile
import re
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# Imports from the cheese factory
from pid import PidFile
import requests
from sqlalchemy import create_engine, Table, select, MetaData, text, and_
from sqlalchemy.dialects.mysql import insert
import sqlalchemy.exc
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from bs4.element import Tag

# Imports from the local filesystem
from ... import custom_logging
from ...database_ops import DatabaseConnect
from ..crosslisted_symbols import USD_STOCK_SYMBOLS

# Put your constants here. These should be named in CAPS

# The timeout to set for multiprocessing tasks (in seconds)
MULTIPROCESSING_TIMEOUT = 60*60
WEBPAGE_LOAD_TIMEOUT_SECS = 30
# Define a start date to use for the full updates
START_DATE = '2017-01-01'

# Put your class definitions here. These should use the CapWords convention.


# Put your function definitions here. These should be lowercase, separated by underscores.


def scrape_listed_equity_data():
    """Use the requests and pandas libs to fetch the current listed equities at 
    https://www.stockex.co.tt/listed-securities/?IdInstrumentType=1&IdSegment=&IdSector=
    and scrape the useful output into a list of dictionaries to write to the db
    """
    try:
        logging.info(
            "Now scraping listing data from all listed equities.")
        # This list of dicts will contain all data to be written to the db
        all_listed_equity_data = []
        listed_stocks_summary_url = "https://www.stockex.co.tt/listed-securities/?IdInstrumentType=1&IdSegment=&IdSector="
        logging.info("Navigating to "+listed_stocks_summary_url)
        listed_stocks_summary_page = requests.get(
            listed_stocks_summary_url, timeout=WEBPAGE_LOAD_TIMEOUT_SECS)
        if listed_stocks_summary_page.status_code != 200:
            raise requests.exceptions.HTTPError(
                "Could not load URL. "+listed_stocks_summary_url)
        else:
            logging.info("Successfully loaded webpage.")
        # get a list of tables from the URL
        dataframe_list = pd.read_html(listed_stocks_summary_page.text)
        # store the series that lists all the current stock symbols
        listed_stock_symbols = dataframe_list[0]['Symbol']
        # remove the suspended char from the symbols
        listed_stock_symbols = listed_stock_symbols.str.replace('\(S\)', '')
        # Go to the main summary page for each symbol
        for symbol in listed_stock_symbols:
            try:
                per_stock_url = f"https://www.stockex.co.tt/manage-stock/{symbol}/"
                logging.info("Navigating to "+per_stock_url)
                news_page = requests.get(
                    per_stock_url, timeout=WEBPAGE_LOAD_TIMEOUT_SECS)
                if news_page.status_code != 200:
                    raise requests.exceptions.HTTPError(
                        "Could not load URL. "+per_stock_url)
                else:
                    logging.info("Successfully loaded webpage.")
                # set up a dict to store the data for this equity
                equity_data = dict(symbol=symbol)
                # use beautifulsoup to get the securityname, sector, status, financial year end, website
                per_stock_page_soup = BeautifulSoup(
                    news_page.text, 'lxml')
                equity_data['security_name'] = per_stock_page_soup.find(
                    text='Security:').find_parent("h2").find_next("h2").text
                equity_sector = per_stock_page_soup.find(
                    text='Sector:').find_parent("h2").find_next("h2").text.title()
                if equity_sector != 'Status:':
                    equity_data['sector'] = equity_sector
                else:
                    equity_data['sector'] = None
                equity_data['status'] = per_stock_page_soup.find(
                    text='Status:').find_parent("h2").find_next("h2").text.title()
                equity_data['financial_year_end'] = per_stock_page_soup.find(
                    text='Financial Year End:').find_parent("h2").find_next("h2").text
                website_url = per_stock_page_soup.find(
                    text='Website:').find_parent("h2").find_next("h2").text
                if website_url != 'Issuers':
                    equity_data['website_url'] = website_url
                else:
                    equity_data['website_url'] = None
                # store the currency that the stock is listed in
                if equity_data['symbol'] in USD_STOCK_SYMBOLS:
                    equity_data['currency'] = 'USD'
                else:
                    equity_data['currency'] = 'TTD'
                # get a list of tables from the URL
                dataframe_list = pd.read_html(news_page.text)
                # use pandas to get the issued share capital and market cap
                equity_data['market_capitalization'] = int(
                    float(dataframe_list[0]['Closing Price'][8]))
                equity_data['issued_share_capital'] = float(
                    re.sub('[ |$|,]', '', dataframe_list[0]['Opening Price'][8]))
                # Now we have all the important information for this equity
                # So we can add the dictionary object to our global list
                # But first we check that this symbol has not been added already
                symbol_already_added = next(
                    (item for item in all_listed_equity_data if item["symbol"] == symbol), False)
                if not symbol_already_added:
                    all_listed_equity_data.append(equity_data)
                # else don't add a duplicate equity
                logging.info("Successfully added basic listing data for: " +
                             equity_data['security_name'])
            except Exception as exc:
                logging.warning(
                    f"Could not load page for equity:{symbol}. Here's what we know: {str(exc)}")
        # set up a dataframe with all our data
        all_listed_equity_data_df = pd.DataFrame(all_listed_equity_data)
        # now find the symbol ids? used for the news page for each symbol
        logging.info('Now trying to fetch symbol ids for news')
        news_url = 'https://www.stockex.co.tt/news/'
        logging.info(f"Navigating to {news_url}")
        news_page = requests.get(
            news_url, timeout=WEBPAGE_LOAD_TIMEOUT_SECS)
        if news_page.status_code != 200:
            raise requests.exceptions.HTTPError(
                "Could not load URL. "+news_url)
        logging.info("Successfully loaded webpage.")
        # get all the options for the dropdown select, since these contain the ids
        news_page_soup = BeautifulSoup(
            news_page.text, 'lxml')
        all_symbol_mappings = news_page_soup.find(id='symbol')
        # now parse the soup and get the symbols and their ids
        symbols = []
        symbol_ids = []
        for mapping in all_symbol_mappings:
            if isinstance(mapping,Tag):
                symbol = mapping.contents[0].split()[0]
                symbol_id = mapping.attrs['value']
                if symbol and symbol_id:
                    symbols.append(symbol)
                    symbol_ids.append(symbol_id)
        # now set up a dataframe
        symbol_id_df = pd.DataFrame(list(zip(symbols, symbol_ids)), 
               columns =['symbol', 'symbol_id'])
        # merge the two dataframes
        all_listed_equity_data_df = pd.merge(all_listed_equity_data_df,symbol_id_df,
            on='symbol',how='left')
         # Now write the data to the database
        with DatabaseConnect() as db_obj:
            listed_equities_table = Table(
                'listed_equities', MetaData(), autoload=True, autoload_with=db_obj.dbengine)
            logging.debug("Inserting scraped data into listed_equities table")
            listed_equities_insert_stmt = insert(listed_equities_table).values(
                all_listed_equity_data_df.to_dict('records'))
            listed_equities_upsert_stmt = listed_equities_insert_stmt.on_duplicate_key_update(
                {x.name: x for x in listed_equities_insert_stmt.inserted})
            result = db_obj.dbcon.execute(listed_equities_upsert_stmt)
            logging.info(
                "Database update successful. Number of rows affected was "+str(result.rowcount))
        return 0
    except Exception as exc:
        logging.exception(
            f"Problem encountered while updating listed equities. Here's what we know: {str(exc)}")
        custom_logging.flush_smtp_logger()


def check_num_equities_in_sector():
    db_connection = None
    try:
        logging.info(
            "Now computing number of equities in each sector.")
        db_connection = DatabaseConnect()
        # set up the tables from the db
        listed_equities_per_sector_table = Table(
            'listed_equities_per_sector', MetaData(), autoload=True, autoload_with=db_connection.dbengine)
        # read the listedequities table into a dataframe
        listed_equities_df = pd.io.sql.read_sql(
            "SELECT sector FROM listed_equities;", db_connection.dbengine)
        # create a copy of the dataframe and drop the duplicates to get all sectors
        unique_listed_equities_df = listed_equities_df.copy().drop_duplicates()
        # get the number of times the sector occurs in the df
        listed_equities_sector_counts_df = listed_equities_df['sector'].value_counts(dropna=False
                                                                                     )
        # map the counts to the unique df
        unique_listed_equities_df['num_listed'] = unique_listed_equities_df['sector'].map(
            listed_equities_sector_counts_df)
        # get the rows that are not na
        unique_listed_equities_df = unique_listed_equities_df[unique_listed_equities_df['num_listed'].notna(
        )]
        # update the table in the db
        listed_equities_per_sector_insert_stmt = insert(
            listed_equities_per_sector_table).values(unique_listed_equities_df.to_dict('records'))
        listed_equities_per_sector_upsert_stmt = listed_equities_per_sector_insert_stmt.on_duplicate_key_update(
            {x.name: x for x in listed_equities_per_sector_insert_stmt.inserted})
        result = db_connection.dbcon.execute(
            listed_equities_per_sector_upsert_stmt)
        logging.info(
            "Database update successful. Number of rows affected was "+str(result.rowcount))
        return 0
    except Exception as exc:
        logging.exception(
            "Problem encountered while calculating number of equities in each sector."+str(exc))
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
        logging.info(
            "Now scraping historical data for all indices.")
        # This list of dicts will contain all data to be written to the db
        all_indices_data = []
        # create a list of all index ids and names to be scraped
        all_ttse_indices = [dict(name="All T&T Index", id=4), dict(name="Composite Index", id=5), dict(
            name="Cross-Listed Index", id=6), dict(name="SME Index", id=15)]
        for ttse_index in all_ttse_indices:
            index_url = f"https://www.stockex.co.tt/indices/?indexId={ttse_index['id']}"
            logging.info("Navigating to "+index_url)
            index_page = requests.get(
                index_url, timeout=WEBPAGE_LOAD_TIMEOUT_SECS)
            if index_page.status_code != 200:
                raise requests.exceptions.HTTPError(
                    f"Could not load URL: {index_page}")
            else:
                logging.info("Successfully loaded webpage.")
            # get a list of tables from the URL
            dataframe_list = pd.read_html(index_page.text)
            # get the table that holds the historical index values
            historical_index_values_df = dataframe_list[1]
            # rename the columns
            historical_index_values_df = historical_index_values_df.rename(
                columns={'Trade Date': 'date', 'Value': 'index_value', 'Change ($)': 'index_change', 'Change (%)': 'change_percent', 'Volume Traded': 'volume_traded'})
            # convert the date column
            historical_index_values_df['date'] = pd.to_datetime(
                historical_index_values_df['date'], format='%d %b %Y')
            # add a series for the index name
            historical_index_values_df['index_name'] = pd.Series(
                data=ttse_index['name'], index=historical_index_values_df.index)
            # convert the dataframe to a list of dicts and add to the large list
            all_indices_data += historical_index_values_df.to_dict('records')
        # Now write the data to the database
        db_connection = DatabaseConnect()
        historical_indices_table = Table(
            'historical_indices_info', MetaData(), autoload=True, autoload_with=db_connection.dbengine)
        logging.debug("Inserting scraped data into historical_indices table")
        insert_stmt = insert(historical_indices_table).values(
            all_indices_data)
        upsert_stmt = insert_stmt.on_duplicate_key_update(
            {x.name: x for x in insert_stmt.inserted})
        result = db_connection.dbcon.execute(upsert_stmt)
        logging.info(
            "Database update successful. Number of rows affected was "+str(result.rowcount))
        return 0
    except Exception as exc:
        logging.exception(
            f"Problem encountered while updating listed equities. Here's what we know: {str(exc)}")
        custom_logging.flush_smtp_logger()
    finally:
        if db_connection is not None:
            db_connection.close()


def scrape_dividend_data():
    """Use the requests and pandas libs to browse through 
    https://www.stockex.co.tt/manage-stock/<symbol> for each listed security
    """
    db_connection = None
    try:
        logging.info("Now trying to scrape dividend data")
        # First read all symbols from the listed_equities table
        all_listed_symbols = []
        db_connection = DatabaseConnect()
        listed_equities_table = Table(
            'listed_equities', MetaData(), autoload=True, autoload_with=db_connection.dbengine)
        selectstmt = select(
            [listed_equities_table.c.symbol])
        result = db_connection.dbcon.execute(selectstmt)
        for row in result:
            all_listed_symbols.append(row[0])
        # now get get the tables listing dividend data for each symbol
        for symbol in all_listed_symbols:
            logging.info(
                f"Now attempting to fetch dividend data for {symbol}")
            try:
                # Construct the full URL using the symbol
                equity_dividend_url = f"https://www.stockex.co.tt/manage-stock/{symbol}"
                logging.info("Navigating to "+equity_dividend_url)
                equity_dividend_page = requests.get(
                    equity_dividend_url, timeout=WEBPAGE_LOAD_TIMEOUT_SECS)
                if equity_dividend_page.status_code != 200:
                    raise requests.exceptions.HTTPError(
                        "Could not load URL. "+equity_dividend_page)
                else:
                    logging.info("Successfully loaded webpage.")
                # set up a dict to store the data for this equity
                equity_dividend_data = dict(symbol=symbol)
                # get the dataframes from the page
                dataframe_list = pd.read_html(equity_dividend_page.text)
                dividend_table = dataframe_list[1]
                # check if dividend data is present
                if len(dividend_table.index) > 1:
                    logging.info(f"Dividend data present for {symbol}")
                    # remove the columns we don't need
                    dividend_table.drop(
                        dividend_table.columns[[0, 2]], axis=1, inplace=True)
                    # set the column names
                    dividend_table.rename(
                        {'Record Date': 'record_date', 'Dividend Amount': 'dividend_amount', 'Currency': 'currency'}, axis=1, inplace=True)
                    # get the record date into the appropriate format
                    dividend_table['record_date'] = dividend_table['record_date'].map(
                        lambda x: datetime.strptime(x, '%d %b %Y'), na_action='ignore')
                    # get the dividend amount into the appropriate format
                    dividend_table['dividend_amount'] = pd.to_numeric(
                        dividend_table['dividend_amount'].str.replace('$', ''), errors='coerce')
                    # add a series for the symbol
                    dividend_table['symbol'] = pd.Series(
                        symbol, index=dividend_table.index)
                    logging.info(
                        "Successfully fetched dividend data for "+symbol)
                    # check if currency is missing from column
                    if dividend_table['currency'].isnull().values.any():
                        logging.warning(
                            f"Currency seems to be missing from the dividend table for {symbol}. We will autofill with TTD, but this may be incorrect.")
                        dividend_table['currency'].fillna(
                            'TTD', inplace=True)
                    # now write the dataframe to the db
                    logging.info(
                        f"Now writing dividend data for {symbol} to db.")
                    historical_dividend_info_table = Table(
                        'historical_dividend_info', MetaData(), autoload=True, autoload_with=db_connection.dbengine)
                    # if we had any errors, the values will be written as their defaults (0 or null)
                    # write the data to the db
                    execute_completed_successfully = False
                    execute_failed_times = 0
                    while not execute_completed_successfully and execute_failed_times < 5:
                        try:
                            insert_stmt = insert(historical_dividend_info_table).values(
                                dividend_table.to_dict('records'))
                            upsert_stmt = insert_stmt.on_duplicate_key_update(
                                {x.name: x for x in insert_stmt.inserted})
                            result = db_connection.dbcon.execute(
                                upsert_stmt)
                            execute_completed_successfully = True
                        except sqlalchemy.exc.OperationalError as operr:
                            logging.warning(str(operr))
                            time.sleep(2)
                            execute_failed_times += 1
                    logging.info(
                        "Number of rows affected in the historical_dividend_info table was "+str(result.rowcount))
                else:
                    logging.info(
                        f"No dividend data found for {symbol}. Skipping.")
            except Exception as e:
                logging.error(
                    f"Unable to scrape dividend data for {symbol}", exc_info=e)
        return 0
    except Exception as ex:
        logging.exception("Error encountered while scraping dividend data.")
        custom_logging.flush_smtp_logger()
    finally:
        # Always close the database connection
        if db_connection is not None:
            db_connection.close()
            logging.info("Successfully closed database connection")


def scrape_equity_summary_data(dates_to_fetch, all_listed_symbols):
    """
    In a new process, use the requests, beautifulsoup and pandas libs to scrape data from
    https://www.stockex.co.tt/market-quote/
    for the list of dates passed to this function.
    Gather the data into a dict, and write that dict to the DB
    """
    # declare a string to identify this PID
    pid_string = " in PID: "+str(os.getpid())
    db_connect = None
    try:
        logging.info(
            "Now opening using pandas to fetch daily summary data"+pid_string)
        # set up the field names for the tables ( we will set the date column after)
        market_summary_data_keys = ['index_name', 'index_value', 'index_change', 'change_percent',
                                    'volume_traded', 'value_traded', 'num_trades']
        daily_stock_data_keys = ['symbol', 'open_price', 'high', 'low', 'os_bid', 'os_bid_vol', 'os_offer',
                                 'os_offer_vol', 'last_sale_price', 'was_traded_today', 'volume_traded', 'close_price', 'change_dollars']
        # set up the database connection to write data to the db
        db_connect = DatabaseConnect()
        logging.debug("Successfully connected to database"+pid_string)
        # Reflect the tables already created in our db
        historical_indices_info_table = Table(
            'historical_indices_info', MetaData(), autoload=True, autoload_with=db_connect.dbengine)
        daily_stock_summary_table = Table(
            'daily_stock_summary', MetaData(), autoload=True, autoload_with=db_connect.dbengine)
        listed_equities_table = Table(
            'listed_equities', MetaData(), autoload=True, autoload_with=db_connect.dbengine)
        # now fetch the data at each url(each market trading date)
        for index, fetch_date in enumerate(dates_to_fetch):
            try:
                logging.info(
                    f"Now loading page {str(index)} of {str(len(dates_to_fetch))} {pid_string}")
                # get a date object suitable for the db
                fetch_date_db = datetime.strptime(fetch_date, '%Y-%m-%d')
                # for each date, we need to navigate to this summary page for that day
                url_summary_page = f"https://www.stockex.co.tt/market-quote/?TradeDate={fetch_date}"
                logging.info(
                    f"Navigating to {url_summary_page} {pid_string}")
                http_get_req = requests.get(
                    url_summary_page, timeout=WEBPAGE_LOAD_TIMEOUT_SECS)
                if http_get_req.status_code != 200:
                    raise requests.exceptions.HTTPError(
                        "Could not load URL. "+url_summary_page+pid_string)
                else:
                    logging.info("Successfully loaded webpage.")
                # get a list of tables from the URL
                dataframe_list = pd.read_html(http_get_req.text)
                # if this is a valid trading day, extract the values we need from the tables
                if len(dataframe_list[00].index) > 4:
                    logging.info("This is a valid trading day.")
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
                        market_indices_table.columns[0], axis=1, inplace=True)
                    # set the names of columns
                    market_indices_table.columns = market_summary_data_keys
                    # remove all the '-' in the dataframe
                    market_indices_table.replace('–', None, inplace=True)
                    # set the datatype of the columns
                    market_indices_table['index_name'] = market_indices_table['index_name'].astype(
                        str).str.title()
                    market_indices_table['index_value'] = pd.to_numeric(
                        market_indices_table['index_value'], errors='coerce')
                    market_indices_table['index_change'] = pd.to_numeric(
                        market_indices_table['index_change'], errors='coerce')
                    market_indices_table['change_percent'] = pd.to_numeric(
                        market_indices_table['change_percent'], errors='coerce')
                    market_indices_table['volume_traded'] = pd.to_numeric(
                        market_indices_table['volume_traded'], errors='coerce')
                    market_indices_table['value_traded'] = pd.to_numeric(
                        market_indices_table['value_traded'], errors='coerce')
                    market_indices_table['num_trades'] = pd.to_numeric(
                        market_indices_table['num_trades'], errors='coerce')
                    # add a series containing the date
                    market_indices_table['date'] = pd.Series(
                        fetch_date_db, index=market_indices_table.index)
                    market_indices_table.fillna(0, inplace=True)
                    # now write the dataframe to the db
                    logging.info(
                        "Finished wrangling market indices data. Now writing to db.")
                    # if we had any errors, the values will be written as their defaults (0 or null)
                    # wrote the data to the db
                    execute_completed_successfully = False
                    execute_failed_times = 0
                    while not execute_completed_successfully and execute_failed_times < 5:
                        try:
                            insert_stmt = insert(historical_indices_info_table).values(
                                market_indices_table.to_dict('records'))
                            upsert_stmt = insert_stmt.on_duplicate_key_update(
                                {x.name: x for x in insert_stmt.inserted})
                            result = db_connect.dbcon.execute(
                                upsert_stmt)
                            execute_completed_successfully = True
                            logging.info("Successfully scraped and wrote to db market indices data for " +
                                 fetch_date+pid_string)
                            logging.info(
                                "Number of rows affected in the historical_indices_summary table was "+str(result.rowcount)+pid_string)
                        except sqlalchemy.exc.OperationalError as operr:
                            logging.warning(str(operr))
                            time.sleep(2)
                            execute_failed_times += 1
                    # now lets try to wrangle the daily data for stocks
                    all_daily_stock_data = []
                    for shares_table in [ordinary_shares_table, preference_shares_table, second_tier_shares_table, mutual_funds_shares_table,
                                         sme_shares_table, usd_equity_shares_table]:
                        if not shares_table.empty:
                            # remove the first row from each table since its a header row
                            shares_table.drop(shares_table.index[0])
                            # remove the column with the up and down symbols
                            shares_table.drop(
                                shares_table.columns[0], axis=1, inplace=True)
                            # set the names of columns
                            shares_table.columns = daily_stock_data_keys
                            # remove the unneeded characters from the symbols
                            # note that these characters come after a space
                            shares_table['symbol'] = shares_table['symbol'].str.split(
                                " ", 1).str.get(0)
                            # replace the last sale date with a boolean
                            # if the last sale date is the current date being queried, return 1, else return 0
                            shares_table['was_traded_today'] = shares_table['was_traded_today'].map(lambda x: 1 if (
                                datetime.strptime(x, '%d-%m-%Y') == datetime.strptime(fetch_date, '%Y-%m-%d')) else 0, na_action='ignore')
                            # set the datatype of the columns
                            shares_table['open_price'] = pd.to_numeric(
                                shares_table['open_price'], errors='coerce')
                            shares_table['high'] = pd.to_numeric(
                                shares_table['high'], errors='coerce')
                            shares_table['low'] = pd.to_numeric(
                                shares_table['low'], errors='coerce')
                            shares_table['os_bid'] = pd.to_numeric(
                                shares_table['os_bid'], errors='coerce')
                            shares_table['os_bid_vol'] = pd.to_numeric(
                                shares_table['os_bid_vol'], errors='coerce')
                            shares_table['os_offer'] = pd.to_numeric(
                                shares_table['os_offer'], errors='coerce')
                            shares_table['os_offer_vol'] = pd.to_numeric(
                                shares_table['os_offer_vol'], errors='coerce')
                            shares_table['last_sale_price'] = pd.to_numeric(
                                shares_table['last_sale_price'], errors='coerce')
                            shares_table['volume_traded'] = pd.to_numeric(
                                shares_table['volume_traded'], errors='coerce')
                            shares_table['close_price'] = pd.to_numeric(
                                shares_table['close_price'], errors='coerce')
                            shares_table['change_dollars'] = pd.to_numeric(
                                shares_table['change_dollars'], errors='coerce')
                            # if the high and low columns are 0, replace them with the open price
                            shares_table['high'] = shares_table.apply(lambda x: x.open_price if (
                                pd.isna(x.high)) else x.high, axis=1)
                            shares_table['low'] = shares_table.apply(lambda x: x.open_price if (
                                pd.isna(x.low)) else x.low, axis=1)
                            # replace certain column null values with 0
                            shares_table['change_dollars'].fillna(
                                0, inplace=True)
                            shares_table['volume_traded'].fillna(
                                0, inplace=True)
                            shares_table['os_bid_vol'].fillna(
                                0, inplace=True)
                            shares_table['os_offer_vol'].fillna(
                                0, inplace=True)
                            # create a series for the value traded
                            value_traded_series = pd.Series(
                                0, index=shares_table.index).astype(float)
                            # set the name of the series
                            value_traded_series.rename("value_traded")
                            # add the series to the dateframe
                            shares_table = shares_table.assign(
                                value_traded=value_traded_series)
                            # calculate the value traded for today
                            shares_table['value_traded'] = shares_table.apply(
                                lambda x: x.volume_traded * x.last_sale_price, axis=1)
                            # create a series containing the date
                            date_series = pd.Series(
                                fetch_date_db, index=shares_table.index)
                            # set the name of the series
                            date_series.rename("date")
                            # add the series to the dateframe
                            shares_table = shares_table.assign(
                                date=date_series)
                            # replace the nan with None
                            shares_table = shares_table.replace({np.nan: None})
                            # add all values to the large list
                            all_daily_stock_data += shares_table.to_dict(
                                'records')
                    # now insert the data into the db
                    execute_completed_successfully = False
                    execute_failed_times = 0
                    while not execute_completed_successfully and execute_failed_times < 5:
                        try:
                            insert_stmt = insert(
                                daily_stock_summary_table).values(all_daily_stock_data)
                            upsert_stmt = insert_stmt.on_duplicate_key_update(
                                {x.name: x for x in insert_stmt.inserted})
                            result = db_connect.dbcon.execute(
                                upsert_stmt)
                            execute_completed_successfully = True
                            logging.info("Successfully scraped and wrote to db daily equity/shares data for " +
                                fetch_date+pid_string)
                            logging.info(
                                "Number of rows affected in the daily_stock_summary table was "+str(result.rowcount)+pid_string)
                        except sqlalchemy.exc.OperationalError as operr:
                            logging.warning(str(operr))
                            time.sleep(2)
                            execute_failed_times += 1
                else:
                    logging.warning(
                        "This date is not a valid trading date: "+fetch_date+pid_string)
            except KeyError as keyerr:
                logging.warning(
                    "Could not find a required key on date "+fetch_date+pid_string+str(keyerr))
            except IndexError as idxerr:
                logging.warning(
                    "Could not locate index in a list. "+fetch_date+pid_string+str(idxerr))
            except requests.exceptions.Timeout as timeerr:
                logging.error(
                    "Could not load URL in time. Maybe website is down? "+fetch_date+pid_string, exc_info=timeerr)
            except requests.exceptions.HTTPError as httperr:
                logging.error("HTTP Error!", exc_info=httperr)
        return 0
    except Exception:
        logging.exception(
            "Could not complete historical_indices_summary and daily_stock_summary update."+pid_string)
        custom_logging.flush_smtp_logger()
    finally:
        # Always close the database connection
        if db_connect is not None:
            db_connect.close()
            logging.info("Successfully closed database connection"+pid_string)


def update_equity_summary_data(start_date):
    """ 
    Create the list of dates that we need to scrape data from https://www.stockex.co.tt/market-quote/
    for, based on the start_date specified and the dates already in the historical_indices_info table
    """
    logging.info("Now updating daily market summary data.")
    db_connect = None
    try:
        db_connect = DatabaseConnect()
        logging.info("Successfully connected to database.")
        # Reflect the tables already created in our db
        logging.info("Reading existing data from tables in database...")
        historical_indices_info_table = Table(
            'historical_indices_info', MetaData(), autoload=True, autoload_with=db_connect.dbengine)
        listed_equities_table = Table(
            'listed_equities', MetaData(), autoload=True, autoload_with=db_connect.dbengine)
        # Now get the dates that we already have recorded (from the historical indices table)
        logging.info("Creating list of dates to fetch.")
        dates_already_recorded = []
        select_stmt = select([historical_indices_info_table.c.date])
        result = db_connect.dbcon.execute(select_stmt)
        for row in result:
            # We only have a single element in each row tuple, which is the date
            dates_already_recorded.append(row[0])
        # Also get a list of all valid symbols from the db
        all_listed_symbols = []
        select_stmt = select([listed_equities_table.c.symbol])
        result = db_connect.dbcon.execute(select_stmt)
        for row in result:
            # We only have a single element in each row tuple, which is the symbol
            all_listed_symbols.append(row[0])
        # We want to gather data on all trading days since the start date, so we create a list
        # of all dates that we need to gather still
        dates_to_fetch = []
        fetch_date = datetime.strptime(start_date, "%Y-%m-%d")
        logging.info(
            "Getting all dates that are not already fetched and are not weekends.")
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
        logging.info(
            "List of dates to fetch built. Now splitting list by core.")
        num_cores = multiprocessing.cpu_count()
        logging.info("This machine has "+str(num_cores)+" logical CPU cores.")
        list_length = len(dates_to_fetch)
        dates_to_fetch_sublists = [dates_to_fetch[i*list_length // num_cores: (i+1)*list_length // num_cores]
                                   for i in range(num_cores)]
        logging.info(
            "Lists split successfully.")
        return dates_to_fetch_sublists, all_listed_symbols
    except Exception as ex:
        logging.exception(
            f"We ran into a problem while trying to build the list of dates to scrape market summary data for. Here's what we know: {str(ex)}")
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
        today_date = datetime.now().strftime('%Y-%m-%d')
        db_connect = DatabaseConnect()
        logging.info("Successfully connected to database")
        logging.info(
            f"Now opening using pandas to fetch daily shares data for today ({today_date})")
        daily_shares_data_keys = ['symbol', 'open_price', 'high', 'low', 'os_bid', 'os_bid_vol', 'os_offer',
                                  'os_offer_vol', 'last_sale_price', 'was_traded_today', 'volume_traded', 'close_price', 'change_dollars']
        # load the daily summary table
        daily_stock_summary_table = Table(
            'daily_stock_summary', MetaData(), autoload=True, autoload_with=db_connect.dbengine)
        urlsummarypage = f"https://www.stockex.co.tt/market-quote/?TradeDate={today_date}"
        logging.info("Navigating to "+urlsummarypage)
        http_get_req = requests.get(urlsummarypage, timeout=10)
        if http_get_req.status_code != 200:
            raise requests.exceptions.HTTPError(
                "Could not load URL to update latest daily data "+urlsummarypage)
        else:
            logging.info("Successfully loaded webpage.")
         # get a list of tables from the URL
        dataframe_list = pd.read_html(http_get_req.text)
        # if this is a valid trading day, extract the values we need from the tables
        if len(dataframe_list[00].index) == 8:
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
            all_daily_stock_data = []
            for shares_table in [ordinary_shares_table, preference_shares_table, second_tier_shares_table, mutual_funds_shares_table,
                                 sme_shares_table, usd_equity_shares_table]:
                # remove the first row from each table since its a header row
                shares_table.drop(shares_table.index[0])
                if not shares_table.empty:
                    # remove the column with the up and down symbols
                    shares_table.drop(
                        shares_table.columns[0], axis=1, inplace=True)
                    # set the names of columns
                    shares_table.columns = daily_shares_data_keys
                    # remove the unneeded characters from the symbols
                    # note that these characters come after a space
                    shares_table['symbol'] = shares_table['symbol'].str.split(
                        " ", 1).str.get(0)
                    # replace the last sale date with a boolean
                    # if the last sale date is the current date being queried, return 1, else return 0
                    shares_table['was_traded_today'] = shares_table['was_traded_today'].map(lambda x: 1 if (
                        datetime.strptime(x, '%d-%m-%Y') == datetime.strptime(today_date, '%Y-%m-%d')) else 0, na_action='ignore')
                    # set the datatype of the columns
                    shares_table['open_price'] = pd.to_numeric(
                        shares_table['open_price'], errors='coerce')
                    shares_table['high'] = pd.to_numeric(
                        shares_table['high'], errors='coerce')
                    shares_table['low'] = pd.to_numeric(
                        shares_table['low'], errors='coerce')
                    shares_table['os_bid'] = pd.to_numeric(
                        shares_table['os_bid'], errors='coerce')
                    shares_table['os_bid_vol'] = pd.to_numeric(
                        shares_table['os_bid_vol'], errors='coerce')
                    shares_table['os_offer'] = pd.to_numeric(
                        shares_table['os_offer'], errors='coerce')
                    shares_table['os_offer_vol'] = pd.to_numeric(
                        shares_table['os_offer_vol'], errors='coerce')
                    shares_table['last_sale_price'] = pd.to_numeric(
                        shares_table['last_sale_price'], errors='coerce')
                    shares_table['volume_traded'] = pd.to_numeric(
                        shares_table['volume_traded'], errors='coerce')
                    shares_table['close_price'] = pd.to_numeric(
                        shares_table['close_price'], errors='coerce')
                    shares_table['change_dollars'] = pd.to_numeric(
                        shares_table['change_dollars'], errors='coerce')
                    # if the high and low columns are 0, replace them with the open price
                    shares_table['high'] = shares_table.apply(lambda x: x.open_price if (
                        pd.isna(x.high)) else x.high, axis=1)
                    shares_table['low'] = shares_table.apply(lambda x: x.open_price if (
                        pd.isna(x.low)) else x.low, axis=1)
                    # replace certain column null values with 0
                    shares_table['change_dollars'].fillna(
                        0, inplace=True)
                    shares_table['volume_traded'].fillna(
                        0, inplace=True)
                    shares_table['os_bid_vol'].fillna(
                        0, inplace=True)
                    shares_table['os_offer_vol'].fillna(
                        0, inplace=True)
                    # create a series for the value traded
                    value_traded_series = pd.Series(
                        0, index=shares_table.index).astype(float)
                    # set the name of the series
                    value_traded_series.rename("value_traded")
                    # add the series to the dateframe
                    shares_table = shares_table.assign(
                        value_traded=value_traded_series)
                    # calculate the value traded for today
                    shares_table['value_traded'] = shares_table.apply(
                        lambda x: x.volume_traded * x.last_sale_price, axis=1)
                    # create a series containing the date
                    date_series = pd.Series(
                        datetime.strptime(today_date, '%Y-%m-%d'), index=shares_table.index)
                    # set the name of the series
                    date_series.rename("date")
                    # add the series to the dateframe
                    shares_table = shares_table.assign(
                        date=date_series)
                    # replace the nan with None
                    shares_table = shares_table.replace({np.nan: None})
                    # add all values to the large list
                    all_daily_stock_data += shares_table.to_dict(
                        'records')
            # now insert the data into the db
            execute_completed_successfully = False
            execute_failed_times = 0
            while not execute_completed_successfully and execute_failed_times < 5:
                try:
                    insert_stmt = insert(
                        daily_stock_summary_table).values(all_daily_stock_data)
                    upsert_stmt = insert_stmt.on_duplicate_key_update(
                        {x.name: x for x in insert_stmt.inserted})
                    result = db_connect.dbcon.execute(
                        upsert_stmt)
                    execute_completed_successfully = True
                    logging.info(
                        "Successfully scraped and wrote to db daily equity/shares data for daily trades.")
                    logging.info(
                        "Number of rows affected in the daily_stock_summary table was "+str(result.rowcount))
                except sqlalchemy.exc.OperationalError as operr:
                    logging.warning(str(operr))
                    time.sleep(2)
                    execute_failed_times += 1
        else:
            logging.warning("No data found for today.")
        return 0
    except Exception:
        logging.exception("Could not load daily data for today!")
    finally:
        # Always close the database connection
        if db_connect is not None:
            db_connect.close()
            logging.info("Successfully closed database connection")


def update_technical_analysis_data():
    """
    Calculate/scrape the data needed for the technical_analysis_summary table
    """
    db_connect = None
    try:
        db_connect = DatabaseConnect()
        logging.info("Successfully connected to database")
        logging.info(
            "Now using pandas to fetch latest technical analysis data from https://www.stockex.co.tt/manage-stock/")
        # load the tables that we require
        listed_equities_table = Table(
            'listed_equities', MetaData(), autoload=True, autoload_with=db_connect.dbengine)
        technical_analysis_summary_table = Table(
            'technical_analysis_summary', MetaData(), autoload=True, autoload_with=db_connect.dbengine)
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
                logging.info("Navigating to "+stock_summary_page +
                             " to fetch technical summary data.")
                http_get_req = requests.get(
                    stock_summary_page, timeout=WEBPAGE_LOAD_TIMEOUT_SECS)
                if http_get_req.status_code != 200:
                    raise requests.exceptions.HTTPError(
                        "Could not load URL "+stock_summary_page)
                else:
                    logging.info("Successfully loaded webpage.")
                # get a list of tables from the URL
                dataframe_list = pd.read_html(http_get_req.text)
                # table 2 contains the data we need
                technical_analysis_table = dataframe_list[0]
                # create a dict to hold the data that we are interested in
                stock_technical_data = dict(symbol=symbol)
                # fill all the nan values with 0s
                technical_analysis_table.fillna(0, inplace=True)
                # get the values that we are interested in from the table
                stock_technical_data['last_close_price'] = float(
                    technical_analysis_table['Closing Price'][0].replace('$', ''))
                stock_technical_data['high_52w'] = float(
                    technical_analysis_table['Change'][4].replace('$', ''))
                # leaving out the 52w low because it is not correct from the ttse site
                #stock_technical_data['low_52w'] = float(
                #    technical_analysis_table['Change%'][4].replace('$', ''))
                stock_technical_data['wtd'] = float(
                    technical_analysis_table['Opening Price'][6].replace('%', ''))
                stock_technical_data['mtd'] = float(
                    technical_analysis_table['Closing Price'][6].replace('%', ''))
                stock_technical_data['ytd'] = float(
                    technical_analysis_table['Change%'][6].replace('%', ''))
                # calculate our other required values
                # first calculate the SMAs
                # calculate sma20
                closing_quotes_last_20d_df = pd.io.sql.read_sql(
                    f"SELECT date,close_price FROM daily_stock_summary WHERE symbol='{symbol}' order by date desc limit 20;", db_connect.dbengine)
                sma20_df = closing_quotes_last_20d_df.rolling(window=20).mean()
                # get the last row value
                stock_technical_data['sma_20'] = sma20_df['close_price'].iloc[-1]
                # calculate sma200
                closing_quotes_last200d_df = pd.io.sql.read_sql(
                    f"SELECT date,close_price FROM daily_stock_summary WHERE symbol='{symbol}' order by date desc limit 200;", db_connect.dbengine)
                sma200_df = closing_quotes_last200d_df.rolling(
                    window=200).mean()
                # get the last row value
                stock_technical_data['sma_200'] = sma200_df['close_price'].iloc[-1]
                # calculate beta
                # first get the closing prices and change dollars for this stock for the last year
                stock_change_df = pd.io.sql.read_sql(
                    f"SELECT close_price,change_dollars FROM daily_stock_summary WHERE symbol='{symbol}' order by date desc limit 365;", db_connect.dbengine)
                # using apply function to create a new column for the stock percent change
                stock_change_df['change_percent'] = (
                    stock_change_df['change_dollars'] * 100) / stock_change_df['close_price']
                # get the market percentage change
                market_change_df = pd.io.sql.read_sql(
                    f"SELECT change_percent FROM historical_indices_info WHERE index_name='Composite Totals' order by date desc limit 365;", db_connect.dbengine)
                # now calculate the beta
                stock_change_df['beta'] = (stock_change_df['change_percent'].rolling(window=365).cov(
                    other=market_change_df['change_percent'])) / market_change_df['change_percent'].rolling(window=365).var()
                # store the beta
                stock_technical_data['beta'] = stock_change_df['beta'].iloc[-1]
                # now calculate the adtv
                volume_traded_df = pd.io.sql.read_sql(
                    f"SELECT volume_traded FROM daily_stock_summary WHERE symbol='{symbol}' order by date desc limit 30;", db_connect.dbengine)
                adtv_df = volume_traded_df.rolling(window=30).mean()
                stock_technical_data['adtv'] = adtv_df['volume_traded'].iloc[-1]
                # calculate the 52w low
                stock_technical_data['low_52w'] = stock_change_df['close_price'].min()
                # filter out nan from this dict
                for key in stock_technical_data:
                    if pd.isna(stock_technical_data[key]):
                        stock_technical_data[key] = None
                # add our dict for this stock to our large list
                all_technical_data.append(stock_technical_data)
            except KeyError as keyerr:
                logging.warning(
                    "Could not find a required key "+str(keyerr))
            except IndexError as idxerr:
                logging.warning(
                    "Could not locate index in a list. "+str(idxerr))
        # now insert the data into the db
        execute_completed_successfully = False
        execute_failed_times = 0
        logging.info("Now trying to insert data into database.")
        while not execute_completed_successfully and execute_failed_times < 5:
            try:
                technical_analysis_summary_insert_stmt = insert(
                    technical_analysis_summary_table).values(all_technical_data)
                technical_analysis_summary_upsert_stmt = technical_analysis_summary_insert_stmt.on_duplicate_key_update(
                    {x.name: x for x in technical_analysis_summary_insert_stmt.inserted})
                result = db_connect.dbcon.execute(
                    technical_analysis_summary_upsert_stmt)
                execute_completed_successfully = True
            except sqlalchemy.exc.OperationalError as operr:
                logging.warning(str(operr))
                time.sleep(1)
                execute_failed_times += 1
            logging.info(
                "Successfully scraped and wrote to db technical summary data.")
            logging.info(
                "Number of rows affected in the technical analysis summary table was "+str(result.rowcount))
        return 0
    except requests.exceptions.Timeout as timeerr:
        logging.error(
            "Could not load URL in time. Maybe website is down? "+str(timeerr))
    except requests.exceptions.HTTPError as httperr:
        logging.error(str(httperr))
    except Exception:
        logging.exception(
            "Could not complete technical analysis summary data update.")
        custom_logging.flush_smtp_logger()
    finally:
        # Always close the database connection
        if db_connect is not None:
            db_connect.close()
            logging.info("Successfully closed database connection")


def main(args):
    """The main steps in coordinating the scraping"""
    try:
        # Set up logging for this module
        q_listener, q = custom_logging.setup_logging(
            logdirparent=str(os.path.dirname(os.path.realpath(__file__))),
            logfilestandardname=os.path.basename(__file__),
            stdoutlogginglevel=logging.DEBUG,
            smtploggingenabled=True,
            smtplogginglevel=logging.ERROR,
            smtpmailhost='localhost',
            smtpfromaddr='server1@trinistats.com',
            smtptoaddr=['latchmepersad@gmail.com'],
            smtpsubj='Automated report from Python script: '+os.path.basename(__file__))
        # Set up a pidfile to ensure that only one instance of this script runs at a time
        with PidFile(piddir=tempfile.gettempdir()):
            # set the start date based on the the full history option
            if args.full_history:
                start_date = START_DATE
            else:
                start_date = (datetime.now() +
                              relativedelta(months=-1)).strftime('%Y-%m-%d')
            # run all functions within a multiprocessing pool
            with multiprocessing.Pool(os.cpu_count(), custom_logging.logging_worker_init, [q]) as multipool:
                logging.info("Now starting TTSE scraper.")
                # check if this is the daily update (run inside the trading day)
                if args.daily_update:
                    multipool.apply_async(
                        update_daily_trades, ())
                else:
                    # else this is a full update (run once a day)
                    multipool.apply_async(scrape_listed_equity_data, ())
                    multipool.apply_async(check_num_equities_in_sector, ())
                    multipool.apply_async(scrape_dividend_data, ())
                    # block on the next function to wait until the dates are ready
                    dates_to_fetch_sublists, all_listed_symbols = multipool.apply(
                        update_equity_summary_data, (start_date,))
                    # now call the individual workers to fetch these dates
                    async_results = []
                    for core_date_list in dates_to_fetch_sublists:
                        async_results.append(multipool.apply_async(
                            scrape_equity_summary_data, (core_date_list, all_listed_symbols)))
                    # update tShe technical analysis stock data
                    multipool.apply_async(update_technical_analysis_data, ())
                    # wait until all workers finish fetching data before continuing
                    for result in async_results:
                        result.wait()
                multipool.close()
                multipool.join()
                logging.info(os.path.basename(__file__) +
                             " executed successfully.")
                q_listener.stop()
                return 0
    except Exception as exc:
        logging.error(
            f"Error in script {os.path.basename(__file__)}", exc_info=exc)
        custom_logging.flush_smtp_logger()


# If this script is being run from the command-line, then run the main() function
if __name__ == "__main__":
    # first check the arguements given to this script
    parser = argparse.ArgumentParser()
    parser.add_argument("-f",
                        "--full_history", help="Record all data from 2010 to now", action="store_true")
    parser.add_argument("-d",
                        "--daily_update", help="Only update data for the daily summary for today", action="store_true")
    args = parser.parse_args()
    main(args)
