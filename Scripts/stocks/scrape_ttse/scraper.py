#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
This module is used to scrape the Trinidad and Tobago Stock Exchange at https://stockex.co.tt/
:returns: 0 if successful
:raises Exception if any issues are encountered
"""

# Put all your imports here, one per line.
# However multiple imports from the same lib are allowed on a line.
# Imports from Python standard libraries
import sys
import logging
import os
from pathlib import Path
from datetime import datetime, timedelta
from dateutil.parser import parse
from dateutil.relativedelta import relativedelta
import csv
import shutil
from decimal import Decimal
import argparse
import json
import getopt
import multiprocessing
import time
import tempfile
import re

# Imports from the cheese factory
from pid import PidFile
import requests
import urllib.request
import urllib.parse
from sqlalchemy import create_engine, Table, select, MetaData, text, and_
from sqlalchemy.dialects.mysql import insert
import sqlalchemy.exc
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from selenium.common.exceptions import WebDriverException
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup

# Imports from the local filesystem
import ttsescraperconfig
import customlogging

# Put your constants here. These should be named in CAPS

# Most stocks on the TTSE have prices and dividends listed in TTD. These are the exceptions.
# The following stocks have both prices and dividends listed in USD on the exchange
USD_STOCK_SYMBOLS = ['MPCCEL']
# These have prices in TTD, but dividends in USD
USD_DIVIDEND_SYMBOLS = ['SFC','FCI']
# These have prices listed in TTD, but dividends in JMD
JMD_DIVIDEND_SYMBOLS = ['GKC','JMMBGL','NCBFG']
# These have prices in TTD, but dividends in BBD
BBD_DIVIDEND_SYMBOLS = ['CPFV']
# The timeout to set for multiprocessing tasks (in seconds)
MULTIPROCESSING_TIMEOUT = 60*60
WEBPAGE_LOAD_TIMEOUT_SECS = 30

# Put your class definitions here. These should use the CapWords convention.


class DatabaseConnect:
    """
    Manages connections the the backend MySQL database
    """

    dbcon = None
    dbengine = None

    def __init__(self,):
        logging.debug("Creating a new DatabaseConnect object.")
        # Get the required login info from our config file
        dbuser = ttsescraperconfig.dbusername
        dbpass = ttsescraperconfig.dbpassword
        dbaddress = ttsescraperconfig.dbaddress
        dbschema = ttsescraperconfig.schema
        self.dbengine = create_engine("mysql://"+dbuser+":"+dbpass+"@"+dbaddress+"/" +
                                      dbschema, echo=False)
        self.dbcon = self.dbengine.connect()
        if self.dbcon:
            logging.debug("Connected to database successfully")
        else:
            raise ConnectionError(
                "Could not connect to database at "+dbaddress)

    def close(self,):
        """
        Close the database connection
        """
        if self.dbengine:
            self.dbengine.dispose()


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
                per_stock_page = requests.get(
                    per_stock_url, timeout=WEBPAGE_LOAD_TIMEOUT_SECS)
                if per_stock_page.status_code != 200:
                    raise requests.exceptions.HTTPError(
                        "Could not load URL. "+per_stock_url)
                else:
                    logging.info("Successfully loaded webpage.")
                # set up a dict to store the data for this equity
                equity_data = dict(symbol=symbol)
                # use beautifulsoup to get the securityname, sector, status, financial year end, website
                per_stock_page_soup = BeautifulSoup(
                    per_stock_page.text, 'lxml')
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
                dataframe_list = pd.read_html(per_stock_page.text)
                # use pandas to get the issued share capital and market cap
                equity_data['market_capitalization'] = int(
                    float(dataframe_list[0]['Opening Price'][8]))
                equity_data['issued_share_capital'] = float(
                    re.sub('[ |$|,]', '', dataframe_list[0]['Closing Price'][8]))
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
        # Now write the data to the database
        db_connection = DatabaseConnect()
        listed_equities_table = Table(
            'listed_equities', MetaData(), autoload=True, autoload_with=db_connection.dbengine)
        logging.debug("Inserting scraped data into listed_equities table")
        listed_equities_insert_stmt = insert(listed_equities_table).values(
            all_listed_equity_data)
        listed_equities_upsert_stmt = listed_equities_insert_stmt.on_duplicate_key_update(
            {x.name: x for x in listed_equities_insert_stmt.inserted})
        result = db_connection.dbcon.execute(listed_equities_upsert_stmt)
        logging.info(
            "Database update successful. Number of rows affected was "+str(result.rowcount))
        return 0
    except Exception as exc:
        logging.exception(
            f"Problem encountered while updating listed equities. Here's what we know: {str(exc)}")
        customlogging.flush_smtp_logger()
    finally:
        if 'db_connection' in locals() and db_connection is not None:
            db_connection.close()


def check_num_equities_in_sector():
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
        customlogging.flush_smtp_logger()
    finally:
        if 'db_connection' in locals() and db_connection is not None:
            db_connection.close()


def scrape_historical_indices_data():
    """Use the requests and pandas libs to fetch data for all indices at 
    https://www.stockex.co.tt/indices/
    and scrape the useful output into a list of dictionaries to write to the db
    """
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
        customlogging.flush_smtp_logger()
    finally:
        if 'db_connection' in locals() and db_connection is not None:
            db_connection.close()


def scrape_dividend_data():
    """Use the requests and pandas libs to browse through 
    https://www.stockex.co.tt/manage-stock/<symbol> for each listed security
    """
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
        customlogging.flush_smtp_logger()
    finally:
        # Always close the database connection
        if 'db_connection' in locals() and db_connection is not None:
            db_connection.close()
            logging.info("Successfully closed database connection")


def scrape_historical_data():
    """Use the Selenium module to open the Chrome browser and browse through
    all listed equity securities at the URL
    https://www.stockex.co.tt/controller.php?action=listed_companies
    For each listed security, press the history button and get
    all data from 2010 to the current date
    """
    try:
        # This list of dicts will contain all data to be written to the db
        allhistoricaldata = []
        # First open the browser
        logging.info(
            "Now opening the Chrome browser to fetch historical stock data.")
        options = Options()
        options.headless = True
        driver = webdriver.Chrome(options=options)
        # Then go the stockex.co.tt URL that lists all equities
        logging.debug(
            "Navigating to https://stockex.co.tt/controller.php?action=listed_companies")
        driver.get("https://stockex.co.tt/controller.php?action=listed_companies")
        # All equities are wrapped in an 'a' tag, so we filter all of these elements
        # into an array
        pagelinks = driver.find_elements_by_tag_name("a")
        listedstockcodes = []
        # For each 'a' element that we found
        for link in pagelinks:
            # All valid equities contain the StockCode keyword in their href
            if "StockCode" in link.get_attribute("href"):
                # Instead of loading this main URL, we simply get the Stock Code for
                # each equity
                stockurl = link.get_attribute("href")
                stockcode = stockurl.split("StockCode=", 1)[1]
                listedstockcodes.append(stockcode)
        # Now we go through each equity in our list and navigate to their historical page
        logging.info("Found links for " +
                     str(len(listedstockcodes))+" equities.")
        logging.info("Now downloading the historical data for each.")
        for stockcode in listedstockcodes:
            logging.info(
                "Now trying to fetch historical data for stock code:"+stockcode)
            try:
                # Construct the full URL using the stock code
                equityhistoryurl = "https://www.stockex.co.tt/controller.php?action=view_stock_history&StockCode=" + stockcode
                # Load the URL for this equity
                driver.get(equityhistoryurl)
                # Check if there is an alert
                try:
                    WebDriverWait(driver, 3).until(EC.alert_is_present(),
                                                   'Timed out waiting for PA creation ' +
                                                   'confirmation popup to appear.')
                    # Dismiss the alert asking you to download flash player
                    driver.switch_to.alert.dismiss()
                except TimeoutException:
                    pass
                # First find the field to enter the start date
                startdateinputfield = driver.find_element_by_id("StartDate")
                startdateinputfield.clear()
                # Then enter 01/01/2010 in the start date field
                startdateinputfield.send_keys(START_DATE)
                # Now find the field to enter the ending date
                enddateinputfield = driver.find_element_by_id("EndDate")
                enddateinputfield.clear()
                # Then enter the ending date as today
                currentdate = f"{datetime.now():%m/%d/%Y}"
                enddateinputfield.send_keys(str(currentdate))
                # Now find the submit button and click it
                # Note that the page has several submit buttons for some reason
                submitbuttons = driver.find_elements_by_name("Submit")
                for button in submitbuttons:
                    if button.get_attribute("value") == "Submit":
                        button.click()
                # Now we are going to download the csv containing this data
                # Set the download location
                historicaldatafile = Path(
                    tempfile.gettempdir(), "historicaldata.csv")
                # Delete the file if it exists
                try:
                    os.remove(historicaldatafile)
                except OSError:
                    pass
                # The link we want to find is wrapped in an "a" tag
                importantelements = driver.find_elements_by_tag_name("a")
                for importantelement in importantelements:
                    if importantelement.text == "Download CSV":
                        downloadURL = importantelement.get_attribute("href")
                        # Now actually download the file to this location
                        with urllib.request.urlopen(downloadURL) as response, open(historicaldatafile, 'wb') as outfile:
                            shutil.copyfileobj(response, outfile)
                        break
                # Create some dictionaries to store our data
                stockhistorydates = []
                closingquotes = []
                changedollars = []
                volumetraded = []
                # Now open our downloaded file to read and parse the contents
                with open(historicaldatafile, 'r') as filestream:
                    csvreader = csv.reader(filestream)
                    for csvline in csvreader:
                        # Each comma-separated value represents something to add to our list
                        stockhistorydates.append(csvline[0])
                        closingquotes.append(csvline[1])
                        changedollars.append(csvline[2])
                        volumetraded.append(csvline[4])
                # Now we have all the important historical information for this equity
                # So we can add the lists to our global dictionary
                # But first we need to get the symbol for this security
                symbolelement = driver.find_element_by_xpath(
                    "//*[contains(text(), 'Symbol :')]")
                # Find the parent of this element, which contains the actual symbol
                symbolelement = symbolelement.find_element_by_xpath("./..")
                symbol = symbolelement.text.split(": ", 1)[1]
                # Check if data for this symbol has already been added
                symbolalreadyadded = False
                for historicaldata in allhistoricaldata:
                    if historicaldata['symbol'] == symbol:
                        symbolalreadyadded = True
                # If we have not already added data for this symbol, then we continue
                if not symbolalreadyadded:
                    # Now create our list of dicts containing all of the dividends
                    for index, date in enumerate(stockhistorydates):
                        # For each record, check that our date is valid
                        try:
                            parseddate = parse(date, fuzzy=True).date()
                            # Also remove any commas in the volume traded
                            # and try cast to an int
                            volumetraded[index] = int(
                                volumetraded[index].replace(",", ""))
                            # If the parse does not throw an errors, continue
                            historicaldata = {'date': parseddate,
                                              'closingquote': closingquotes[index],
                                              'changedollars': changedollars[index],
                                              'volumetraded': volumetraded[index],
                                              'symbol': symbol}
                            allhistoricaldata.append(historicaldata)
                        except ValueError:
                            # If the parsing does throw an error, then we do not add the element
                            # as it is not valid
                            pass
                    logging.info(
                        "Successfully fetched historical data for "+symbol)
                else:
                    logging.info("Symbol already added. Skipping.")
            except Exception as e:
                logging.error(
                    "Unable to scrape historical data for stock code:"+stockcode+". "+str(e))
        # Now write the data to the database
        db_connect = DatabaseConnect()
        # Each table is mapped to a class, so we create references to those classes
        historicalstockinfo_table = Table(
            'historicalstockinfo', MetaData(), autoload=True, autoload_with=db_connect.dbengine)
        listedequities_table = Table(
            'listedequities', MetaData(), autoload=True, autoload_with=db_connect.dbengine)
        logging.info("Preparing data for the historicalstockinfo table...")
        # Now select the symbols and stockcodes from our existing securities
        selectstmt = select(
            [listedequities_table.c.symbol, listedequities_table.c.stockcode])
        result = db_connect.dbcon.execute(selectstmt)
        for row in result:
            # The first element in our row tuple is the symbol, and the second is our stockcode
            for historicaldatatoinsert in allhistoricaldata:
                # Map the symbol for each equity to an stockcode in our table
                if historicaldatatoinsert.get('symbol', None) == row[0]:
                    historicaldatatoinsert['stockcode'] = row[1]
                    # Check whether this data is for a non-TTD equity
                    if historicaldatatoinsert['stockcode'] in USD_STOCK_CODES:
                        historicaldatatoinsert['currency'] = 'USD'
                    else:
                        historicaldatatoinsert['currency'] = 'TTD'
                    # Now remove our unneeded columns
                    historicaldatatoinsert.pop('symbol', None)
        # Now we add the new data into our db
        logging.info("Inserting historical stock info data into database.")
        insert_stmt = insert(historicalstockinfo_table).values(
            allhistoricaldata)
        on_duplicate_key_stmt = insert_stmt.on_duplicate_key_update(
            closingquote=insert_stmt.inserted.closingquote,
            changedollars=insert_stmt.inserted.changedollars,
            volumetraded=insert_stmt.inserted.volumetraded,
            currency=insert_stmt.inserted.currency,
        )
        result = db_connect.dbcon.execute(on_duplicate_key_stmt)
        logging.info("Number of rows affected was "+str(result.rowcount))
        logging.info(
            "Successfully wrote data for the historicalstockinfo table into database.")
        db_connect.close()
        return 0
    except Exception as ex:
        logging.exception(
            "We ran into a problem while trying to fetch the historical stock data.")
        customlogging.flush_smtp_logger()
    finally:
        if 'driver' in locals() and driver is not None:
            # Always close the browser
            driver.quit()
            logging.info(
                "Successfully closed web browser used to fetch historic stock data.")


def update_dividend_yield():
    """This function goes through each equity listed in the listedequity table
    and calculates the dividend yield for that equity for each year that we have data on it
    """
    try:
        logging.info("Now calculating dividend yields.")
        db_connect = DatabaseConnect()
        # get the tables that we need
        historical_dividend_info_table = Table(
            'historical_dividend_info', MetaData(), autoload=True, autoload_with=db_connect.dbengine)
        listed_equities_table = Table(
            'listed_equities', MetaData(), autoload=True, autoload_with=db_connect.dbengine)
        dividend_yield_table = Table(
            'dividend_yield', MetaData(), autoload=True, autoload_with=db_connect.dbengine)
        # Now get all dividend data stored in the db
        logging.info("Now fetching all dividend data listed in DB.")
        select_stmt = select([historical_dividend_info_table.c.dividend_amount, historical_dividend_info_table.c.record_date,
                              historical_dividend_info_table.c.symbol, historical_dividend_info_table.c.currency])
        result = db_connect.dbcon.execute(select_stmt)
        # And store them in lists
        all_dividend_data = []
        # Also create an extra list to find a set from
        dividend_symbols = []
        for row in result:
            all_dividend_data.append(
                dict(amount=row[0], date=row[1], symbol=row[2], currency=row[3]))
            dividend_symbols.append(row[2])
        # Get a unique list of the symbols that have dividends above
        unique_symbols = list(set(dividend_symbols))
        # Then for each equity, get their latest closing share prices
        logging.info(
            "Now fetching last closing price for each stock.")
        stock_closing_quotes = []
        # set up a list of datetimes to fetch dividend data for
        datetime_years_to_fetch = []
        current_year = datetime.now().year
        temp_date = datetime.strptime('2010-12-31', '%Y-%m-%d')
        while temp_date.year < current_year:
            datetime_years_to_fetch.append(temp_date)
            temp_date += relativedelta(years=1)
        # get the last closing quote for each listed stock
        for symbol in unique_symbols:
            # store the years that we are interested in calculating dividends for
            for date in datetime_years_to_fetch:
                try:
                    select_stmt = text(
                        "SELECT close_price FROM daily_stock_summary WHERE symbol = :sym ORDER BY date DESC LIMIT 1;")
                    result = db_connect.dbcon.execute(
                        select_stmt, sym=symbol)
                    row = result.fetchone()
                    stock_closing_quotes.append(
                        dict(symbol=symbol, close_price=row[0], date=date))
                except Exception as exc:
                    # if we do not have quotes for a particular year, simply ignore it
                    logging.warning(
                        f"Could not find a closing quote for {symbol}. Info: {str(exc)}")
        # Now we need to compute the total dividend amount per year
        logging.info("Calculating total dividends paid per year per stock.")
        # Create a list of dictionaries to store the dividend per year per stockcode
        dividend_yearly_data = []
        for symbol in unique_symbols:
            # Create a dictionary to store data for this stockcode
            equity_yearly_data = dict(symbol=symbol)
            # Also store the currency for one of the dividends
            currency_stored = False
            # go through each year that we are interested in and check if we have dividend data
            # for that year for this stockcode
            for date in datetime_years_to_fetch:
                # check if we have dividend data for this year
                for dividend_data in all_dividend_data:
                    if dividend_data['symbol'] == symbol and dividend_data['date'].year == date.year:
                        # Get the year of this dividend entry for this equity
                        if (str(date.year)+"_dividends") in equity_yearly_data:
                            # If a key has already been created in the dict for this year,
                            # then we simply add our amount to this
                            equity_yearly_data[str(
                                date.year)+"_dividends"] += Decimal(dividend_data['amount'])
                        else:
                            # Else create a new key in the dictionary for this year pair
                            equity_yearly_data[str(date.year)+"_dividends"] = Decimal(
                                dividend_data['amount'])
                        # Get the currency if we haven't already
                        if not currency_stored:
                            equity_yearly_data['currency'] = dividend_data['currency']
                            currency_stored = True
                # if we did not find any dividend data for this equity id and year, store 0
                if not str(date.year)+"_dividends" in equity_yearly_data:
                    equity_yearly_data[str(
                        date.year)+"_dividends"] = Decimal(0.00)
                    # set the currency as TTD for these 0 dividend equities
                    equity_yearly_data['currency'] = 'TTD'
            # Then store the dict in our list of all dividend yearly data
            dividend_yearly_data.append(equity_yearly_data)
        # Set up a list to store our dividendyielddata
        dividend_yield_data = []
        # Now get our conversion rates to convert between TTD and USD/JMD
        global TTD_USD
        global TTD_JMD
        global TTD_BBD
        if TTD_USD:
            # Calculate our dividend yields using our values
            for dividend_data in dividend_yearly_data:
                for stock_quote in stock_closing_quotes:
                    if (stock_quote['symbol'] == dividend_data['symbol']) and (str(stock_quote['date'].year)+"_dividends" in dividend_data):
                        # If we have matched our stock data and our dividend data (by stockcode and year)
                        # Check if this is a USD listed equity id
                        global USD_STOCK_SYMBOLS
                        if stock_quote['symbol'] in USD_STOCK_SYMBOLS:
                            # all of the USD listed equities so far have dividends in USD as well, so we don't need to convert
                            dividend_yield = dividend_data[str(
                                stock_quote['date'].year)+"_dividends"]*100/stock_quote['close_price']
                            # Add this value to our list
                            dividend_yield_data.append({'yield_percent': dividend_yield, 'date': stock_quote['date'],
                                                        'symbol': stock_quote['symbol']})
                        else:
                            # else this equity is listed in TTD
                            # Check currencies, and use a multiplier for the conversion rate for dividends in other currencies
                            conrate = Decimal(1.00)
                            if dividend_data['currency'] == "USD":
                                conrate = 1/TTD_USD
                            elif dividend_data['currency'] == "JMD":
                                conrate = 1/TTD_JMD
                            elif dividend_data['currency'] == "BBD":
                                conrate = 1/TTD_BBD
                            else:
                                pass
                                # Else our conrate should remain 1
                            # Now calculate the dividend yield for the year
                            dividend_yield = dividend_data[str(
                                stock_quote['date'].year)+"_dividends"]*conrate*100/stock_quote['close_price']
                            # Add this value to our list
                            dividend_yield_data.append({'yield_percent': dividend_yield, 'date': stock_quote['date'],
                                                        'symbol': stock_quote['symbol']})
        else:
            raise ConnectionError(
                "Could not connect to API to convert currencies. Status code "+api_response_ttd.status_code+". Reason: "+api_response_ttd.reason)
        logging.info("Dividend yield calculated successfully.")
        logging.info("Inserting data into database.")
        insert_stmt = insert(dividend_yield_table).values(dividend_yield_data)
        upsert_stmt = insert_stmt.on_duplicate_key_update(
            {x.name: x for x in insert_stmt.inserted})
        result = db_connect.dbcon.execute(upsert_stmt)
        logging.info("Number of rows affected was "+str(result.rowcount))
        logging.info(
            "Successfully wrote data for the dividend_yield table into database.")
        return 0
    except Exception as ex:
        logging.exception(
            "We ran into an error while calculating dividend yields.")
        customlogging.flush_smtp_logger()
    finally:
        if db_connect.dbengine:
            # Always close the database engine
            db_connect.dbengine.close()
            logging.info("Successfully disconnected from database.")


def scrape_equity_summary_data(dates_to_fetch, all_listed_symbols):
    """
    In a new process, use the requests, beautifulsoup and pandas libs to scrape data from
    https://www.stockex.co.tt/market-quote/
    for the list of dates passed to this function.
    Gather the data into a dict, and write that dict to the DB
    """
    # declare a string to identify this PID
    pid_string = " in PID: "+str(os.getpid())
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
                        except sqlalchemy.exc.OperationalError as operr:
                            logging.warning(str(operr))
                            time.sleep(2)
                            execute_failed_times += 1
                    logging.info("Successfully scraped and wrote to db market indices data for " +
                                 fetch_date+pid_string)
                    logging.info(
                        "Number of rows affected in the historical_indices_summary table was "+str(result.rowcount)+pid_string)
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
                        except sqlalchemy.exc.OperationalError as operr:
                            logging.warning(str(operr))
                            time.sleep(2)
                            execute_failed_times += 1
                    logging.info("Successfully scraped and wrote to db daily equity/shares data for " +
                                 fetch_date+pid_string)
                    logging.info(
                        "Number of rows affected in the daily_stock_summary table was "+str(result.rowcount)+pid_string)
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
        customlogging.flush_smtp_logger()
    finally:
        # Always close the database connection
        if 'db_connect' in locals() and db_connect is not None:
            db_connect.close()
            logging.info("Successfully closed database connection"+pid_string)


def update_equity_summary_data(start_date):
    """ 
    Create the list of dates that we need to scrape data from https://www.stockex.co.tt/market-quote/
    for, based on the start_date specified and the dates already in the historical_indices_info table
    """
    logging.info("Now updating daily market summary data.")
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
        customlogging.flush_smtp_logger()


def update_daily_trades():
    """
    Open the Chrome browser and browse through
    https://stockex.co.tt/controller.php?action=view_quote which shows trading for the last day
    Gather the data into a dict, and write that dict to the DB
    :returns: 0 if successful
    :raises Exception if any issues are encountered
    """
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
                except sqlalchemy.exc.OperationalError as operr:
                    logging.warning(str(operr))
                    time.sleep(2)
                    execute_failed_times += 1
            logging.info(
                "Successfully scraped and wrote to db daily equity/shares data for ")
            logging.info(
                "Number of rows affected in the daily_stock_summary table was "+str(result.rowcount))
        else:
            logging.warning("No data found for today.")
        return 0
    except Exception:
        logging.exception("Could not load daily data for today!")
    finally:
        # Always close the database connection
        if 'db_connect' in locals() and db_connect is not None:
            db_connect.close()
            logging.info("Successfully closed database connection")


def update_technical_analysis_data():
    """
    Calculate/scrape the data needed for the technical_analysis_summary table
    """
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
                stock_technical_data['low_52w'] = float(
                    technical_analysis_table['Change%'][4].replace('$', ''))
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
        customlogging.flush_smtp_logger()
    finally:
        # Always close the database connection
        if 'db_connect' in locals() and db_connect is not None:
            db_connect.close()
            logging.info("Successfully closed database connection")


def calculate_fundamental_analysis_ratios(TTD_JMD, TTD_USD, TTD_BBD):
    """
    Calculate the important ratios for fundamental analysis, based off our manually entered data from the financial statements
    """
    audited_raw_table_name = 'audited_fundamental_raw_data'
    audited_calculated_table_name = 'audited_fundamental_calculated_data'
    listed_equities_table_name = 'listed_equities'
    daily_stock_summary_table_name = 'daily_stock_summary'
    historical_dividend_info_table_name = 'historical_dividend_info'
    try:
        db_connect = DatabaseConnect()
        logging.info("Successfully connected to database")
        logging.info("Now reading raw fundamental data from db")
        # load the audited raw data table
        audited_raw_table = Table(
            audited_raw_table_name, MetaData(), autoload=True, autoload_with=db_connect.dbengine)
        listedequities_table = Table(
            listed_equities_table_name, MetaData(), autoload=True, autoload_with=db_connect.dbengine)
        audited_calculated_table = Table(
            audited_calculated_table_name, MetaData(), autoload=True, autoload_with=db_connect.dbengine)
        # read the audited raw table as a pandas df
        audited_raw_df = pd.io.sql.read_sql(
            f"SELECT * FROM {audited_raw_table_name} ORDER BY symbol,date;", db_connect.dbengine)
        # create new dataframe for calculated ratios
        audited_calculated_df = pd.DataFrame()
        audited_calculated_df['symbol'] = audited_raw_df['symbol'].copy()
        audited_calculated_df['date'] = audited_raw_df['date'].copy()
        # calculate the average equity
        average_equity_df = pd.DataFrame()
        average_equity_df['symbol'] = audited_calculated_df['symbol'].copy()
        average_equity_df['total_stockholders_equity'] = audited_raw_df['total_shareholders_equity'].copy(
        )
        average_equity_df['average_stockholders_equity'] = average_equity_df.groupby('symbol')['total_stockholders_equity'].apply(
            lambda x: (x + x.shift(1))/2)
        # calculate the return on equity
        audited_calculated_df['RoE'] = audited_raw_df['net_income'] / \
            average_equity_df['average_stockholders_equity']
        # now calculate the return on invested capital
        audited_calculated_df['RoIC'] = audited_raw_df['profit_after_tax'] / \
            audited_raw_df['total_shareholders_equity']
        # now calculate the working capital
        audited_calculated_df['working_capital'] = audited_raw_df['total_assets'] / \
            audited_raw_df['total_liabilities']
        # copy basic earnings per share
        audited_calculated_df['EPS'] = audited_raw_df['basic_earnings_per_share']
        # calculate price to earnings ratio
        # first get the latest share price data
        # get the latest date from the daily stock table
        latest_stock_date = pd.io.sql.read_sql(
            f"SELECT date FROM {daily_stock_summary_table_name} ORDER BY date DESC LIMIT 1;", db_connect.dbengine)['date'][0].strftime('%Y-%m-%d')
        # then get the share price for each listed stock at this date
        share_price_df = pd.io.sql.read_sql(
            f"SELECT symbol,close_price FROM {daily_stock_summary_table_name} WHERE date='{latest_stock_date}';", db_connect.dbengine)
        # create a merged df to calculate the p/e
        price_to_earnings_df = pd.merge(
            audited_raw_df, share_price_df, how='inner', on='symbol')
        # calculate a conversion rate for the stock price
        price_to_earnings_df['share_price_conversion_rates'] =  price_to_earnings_df.apply(lambda x: TTD_USD if 
                        x.currency == 'USD' else (TTD_JMD if x.currency == 'JMD' else (TTD_BBD if x.currency == 'BBD' else 1.00)), axis=1)
        audited_calculated_df['price_to_earnings_ratio'] = price_to_earnings_df['close_price'] * price_to_earnings_df['share_price_conversion_rates'] / \
            price_to_earnings_df['basic_earnings_per_share']
        # now calculate the price to dividend per share ratio
        # first get the dividends per share
        dividends_df = pd.io.sql.read_sql(
            f"SELECT symbol,record_date,dividend_amount FROM {historical_dividend_info_table_name};", db_connect.dbengine)
        # merge this df with the share_price_df
        dividends_df = pd.merge(
            share_price_df, dividends_df, how='inner', on='symbol')
        # we need to set up a series with the conversion factors for different currencies
        symbols_list = dividends_df['symbol'].to_list()
        conversion_rates = []
        for symbol in symbols_list:
            if symbol in USD_DIVIDEND_SYMBOLS:
                conversion_rates.append(1/TTD_USD)
            elif symbol in JMD_DIVIDEND_SYMBOLS:
                conversion_rates.append(1/TTD_JMD)
            elif symbol in BBD_DIVIDEND_SYMBOLS:
                conversion_rates.append(1/TTD_BBD)
            else:
                conversion_rates.append(1.00)
        # now add this new series to our df
        dividends_df['dividend_conversion_rates'] = pd.Series(conversion_rates,index=dividends_df.index)
        # calculate the price to dividend per share ratio
        audited_calculated_df['price_to_dividends_per_share_ratio'] = dividends_df['close_price'] / \
            (dividends_df['dividend_amount']*dividends_df['dividend_conversion_rates'])
        # now calculate the eps growth rate
        audited_calculated_df['EPS_growth_rate'] = audited_raw_df['basic_earnings_per_share'].diff(
        )*100
        # now calculate the price to earnings-to-growth ratio
        audited_calculated_df['PEG'] = audited_calculated_df['price_to_earnings_ratio'] / \
            audited_calculated_df['EPS_growth_rate']
        audited_calculated_df = audited_calculated_df.where(
            pd.notnull(audited_calculated_df), None)
        # calculate dividend yield and dividend payout ratio
        # note that the price_to_earnings_df contains the share price
        audited_calculated_df['dividend_yield'] = 100 * \
            price_to_earnings_df['dividends_per_share'] / (price_to_earnings_df['close_price'] * price_to_earnings_df['share_price_conversion_rates'])
        audited_calculated_df['dividend_payout_ratio'] = 100* price_to_earnings_df['total_dividends_paid'] / \
            price_to_earnings_df['net_income']
        # calculate the book value per share (BVPS)
        audited_calculated_df['book_value_per_share'] = (audited_raw_df['total_assets'] - audited_raw_df['total_liabilities']) / \
                                                        audited_raw_df['total_shares_outstanding']
        # calculate the price to book ratio
        audited_calculated_df['price_to_book_ratio'] = (price_to_earnings_df['close_price'] * price_to_earnings_df['share_price_conversion_rates']) / \
            ((price_to_earnings_df['total_assets'] - price_to_earnings_df['total_liabilities']) / \
                                                        price_to_earnings_df['total_shares_outstanding'])
        # replace inf with None
        audited_calculated_df = audited_calculated_df.replace([np.inf, -np.inf], None)
        # now write the df to the database
        logging.info("Now writing fundamental data to database.")
        execute_completed_successfully = False
        execute_failed_times = 0
        while not execute_completed_successfully and execute_failed_times < 5:
            try:
                insert_stmt = insert(
                    audited_calculated_table).values(audited_calculated_df.to_dict('records'))
                upsert_stmt = insert_stmt.on_duplicate_key_update(
                    {x.name: x for x in insert_stmt.inserted})
                result = db_connect.dbcon.execute(
                    upsert_stmt)
                execute_completed_successfully = True
            except sqlalchemy.exc.OperationalError as operr:
                logging.warning(str(operr))
                time.sleep(1)
                execute_failed_times += 1
            logging.info(
                "Successfully scraped and wrote fundamental data to db.")
            logging.info(
                "Number of rows affected in the audited fundamental calculated table was "+str(result.rowcount))
        pass
    except Exception:
        logging.exception(
            "Could not complete fundamental data update.")
        customlogging.flush_smtp_logger()
    finally:
        # Always close the database connection
        if 'db_connect' in locals() and db_connect is not None:
            db_connect.close()
            logging.info("Successfully closed database connection")

def fetch_latest_currency_conversion_rates():
    logging.debug("Now trying to fetch latest currency conversions.")
    api_response_ttd = requests.get(
            url="https://fcsapi.com/api-v2/forex/base_latest?symbol=TTD&type=forex&access_key=o9zfwlibfXciHoFO4LQU2NfTwt2vEk70DAiOH1yb2ao4tBhNmm")
    if (api_response_ttd.status_code == 200):
        # store the conversion rates that we need
        TTD_JMD = float(json.loads(api_response_ttd.content.decode('utf-8'))['response']['JMD'])
        TTD_USD = float(json.loads(api_response_ttd.content.decode('utf-8'))['response']['USD'])
        TTD_BBD = float(json.loads(api_response_ttd.content.decode('utf-8'))['response']['BBD'])
        logging.debug("Currency conversions fetched correctly.")
        return TTD_JMD, TTD_USD, TTD_BBD
    else:
        logging.exception(f"Cannot load URL for currency conversions.{api_response_ttd.status_code},{api_response_ttd.reason},{api_response_ttd.url}")

def main():
    """The main steps in coordinating the scraping"""
    try:
        # Set up logging for this module
        q_listener, q = customlogging.setup_logging(
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
            # first check the arguements given to this script
            parser = argparse.ArgumentParser()
            parser.add_argument("-f",
                                "--full_history", help="Record all data from 2010 to now", action="store_true")
            parser.add_argument("-d",
                                "--daily_update", help="Only update data for the daily summary for today", action="store_true")
            args = parser.parse_args()
            # set the start date based on the the full history option
            if args.full_history:
                start_date = '2017-01-01'
            else:
                start_date = (datetime.now() +
                              relativedelta(months=-1)).strftime('%Y-%m-%d')
            # run all functions within a multiprocessing pool
            with multiprocessing.Pool(os.cpu_count(), customlogging.logging_worker_init, [q]) as multipool:
                logging.info("Now starting TTSE scraper.")
                # check if this is the daily update (run inside the trading day)
                if args.daily_update:
                    multipool.apply_async(
                        update_daily_trades, ())
                else:
                    # else this is a full update (run once a day)
                    # get the latest conversion rates
                    TTD_JMD, TTD_USD, TTD_BBD = multipool.apply(fetch_latest_currency_conversion_rates,())
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
                    # wait until all workers finish fetching data before continuing
                    for result in async_results:
                        result.wait()
                    # now run functions that depend on this raw data
                    multipool.apply_async(update_technical_analysis_data, ())
                    multipool.apply_async(
                        calculate_fundamental_analysis_ratios, (TTD_JMD, TTD_USD, TTD_BBD))
                    ###### Not updated #############
                    # multipool.apply_async(scrape_historical_indices_data, ())
                    # multipool.apply_async(scrape_historical_data, ())
                multipool.close()
                multipool.join()
                logging.info(os.path.basename(__file__) +
                             " executed successfully.")
                q_listener.stop()
    except Exception as exc:
        logging.error(
            f"Error in script {os.path.basename(__file__)}", exc_info=exc)
        customlogging.flush_smtp_logger()


# If this script is being run from the command-line, then run the main() function
if __name__ == "__main__":
    main()
