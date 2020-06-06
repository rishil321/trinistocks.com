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
import argparse

# Imports from the cheese factory
from pid import PidFile
import requests
import urllib.request
import urllib.parse
from sqlalchemy import create_engine, Table, select, MetaData, text, and_
from sqlalchemy.dialects.mysql import insert
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from selenium.common.exceptions import WebDriverException
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import pandas as pd
import numpy as np

# Imports from the local filesystem
import ttsescraperconfig
import customlogging

# Put your constants here. These should be named in CAPS.
START_DATE = None
# The following equity ids are listed in USD on the exchange (all others are listed in TTD)
USD_STOCK_CODES = [146]
# The timeout to set for multiprocessing tasks (in seconds)
MULTIPROCESSING_TIMEOUT = 60*60

# Put your global variables here.


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
        if self.dbengine:
            self.dbengine.dispose()


# Put your function definitions here. These should be lowercase, separated by underscores.


def scrape_listed_equity_data():
    """Use the selenium module to open the Chrome browser and browse through
    all listed equity securities at the URL 
    https://www.stockex.co.tt/controller.php?action=listed_companies
    and scrape the useful output into a list of dictionaries  
    """
    try:
        logging.info(
            "Now scraping basic listing data from all listed equities.")
        # This list of dicts will contain all data to be written to the db
        all_listed_equity_data = []
        # First open the browser
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
        listed_equity_urls = []
        # For each 'a' element that we found
        for link in pagelinks:
            # All valid equities contain the StockCode keyword in their href
            if "StockCode" in link.get_attribute("href"):
                # So we add the links to all equities to our array
                listed_equity_urls.append(link.get_attribute("href"))
        # Now we go through each equity in our list and navigate to their page
        logging.info("Found links for " +
                     str(len(listed_equity_urls))+" equities.")
        for equity_url in listed_equity_urls:
            # Load the URL for this equity
            driver.get(equity_url)
            # Dismiss the alert asking you to download flash player
            driver.switch_to.alert.dismiss()
            # Create a dictionary to store the important elements extracted from the page
            equity_data = {}
            # get the stock code from the URL
            parsed_url = urllib.parse.urlparse(equity_url)
            equity_data['stockcode'] = urllib.parse.parse_qs(parsed_url.query)[
                'StockCode'][0]
            # Find all elements on this page with the <td> tag
            # All important values for the equity are wrapped in these tags
            important_elements = driver.find_elements_by_tag_name("td")
            # Go through each important element and identify certain key values that we want to add
            for index, element in enumerate(important_elements):
                if element.text is not None:
                    # We first check the symbol for this equity, to see if we have already scraped it
                    # since symbols must be unique
                    if "Symbol" in element.text:
                        securitysymbol = element.text[9:]
                        equity_data["symbol"] = securitysymbol
                    if "Company Name" in element.text:
                        securityname = element.text[15:]
                        equity_data["securityname"] = securityname
                    if "Status" in element.text:
                        securitystatus = element.text[9:]
                        equity_data["status"] = securitystatus
                    if "Sector" in element.text:
                        securitysector = element.text[9:]
                        equity_data["sector"] = securitysector
                    if "Issued Share Capital" in element.text:
                        # The value for this is not stored in the same column, but in
                        # the subsequent two columns
                        issuedsharecapital = important_elements[index+2].text
                        # Remove any commas in the scraped value and convert to int
                        issuedsharecapital = int(
                            issuedsharecapital.replace(",", ""))
                        equity_data["issuedsharecapital"] = issuedsharecapital
                    if "Market Capitalization" in element.text:
                        # The value for this is not stored in the same column, but in
                        # the subsequent two columns
                        marketcapitalization = important_elements[index+2].text[1:]
                        # Remove any commas in the scraped value and convert to decimal
                        marketcapitalization = Decimal(
                            marketcapitalization.replace(",", ""))
                        equity_data["marketcapitalization"] = marketcapitalization
            # Now we have all the important information for this equity
            # So we can add the dictionary object to our global list
            # But first we check that this symbol has not been added already
            # Create a boolean to check for duplicated symbols
            symbol_already_added = False
            if all_listed_equity_data:
                # if there are objects in the list
                for addedsecuritydata in all_listed_equity_data:
                    if addedsecuritydata['symbol'] == securitysymbol:
                        symbol_already_added = True
            if not symbol_already_added:
                all_listed_equity_data.append(equity_data)
            logging.info("Successfully added basic listing data for: " +
                         equity_data['securityname'])
        # Now write the data to the database
        db_connection = DatabaseConnect()
        listed_equities_table = Table(
            'listedequities', MetaData(), autoload=True, autoload_with=db_connection.dbengine)
        logging.debug("Inserting data into listedequities table")
        insert_stmt = insert(listed_equities_table).values(
            all_listed_equity_data)
        on_duplicate_key_stmt = insert_stmt.on_duplicate_key_update(
            stockcode=insert_stmt.inserted.stockcode,
            securityname=insert_stmt.inserted.securityname,
            symbol=insert_stmt.inserted.symbol,
            status=insert_stmt.inserted.status,
            sector=insert_stmt.inserted.sector,
            issuedsharecapital=insert_stmt.inserted.issuedsharecapital,
            marketcapitalization=insert_stmt.inserted.marketcapitalization
        )
        result = db_connection.dbcon.execute(on_duplicate_key_stmt)
        db_connection.close()
        logging.info(
            "Database update successful. Number of rows affected was "+str(result.rowcount))
        return 0
    except Exception:
        logging.exception("Problem encountered in " +
                          scrape_listed_equity_data.__name__)
        customlogging.flush_smtp_logger()
    finally:
        if 'driver' in locals() and driver is not None:
            # Always close the browser
            driver.quit()
            logging.info("Successfully closed web browser in " +
                         scrape_listed_equity_data.__name__)


def scrape_dividend_data():
    """Use the Selenium module to open the Chrome browser and browse through
    all listed equity securities at the URL 
    https://www.stockex.co.tt/controller.php?action=listed_companies
    For each listed security, press the corporate action button and get
    all data from 2010 to the current date 
    """
    # Create a variable for our webdriver
    driver = None
    try:
        logging.info("Now trying to scrape dividend data")
        # This list of dicts will contain all data to be written to the db
        all_dividend_data_scraped = []
        # First open the browser
        logging.debug(
            "Now opening the Chrome browser to scrape dividend data.")
        options = Options()
        options.headless = True
        driver = webdriver.Chrome(options=options)
        # Then go the stockex.co.tt URL that lists all equities
        driver.get("https://stockex.co.tt/controller.php?action=listed_companies")
        logging.debug(
            "Navigating to https://stockex.co.tt/controller.php?action=listed_companies")
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
        # Now we go through each equity in our list and navigate to their dividend page
        logging.info("Found links for "+str(len(listedstockcodes)) +
                     " equities. Now navigating to them to fetch dividend data.")
        for stockcode in listedstockcodes:
            logging.info(
                "Now attempting to fetch dividend data for stockcode:"+stockcode)
            try:
                # Construct the full URL using the stock code
                equitydividendurl = "https://www.stockex.co.tt/controller.php?action=view_stock_profile&StockCode=" + stockcode
                # Load the URL for this equity
                driver.get(equitydividendurl)
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
                # All of the elements that we want to extract are wrapped in td tags
                importantelements = driver.find_elements_by_tag_name("td")
                # Get the text for each importantelement
                importantelementstext = []
                for parentelement in importantelements:
                    # Try to find a p element inside each td
                    try:
                        childelement = parentelement.find_element_by_tag_name(
                            "p")
                        if childelement.text is not None:
                            importantelementstext.append(childelement.text)
                    except Exception:
                        # If we do not find a p element, then simply continue
                        pass
                # Now create several dictionaries to store the important elements extracted from the page
                dividenddates = []
                dividendamounts = []
                dividendcurrencies = []
                # Go through each important element and identify certain key values that we want to add
                for index, element in enumerate(importantelementstext):
                    # First find the word Currency
                    if "Currency" in element:
                        currencyindex = index
                        # After the word Currency, we start fetching our useful data
                        # Record dates should be the first element available
                        # So we slice our list from here, stepping by 5 each time
                        dividenddates = \
                            importantelementstext[currencyindex +
                                                  1:len(importantelements):5]
                        # Then we record our dividend amount
                        dividendamounts = \
                            importantelementstext[currencyindex +
                                                  2:len(importantelements):5]
                        # Then we record our dividend currency
                        dividendcurrencies = \
                            importantelementstext[currencyindex +
                                                  5:len(importantelements):5]
                # Now we have all the important dividend information for this equity
                # So we can add the lists to our global dictionary
                # But first we need to get the symbol for this security
                symbolelement = driver.find_element_by_xpath(
                    "//*[contains(text(), 'Symbol :')]")
                # Find the parent of this element, which contains the actual symbol
                symbolelement = symbolelement.find_element_by_xpath("./..")
                symbol = symbolelement.text.split(": ", 1)[1]
                # Check if data for this symbol has already been added
                symbolalreadyadded = False
                for dividenddata in all_dividend_data_scraped:
                    if dividenddata['symbol'] == symbol:
                        symbolalreadyadded = True
                        break
                # If we have not already added data for this symbol, then we continue
                if not symbolalreadyadded:
                    # Now create our list of dicts containing all of the dividends
                    for index, date in enumerate(dividenddates):
                        # For each record, check that our date is valid
                        try:
                            parseddate = parse(date, fuzzy=True).date()
                            # If the parse does not throw an errors, continue
                            # Check if multiple duplicate dates might be present
                            datealreadyadded = False
                            for dividenddata in all_dividend_data_scraped:
                                if dividenddata['symbol'] == symbol:
                                    if dividenddata['date'] == parseddate:
                                        # If the date has already been added for this
                                        # equity, then just sum the dividend amounts
                                        dividenddata['dividendamount'] = str(
                                            Decimal(dividenddata['dividendamount']) + Decimal(dividendamounts[index]))
                                        datealreadyadded = True
                            if not datealreadyadded:
                                # If the date has not been added yet for this symbol, then
                                # add the entire new record
                                dividenddata = {'date': parseddate,
                                                'dividendamount': dividendamounts[index],
                                                'currency': dividendcurrencies[index],
                                                'symbol': symbol}
                                all_dividend_data_scraped.append(dividenddata)
                        except ValueError:
                            # If the parsing does throw an error, then we do not add the element
                            # as it is not a valid date
                            pass
                    logging.debug(
                        "Successfully fetched dividend data for "+symbol)
                else:
                    logging.debug("Symbol already added. Skipping.")
            except Exception as e:
                logging.error(
                    "Unable to scrape dividend data for stockcode:"+stockcode+". "+str(e))
        # Now write the data to the database
        db_connect = DatabaseConnect()
        # Reflect the tables already created in our db
        historicaldividendinfo_table = Table(
            'historicaldividendinfo', MetaData(), autoload=True, autoload_with=db_connect.dbengine)
        listedequities_table = Table(
            'listedequities', MetaData(), autoload=True, autoload_with=db_connect.dbengine)
        logging.info("Inserting data into historicaldividendinfo table.")
        # Now select the symbols and stockcodes from our existing securities
        selectstmt = select(
            [listedequities_table.c.symbol, listedequities_table.c.stockcode])
        result = db_connect.dbcon.execute(selectstmt)
        for row in result:
            # The first element in our row tuple is the symbol, and the second is our stockcode
            # Map the symbol for each equity to an stockcode in our table
            for dividenddatatoinsert in all_dividend_data_scraped:
                if dividenddatatoinsert['symbol'] == row[0]:
                    dividenddatatoinsert['stockcode'] = row[1]
        # Now remove our unneeded columns
        for dividenddatatoinsert in all_dividend_data_scraped:
            dividenddatatoinsert.pop('symbol', None)
        insert_stmt = insert(historicaldividendinfo_table).values(
            all_dividend_data_scraped)
        on_duplicate_key_stmt = insert_stmt.on_duplicate_key_update(
            dividendamount=insert_stmt.inserted.dividendamount,
            currency=insert_stmt.inserted.currency,
        )
        result = db_connect.dbcon.execute(on_duplicate_key_stmt)
        logging.info(
            "Successfully wrote data for the historicaldividenddata table into database.")
        logging.info("Number of rows affected was "+str(result.rowcount))
        db_connect.close()
        return 0
    except Exception as ex:
        logging.exception("Error encountered while scraping dividend data.")
        customlogging.flush_smtp_logger()
    finally:
        if 'driver' in locals() and driver is not None:
            # Always close the browser
            driver.quit()
            logging.debug(
                "Successfully closed web browser used to scrape dividend data.")


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
        # Each table is mapped to a class, so we create references to those classes
        historicaldividendinfo_table = Table(
            'historicaldividendinfo', MetaData(), autoload=True, autoload_with=db_connect.dbengine)
        listedequities_table = Table(
            'listedequities', MetaData(), autoload=True, autoload_with=db_connect.dbengine)
        dividendyield_table = Table(
            'dividendyield', MetaData(), autoload=True, autoload_with=db_connect.dbengine)
        # Now get all dividends payments, their dates and their stockcodes
        logging.info("Now fetching all dividend data listed in DB.")
        selectstmt = select([historicaldividendinfo_table.c.dividendamount, historicaldividendinfo_table.c.date,
                             historicaldividendinfo_table.c.stockcode, historicaldividendinfo_table.c.currency])
        result = db_connect.dbcon.execute(selectstmt)
        # And store them in lists
        all_dividend_data = []
        # Also create an extra list to find a set from
        dividend_stock_codes = []
        for row in result:
            all_dividend_data.append(
                dict(amount=row[0], date=row[1], stockcode=row[2], currency=row[3]))
            dividend_stock_codes.append(row[2])
        # Get a unique list of the stockcodes that have dividends above
        unique_stock_codes = list(set(dividend_stock_codes))
        # Then for each equity, get their share prices on the last day of the year
        logging.info(
            "Now fetching required historical stock info listed in DB.")
        stockyearlydata = []
        # ensure that we get the closing quote for each year from 2010 to now
        datetime_years_to_fetch = []
        current_year = datetime.now().year
        temp_date = datetime.strptime('2010-01-01', '%Y-%m-%d')
        while temp_date.year < current_year:
            datetime_years_to_fetch.append(temp_date)
            temp_date += relativedelta(years=1)
        for stock_code in unique_stock_codes:
            # for each year in our list, get the last closingquote for the year and store it
            for datetime_year in datetime_years_to_fetch:
                selectstmt = text(
                    "SELECT closingquote,date FROM historicalstockinfo WHERE YEAR(date) = :yr AND stockcode = :eq ORDER BY date DESC LIMIT 1;")
                result = db_connect.dbcon.execute(
                    selectstmt, yr=datetime_year.year, eq=stock_code)
                row = result.fetchone()
                try:
                    stockyearlydata.append(
                        dict(stockcode=stock_code, closingquote=row[0], date=row[1]))
                except TypeError:
                    # if we do not have quotes for a particular year, simply ignore it
                    pass
        # Now we need to compute the total dividend amount per year
        # Create a list of dictionaries to store the dividend per year per stockcode
        dividend_yearly_data = []
        for stock_code in unique_stock_codes:
            # Create a dictionary to store data for this stockcode
            equity_yearly_data = dict(stockcode=stock_code)
            # Also store the currency for one of the dividends
            currencystored = False
            # go through each year that we are interested in and check if we have dividend data
            # for that year for this stockcode
            for datetime_year in datetime_years_to_fetch:
                # check if we have dividend data for this year
                for dividend_data in all_dividend_data:
                    if dividend_data['stockcode'] == stock_code and dividend_data['date'].year == datetime_year.year:
                        # Get the year of this dividend entry for this equity
                        if (str(datetime_year.year)+"_dividends") in equity_yearly_data:
                            # If a key has already been created in the dict for this year,
                            # then we simply add our amount to this
                            equity_yearly_data[str(
                                datetime_year.year)+"_dividends"] += Decimal(dividend_data['amount'])
                        else:
                            # Else create a new key in the dictionary for this year pair
                            equity_yearly_data[str(datetime_year.year)+"_dividends"] = Decimal(
                                dividend_data['amount'])
                        # Get the currency if we haven't already
                        if not currencystored:
                            equity_yearly_data['currency'] = dividend_data['currency']
                            currencystored = True
                # if we did not find any dividend data for this equity id and year, store 0
                if not str(datetime_year.year)+"_dividends" in equity_yearly_data:
                    equity_yearly_data[str(
                        datetime_year.year)+"_dividends"] = Decimal(0.00)
                    # set the currency as TTD for these 0 dividend equities
                    equity_yearly_data['currency'] = 'TTD'
            # Then store the dict in our list of all dividend yearly data
            dividend_yearly_data.append(equity_yearly_data)
        # Set up a list to store our dividendyielddata
        dividendyielddata = []
        # Now get our conversion rates to convert between TTD and USD/JMD
        api_response_ttd = requests.get(
            url="https://fcsapi.com/api-v2/forex/base_latest?symbol=TTD&type=forex&access_key=o9zfwlibfXciHoFO4LQU2NfTwt2vEk70DAiOH1yb2ao4tBhNmm")
        if (api_response_ttd.status_code == 200):
            ttd_jmd = Decimal(json.loads(
                api_response_ttd.content.decode('utf-8'))['response']['JMD'])
            usd_ttd = Decimal(
                1.00)/Decimal(json.loads(api_response_ttd.content.decode('utf-8'))['response']['USD'])
            # Calculate our dividend yields using our values
            for dividend_data in dividend_yearly_data:
                for stdata in stockyearlydata:
                    if (stdata['stockcode'] == dividend_data['stockcode']) and (str(stdata['date'].year)+"_dividends" in dividend_data):
                        # If we have matched our stock data and our dividend data (by stockcode and year)
                        # Check if this is a USD listed equity id
                        if stdata['stockcode'] in USD_STOCK_CODES:
                            # all of the USD listed equities so far have dividends in USD as well, so we don't need to convert
                            dividendyield = dividend_data[str(
                                stdata['date'].year)+"_dividends"]*100/stdata['closingquote']
                            # Add this value to our list
                            dividendyielddata.append({'yieldpercent': dividendyield, 'date': stdata['date'],
                                                      'stockcode': stdata['stockcode']})
                        else:
                            # else this equity is listed in TTD
                            # Check currencies, and use a multiplier for the conversion rate for dividends in other currencies
                            conrate = Decimal(1.00)
                            if dividend_data['currency'] == "USD":
                                conrate = usd_ttd
                            elif dividend_data['currency'] == "JMD":
                                conrate = ttd_jmd
                            else:
                                pass
                                # Else our conrate should remain 1
                            # Now calculate the dividend yield for the year
                            dividendyield = dividend_data[str(
                                stdata['date'].year)+"_dividends"]*conrate*100/stdata['closingquote']
                            # Add this value to our list
                            dividendyielddata.append({'yieldpercent': dividendyield, 'date': stdata['date'],
                                                      'stockcode': stdata['stockcode']})
        else:
            raise ConnectionError(
                "Could not connect to API to convert currencies. Status code "+api_response_ttd.status_code+". Reason: "+api_response_ttd.reason)
        logging.info("Dividend yield calculated successfully.")
        logging.info("Inserting data into database.")
        insert_stmt = insert(dividendyield_table).values(dividendyielddata)
        on_duplicate_key_stmt = insert_stmt.on_duplicate_key_update(
            yieldpercent=insert_stmt.inserted.yieldpercent,
        )
        result = db_connect.dbcon.execute(on_duplicate_key_stmt)
        logging.info("Number of rows affected was "+str(result.rowcount))
        logging.info(
            "Successfully wrote data for the dividendyield table into database.")
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


def scrape_equity_summary_data(datestofetch, alllistedsymbols):
    """
    In a new process, open the Chrome browser and browse through
    https://stockex.co.tt/controller.php?action=view_quote&TradingDate=03/13/2020
    for the date range passed to the function.
    Gather the data into a dict, and write that dict to the DB
    :param datestofetch: a list containing the dates that this process should parse 
    :param alllistedsymbols: a list containing all valid symbols in the DB
    :returns: 0 if successful
    :raises Exception if any issues are encountered
    """
    try:
        # declare a string to identify this PID
        pidstring = " in PID: "+str(os.getpid())
        logging.info(
            "Now opening using pandas to fetch market summary data"+pidstring)
        # set up a dictionary to store the market summary data (per day) and initialize the values to 0
        market_summary_data = dict()
        # set up the keys for the dict, and set some dummy keys to make sure the lists are equal lengths
        market_summary_data_keys = [['compositetotalsindexvalue', 'compositetotalsindexchange', 'compositetotalschange',
                                     'compositetotalsvolumetraded', 'compositetotalsvaluetraded', 'compositetotalsnumtrades'],
                                    ['alltnttotalsindexvalue', 'alltnttotalsindexchange', 'alltnttotalschange',
                                        'alltnttotalsvolumetraded', 'alltnttotalsvaluetraded', 'alltnttotalsnumtrades'],
                                    ['crosslistedtotalsindexvalue', 'crosslistedtotalsindexchange', 'crosslistedtotalschange',
                                        'crosslistedtotalsvolumetraded', 'crosslistedtotalsvaluetraded', 'crosslistedtotalsnumtrades'],
                                    ['smetotalsindexvalue', 'smetotalsindexchange', 'smetotalschange',
                                        'smetotalsvolumetraded', 'smetotalsvaluetraded', 'smetotalsnumtrades'],
                                    ['dummykey', 'dummykey', 'dummykey',
                                     'mutualfundstotalsvolumetraded', 'mutualfundstotalsvaluetraded', 'mutualfundstotalsnumtrades'],
                                    ['dummykey', 'dummykey', 'dummykey',
                                     'secondtiertotalsvolumetraded', 'secondtiertotalsvaluetraded', 'secondtiertotalsnumtrades'],
                                    ['dummykey', 'dummykey', 'dummykey',
                                     'usdequitytotalsvolumetraded', 'usdequitytotalsvaluetraded', 'usdequitytotalsnumtrades']
                                    ]
        for inner_list in market_summary_data_keys:
            for key in inner_list:
                market_summary_data[key] = 0
        # set up another dictionary to store the daily data for all equities (shares)
        daily_shares_data = dict()
        # set up a list of all the required keys in the dict
        daily_shares_data_keys = ['openprice', 'high', 'low', 'osbid', 'osbidvol', 'osoffer',
                                  'osoffervol', 'saleprice', 'closeprice', 'volumetraded', 'valuetraded', 'changedollars']
        # initialize all keys to be 0
        for key in daily_shares_data_keys:
            daily_shares_data[key] = 0
        # set up the database connection to write data to the db
        db_connect = DatabaseConnect()
        logging.debug("Successfully connected to database"+pidstring)
        # Reflect the tables already created in our db
        historicalmarketsummary_table = Table(
            'historicalmarketsummary', MetaData(), autoload=True, autoload_with=db_connect.dbengine)
        dailyequitysummary_table = Table(
            'dailyequitysummary', MetaData(), autoload=True, autoload_with=db_connect.dbengine)
        listedequities_table = Table(
            'listedequities', MetaData(), autoload=True, autoload_with=db_connect.dbengine)
        # prepare the insert statement for the historicalmarketsummary table
        # note that we will actually execute this statement later on
        market_summary_insert_stmt = insert(historicalmarketsummary_table).values(
            market_summary_data)
        market_summary_on_duplicate_key_stmt = market_summary_insert_stmt.on_duplicate_key_update(
            {x.name: x for x in market_summary_insert_stmt.inserted})
        # prepare the insert statement for the dailyequitysummary
        daily_equity_summary_insert_stmt = insert(dailyequitysummary_table).values(
            daily_shares_data)
        daily_equity_summary_on_duplicate_key_stmt = daily_equity_summary_insert_stmt.on_duplicate_key_update(
            {x.name: x for x in daily_equity_summary_insert_stmt.inserted})
        # now fetch the data at each url(each market trading date)
        for index, fetchdate in enumerate(datestofetch):
            logging.info("Now loading webpage "+str(index) +
                         " of "+str(len(datestofetch))+pidstring)
            # get a date object suitable for the db
            fetchdatedb = datetime.strftime(
                datetime.strptime(fetchdate, '%m/%d/%Y'), '%Y-%m-%d')
            # store the date
            market_summary_data['date'] = fetchdatedb
            # for each date, we need to navigate to this summary page for that day
            urlsummarypage = "https://stockex.co.tt/controller.php?action=view_quote&TradingDate="+fetchdate
            logging.info("Navigating to "+urlsummarypage+pidstring)
            http_get_req = requests.get(urlsummarypage)
            # get a list of tables from the URL
            dataframe_list = pd.read_html(http_get_req.text)
            # if this is a valid trading day, extract the values we need from the tables
            if dataframe_list[1][0][0].startswith("Daily Equity Summary for "):
                # get the tables holding data for all the shares
                market_summary_table = dataframe_list[3]
                ordinary_shares_table = dataframe_list[4]
                preference_shares_table = dataframe_list[5]
                second_tier_shares_table = dataframe_list[6]
                mutual_funds_shares_table = dataframe_list[7]
                sme_shares_table = dataframe_list[8]
                usd_equity_shares_table = dataframe_list[9]
                # extract the values required from the tables
                try:
                    # first extract the data from the market summary table
                    # go through the rows and columns of the table to extract the data we need
                    for row_index in range(1, 8):
                        for column_index in range(2, 8):
                            # check if the data is null in the dataframe
                            if not pd.isnull(market_summary_table[column_index][row_index]):
                                # check if the value needs to be converted to a float or int
                                if any(partial_key_name in market_summary_data_keys[row_index-1][column_index-2] for partial_key_name in ['volumetraded', 'numtrades']):
                                    market_summary_data[market_summary_data_keys[row_index-1][column_index-2]] = int(
                                        market_summary_table[column_index][row_index])
                                else:
                                    market_summary_data[market_summary_data_keys[row_index-1][column_index-2]] = float(
                                        market_summary_table[column_index][row_index])
                            else:
                                pass
                    # pop the dummy key so that we can insert this dict into the db later
                    market_summary_data.pop('dummykey',None)
                    if not pd.isnull(market_summary_table[2][1]):
                        market_summary_data['compositetotalsindexvalue'] = float(
                            market_summary_table[2][1])
                    if not pd.isnull(market_summary_table[3][1]):
                        market_summary_data['compositetotalsindexchange'] = float(
                            market_summary_table[3][1])
                    if not pd.isnull(market_summary_table[4][1]):
                        market_summary_data['compositetotalschange'] = float(
                            market_summary_table[4][1])
                    if not pd.isnull(market_summary_table[5][1]):
                        market_summary_data['compositetotalsvolumetraded'] = int(
                            market_summary_table[5][1])
                    if not pd.isnull(market_summary_table[6][1]):
                        market_summary_data['compositetotalsvaluetraded'] = float(
                            market_summary_table[6][1])
                    if not pd.isnull(market_summary_table[7][1]):
                        market_summary_data['compositetotalsnumtrades'] = int(
                            market_summary_table[7][1])
                    if not pd.isnull(market_summary_table[2][2]):
                        market_summary_data['alltnttotalsindexvalue'] = float(
                            market_summary_table[2][2])
                    if not pd.isnull(market_summary_table[3][2]):
                        market_summary_data['alltnttotalsindexchange'] = float(
                            market_summary_table[3][2])
                    if not pd.isnull(market_summary_table[4][2]):
                        market_summary_data['alltnttotalschange'] = float(
                            market_summary_table[4][2])
                    if not pd.isnull(market_summary_table[5][2]):
                        market_summary_data['alltnttotalsvolumetraded'] = int(
                            market_summary_table[5][2])
                    if not pd.isnull(market_summary_table[6][2]):
                        market_summary_data['alltnttotalsvaluetraded'] = float(
                            market_summary_table[6][2])
                    if not pd.isnull(market_summary_table[7][2]):
                        market_summary_data['alltnttotalsnumtrades'] = int(
                            market_summary_table[7][2])
                    if not pd.isnull(market_summary_table[2][3]):
                        market_summary_data['crosslistedtotalsindexvalue'] = float(
                            market_summary_table[2][3])
                    if not pd.isnull(market_summary_table[3][3]):
                        market_summary_data['crosslistedtotalsindexchange'] = float(
                            market_summary_table[3][3])
                    if not pd.isnull(market_summary_table[4][3]):
                        market_summary_data['crosslistedtotalschange'] = float(
                            market_summary_table[4][3])
                    if not pd.isnull(market_summary_table[5][3]):
                        market_summary_data['crosslistedtotalsvolumetraded'] = int(
                            market_summary_table[5][3])
                    if not pd.isnull(market_summary_table[6][3]):
                        market_summary_data['crosslistedtotalsvaluetraded'] = float(
                            market_summary_table[6][3])
                    if not pd.isnull(market_summary_table[7][3]):
                        market_summary_data['crosslistedtotalsnumtrades'] = int(
                            market_summary_table[7][3])
                    if not pd.isnull(market_summary_table[2][4]):
                        market_summary_data['smetotalsindexvalue'] = float(
                            market_summary_table[2][4])
                    if not pd.isnull(market_summary_table[3][4]):
                        market_summary_data['smetotalsindexchange'] = float(
                            market_summary_table[3][4])
                    if not pd.isnull(market_summary_table[4][4]):
                        market_summary_data['smetotalschange'] = float(
                            market_summary_table[4][4])
                    if not pd.isnull(market_summary_table[5][4]):
                        market_summary_data['smetotalsvolumetraded'] = int(
                            market_summary_table[5][4])
                    if not pd.isnull(market_summary_table[6][4]):
                        market_summary_data['smetotalsvaluetraded'] = float(
                            market_summary_table[6][4])
                    if not pd.isnull(market_summary_table[7][4]):
                        market_summary_data['smetotalsnumtrades'] = int(
                            market_summary_table[7][4])
                    if not pd.isnull(market_summary_table[5][5]):
                        market_summary_data['mutualfundstotalsvolumetraded'] = int(
                            market_summary_table[5][5])
                    if not pd.isnull(market_summary_table[6][5]):
                        market_summary_data['mutualfundstotalsvaluetraded'] = float(
                            market_summary_table[6][5])
                    if not pd.isnull(market_summary_table[7][5]):
                        market_summary_data['mutualfundstotalsnumtrades'] = int(
                            market_summary_table[7][5])
                    if not pd.isnull(market_summary_table[5][6]):
                        market_summary_data['secondtiertotalsvolumetraded'] = int(
                            market_summary_table[5][6])
                    if not pd.isnull(market_summary_table[6][6]):
                        market_summary_data['secondtiertotalsvaluetraded'] = float(
                            market_summary_table[6][6])
                    if not pd.isnull(market_summary_table[7][6]):
                        market_summary_data['secondtiertotalsnumtrades'] = int(
                            market_summary_table[7][6])
                    if not pd.isnull(market_summary_table[5][7]):
                        market_summary_data['usdequitytotalsvolumetraded'] = int(
                            market_summary_table[5][7])
                    if not pd.isnull(market_summary_table[6][7]):
                        market_summary_data['usdequitytotalsvaluetraded'] = float(
                            market_summary_table[6][7])
                    if not pd.isnull(market_summary_table[7][7]):
                        market_summary_data['usdequitytotalsnumtrades'] = int(
                            market_summary_table[7][7])
                    # then extract the data from all of the equity (shares) tables
                    for shares_table in [ordinary_shares_table, preference_shares_table, second_tier_shares_table, mutual_funds_shares_table,
                                         sme_shares_table, usd_equity_shares_table]:
                        pass
                except KeyError as keyerr:
                    logging.warning(
                        "Could not find a required key on date "+fetchdatedb+pidstring+str(keyerr))
                except IndexError as idxerr:
                    logging.warning(
                        "Could not locate index in a list. "+fetchdatedb+pidstring+str(idxerr))
            # now write the dict to the db
            # if we had any errors, the values will be written as their defaults (0 or null)
            result = db_connect.dbcon.execute(
                market_summary_on_duplicate_key_stmt)
            logging.info("Successfully scraped market data for " +
                         fetchdatedb+pidstring)
            logging.info(
                "Number of rows affected in the historicalmarketsummary table was "+str(result.rowcount)+pidstring)
        #     # set up a variable to retry page loads on Internet failures
        #     webretries = 1
        #     # check if the page was loaded or has failed enough times
        #     pageloaded = False
        #     failmaxtimes = 3
        #     while not pageloaded and webretries <= failmaxtimes:
        #         try:
        #             # get a date object suitable for the db
        #             fetchdatedb = datetime.strftime(
        #                 datetime.strptime(fetchdate, '%m/%d/%Y'), '%Y-%m-%d')
        #             # start a dictionary to write to the db
        #             marketsummarydata = dict(date=fetchdatedb)
        #             # for each date, we need to navigate to this summary page for that day
        #             urlsummarypage = "https://stockex.co.tt/controller.php?action=view_quote&TradingDate="+fetchdate
        #             logging.info("Navigating to "+urlsummarypage+pidstring)
        #             driver.get(urlsummarypage)
        #             # All important data for us is wrapped in <p> tags, so we first find these elements
        #             textelements = driver.find_elements_by_tag_name("p")
        #             # Then we create a list with the text from all of these elements
        #             textblocksonpage = []
        #             for textelement in textelements:
        #                 if textelement.text is not None:
        #                     textblocksonpage.append(textelement.text)
        #             # Now check if an error message was thrown
        #             errormessage = "Sorry. No data for date selected. Kindly select another date."
        #             if errormessage in textblocksonpage:
        #                 logging.info(
        #                     "No data is available for: "+fetchdate+pidstring)
        #                 pageloaded = True
        #             # if our error message was not thrown, then we continue
        #             else:
        #                 logging.info(
        #                     "Found market summary date for: "+fetchdate+pidstring)
        #                 # find the index of the first line of each important row
        #                 try:
        #                     compositetotalsindex = textblocksonpage.index(
        #                         "Composite Totals")
        #                     # Now use the index to access interesting data
        #                     marketsummarydata['compositetotalsindexvalue'] = float(
        #                         textblocksonpage[compositetotalsindex+1].replace(",", ""))
        #                     marketsummarydata['compositetotalsindexchange'] = float(
        #                         textblocksonpage[compositetotalsindex+2].replace(",", ""))
        #                     marketsummarydata['compositetotalschange'] = float(
        #                         textblocksonpage[compositetotalsindex+3].replace(",", ""))
        #                     marketsummarydata['compositetotalsvolumetraded'] = int(
        #                         textblocksonpage[compositetotalsindex+4].replace(",", ""))
        #                     marketsummarydata['compositetotalsvaluetraded'] = float(
        #                         textblocksonpage[compositetotalsindex+5].replace(",", ""))
        #                     marketsummarydata['compositetotalsnumtrades'] = int(
        #                         textblocksonpage[compositetotalsindex+6].replace(",", ""))
        #                 except ValueError as ex:
        #                     logging.info(
        #                         'Could not find Composite Totals for this date'+pidstring)
        #                     marketsummarydata['compositetotalsindexvalue'] = None
        #                     marketsummarydata['compositetotalsindexchange'] = None
        #                     marketsummarydata['compositetotalschange'] = None
        #                     marketsummarydata['compositetotalsvolumetraded'] = None
        #                     marketsummarydata['compositetotalsvaluetraded'] = None
        #                     marketsummarydata['compositetotalsnumtrades'] = None
        #                 try:
        #                     alltnttotalsindex = textblocksonpage.index(
        #                         "All T&T Totals")
        #                     marketsummarydata['alltnttotalsindexvalue'] = float(
        #                         textblocksonpage[alltnttotalsindex+1].replace(",", ""))
        #                     marketsummarydata['alltnttotalsindexchange'] = float(
        #                         textblocksonpage[alltnttotalsindex+2].replace(",", ""))
        #                     marketsummarydata['alltnttotalschange'] = float(
        #                         textblocksonpage[alltnttotalsindex+3].replace(",", ""))
        #                     marketsummarydata['alltnttotalsvolumetraded'] = int(
        #                         textblocksonpage[alltnttotalsindex+4].replace(",", ""))
        #                     marketsummarydata['alltnttotalsvaluetraded'] = float(
        #                         textblocksonpage[alltnttotalsindex+5].replace(",", ""))
        #                     marketsummarydata['alltnttotalsnumtrades'] = int(
        #                         textblocksonpage[alltnttotalsindex+6].replace(",", ""))
        #                 except ValueError:
        #                     logging.info(
        #                         'Could not find All TNT Totals for this date'+pidstring)
        #                     marketsummarydata['alltnttotalsindexvalue'] = None
        #                     marketsummarydata['alltnttotalsindexchange'] = None
        #                     marketsummarydata['alltnttotalschange'] = None
        #                     marketsummarydata['alltnttotalsvolumetraded'] = None
        #                     marketsummarydata['alltnttotalsvaluetraded'] = None
        #                     marketsummarydata['alltnttotalsnumtrades'] = None
        #                 try:
        #                     crosslistedtotalsindex = textblocksonpage.index(
        #                         "Cross Listed Totals")
        #                     marketsummarydata['crosslistedtotalsindexvalue'] = float(
        #                         textblocksonpage[crosslistedtotalsindex+1].replace(",", ""))
        #                     marketsummarydata['crosslistedtotalsindexchange'] = float(
        #                         textblocksonpage[crosslistedtotalsindex+2].replace(",", ""))
        #                     marketsummarydata['crosslistedtotalschange'] = float(
        #                         textblocksonpage[crosslistedtotalsindex+3].replace(",", ""))
        #                     marketsummarydata['crosslistedtotalsvolumetraded'] = int(
        #                         textblocksonpage[crosslistedtotalsindex+4].replace(",", ""))
        #                     marketsummarydata['crosslistedtotalsvaluetraded'] = float(
        #                         textblocksonpage[compositetotalsindex+5].replace(",", ""))
        #                     marketsummarydata['crosslistedtotalsnumtrades'] = int(
        #                         textblocksonpage[compositetotalsindex+6].replace(",", ""))
        #                 except ValueError:
        #                     logging.info(
        #                         'Could not find Crosslisted Totals for this date'+pidstring)
        #                     marketsummarydata['crosslistedtotalsindexvalue'] = None
        #                     marketsummarydata['crosslistedtotalsindexchange'] = None
        #                     marketsummarydata['crosslistedtotalschange'] = None
        #                     marketsummarydata['crosslistedtotalsvolumetraded'] = None
        #                     marketsummarydata['crosslistedtotalsvaluetraded'] = None
        #                     marketsummarydata['crosslistedtotalsnumtrades'] = None
        #                 try:
        #                     smetotalsindex = textblocksonpage.index(
        #                         "SME Totals")
        #                     marketsummarydata['smetotalsindexvalue'] = float(
        #                         textblocksonpage[smetotalsindex+1].replace(",", ""))
        #                     marketsummarydata['smetotalsindexchange'] = float(
        #                         textblocksonpage[smetotalsindex+2].replace(",", ""))
        #                     marketsummarydata['smetotalschange'] = float(
        #                         textblocksonpage[smetotalsindex+3].replace(",", ""))
        #                     marketsummarydata['smetotalsvolumetraded'] = int(
        #                         textblocksonpage[smetotalsindex+4].replace(",", ""))
        #                     marketsummarydata['smetotalsvaluetraded'] = float(
        #                         textblocksonpage[smetotalsindex+5].replace(",", ""))
        #                     marketsummarydata['smetotalsnumtrades'] = int(
        #                         textblocksonpage[smetotalsindex+6].replace(",", ""))
        #                 except ValueError:
        #                     logging.info(
        #                         'Could not find SME Totals for this date'+pidstring)
        #                     marketsummarydata['smetotalsindexvalue'] = None
        #                     marketsummarydata['smetotalsindexchange'] = None
        #                     marketsummarydata['smetotalschange'] = None
        #                     marketsummarydata['smetotalsvolumetraded'] = None
        #                     marketsummarydata['smetotalsvaluetraded'] = None
        #                     marketsummarydata['smetotalsnumtrades'] = None
        #                 try:
        #                     mutualfundstotalsindex = textblocksonpage.index(
        #                         "Mutual Funds Totals")
        #                     marketsummarydata['mutualfundstotalsvolumetraded'] = int(
        #                         textblocksonpage[mutualfundstotalsindex+4].replace(",", ""))
        #                     marketsummarydata['mutualfundstotalsvaluetraded'] = float(
        #                         textblocksonpage[mutualfundstotalsindex+5].replace(",", ""))
        #                     marketsummarydata['mutualfundstotalsnumtrades'] = int(
        #                         textblocksonpage[mutualfundstotalsindex+6].replace(",", ""))
        #                 except ValueError:
        #                     logging.info(
        #                         'Could not find Mutual Funds Totals for this date'+pidstring)
        #                     marketsummarydata['mutualfundstotalsvolumetraded'] = None
        #                     marketsummarydata['mutualfundstotalsvaluetraded'] = None
        #                     marketsummarydata['mutualfundstotalsnumtrades'] = None
        #                 try:
        #                     secondtiertotalsindex = textblocksonpage.index(
        #                         "Second Tier Totals")
        #                     marketsummarydata['secondtiertotalsnumtrades'] = int(
        #                         textblocksonpage[secondtiertotalsindex+6].replace(",", ""))
        #                 except ValueError:
        #                     logging.info(
        #                         'Could not find Second Tier Totals for this date'+pidstring)
        #                     marketsummarydata['secondtiertotalsnumtrades'] = None
        #                 # add our dict to our list
        #                 allmarketsummarydata.append(marketsummarydata)
        #                 # Now parse the dailyequitysummary data
        #                 tablerows = driver.find_elements_by_tag_name("tr")
        #                 for row in tablerows:
        #                     # for each row in the table, get the td elements
        #                     rowcells = row.find_elements_by_tag_name("td")
        #                     # If we have exactly 14 elements in the row, then this is a valid row
        #                     if len(rowcells) == 14:
        #                         # we need to get the symbol and the sale date to test/validate the row data
        #                         testsymbol = rowcells[1].text
        #                         testsaledate = rowcells[10].text
        #                         # first check that the word in rowcells[1] is actually a valid symbol
        #                         if testsymbol in alllistedsymbols and testsaledate != ' ':
        #                             # if it is a valid symbol, check the last sale date
        #                             lastsaledate = datetime.strptime(
        #                                 testsaledate, '%d/%m/%y')
        #                             currentfetchdate = datetime.strptime(
        #                                 fetchdate, '%m/%d/%Y')
        #                             # create a dictionary to store data
        #                             equitytradingdata = dict(
        #                                 date=fetchdatedb)
        #                             # and start storing our useful data
        #                             equitytradingdata['symbol'] = rowcells[1].text
        #                             # if the last sale date is the date that we have fetched
        #                             if lastsaledate == currentfetchdate:
        #                                 # check if a value is present in each cell
        #                                 openprice = rowcells[2].text
        #                                 if openprice != ' ':
        #                                     equitytradingdata['openprice'] = float(
        #                                         openprice.replace(",", ""))
        #                                 else:
        #                                     equitytradingdata['openprice'] = 0
        #                                 high = rowcells[3].text
        #                                 if high != ' ':
        #                                     equitytradingdata['high'] = float(
        #                                         high.replace(",", ""))
        #                                 else:
        #                                     equitytradingdata['high'] = 0
        #                                 low = rowcells[4].text
        #                                 if low != ' ':
        #                                     equitytradingdata['low'] = float(
        #                                         low.replace(",", ""))
        #                                 else:
        #                                     equitytradingdata['low'] = 0
        #                                 osbid = rowcells[5].text
        #                                 if osbid != ' ':
        #                                     equitytradingdata['osbid'] = float(
        #                                         osbid.replace(",", ""))
        #                                 else:
        #                                     equitytradingdata['osbid'] = 0
        #                                 osbidvol = rowcells[6].text
        #                                 if osbidvol != ' ':
        #                                     equitytradingdata['osbidvol'] = int(
        #                                         osbidvol.replace(",", ""))
        #                                 else:
        #                                     equitytradingdata['osbidvol'] = 0
        #                                 osoffer = rowcells[7].text
        #                                 if osoffer != ' ':
        #                                     equitytradingdata['osoffer'] = float(
        #                                         osoffer.replace(",", ""))
        #                                 else:
        #                                     equitytradingdata['osoffer'] = 0
        #                                 osoffervol = rowcells[8].text
        #                                 if osoffervol != ' ':
        #                                     equitytradingdata['osoffervol'] = int(
        #                                         osoffervol.replace(",", ""))
        #                                 else:
        #                                     equitytradingdata['osoffervol'] = 0
        #                                 saleprice = rowcells[9].text
        #                                 if saleprice != ' ':
        #                                     equitytradingdata['saleprice'] = float(
        #                                         saleprice.replace(",", ""))
        #                                 else:
        #                                     equitytradingdata['saleprice'] = 0
        #                                 volumetraded = rowcells[11].text
        #                                 if volumetraded != ' ':
        #                                     equitytradingdata['volumetraded'] = int(
        #                                         volumetraded.replace(",", ""))
        #                                 else:
        #                                     equitytradingdata['volumetraded'] = 0
        #                                 closeprice = rowcells[12].text
        #                                 if closeprice != ' ':
        #                                     equitytradingdata['closeprice'] = float(
        #                                         closeprice.replace(",", ""))
        #                                 else:
        #                                     equitytradingdata['closeprice'] = 0
        #                                 changedollars = rowcells[13].text
        #                                 if changedollars != ' ':
        #                                     equitytradingdata['changedollars'] = float(
        #                                         changedollars.replace(",", ""))
        #                                 else:
        #                                     equitytradingdata['changedollars'] = 0.00
        #                             else:
        #                                 # else if the last sale date does not match the current date being checked, simply store 0s
        #                                 for key in ['openprice', 'high', 'low', 'osbid', 'osbidvol', 'osoffer', 'osoffervol', 'saleprice', 'volumetraded', 'closeprice', 'changedollars']:
        #                                     equitytradingdata[key] = 0
        #                                 # now add our dictionary to our list
        #                             allequitytradingdata.append(
        #                                 equitytradingdata)
        #                 pageloaded = True
        #         except WebDriverException as ex:
        #             logging.error(
        #                 "Problem found while fetching date at "+fetchdate+pidstring+" : "+str(ex))
        #             timewaitsecs = 30
        #             time.sleep(timewaitsecs)
        #             logging.error("Waited "+str(timewaitsecs)+" seconds. Now retrying " +
        #                           fetchdate+pidstring+" attempt ("+str(webretries)+"/"+str(failmaxtimes)+")")
        #             webretries += 1
        # Now write the data to the db if there is data to write
        # if allequitytradingdata:
        #     # convert symbols into stockcodes for the dailyequitysummary table
        #     selectstmt = select(
        #         [listedequities_table.c.symbol, listedequities_table.c.stockcode])
        #     result = db_connect.dbcon.execute(selectstmt)
        #     for row in result:
        #         # The first element in our row tuple is the symbol, and the second is our stockcode
        #         for equitytradingdata in allequitytradingdata:
        #             # Map the symbol for each equity to an stockcode in our table
        #             if equitytradingdata['symbol'] == row[0]:
        #                 equitytradingdata['stockcode'] = row[1]
        #     # Calculate the valuetraded for each row and remove our unneeded columns
        #     for equitytradingdata in allequitytradingdata:
        #         equitytradingdata['valuetraded'] = float(
        #             equitytradingdata['saleprice']) * float(equitytradingdata['volumetraded'])
        #         equitytradingdata.pop('symbol', None)
        #     # insert data into dailyequitysummary table
        #     insert_stmt = insert(dailyequitysummary_table).values(
        #         allequitytradingdata)
        #     on_duplicate_key_stmt = insert_stmt.on_duplicate_key_update(
        #         openprice=insert_stmt.inserted.openprice,
        #         high=insert_stmt.inserted.high,
        #         low=insert_stmt.inserted.low,
        #         osbid=insert_stmt.inserted.osbid,
        #         osbidvol=insert_stmt.inserted.osbidvol,
        #         osoffer=insert_stmt.inserted.osoffer,
        #         osoffervol=insert_stmt.inserted.osoffervol,
        #         saleprice=insert_stmt.inserted.saleprice,
        #         volumetraded=insert_stmt.inserted.volumetraded,
        #         valuetraded=insert_stmt.inserted.valuetraded,
        #         closeprice=insert_stmt.inserted.closeprice,
        #         changedollars=insert_stmt.inserted.changedollars
        #     )
        #     result = db_connect.dbcon.execute(on_duplicate_key_stmt)
        #     logging.info(
        #         "Number of rows affected in the dailyequitysummary table was "+str(result.rowcount)+pidstring)
        db_connect.close()
        return 0
    except Exception:
        logging.exception("Could not complete dailyequitysummary update.")
        customlogging.flush_smtp_logger()
    finally:
        if 'driver' in locals() and driver is not None:
            # Always close the browser
            driver.quit()
            logging.info("Successfully closed web browser."+pidstring)


def update_equity_summary_data():
    """Use the selenium module to open the Chrome browser and browse through
    every day of trading summaries listed at  
    https://stockex.co.tt/controller.php?action=view_quote&TradingDate=03/13/2020
    and scrape the useful output into a list of dictionaries for the DB
    """
    logging.info("Now updating market summary data.")
    try:
        db_connect = DatabaseConnect()
        logging.info("Successfully connected to database.")
        # Reflect the tables already created in our db
        logging.info("Reading existing data from tables in database...")
        historicalmarketsummary_table = Table(
            'historicalmarketsummary', MetaData(), autoload=True, autoload_with=db_connect.dbengine)
        listedequities_table = Table(
            'listedequities', MetaData(), autoload=True, autoload_with=db_connect.dbengine)
        # Now select the dates that we already have recorded
        logging.info("Creating list of dates to fetch.")
        datesalreadyrecorded = []
        selectstmt = select([historicalmarketsummary_table.c.date])
        result = db_connect.dbcon.execute(selectstmt)
        for row in result:
            # We only have a single element in each row tuple, which is the date
            datesalreadyrecorded.append(row[0])
        # Also get a list of all valid symbols from the db
        alllistedsymbols = []
        selectstmt = select([listedequities_table.c.symbol])
        result = db_connect.dbcon.execute(selectstmt)
        for row in result:
            # We only have a single element in each row tuple, which is the symbol
            alllistedsymbols.append(row[0])
        # We want to gather data on all trading days since 01/01/2010, so we create a list
        # of all dates that we need to gather still
        datestofetch = []
        fetchdate = datetime.strptime(START_DATE, "%m/%d/%Y")
        logging.info(
            "Getting all dates that are not already fetched and are not weekends.")
        # TODO: Extend holidays library for Trinidad and Tobago
        # Get all dates until yesterday
        while fetchdate < datetime.now():
            # if we do not have info on this date already and this is a weekday (stock markets close on weekends)
            if (fetchdate.date() not in datesalreadyrecorded) and (fetchdate.weekday() < 5):
                # add this date to be fetched
                datestofetch.append(fetchdate.strftime("%m/%d/%Y"))
            # increment the date by one day
            fetchdate += timedelta(days=1)
        # now split our datestofetch list into sublists to multithread
        logging.info(
            "List of dates to fetch built. Now splitting list by core.")
        numcores = multiprocessing.cpu_count()
        logging.info("This machine has "+str(numcores)+" logical CPU cores.")
        listlength = len(datestofetch)
        dates_to_fetch_sublists = [datestofetch[i*listlength // numcores: (i+1)*listlength // numcores]
                                   for i in range(numcores)]
        return dates_to_fetch_sublists, alllistedsymbols
    except Exception as ex:
        logging.exception(
            "We ran into a problem while trying to fetch the historical markey summary data.")
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
        db_connect = DatabaseConnect()
        logging.info("Successfully connected to database")
        # Reflect the tables already created in our db
        dailyequitysummary_table = Table(
            'dailyequitysummary', MetaData(), autoload=True, autoload_with=db_connect.dbengine)
        listedequities_table = Table(
            'listedequities', MetaData(), autoload=True, autoload_with=db_connect.dbengine)
        # Also get a list of all valid symbols from the db
        alllistedsymbols = []
        selectstmt = select([listedequities_table.c.symbol])
        result = db_connect.dbcon.execute(selectstmt)
        for row in result:
            # We only have a single element in each row tuple, which is the symbol
            alllistedsymbols.append(row[0])
        # This list of dicts will contain all data to be written to the db
        allequitytradingdata = []
        # open the browser
        logging.info("Now opening the Chrome browser")
        options = Options()
        options.headless = True
        driver = webdriver.Chrome(options=options)
        # set up a variable to retry page loads on Internet failures
        webretries = 1
        # check if the page was loaded or has failed enough times
        pageloaded = False
        failmaxtimes = 3
        while not pageloaded and webretries <= failmaxtimes:
            try:
                # Then go the stockex.co.tt URL that lists all equities traded for today
                todayequitysummaryurl = "https://stockex.co.tt/controller.php?action=view_quote"
                logging.info("Navigating to "+todayequitysummaryurl)
                driver.get(todayequitysummaryurl)
                # first find the data of the info that we are fetching
                tradingdate = driver.find_element_by_name(
                    "TradingDate").get_attribute("value")
                logging.info("Now parsing data for: "+tradingdate)
                tablerows = driver.find_elements_by_tag_name("tr")
                for row in tablerows:
                    # for each row in the table, get the td elements
                    rowcells = row.find_elements_by_tag_name("td")
                    # If we have exactly 14 elements in the row, then this is a valid row
                    if len(rowcells) == 14:
                        # we need to get the symbol and the sale date to test/validate the row data
                        testsymbol = rowcells[1].text
                        testsaledate = rowcells[10].text
                        # first check that the word in rowcells[1] is actually a valid symbol
                        if testsymbol in alllistedsymbols and testsaledate != ' ':
                            # if it is a valid symbol, check the last sale date
                            lastsaledate = datetime.strptime(
                                testsaledate, '%d/%m/%y')
                            currentfetchdate = datetime.strptime(
                                tradingdate, '%Y-%m-%d')
                            # if the last sale date is the date that we have fetched
                            if lastsaledate == currentfetchdate:
                                # then create a dictionary to store data
                                equitytradingdata = dict(date=tradingdate)
                                # and start storing our useful data
                                equitytradingdata['symbol'] = rowcells[1].text
                                # for each value, check if a value is present
                                openprice = rowcells[2].text
                                if openprice != ' ':
                                    equitytradingdata['openprice'] = float(
                                        openprice.replace(",", ""))
                                else:
                                    equitytradingdata['openprice'] = None
                                high = rowcells[3].text
                                if high != ' ':
                                    equitytradingdata['high'] = float(
                                        high.replace(",", ""))
                                else:
                                    equitytradingdata['high'] = None
                                low = rowcells[4].text
                                if low != ' ':
                                    equitytradingdata['low'] = float(
                                        low.replace(",", ""))
                                else:
                                    equitytradingdata['low'] = None
                                osbid = rowcells[5].text
                                if osbid != ' ':
                                    equitytradingdata['osbid'] = float(
                                        osbid.replace(",", ""))
                                else:
                                    equitytradingdata['osbid'] = None
                                osbidvol = rowcells[6].text
                                if osbidvol != ' ':
                                    equitytradingdata['osbidvol'] = int(
                                        osbidvol.replace(",", ""))
                                else:
                                    equitytradingdata['osbidvol'] = None
                                osoffer = rowcells[7].text
                                if osoffer != ' ':
                                    equitytradingdata['osoffer'] = float(
                                        osoffer.replace(",", ""))
                                else:
                                    equitytradingdata['osoffer'] = None
                                osoffervol = rowcells[8].text
                                if osoffervol != ' ':
                                    equitytradingdata['osoffervol'] = int(
                                        osoffervol.replace(",", ""))
                                else:
                                    equitytradingdata['osoffervol'] = None
                                saleprice = rowcells[9].text
                                if saleprice != ' ':
                                    equitytradingdata['saleprice'] = float(
                                        saleprice.replace(",", ""))
                                else:
                                    equitytradingdata['saleprice'] = None
                                volumetraded = rowcells[11].text
                                if volumetraded != ' ':
                                    equitytradingdata['volumetraded'] = int(
                                        volumetraded.replace(",", ""))
                                else:
                                    equitytradingdata['volumetraded'] = None
                                closeprice = rowcells[12].text
                                if closeprice != ' ':
                                    equitytradingdata['closeprice'] = float(
                                        closeprice.replace(",", ""))
                                else:
                                    equitytradingdata['closeprice'] = None
                                changedollars = rowcells[13].text
                                if changedollars != ' ':
                                    equitytradingdata['changedollars'] = float(
                                        changedollars.replace(",", ""))
                                else:
                                    equitytradingdata['changedollars'] = None
                                # now add our dictionary to our list
                                allequitytradingdata.append(
                                    equitytradingdata)
                pageloaded = True
            except WebDriverException as ex:
                logging.error(
                    "Problem found while fetching date at "+tradingdate+" : "+str(ex))
                timewaitsecs = 30
                time.sleep(timewaitsecs)
                logging.error("Waited "+str(timewaitsecs)+" seconds. Now retrying " +
                              tradingdate+" attempt ("+str(webretries)+"/"+str(failmaxtimes)+")")
                webretries += 1
        # Now write the data to the db
        logging.info("Now writing scraped data to DB")
        # convert symbols into stockcodes for the dailyequitysummary table
        selectstmt = select(
            [listedequities_table.c.symbol, listedequities_table.c.stockcode])
        result = db_connect.dbcon.execute(selectstmt)
        for row in result:
            # The first element in our row tuple is the symbol, and the second is our stockcode
            for equitytradingdata in allequitytradingdata:
                # Map the symbol for each equity to an stockcode in our table
                if equitytradingdata['symbol'] == row[0]:
                    equitytradingdata['stockcode'] = row[1]
        # Calculate the valuetraded for each row and remove our unneeded columns
        for equitytradingdata in allequitytradingdata:
            equitytradingdata['valuetraded'] = float(
                equitytradingdata['saleprice']) * float(equitytradingdata['volumetraded'])
            equitytradingdata.pop('symbol', None)
        # insert data into dailyequitysummary table
        insert_stmt = insert(dailyequitysummary_table).values(
            allequitytradingdata)
        on_duplicate_key_stmt = insert_stmt.on_duplicate_key_update(
            openprice=insert_stmt.inserted.openprice,
            high=insert_stmt.inserted.high,
            low=insert_stmt.inserted.low,
            osbid=insert_stmt.inserted.osbid,
            osbidvol=insert_stmt.inserted.osbidvol,
            osoffer=insert_stmt.inserted.osoffer,
            osoffervol=insert_stmt.inserted.osoffervol,
            saleprice=insert_stmt.inserted.saleprice,
            volumetraded=insert_stmt.inserted.volumetraded,
            valuetraded=insert_stmt.inserted.valuetraded,
            closeprice=insert_stmt.inserted.closeprice,
            changedollars=insert_stmt.inserted.changedollars
        )
        result = db_connect.dbcon.execute(on_duplicate_key_stmt)
        logging.info(
            "Number of rows affected in the dailyequitysummary table was "+str(result.rowcount))
        return 0
    except Exception:
        logging.exception("Could not complete daily trade update.")
        customlogging.flush_smtp_logger()
    finally:
        if 'driver' in locals() and driver is not None:
            # Always close the browser
            driver.quit()
            logging.info(
                "Successfully closed web browser in daily trade update.")


def main():
    """The main steps in coordinating the scraping"""
    try:
        # Set up logging for this module
        q_listener, q = customlogging.setup_logging(
            logdirparent=str(os.path.dirname(os.path.realpath(__file__))),
            logfilestandardname=os.path.basename(__file__),
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
            global START_DATE
            if args.full_history:
                START_DATE = '01/01/2010'
            else:
                START_DATE = (datetime.now() +
                              relativedelta(months=-1)).strftime('%m/%d/%Y')
            # run all functions within a multiprocessing pool
            with multiprocessing.Pool(os.cpu_count(), customlogging.logging_worker_init, [q]) as multipool:
                logging.info("Now starting TTSE scraper.")
                if args.daily_update:
                    multipool.apply_async(
                        update_daily_trades, ())
                else:
                    # multipool.apply_async(scrape_listed_equity_data, ())
                    # multipool.apply_async(scrape_dividend_data, ())
                    # multipool.apply_async(scrape_historical_data, ())
                    # multipool.apply_async(update_dividend_yield, ())
                    # block on the next function to wait until the dates are ready
                    dates_to_fetch_sublists, alllistedsymbols = multipool.apply(
                        update_equity_summary_data, ())
                    # now call the individual workers to fetch these dates
                    for coredatelist in dates_to_fetch_sublists:
                        multipool.apply_async(
                            scrape_equity_summary_data, (coredatelist, alllistedsymbols))
                multipool.close()
                multipool.join()
                q_listener.stop()
    except Exception as exc:
        logging.exception("Error in script " +
                          os.path.basename(__file__)+": "+str(exc))
        customlogging.flush_smtp_logger()
        sys.exit(1)
    else:
        logging.info(os.path.basename(__file__)+" executed successfully.")
        sys.exit(0)


# If this script is being run from the command-line, then run the main() function
if __name__ == "__main__":
    main()
