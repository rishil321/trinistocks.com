#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""This script uses Firefox and Selenium to pull the historical data from the TTSE website
and store it in a db"""

# Put all your imports here, one per line. However multiple imports from the same lib are allowed on a line.
# Imports from Python standard lib
import logging
import sys
import os
import traceback
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
# Imports from the cheese factory
from pid import PidFile
import requests
import urllib.request
from sqlalchemy import create_engine, Table, select, MetaData, text, and_
from sqlalchemy.dialects.mysql import insert
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from selenium.common.exceptions import WebDriverException
from selenium import webdriver
from selenium.webdriver.firefox.options import Options, FirefoxProfile
# Imports from the local machine
import ttsescraperconfig
from customlogging import customlogging

# Put your constants here. These should be named in CAPS.

# Put your global variables here.
# The following equity ids are listed in USD on the exchange (all others are listed in TTD)
usdequityids = [144]

# Put your class definitions here. These should use the CapWords convention.

# Put your function definitions here. These should be lower-case, separated by underscores.


def scrape_listed_equity_data():
    """Use the selenium module to open the Firefox browser and browse through
    all listed equity securities at the URL 
    https://www.stockex.co.tt/controller.php?action=listed_companies
    and scrape the useful output into a list of dictionaries  
    """
    try:
        # This list of dicts will contain all data to be written to the db
        alllistedequitydata = []
        # First open the browser
        logging.info("Now opening the Firefox browser")
        options = Options()
        options.headless = True
        profile = FirefoxProfile()
        profile.set_preference('security.tls.version.enable-deprecated', True)
        driver = webdriver.Firefox(profile, options=options)
        # Then go the stockex.co.tt URL that lists all equities
        logging.info(
            "Navigating to https://stockex.co.tt/controller.php?action=listed_companies")
        driver.get("https://stockex.co.tt/controller.php?action=listed_companies")
        # All equities are wrapped in an 'a' tag, so we filter all of these elements
        # into an array
        pagelinks = driver.find_elements_by_tag_name("a")
        listedequities = []
        # For each 'a' element that we found
        for link in pagelinks:
            # All valid equities contain the StockCode keyword in their href
            if "StockCode" in link.get_attribute("href"):
                # So we add the links to all equities to our array
                listedequities.append(link.get_attribute("href"))
        # Now we go through each equity in our list and navigate to their page
        logging.info("Found links for "+str(len(listedequities))+" equities.")
        for equity in listedequities:
            # Create a boolean to check for duplicated symbols
            symbolalreadyadded = False
            # Load the URL for this equity
            driver.get(equity)
            # Dismiss the alert asking you to download flash player
            driver.switch_to.alert.dismiss()
            # Find all elements on this page with the <td> tag
            # All important values for the equity are wrapped in these tags
            importantelements = driver.find_elements_by_tag_name("td")
            # Create a dictionary to store the important elements extracted from the page
            listedsecuritydata = {}
            # Go through each important element and identify certain key values that we want to add
            for index, element in enumerate(importantelements):
                if element.text is not None:
                    # We first check the symbol for this equity, to see if we have already scraped it
                    # since symbols must be unique
                    if "Symbol" in element.text:
                        securitysymbol = element.text[9:]
                        listedsecuritydata["symbol"] = securitysymbol
                    if "Company Name" in element.text:
                        securityname = element.text[15:]
                        listedsecuritydata["securityname"] = securityname
                    if "Status" in element.text:
                        securitystatus = element.text[9:]
                        listedsecuritydata["status"] = securitystatus
                    if "Sector" in element.text:
                        securitysector = element.text[9:]
                        listedsecuritydata["sector"] = securitysector
                    if "Issued Share Capital" in element.text:
                        # The value for this is not stored in the same column, but in
                        # the subsequent two columns
                        issuedsharecapital = importantelements[index+2].text
                        # Remove any commas in the scraped value and convert to int
                        issuedsharecapital = int(
                            issuedsharecapital.replace(",", ""))
                        listedsecuritydata["issuedsharecapital"] = issuedsharecapital
                    if "Market Capitalization" in element.text:
                        # The value for this is not stored in the same column, but in
                        # the subsequent two columns
                        marketcapitalization = importantelements[index+2].text[1:]
                        # Remove any commas in the scraped value and convert to decimal
                        marketcapitalization = Decimal(
                            marketcapitalization.replace(",", ""))
                        listedsecuritydata["marketcapitalization"] = marketcapitalization
            # Now we have all the important information for this equity
            # So we can add the dictionary object to our global list
            # But first we check that this symbol has not been added already
            if alllistedequitydata:
                for addedsecuritydata in alllistedequitydata:
                    if addedsecuritydata['symbol'] == securitysymbol:
                        symbolalreadyadded = True
            if not symbolalreadyadded:
                alllistedequitydata.append(listedsecuritydata)
            logging.info("Successfully added data for: " +
                         listedsecuritydata['securityname'])
        # And return our list of dicts containing the scraped data
        return alllistedequitydata
    except Exception as ex:
        raise
    finally:
        if 'driver' in locals() and driver is not None:
            # Always close the browser
            driver.quit()
            logging.info("Successfully closed web browser.")


def write_listed_equity_data_to_db(listedequitydata):
    """This function takes the list of dicts containing data that was scraped
    from the website and writes it to our MySQL database
    """
    engine = None
    try:
        logging.info("Attemping to write listed equity data into database...")
        # First fetch all the necessary login data from our config file
        dbuser = ttsescraperconfig.dbusername
        dbpass = ttsescraperconfig.dbpassword
        dbaddress = ttsescraperconfig.dbaddress
        dbschema = ttsescraperconfig.schema
        engine = create_engine("mysql://"+dbuser+":"+dbpass+"@"+dbaddress+"/" +
                               dbschema, echo=False)
        # Try to create a connection to the db
        dbcon = engine.connect()
        if engine.connect():
            logging.info("Successfully connected to database.")
            # Each table is mapped to a class, so we create references to those classes
            listedequitiestable = Table(
                'listedequities', MetaData(), autoload=True, autoload_with=engine)
            # We first handle the data for the listedequities table
            logging.info("Preparing data for the listedequities table...")
            # Find the equityid for each equity in the listedequities table
            selectstmt = select(
                [listedequitiestable.c.symbol, listedequitiestable.c.equityid])
            result = dbcon.execute(selectstmt)
            for row in result:
                # Map the symbol for each equity to an equityid in our table
                for securitydata in listedequitydata:
                    if securitydata['symbol'] == row[0]:
                        securitydata['equityid'] = row[1]
            # Insert any new data into our table
            logging.info("Inserting data into listedequities table")
            insert_stmt = insert(listedequitiestable).values(listedequitydata)
            on_duplicate_key_stmt = insert_stmt.on_duplicate_key_update(
                symbol=insert_stmt.inserted.symbol,
                status=insert_stmt.inserted.status,
                sector=insert_stmt.inserted.sector,
                issuedsharecapital=insert_stmt.inserted.issuedsharecapital,
                marketcapitalization=insert_stmt.inserted.marketcapitalization
            )
            result = dbcon.execute(on_duplicate_key_stmt)
            logging.info("Number of rows affected was "+str(result.rowcount))
            return 0
        else:
            raise Exception("Failed to connect to database.")
    except:
        raise
    finally:
        if engine is not None:
            # Close the SQLAlchemy engine
            engine.dispose()
            logging.info("Successfully disconnected from database.")


def scrape_dividend_data():
    """Use the Selenium module to open the Firefox browser and browse through
    all listed equity securities at the URL 
    https://www.stockex.co.tt/controller.php?action=listed_companies
    For each listed security, press the corporate action button and get
    all data from 2010 to the current date 
    """
    # Create a variable for our webdriver
    driver = None
    try:
        # This list of dicts will contain all data to be written to the db
        alldividenddatascraped = []
        # First open the browser
        logging.info(
            "Now opening the Firefox browser to scrape dividend data.")
        options = Options()
        options.headless = True
        profile = FirefoxProfile()
        profile.set_preference('security.tls.version.enable-deprecated', True)
        driver = webdriver.Firefox(profile, options=options)
        # Then go the stockex.co.tt URL that lists all equities
        driver.get("https://stockex.co.tt/controller.php?action=listed_companies")
        logging.info(
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
                     " equities. Now navigating to all.")
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
                startdateinputfield.send_keys("01/01/2010")
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
                dividendrecorddates = []
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
                        dividendrecorddates = \
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
                for dividenddata in alldividenddatascraped:
                    if dividenddata['symbol'] == symbol:
                        symbolalreadyadded = True
                        break
                # If we have not already added data for this symbol, then we continue
                if not symbolalreadyadded:
                    # Now create our list of dicts containing all of the dividends
                    for index, date in enumerate(dividendrecorddates):
                        # For each record, check that our date is valid
                        try:
                            parseddate = parse(date, fuzzy=True).date()
                            # If the parse does not throw an errors, continue
                            # Check if multiple duplicate dates might be present
                            datealreadyadded = False
                            for dividenddata in alldividenddatascraped:
                                if dividenddata['symbol'] == symbol:
                                    if dividenddata['recorddate'] == parseddate:
                                        # If the date has already been added for this
                                        # equity, then just sum the dividend amounts
                                        dividenddata['dividendamount'] = str(
                                            Decimal(dividenddata['dividendamount']) + Decimal(dividendamounts[index]))
                                        datealreadyadded = True
                            if not datealreadyadded:
                                # If the date has not been added yet for this symbol, then
                                # add the entire new record
                                dividenddata = {'recorddate': parseddate,
                                                'dividendamount': dividendamounts[index],
                                                'currency': dividendcurrencies[index],
                                                'symbol': symbol}
                                alldividenddatascraped.append(dividenddata)
                        except ValueError:
                            # If the parsing does throw an error, then we do not add the element
                            # as it is not a valid date
                            pass
                    logging.info(
                        "Successfully fetched dividend data for "+symbol)
                else:
                    logging.info("Symbol already added. Skipping.")
            except Exception as e:
                logging.error(
                    "Unable to scrape dividend data for stockcode:"+stockcode+". "+str(e))
        # And return our list of dicts containing the scraped data
        return alldividenddatascraped
    except Exception as ex:
        raise
    finally:
        if 'driver' in locals() and driver is not None:
            # Always close the browser
            driver.quit()
            logging.info("Successfully closed web browser.")


def write_dividend_data_to_db(alldividenddatatoinsert):
    """This function takes the list of dicts containing dividend data that was scraped
    from the website and writes it to our MySQL database
    """
    # Create a variable for our database engine
    engine = None
    try:
        logging.info("Attempting to write dividend data into database...")
        # First fetch all the necessary login data from our config file
        dbuser = ttsescraperconfig.dbusername
        dbpass = ttsescraperconfig.dbpassword
        dbaddress = ttsescraperconfig.dbaddress
        dbschema = ttsescraperconfig.schema
        engine = create_engine("mysql://"+dbuser+":"+dbpass+"@"+dbaddress+"/" +
                               dbschema, echo=False)
        # Try to create a connection to the db
        dbcon = engine.connect()
        if dbcon:
            logging.info("Successfully connected to database.")
            # Reflect the tables already created in our db
            historicaldividendinfo = Table(
                'historicaldividendinfo', MetaData(), autoload=True, autoload_with=engine)
            listedequities = Table(
                'listedequities', MetaData(), autoload=True, autoload_with=engine)
            # Now select the symbols and equityids from our existing securities
            selectstmt = select(
                [listedequities.c.symbol, listedequities.c.equityid])
            result = dbcon.execute(selectstmt)
            for row in result:
                # The first element in our row tuple is the symbol, and the second is our equityid
                # Map the symbol for each equity to an equityid in our table
                for dividenddatatoinsert in alldividenddatatoinsert:
                    if dividenddatatoinsert['symbol'] == row[0]:
                        dividenddatatoinsert['equityid'] = row[1]
            # Now remove our unneeded columns
            for dividenddatatoinsert in alldividenddatatoinsert:
                dividenddatatoinsert.pop('symbol', None)
            # Now we add the new data into our db
            logging.info("Inserting data into database.")
            insert_stmt = insert(historicaldividendinfo).values(
                alldividenddatatoinsert)
            on_duplicate_key_stmt = insert_stmt.on_duplicate_key_update(
                dividendamount=insert_stmt.inserted.dividendamount,
                currency=insert_stmt.inserted.currency,
            )
            result = dbcon.execute(on_duplicate_key_stmt)
            logging.info("Number of rows affected was "+str(result.rowcount))
            logging.info(
                "Successfully wrote data for the historicaldividenddata table into database.")
            return 0
        else:
            raise ConnectionError("Failed to connect to database.")
    except:
        raise
    finally:
        if engine is not None:
            # Always close the database engine
            engine.dispose()
            logging.info("Successfully disconnected from database.")


def scrape_historical_data():
    """Use the Selenium module to open the Firefox browser and browse through
    all listed equity securities at the URL 
    https://www.stockex.co.tt/controller.php?action=listed_companies
    For each listed security, press the history button and get
    all data from 2010 to the current date 
    """
    try:
        # This list of dicts will contain all data to be written to the db
        allhistoricaldata = []
        # First open the browser
        logging.info("Now opening the Firefox browser")
        options = Options()
        options.headless = True
        profile = FirefoxProfile()
        profile.set_preference('security.tls.version.enable-deprecated', True)
        driver = webdriver.Firefox(profile, options=options)
        # Then go the stockex.co.tt URL that lists all equities
        logging.info(
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
                startdateinputfield.send_keys("01/01/2010")
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
                # The link we want to find is wrapped in an "a" tag
                importantelements = driver.find_elements_by_tag_name("a")
                for importantelement in importantelements:
                    if importantelement.text == "Download CSV":
                        downloadURL = importantelement.get_attribute("href")
                        # Set the download location to the same directory of this script
                        historicaldatafile = os.path.dirname(os.path.realpath(
                            __file__)) + os.path.sep + "historicaldata.csv"
                        # Delete the file if it exists
                        try:
                            os.remove(historicaldatafile)
                        except OSError:
                            pass
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
                filestream = open('historicaldata.csv', 'r')
                csvreader = csv.reader(filestream)
                for csvline in csvreader:
                    # Each comma-separated value represents something to add to our list
                    stockhistorydates.append(csvline[0])
                    closingquotes.append(csvline[1])
                    changedollars.append(csvline[2])
                    volumetraded.append(csvline[4])
                filestream.close()
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
        # Then close the browser
        driver.quit()
        # And return our list of dicts containing the scraped data
        return allhistoricaldata
    except Exception as ex:
        raise
    finally:
        if 'driver' in locals() and driver is not None:
            # Always close the browser
            driver.quit()
            logging.info("Successfully closed web browser.")


def write_historical_data_to_db(allhistoricaldatatoinsert):
    """This function takes the list of dicts containing historical data that was scraped
    from the website and writes it to our MySQL database
    """
    engine = None
    try:
        logging.info("Attemping to write historical data into database...")
        # First fetch all the necessary login data from our config file
        dbuser = ttsescraperconfig.dbusername
        dbpass = ttsescraperconfig.dbpassword
        dbaddress = ttsescraperconfig.dbaddress
        dbschema = ttsescraperconfig.schema
        engine = create_engine("mysql://"+dbuser+":"+dbpass+"@"+dbaddress+"/" +
                               dbschema, echo=False)
        # Try to create a connection to the db
        dbcon = engine.connect()
        if engine.connect():
            logging.info("Successfully connected to database.")
            # Each table is mapped to a class, so we create references to those classes
            historicalstockinfotable = Table(
                'historicalstockinfo', MetaData(), autoload=True, autoload_with=engine)
            listedequities = Table(
                'listedequities', MetaData(), autoload=True, autoload_with=engine)
            logging.info("Preparing data for the historicalstockinfo table...")
            # Now select the symbols and equityids from our existing securities
            selectstmt = select(
                [listedequities.c.symbol, listedequities.c.equityid])
            result = dbcon.execute(selectstmt)
            for row in result:
                # The first element in our row tuple is the symbol, and the second is our equityid
                for historicaldatatoinsert in allhistoricaldatatoinsert:
                    # Map the symbol for each equity to an equityid in our table
                    if historicaldatatoinsert['symbol'] == row[0]:
                        historicaldatatoinsert['equityid'] = row[1]
                    # Check whether this data is for a non-TTD equity
                    if historicaldatatoinsert['symbol'] in usdequitysymbols:
                        historicaldatatoinsert['currency'] = 'USD'
                    else:
                        historicaldatatoinsert['currency'] = 'TTD'
            # Now remove our unneeded columns
            for historicaldatatoinsert in allhistoricaldatatoinsert:
                historicaldatatoinsert.pop('symbol', None)
            # Now we add the new data into our db
            logging.info("Inserting data into database.")
            insert_stmt = insert(historicalstockinfotable).values(
                allhistoricaldatatoinsert)
            on_duplicate_key_stmt = insert_stmt.on_duplicate_key_update(
                closingquote=insert_stmt.inserted.closingquote,
                changedollars=insert_stmt.inserted.changedollars,
                volumetraded=insert_stmt.inserted.volumetraded,
                currency=insert_stmt.inserted.currency,
            )
            result = dbcon.execute(on_duplicate_key_stmt)
            logging.info("Number of rows affected was "+str(result.rowcount))
            logging.info(
                "Successfully wrote data for the historicalstockinfo table into database.")
            return 0
        else:
            raise ConnectionError("Failed to connect to database.")
    except Exception as ex:
        raise
    finally:
        if engine is not None:
            # Always close the database engine
            engine.dispose()
            logging.info("Successfully disconnected from database.")


def update_dividend_yield():
    """This function goes through each equity listed in the listedequity table
    and calculates the dividend yield for that equity for each year that we have data on it
    """
    engine = None
    try:
        logging.info("Now calculating dividend yield.")
        # First fetch all the necessary login data from our config file
        dbuser = ttsescraperconfig.dbusername
        dbpass = ttsescraperconfig.dbpassword
        dbaddress = ttsescraperconfig.dbaddress
        dbschema = ttsescraperconfig.schema
        engine = create_engine("mysql://"+dbuser+":"+dbpass+"@"+dbaddress+"/" +
                               dbschema, echo=False)
        # Try to create a connection to the db
        dbcon = engine.connect()
        if engine.connect():
            logging.info("Successfully connected to database.")
            # Each table is mapped to a class, so we create references to those classes
            historicaldividendinfo = Table(
                'historicaldividendinfo', MetaData(), autoload=True, autoload_with=engine)
            listedequities = Table(
                'listedequities', MetaData(), autoload=True, autoload_with=engine)
            dividendyieldtable = Table(
                'dividendyield', MetaData(), autoload=True, autoload_with=engine)
            # Now get all dividends payments, their dates and their equityids
            logging.info("Now fetching all dividend data listed in DB.")
            selectstmt = select([historicaldividendinfo.c.dividendamount, historicaldividendinfo.c.recorddate,
                                 historicaldividendinfo.c.equityid, historicaldividendinfo.c.currency])
            result = dbcon.execute(selectstmt)
            # And store them in lists
            all_dividend_data = []
            # Also create an extra list to find a set from
            dividend_equity_ids = []
            for row in result:
                all_dividend_data.append(
                    dict(amount=row[0], date=row[1], equityid=row[2], currency=row[3]))
                dividend_equity_ids.append(row[2])
            # Get a unique list of the equityids that have dividends above
            unique_equity_ids = list(set(dividend_equity_ids))
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
            for equity_id in unique_equity_ids:
                # for each year in our list, get the last closingquote for the year and store it
                for datetime_year in datetime_years_to_fetch:
                    selectstmt = text(
                        "SELECT closingquote,date FROM historicalstockinfo WHERE YEAR(date) = :yr AND equityid = :eq ORDER BY date DESC LIMIT 1;")
                    result = dbcon.execute(
                        selectstmt, yr=datetime_year.year, eq=equity_id)
                    row = result.fetchone()
                    try:
                        stockyearlydata.append(
                            dict(equityid=equity_id, closingquote=row[0], date=row[1]))
                    except TypeError:
                        # if we do not have quotes for a particular year, simply ignore it
                        pass
            # Now we need to compute the total dividend amount per year
            # Create a list of dictionaries to store the dividend per year per equityid
            dividend_yearly_data = []
            for equity_id in unique_equity_ids:
                # Create a dictionary to store data for this equityid
                equity_yearly_data = dict(equityid=equity_id)
                # Also store the currency for one of the dividends
                currencystored = False
                # go through each year that we are interested in and check if we have dividend data
                # for that year for this equityid
                for datetime_year in datetime_years_to_fetch:
                    # check if we have dividend data for this year
                    for dividend_data in all_dividend_data:
                        if dividend_data['equityid'] == equity_id and dividend_data['date'].year == datetime_year.year:
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
            api_response_usdttd = requests.get(
                url="https://www.freeforexapi.com/api/live?pairs=USDTTD")
            api_response_usdjmd = requests.get(
                url="https://www.freeforexapi.com/api/live?pairs=USDJMD")
            if (api_response_usdttd.status_code == 200) and api_response_usdjmd.status_code == 200:
                jmdusd = Decimal(1.00)/Decimal(json.loads(
                    api_response_usdjmd.content.decode('utf-8'))['rates']['USDJMD']['rate'])
                usdttd = Decimal(json.loads(
                    api_response_usdttd.content.decode('utf-8'))['rates']['USDTTD']['rate'])
                # Calculate our dividend yields using our values
                for dividend_data in dividend_yearly_data:
                    for stdata in stockyearlydata:
                        if (stdata['equityid'] == dividend_data['equityid']) and (str(stdata['date'].year)+"_dividends" in dividend_data):
                            # If we have matched our stock data and our dividend data (by equityid and year)
                            # Check if this is a USD listed equity id
                            if stdata['equityid'] in usdequityids:
                                # all of the USD listed equities so far have dividends in USD as well, so we don't need to convert
                                dividendyield = dividend_data[str(
                                    stdata['date'].year)+"_dividends"]*100/stdata['closingquote']
                                # Add this value to our list
                                dividendyielddata.append({'yieldpercent': dividendyield, 'yielddate': stdata['date'],
                                                          'equityid': stdata['equityid']})
                            else:
                                # else this equity is listed in TTD
                                # Check currencies, and use a multiplier for the conversion rate for dividends in other currencies
                                conrate = Decimal(1.00)
                                if dividend_data['currency'] == "USD":
                                    conrate = usdttd
                                elif dividend_data['currency'] == "JMD":
                                    conrate = jmdusd*usdttd
                                else:
                                    pass
                                    # Else our conrate should remain 1
                                # Now calculate the dividend yield for the year
                                dividendyield = dividend_data[str(
                                    stdata['date'].year)+"_dividends"]*conrate*100/stdata['closingquote']
                                # Add this value to our list
                                dividendyielddata.append({'yieldpercent': dividendyield, 'yielddate': stdata['date'],
                                                          'equityid': stdata['equityid']})
            else:
                raise ConnectionError(
                    "Could not connect to API to convert currencies. Status code "+api_response_usdjmd.status_code+". Reason: "+api_response_usdjmd.reason)
            logging.info("Dividend yield calculated successfully.")
            logging.info("Inserting data into database.")
            insertstmt = dividendyieldtable.insert().prefix_with("IGNORE")
            result = dbcon.execute(insertstmt, dividendyielddata)
            logging.info("Number of rows affected was "+str(result.rowcount))
            logging.info(
                "Successfully wrote data for the dividendyield table into database.")
            return 0
        else:
            raise ConnectionError("Failed to connect to database.")
    except Exception as ex:
        raise
    finally:
        if engine is not None:
            # Always close the database engine
            engine.dispose()
            logging.info("Successfully disconnected from database.")


def scrape_equity_summary_data(datestofetch, alllistedsymbols):
    """
    In a new process, open the Firefox browser and browse through
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
        logging.info("Now opening the Firefox browser"+pidstring)
        options = Options()
        options.headless = True
        options.accept_insecure_certs = True
        profile = FirefoxProfile()
        profile.set_preference('security.tls.version.enable-deprecated', True)
        driver = webdriver.Firefox(profile, options=options)
        # This list of dicts will contain all data to be written to the db
        allmarketsummarydata = []
        allequitytradingdata = []
        for fetchdate in datestofetch:
            # set up a variable to retry page loads on Internet failures
            webretries = 1
            # check if the page was loaded or has failed enough times
            pageloaded = False
            failmaxtimes = 3
            while not pageloaded and webretries <= failmaxtimes:
                try:
                    # get a date object suitable for the db
                    fetchdatedb = datetime.strftime(
                        datetime.strptime(fetchdate, '%m/%d/%Y'), '%Y-%m-%d')
                    # start a dictionary to write to the db
                    marketsummarydata = dict(date=fetchdatedb)
                    # for each date, we need to navigate to this summary page for that day
                    urlsummarypage = "https://stockex.co.tt/controller.php?action=view_quote&TradingDate="+fetchdate
                    logging.info("Navigating to "+urlsummarypage+pidstring)
                    driver.get(urlsummarypage)
                    # All important data for us is wrapped in <p> tags, so we first find these elements
                    textelements = driver.find_elements_by_tag_name("p")
                    # Then we create a list with the text from all of these elements
                    textblocksonpage = []
                    for textelement in textelements:
                        if textelement.text is not None:
                            textblocksonpage.append(textelement.text)
                    # Now check if an error message was thrown
                    errormessage = "Sorry. No data for date selected. Kindly select another date."
                    if errormessage in textblocksonpage:
                        logging.info(
                            "No data is available for: "+fetchdate+pidstring)
                        pageloaded = True
                    # if our error message was not thrown, then we continue
                    else:
                        logging.info(
                            "Found market summary date for: "+fetchdate+pidstring)
                        # find the index of the first line of each important row
                        try:
                            compositetotalsindex = textblocksonpage.index(
                                "Composite Totals")
                            # Now use the index to access interesting data
                            marketsummarydata['compositetotalsindexvalue'] = float(
                                textblocksonpage[compositetotalsindex+1].replace(",", ""))
                            marketsummarydata['compositetotalsindexchange'] = float(
                                textblocksonpage[compositetotalsindex+2].replace(",", ""))
                            marketsummarydata['compositetotalschange'] = float(
                                textblocksonpage[compositetotalsindex+3].replace(",", ""))
                            marketsummarydata['compositetotalsvolumetraded'] = int(
                                textblocksonpage[compositetotalsindex+4].replace(",", ""))
                            marketsummarydata['compositetotalsvaluetraded'] = float(
                                textblocksonpage[compositetotalsindex+5].replace(",", ""))
                            marketsummarydata['compositetotalsnumtrades'] = int(
                                textblocksonpage[compositetotalsindex+6].replace(",", ""))
                        except ValueError as ex:
                            logging.info(
                                'Could not find Composite Totals for this date'+pidstring)
                            marketsummarydata['compositetotalsindexvalue'] = None
                            marketsummarydata['compositetotalsindexchange'] = None
                            marketsummarydata['compositetotalschange'] = None
                            marketsummarydata['compositetotalsvolumetraded'] = None
                            marketsummarydata['compositetotalsvaluetraded'] = None
                            marketsummarydata['compositetotalsnumtrades'] = None
                        try:
                            alltnttotalsindex = textblocksonpage.index(
                                "All T&T Totals")
                            marketsummarydata['alltnttotalsindexvalue'] = float(
                                textblocksonpage[alltnttotalsindex+1].replace(",", ""))
                            marketsummarydata['alltnttotalsindexchange'] = float(
                                textblocksonpage[alltnttotalsindex+2].replace(",", ""))
                            marketsummarydata['alltnttotalschange'] = float(
                                textblocksonpage[alltnttotalsindex+3].replace(",", ""))
                            marketsummarydata['alltnttotalsvolumetraded'] = int(
                                textblocksonpage[alltnttotalsindex+4].replace(",", ""))
                            marketsummarydata['alltnttotalsvaluetraded'] = float(
                                textblocksonpage[alltnttotalsindex+5].replace(",", ""))
                            marketsummarydata['alltnttotalsnumtrades'] = int(
                                textblocksonpage[alltnttotalsindex+6].replace(",", ""))
                        except ValueError:
                            logging.info(
                                'Could not find All TNT Totals for this date'+pidstring)
                            marketsummarydata['alltnttotalsindexvalue'] = None
                            marketsummarydata['alltnttotalsindexchange'] = None
                            marketsummarydata['alltnttotalschange'] = None
                            marketsummarydata['alltnttotalsvolumetraded'] = None
                            marketsummarydata['alltnttotalsvaluetraded'] = None
                            marketsummarydata['alltnttotalsnumtrades'] = None
                        try:
                            crosslistedtotalsindex = textblocksonpage.index(
                                "Cross Listed Totals")
                            marketsummarydata['crosslistedtotalsindexvalue'] = float(
                                textblocksonpage[crosslistedtotalsindex+1].replace(",", ""))
                            marketsummarydata['crosslistedtotalsindexchange'] = float(
                                textblocksonpage[crosslistedtotalsindex+2].replace(",", ""))
                            marketsummarydata['crosslistedtotalschange'] = float(
                                textblocksonpage[crosslistedtotalsindex+3].replace(",", ""))
                            marketsummarydata['crosslistedtotalsvolumetraded'] = int(
                                textblocksonpage[crosslistedtotalsindex+4].replace(",", ""))
                            marketsummarydata['crosslistedtotalsvaluetraded'] = float(
                                textblocksonpage[compositetotalsindex+5].replace(",", ""))
                            marketsummarydata['crosslistedtotalsnumtrades'] = int(
                                textblocksonpage[compositetotalsindex+6].replace(",", ""))
                        except ValueError:
                            logging.info(
                                'Could not find Crosslisted Totals for this date'+pidstring)
                            marketsummarydata['crosslistedtotalsindexvalue'] = None
                            marketsummarydata['crosslistedtotalsindexchange'] = None
                            marketsummarydata['crosslistedtotalschange'] = None
                            marketsummarydata['crosslistedtotalsvolumetraded'] = None
                            marketsummarydata['crosslistedtotalsvaluetraded'] = None
                            marketsummarydata['crosslistedtotalsnumtrades'] = None
                        try:
                            smetotalsindex = textblocksonpage.index(
                                "SME Totals")
                            marketsummarydata['smetotalsindexvalue'] = float(
                                textblocksonpage[smetotalsindex+1].replace(",", ""))
                            marketsummarydata['smetotalsindexchange'] = float(
                                textblocksonpage[smetotalsindex+2].replace(",", ""))
                            marketsummarydata['smetotalschange'] = float(
                                textblocksonpage[smetotalsindex+3].replace(",", ""))
                            marketsummarydata['smetotalsvolumetraded'] = int(
                                textblocksonpage[smetotalsindex+4].replace(",", ""))
                            marketsummarydata['smetotalsvaluetraded'] = float(
                                textblocksonpage[smetotalsindex+5].replace(",", ""))
                            marketsummarydata['smetotalsnumtrades'] = int(
                                textblocksonpage[smetotalsindex+6].replace(",", ""))
                        except ValueError:
                            logging.info(
                                'Could not find SME Totals for this date'+pidstring)
                            marketsummarydata['smetotalsindexvalue'] = None
                            marketsummarydata['smetotalsindexchange'] = None
                            marketsummarydata['smetotalschange'] = None
                            marketsummarydata['smetotalsvolumetraded'] = None
                            marketsummarydata['smetotalsvaluetraded'] = None
                            marketsummarydata['smetotalsnumtrades'] = None
                        try:
                            mutualfundstotalsindex = textblocksonpage.index(
                                "Mutual Funds Totals")
                            marketsummarydata['mutualfundstotalsvolumetraded'] = int(
                                textblocksonpage[mutualfundstotalsindex+4].replace(",", ""))
                            marketsummarydata['mutualfundstotalsvaluetraded'] = float(
                                textblocksonpage[mutualfundstotalsindex+5].replace(",", ""))
                            marketsummarydata['mutualfundstotalsnumtrades'] = int(
                                textblocksonpage[mutualfundstotalsindex+6].replace(",", ""))
                        except ValueError:
                            logging.info(
                                'Could not find Mutual Funds Totals for this date'+pidstring)
                            marketsummarydata['mutualfundstotalsvolumetraded'] = None
                            marketsummarydata['mutualfundstotalsvaluetraded'] = None
                            marketsummarydata['mutualfundstotalsnumtrades'] = None
                        try:
                            secondtiertotalsindex = textblocksonpage.index(
                                "Second Tier Totals")
                            marketsummarydata['secondtiertotalsnumtrades'] = int(
                                textblocksonpage[secondtiertotalsindex+6].replace(",", ""))
                        except ValueError:
                            logging.info(
                                'Could not find Second Tier Totals for this date'+pidstring)
                            marketsummarydata['secondtiertotalsnumtrades'] = None
                        # add our dict to our list
                        allmarketsummarydata.append(marketsummarydata)
                        # Now parse the dailyequitysummary data
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
                                        fetchdate, '%m/%d/%Y')
                                    # if the last sale date is the date that we have fetched
                                    if lastsaledate == currentfetchdate:
                                        # then create a dictionary to store data
                                        equitytradingdata = dict(
                                            date=fetchdatedb)
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
                        "Problem found while fetching date at "+fetchdate+pidstring+" : "+str(ex))
                    timewaitsecs = 30
                    time.sleep(timewaitsecs)
                    logging.error("Waited "+str(timewaitsecs)+" seconds. Now retrying " +
                                  fetchdate+pidstring+" attempt ("+str(webretries)+"/"+str(failmaxtimes)+")")
                    webretries += 1
        # Now write the data to the db if there is data to write
        logging.info("Now writing scraped data to DB"+pidstring)
        # Create a variable for our database engine
        dbengine = None
        dbuser = ttsescraperconfig.dbusername
        dbpass = ttsescraperconfig.dbpassword
        dbaddress = ttsescraperconfig.dbaddress
        dbschema = ttsescraperconfig.schema
        dbengine = create_engine("mysql://"+dbuser+":"+dbpass+"@"+dbaddress+"/" +
                                 dbschema, echo=False)
        # Try to create a connection to the db
        with dbengine.connect() as dbcon:
            logging.info("Successfully connected to database"+pidstring)
            # Reflect the tables already created in our db
            historicalmarketsummarytable = Table(
                'historicalmarketsummary', MetaData(), autoload=True, autoload_with=dbengine)
            dailyequitysummarytable = Table(
                'dailyequitysummary', MetaData(), autoload=True, autoload_with=dbengine)
            listedequitiestable = Table(
                'listedequities', MetaData(), autoload=True, autoload_with=dbengine)
            if allmarketsummarydata:
                # insert data into historicalmarketsummary table
                insert_stmt = insert(historicalmarketsummarytable).values(
                    allmarketsummarydata)
                on_duplicate_key_stmt = insert_stmt.on_duplicate_key_update(
                    compositetotalsindexvalue=insert_stmt.inserted.compositetotalsindexvalue,
                    compositetotalsindexchange=insert_stmt.inserted.compositetotalsindexchange,
                    compositetotalschange=insert_stmt.inserted.compositetotalschange,
                    compositetotalsvolumetraded=insert_stmt.inserted.compositetotalsvolumetraded,
                    compositetotalsvaluetraded=insert_stmt.inserted.compositetotalsvaluetraded,
                    compositetotalsnumtrades=insert_stmt.inserted.compositetotalsnumtrades,
                    alltnttotalsindexvalue=insert_stmt.inserted.alltnttotalsindexvalue,
                    alltnttotalsindexchange=insert_stmt.inserted.alltnttotalsindexchange,
                    alltnttotalschange=insert_stmt.inserted.alltnttotalschange,
                    alltnttotalsvolumetraded=insert_stmt.inserted.alltnttotalsvolumetraded,
                    alltnttotalsvaluetraded=insert_stmt.inserted.alltnttotalsvaluetraded,
                    alltnttotalsnumtrades=insert_stmt.inserted.alltnttotalsnumtrades,
                    crosslistedtotalsindexvalue=insert_stmt.inserted.crosslistedtotalsindexvalue,
                    crosslistedtotalsindexchange=insert_stmt.inserted.crosslistedtotalsindexchange,
                    crosslistedtotalschange=insert_stmt.inserted.crosslistedtotalschange,
                    crosslistedtotalsvolumetraded=insert_stmt.inserted.crosslistedtotalsvolumetraded,
                    crosslistedtotalsvaluetraded=insert_stmt.inserted.crosslistedtotalsvaluetraded,
                    crosslistedtotalsnumtrades=insert_stmt.inserted.crosslistedtotalsnumtrades,
                    smetotalsindexvalue=insert_stmt.inserted.smetotalsindexvalue,
                    smetotalsindexchange=insert_stmt.inserted.smetotalsindexchange,
                    smetotalschange=insert_stmt.inserted.smetotalschange,
                    smetotalsvolumetraded=insert_stmt.inserted.smetotalsvolumetraded,
                    smetotalsvaluetraded=insert_stmt.inserted.smetotalsvaluetraded,
                    smetotalsnumtrades=insert_stmt.inserted.smetotalsnumtrades,
                    mutualfundstotalsvolumetraded=insert_stmt.inserted.mutualfundstotalsvolumetraded,
                    mutualfundstotalsvaluetraded=insert_stmt.inserted.mutualfundstotalsvaluetraded,
                    mutualfundstotalsnumtrades=insert_stmt.inserted.mutualfundstotalsnumtrades,
                    secondtiertotalsnumtrades=insert_stmt.inserted.secondtiertotalsnumtrades
                )
                result = dbcon.execute(on_duplicate_key_stmt)
                logging.info(
                    "Number of rows affected in the historicalmarketsummary table was "+str(result.rowcount)+pidstring)
            if allequitytradingdata:
                # convert symbols into equityids for the dailyequitysummary table
                selectstmt = select(
                    [listedequitiestable.c.symbol, listedequitiestable.c.equityid])
                result = dbcon.execute(selectstmt)
                for row in result:
                    # The first element in our row tuple is the symbol, and the second is our equityid
                    for equitytradingdata in allequitytradingdata:
                        # Map the symbol for each equity to an equityid in our table
                        if equitytradingdata['symbol'] == row[0]:
                            equitytradingdata['equityid'] = row[1]
                # Now remove our unneeded columns
                for equitytradingdata in allequitytradingdata:
                    equitytradingdata.pop('symbol', None)
                # insert data into dailyequitysummary table
                insert_stmt = insert(dailyequitysummarytable).values(
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
                    closeprice=insert_stmt.inserted.closeprice,
                    changedollars=insert_stmt.inserted.changedollars
                )
                result = dbcon.execute(on_duplicate_key_stmt)
                logging.info(
                    "Number of rows affected in the dailyequitysummary table was "+str(result.rowcount)+pidstring)
        return 0
    except Exception as ex:
        raise
    finally:
        if 'driver' in locals() and driver is not None:
            # Always close the browser
            driver.quit()
            logging.info("Successfully closed web browser."+pidstring)


def update_equity_summary_data():
    """Use the selenium module to open the Firefox browser and browse through
    every day of trading summaries listed at  
    https://stockex.co.tt/controller.php?action=view_quote&TradingDate=03/13/2020
    and scrape the useful output into a list of dictionaries for the DB
    """
    logging.info("Now updating market summary data.")
    # Create a variable for our database engine
    dbengine = None
    try:
        # First fetch all the necessary login data from our config file
        dbuser = ttsescraperconfig.dbusername
        dbpass = ttsescraperconfig.dbpassword
        dbaddress = ttsescraperconfig.dbaddress
        dbschema = ttsescraperconfig.schema
        dbengine = create_engine("mysql://"+dbuser+":"+dbpass+"@"+dbaddress+"/" +
                                 dbschema, echo=False)
        # Try to create a connection to the db
        with dbengine.connect() as dbcon:
            logging.info("Successfully connected to database.")
            # Reflect the tables already created in our db
            logging.info("Reading existing data from tables in database...")
            historicalmarketsummarytable = Table(
                'historicalmarketsummary', MetaData(), autoload=True, autoload_with=dbengine)
            listedequitiestable = Table(
                'listedequities', MetaData(), autoload=True, autoload_with=dbengine)
            # Now select the dates that we already have recorded
            logging.info("Creating list of dates to fetch.")
            datesalreadyrecorded = []
            selectstmt = select([historicalmarketsummarytable.c.date])
            result = dbcon.execute(selectstmt)
            for row in result:
                # We only have a single element in each row tuple, which is the date
                datesalreadyrecorded.append(row[0])
            # Also get a list of all valid symbols from the db
            alllistedsymbols = []
            selectstmt = select([listedequitiestable.c.symbol])
            result = dbcon.execute(selectstmt)
            for row in result:
                # We only have a single element in each row tuple, which is the symbol
                alllistedsymbols.append(row[0])
        # We want to gather data on all trading days since 01/01/2010, so we create a list
        # of all dates that we need to gather still
        datestofetch = []
        fetchdate = datetime(2010, 1, 1)
        logging.info(
            "Get all dates from 2010 that are not already fetched and are not weekends.")
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
        datessublists = [datestofetch[i*listlength // numcores: (i+1)*listlength // numcores]
                         for i in range(numcores)]
        # now call our function to fetch dates for each of this sublists
        processlist = []
        for coredatelist in datessublists:
            process = multiprocessing.Process(
                target=scrape_equity_summary_data, args=(coredatelist, alllistedsymbols))
            process.start()
            processlist.append(process)
        # now poll until all processes are done
        allprocessesdone = False
        while not allprocessesdone:
            processesdone = 0
            for process in processlist:
                if not process.is_alive():
                    processesdone += 1
            if processesdone == len(processlist):
                allprocessesdone = True
                logging.info("All processes complete.")
            else:
                processesrunning = len(processlist) - processesdone
                logging.info("There are "+str(processesrunning) +
                             " processes still running.")
            time.sleep(60)
        logging.debug("All processes completed.")
        return 0
    except Exception as ex:
        raise


def main():
    """Main function for coordinating scraping"""
    try:
        # Set up logging for this module
        logsetup = customlogging.setup_logging(
            logdirparent=str(os.path.dirname(os.path.realpath(__file__))),
            logfilestandardname='dailyscrape',
            smtploggingenabled=True,
            smtplogginglevel=logging.ERROR,
            smtpmailhost='localhost',
            smtpfromaddr='server1@trinistats.com',
            smtptoaddr=['latchmepersad@gmail.com'],
            smtpsubj='Automated report from Python script: '+os.path.basename(__file__))
        if logsetup == 0:
            logging.info("Logging set up successfully.")
        # Set up a pidfile to ensure that only one instance of this script runs at a time
        with PidFile(piddir=tempfile.gettempdir()):
            # logging.info("Updating listed equities and looking for new data")
            # # Scrape basic data for all listed equities
            # alllistedequitydata = scrape_listed_equity_data()
            # # Then write this data to the db
            # write_listed_equity_data_to_db(alllistedequitydata)
            # # Call the function to scrape the dividend data for all securities
            # logging.info("Now trying to scrape dividend data")
            # alldividenddata = scrape_dividend_data()
            # # Then call the function to write this data into the database
            # write_dividend_data_to_db(alldividenddata)
            # # Then call the function to scrape the historical data for all securities
            # logging.info("Now trying to fetch historical data")
            # allhistoricalstockdata = scrape_historical_data()
            # # Then call the function to write this data into the database
            # write_historical_data_to_db(allhistoricalstockdata)
            # # Call the function to scrape market summary data and update DB immediately
            # update_equity_summary_data()
            # # Then call the function to calculate the dividend yield for all stocks and write to
            # # the database immediately
            update_dividend_yield()
    except Exception:
        logging.exception("Error in script "+os.path.basename(__file__))
        customlogging.flush_smtp_logger()
        sys.exit(1)
    else:
        logging.info("The script was executed successfully. " +
                     os.path.basename(__file__))
        sys.exit(0)


if __name__ == "__main__":
    main()
