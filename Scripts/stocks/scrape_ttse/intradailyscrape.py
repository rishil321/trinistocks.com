#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""This Python script uses the Firefox webbrowser to log in to the stockex.co.tt
website and pull data on all listed equities and brokers there.
It then writes the data to a MySQL database.
:param -l OR --logging_level: The level of logging to use 
:returns: 0 if successful
:raises Exception if any issues are encountered
"""

# Put all your imports here, one per line.
# However multiple imports from the same lib are allowed on a line.
# Imports from the Python standard lib
import logging
import sys
import traceback
from decimal import Decimal
from datetime import datetime
import getopt
import argparse
from time import time
import tempfile
import os
# Imports from the cheese factory
from selenium.webdriver.firefox.firefox_profile import FirefoxProfile
from selenium.common.exceptions import WebDriverException
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from sqlalchemy import create_engine, Table, select, MetaData, bindparam
from sqlalchemy.dialects.mysql import insert
from pid import PidFile
# Imports from the local machine
from customlogging import customlogging
import ttsescraperconfig

# Put your constants here. These should be named in CAPS.

# Put your global variables here. All variables should be in lowercase.

# Put your class definitions here. These should use the CapWords convention.

# Put your function definitions here.
# These should be lowercase, separated by underscores.


def updatedailytrades():
    """
    Open the Firefox browser and browse through
    https://stockex.co.tt/controller.php?action=view_quote which shows trading for the last day
    Gather the data into a dict, and write that dict to the DB 
    :returns: 0 if successful
    :raises Exception if any issues are encountered
    """
    try:
        # fetch the list of valid symbols from the db
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
            logging.info("Successfully connected to database")
            # Reflect the tables already created in our db
            dailyequitysummarytable = Table(
                'dailyequitysummary', MetaData(), autoload=True, autoload_with=dbengine)
            listedequitiestable = Table(
                'listedequities', MetaData(), autoload=True, autoload_with=dbengine)
            # Also get a list of all valid symbols from the db
            alllistedsymbols = []
            selectstmt = select([listedequitiestable.c.symbol])
            result = dbcon.execute(selectstmt)
            for row in result:
                # We only have a single element in each row tuple, which is the symbol
                alllistedsymbols.append(row[0])
            # This list of dicts will contain all data to be written to the db
            allequitytradingdata = []
            # open the browser
            logging.info("Now opening the Firefox browser")
            options = Options()
            options.headless = True
            options.accept_insecure_certs = True
            profile = FirefoxProfile()
            profile.set_preference(
                'security.tls.version.enable-deprecated', True)
            driver = webdriver.Firefox(profile, options=options)
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
            # Calculate the valuetraded for each row and remove our unneeded columns
            for equitytradingdata in allequitytradingdata:
                equitytradingdata['valuetraded'] = float(
                    equitytradingdata['saleprice']) * float(equitytradingdata['volumetraded'])
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
                valuetraded=insert_stmt.inserted.valuetraded,
                closeprice=insert_stmt.inserted.closeprice,
                changedollars=insert_stmt.inserted.changedollars
            )
            result = dbcon.execute(on_duplicate_key_stmt)
            logging.info(
                "Number of rows affected in the dailyequitysummary table was "+str(result.rowcount))
            return 0
    except Exception as ex:
        raise
    finally:
        if 'driver' in locals() and driver is not None:
            # Always close the browser
            driver.quit()
            logging.info("Successfully closed web browser.")


def main():
    """The main function for starting the web-parsing process"""
    try:
        # Set up logging for this module
        logsetup = customlogging.setup_logging(
            logdirparent=str(os.path.dirname(os.path.realpath(__file__))),
            logfilestandardname='intradailyscrape',
            smtploggingenabled=True,
            smtplogginglevel=logging.ERROR,
            smtpmailhost='localhost',
            smtpfromaddr='server1@trinistats.com',
            smtptoaddr=['latchmepersad@gmail.com'],
            smtpsubj='Automated report from Python script: '+os.path.basename(__file__))
        if logsetup == 0:
            logging.info("Logging set up successfully.")
        with PidFile(piddir=tempfile.gettempdir()):
            # call the function to scrape the daily data for all securities
            updatedailytrades()
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
