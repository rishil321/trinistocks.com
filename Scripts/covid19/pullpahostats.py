#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
This script goes through the PAHO technical reports website at
(https://www.paho.org/en/technical-reports?topic=4922&d%5Bmin%5D=&d%5Bmax%5D=&page=0),
and downloads all pdf reports from that site. It then parses the reports into dataframes,
creates csvs in a local repo, then syncs that repo to a remote github repo to upload the csvs
:returns : 0 if successful
:raises Exception if any issues are encountered
"""

# Put all your imports here, one per line. However multiple imports from the same lib are allowed on a line.
import sys
from os import listdir
from os.path import isfile, join
import os
import traceback
import camelot
from datetime import datetime
import pandas as pd
from setuplogging import setuplogging
import logging
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.firefox.firefox_profile import FirefoxProfile
from selenium.common.exceptions import WebDriverException
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
import os
from pathlib import Path
import csv
import collections
import glob
import trinistatsconfig
from sqlalchemy import create_engine, Table, select, MetaData, bindparam
from sqlalchemy.dialects.mysql import insert
import git
import re

# Put your constants here. These should be named in CAPS.
PAHO_RAW_REPORTS_DIR_NAME = "paho_raw_reports"
PAHO_CSV_REPORTS_DIR_NAME = "paho_csv_reports"
REPO_USERNAME = "rishil321"
REPO_PERSONAL_ACCESS_TOKEN = trinistatsconfig.githubtoken
REPO_NAME = "covid19_data_sources"
DBUSER = trinistatsconfig.dbusername
DBPASS = trinistatsconfig.dbpassword
DBADDRESS = trinistatsconfig.dbaddress
DBSCHEMA = trinistatsconfig.schema

# Put your global variables here.
paho_raw_reports_dir = None
paho_csv_reports_dir = None

# Put your class definitions here. These should use the CapWords convention.

# Put your function definitions here. These should be lowercase, separated by underscores.
def parse_pdfs():
    """ Go through each pdf in the /pahoreports folder next to this script and check which ones have 
    dates that are not in our database. For each new pdf, get the pandas dataframe and 
    create a list of dictionaries containing the useful data about each PAHO country"
    
    :returns: A list of sublists, with each sublist containing all record from a single parsed pdf
    :raises : raises an exception
    """
    # get all of the pdf files in the dir
    pahopdffiles = [f for f in listdir(paho_raw_reports_dir) if isfile(join(paho_raw_reports_dir, f))]
    # set up a list to hold the data for all pdf files
    all_pdf_data = []
    # read in each pdf file
    for pahopdffile in pahopdffiles:
        try:
            logging.info("Now attempting to read in: "+pahopdffile)
            fullfilepath = os.path.join(paho_raw_reports_dir, pahopdffile)
            tables = camelot.read_pdf(fullfilepath)
            # get the pandas dataframe from each pdf
            pdfdataframe = tables[0].df
            # ensure that this is a valid PAHO COVID19 report
            report_keywords = ['Cumulative','COVID-19','Americas'] 
            if not all(x in pdfdataframe[0].iloc[0] for x in report_keywords):
                logging.error(pahopdffile+" was not recognised as a normal PAHO pdf file. Skipping.")
                continue
            # set up the list to hold the data for this file
            reportdata = []
            # create a variable to store the date of this report
            date = None
            # create a variable to store the last subregion seen
            subregion = None
            # PAHO has different formats for their tables, so we need to check the number of columns in the pdf
            numcolumns = len(pdfdataframe.columns)
            # get the row index for the last country
            lastcountryrowindex = pdfdataframe[1][pdfdataframe[1] == 'Total'].index[0]-1
            for rowindex,rowdata in pdfdataframe.iterrows():
                # set up variables to hold the data for the dict
                country_or_territory_name = None
                confirmed_cases = None
                probable_cases = None
                probable_deaths = None
                recovered = None
                percentage_increase_confirmed = None
                if numcolumns == 6:
                    # this is the old format that they started with
                    if rowindex == 0:
                        # this row contains the date for this report
                        rawdate = rowdata[0].replace('Cumulative suspected and confirmed COVID-19 cases reported by \ncountries and territories in the Americas, as of ','')
                        date = datetime.strptime(rawdate,"%d %B %Y")
                        if not date:
                            raise RuntimeError("Unable to determine the date of this report. Row 0 contained this data: "+
                                            rowdata[0])
                    elif rowindex in range(4,lastcountryrowindex+2):
                        # all these rows contain data for countries/regions
                        # so parse the useful data for each
                        # some of these rows contain subtotals per region/territory
                        if rowdata[0] != '':
                            # store the name of the last seen subregion
                            subregion = rowdata[0]
                        if rowdata[1] == "Subtotal":
                            # on the subtotal rows, store the name for the entire subregion
                            country_or_territory_name = subregion
                        elif rowdata[1] == "Total":
                            # on the last row, store the name All Americas to represent the total
                            country_or_territory_name = "All Americas"
                        else:
                             # else store the name for the specific country
                            country_name = rowdata[1]
                            # note that country names may also have special characters
                            country_name = re.sub('[^A-Za-z0-9,()[] ]+', '', country_name)
                            country_or_territory_name = country_name
                        # for each of the other columns, check if empty, else store the data present in the cell
                        if rowdata[2] == "":
                            # none is used to replace NULL in the db. This represents an unknown quantity
                            confirmed_cases = None
                        else:
                            # remove the comma and parse to an int
                            confirmed_cases = int(rowdata[2].replace(",",""))
                        if rowdata[3] == "":
                            # none is used to replace NULL in the db. This represents an unknown quantity
                            probable_cases = None
                        else:
                            # remove the comma and parse to an int
                            probable_cases = int(rowdata[3].replace(",",""))
                        if rowdata[4] == "":
                            # none is used to replace NULL in the db. This represents an unknown quantity
                            confirmed_deaths = None
                        else:
                            # remove the comma and parse to an int
                            confirmed_deaths = int(rowdata[4].replace(",",""))
                        if rowdata[5] == "":
                            # none is used to replace NULL in the db. This represents an unknown quantity
                            transmission_type = None
                        else:
                            # store this string
                            transmission_type = rowdata[5]
                        # store null data for all other fields that were not present in the old reports
                        probable_deaths = None
                        recovered = None
                        percentage_increase_confirmed = None
                elif numcolumns == 9:
                    # PAHO added in probable cases
                    if rowindex == 0:
                        # this row contains the date for this report
                        rawdate = rowdata[0].split(", as of ")[1]
                        if "\n" in rawdate:
                            rawdate = rawdate.split("\n")[0]
                        try:
                            date = datetime.strptime(rawdate,"%d %B %Y")
                        except ValueError:
                            logging.error("Unable to determine the date of this report. Row 0 contained this data: "+
                                            rowdata[0])
                            raise
                    elif rowindex in range(4,lastcountryrowindex+2):
                        # all these rows contain data for countries/regions
                        # so parse the useful data for each
                        # some of these rows contain subtotals per region/territory
                        if rowdata[0] != '':
                            # store the name of the last seen subregion
                            subregion = rowdata[0]
                        if rowdata[1] == "Subtotal":
                            # on the subtotal rows, store the name for the entire subregion
                            country_or_territory_name = subregion
                        elif rowdata[1] == "Total":
                            # on the last row, store the name All Americas to represent the total
                            country_or_territory_name = "All Americas"
                        else:
                             # else store the name for the specific country
                            country_name = rowdata[1]
                            # note that country names may also have special characters
                            country_name = re.sub('[^A-Za-z0-9,()[] ]+', '', country_name)
                            country_or_territory_name = country_name
                        # for each of the other columns, check if empty, else store the data present in the cell
                        if rowdata[2] == "":
                            # none is used to replace NULL in the db. This represents an unknown quantity
                            confirmed_cases = None
                        else:
                            # there is a report where this column was merged for some reason
                            if "\n" in rowdata[2]:
                                split_numbers = rowdata[2].split("\n")
                                confirmed_cases = int(split_numbers[0].replace(",",""))
                                probable_cases = int(split_numbers[1].replace(",",""))
                                confirmed_deaths = int(split_numbers[2].replace(",",""))
                                probable_deaths = int(split_numbers[3].replace(",",""))
                                recovered = None
                                percentage_increase_confirmed = float(rowdata[7].replace("%",""))
                                transmission_type = rowdata[8]
                                # continue with the next row for this broken report
                                continue
                            else:
                                # remove the comma and parse to an int
                                confirmed_cases = int(rowdata[2].replace(",",""))
                        if rowdata[3] == "":
                            # none is used to replace NULL in the db. This represents an unknown quantity
                            probable_cases = None
                        else:
                            # remove the comma and parse to an int
                            probable_cases = int(rowdata[3].replace(",",""))
                        if rowdata[4] == "":
                            # none is used to replace NULL in the db. This represents an unknown quantity
                            confirmed_deaths = None
                        else:
                            # remove the comma and parse to an int
                            confirmed_deaths = int(rowdata[4].replace(",",""))
                        if rowdata[5] == "":
                            # none is used to replace NULL in the db. This represents an unknown quantity
                            probable_deaths = None
                        else:
                            # store this string
                            probable_deaths = rowdata[5]
                        if rowdata[6] == "":
                            # none is used to replace NULL in the db. This represents an unknown quantity
                            recovered = None
                        else:
                            # store this string
                            recovered = int(rowdata[6].replace(",",""))
                        if rowdata[7] == "":
                            # none is used to replace NULL in the db. This represents an unknown quantity
                            percentage_increase_confirmed = None
                        else:
                            # store this string
                            percentage_increase_confirmed = float(rowdata[7].replace("%",""))
                        if rowdata[8] == "":
                            # none is used to replace NULL in the db. This represents an unknown quantity
                            transmission_type = None
                        else:
                            # store this string
                            transmission_type = rowdata[8]
                elif numcolumns == 10:
                    # PAHO added in country ISO codes and special characters
                    if rowindex == 0:
                        # this row contains the date for this report
                        rawdate = rowdata[0].split(", as of ")[1]
                        if "\n" in rawdate:
                            rawdate = rawdate.split("\n")[0]
                        try:
                            date = datetime.strptime(rawdate,"%d %B %Y")
                        except ValueError:
                            logging.error("Unable to determine the date of this report. Row 0 contained this data: "+
                                            rowdata[0])
                            raise
                    elif rowindex in range(3,lastcountryrowindex+2):
                        # all these rows contain data for countries/regions
                        # so parse the useful data for each
                        # some of these rows contain subtotals per region/territory
                        if rowdata[0] != '':
                            # store the name of the last seen subregion
                            subregion = rowdata[0]
                        if rowdata[2] == "Subtotal":
                            # on the subtotal rows, store the name for the entire subregion
                            country_or_territory_name = subregion
                        elif rowdata[2] == "Total":
                            # on the last row, store the name All Americas to represent the total
                            country_or_territory_name = "All Americas"
                        else:
                            # else store the name for the specific country
                            country_name = rowdata[2]
                            # note that country names may also have special characters
                            country_name = re.sub('[^A-Za-z0-9,()\[\] ]+', '', country_name)
                            country_or_territory_name = country_name
                        # for each of the other columns, check if empty, else store the data present in the cell
                        if rowdata[3] == "":
                            # none is used to replace NULL in the db. This represents an unknown quantity
                            confirmed_cases = None
                        else:
                            # there is a report where this column was merged for some reason
                            if "\n" in rowdata[3]:
                                split_numbers = rowdata[3].split("\n")
                                confirmed_cases = int(split_numbers[0].replace(",",""))
                                probable_cases = int(split_numbers[1].replace(",",""))
                                confirmed_deaths = int(split_numbers[2].replace(",",""))
                                probable_deaths = int(split_numbers[3].replace(",",""))
                                recovered = None
                                percentage_increase_confirmed = float(rowdata[8].replace("%",""))
                                transmission_type = rowdata[9]
                                # continue with the next row for this broken report
                                continue
                            else:
                                # remove the comma and parse to an int
                                confirmed_cases = int(rowdata[3].replace(",",""))
                        if rowdata[4] == "":
                            # none is used to replace NULL in the db. This represents an unknown quantity
                            probable_cases = None
                        else:
                            # remove the comma and parse to an int
                            probable_cases = int(rowdata[4].replace(",",""))
                        if rowdata[5] == "":
                            # none is used to replace NULL in the db. This represents an unknown quantity
                            confirmed_deaths = None
                        else:
                            # remove the comma and parse to an int
                            confirmed_deaths = int(rowdata[5].replace(",",""))
                        if rowdata[6] == "":
                            # none is used to replace NULL in the db. This represents an unknown quantity
                            probable_deaths = None
                        else:
                            # store this string
                            probable_deaths = rowdata[6]
                        if rowdata[7] == "":
                            # none is used to replace NULL in the db. This represents an unknown quantity
                            recovered = None
                        else:
                            # store this string
                            recovered = int(rowdata[7].replace(",",""))
                        if rowdata[8] == "":
                            # none is used to replace NULL in the db. This represents an unknown quantity
                            percentage_increase_confirmed = None
                        else:
                            # store this string
                            percentage_increase_confirmed = float(rowdata[8].replace("%",""))
                        if rowdata[9] == "":
                            # none is used to replace NULL in the db. This represents an unknown quantity
                            transmission_type = None
                        else:
                            # store this string
                            transmission_type = rowdata[9]
                else:
                    logging.error("Unrecognised number of columns in the pdf file. Skipping for now."+
                                  "Check if the report format changed from PAHO.")
                # if we were at least able to scrape the country or territory name, create a dict and add it to the list
                if country_or_territory_name is not None:
                    # set up the dict to store each row of data
                    reportdict = collections.OrderedDict()
                    # add the values to the dict in the order that we want for the report
                    reportdict['date'] = date
                    reportdict['country_or_territory_name'] = country_or_territory_name
                    reportdict['confirmed_cases'] = confirmed_cases
                    reportdict['probable_cases'] = probable_cases
                    reportdict['confirmed_deaths'] = confirmed_deaths
                    reportdict['probable_deaths'] = probable_deaths
                    reportdict['recovered'] = recovered
                    reportdict['percentage_increase_confirmed'] = percentage_increase_confirmed
                    reportdict['transmission_type'] = transmission_type
                    # now add this dict to our list for this report/pdf
                    reportdata.append(reportdict)
            # once we are done adding all data for this pdf, add this pdf report to the list of all reports
            # if the reportdata list is not empty
            if reportdata:
                all_pdf_data.append(reportdata)
                logging.info("Successfully parsed "+pahopdffile)
        except Exception as exc:
            logging.exception("Problem found while parsing "+pahopdffile)
            raise
    logging.info("Completed parsing all pdfs in folder.")
    return all_pdf_data
            
def download_pdfs():
    """Use selenium and the firefox browser to download all COVID19 reports from the 
    PAHO website"""
    try:
        # create the download folder if it does not exist already
        Path(paho_raw_reports_dir).mkdir(parents=True, exist_ok=True)
        # remove all current pdfs in the download folder
        filelist = [ f for f in os.listdir(paho_raw_reports_dir) if f.endswith(".pdf") ]
        for f in filelist:
            os.remove(os.path.join(paho_raw_reports_dir, f))
        # open the browser
        logging.info("Now opening the Firefox browser")
        options = Options()
        options.headless = True
        options.accept_insecure_certs = True
        profile = FirefoxProfile()
        profile.set_preference('security.tls.version.enable-deprecated', True)
        # set the download location of the pdfs and remove the download prompt
        profile.set_preference("browser.altClickSave", True)
        profile.set_preference("browser.download.folderList", 2)
        profile.set_preference("browser.download.panel.shown", False)
        profile.set_preference("browser.download.manager.showWhenStarting", False)
        profile.set_preference("browser.download.dir", paho_raw_reports_dir)
        profile.set_preference("browser.download.useDownloadDir", True)
        profile.set_preference("browser.helperApps.neverAsk.saveToDisk", 
                            "application/pdf,application/x-pdf,application/octet-stream,application/x-winzip,application/x-gzip")
        profile.set_preference("browser.download.manager.alertOnEXEOpen", False)
        profile.set_preference("browser.download.manager.showWhenStarting", False);
        profile.set_preference("browser.download.manager.focusWhenStarting", False);
        profile.set_preference("browser.helperApps.alwaysAsk.force", False);
        profile.set_preference("browser.download.manager.alertOnEXEOpen", False);
        profile.set_preference("browser.download.manager.closeWhenDone", True);
        profile.set_preference("browser.download.manager.showAlertOnComplete", False);
        profile.set_preference("browser.download.manager.useWindow", False);
        profile.set_preference("services.sync.prefs.sync.browser.download.manager.showWhenStarting", False);
        profile.set_preference("pdfjs.disabled", True)
        driver = webdriver.Firefox(profile, options=options)
        # Go the PAHO website that holds the reports
        reports_present_on_page = True
        page_number = 0
        pahoreporturl = "https://www.paho.org/en/technical-reports?topic=4922&d%5Bmin%5D=&d%5Bmax%5D=&page="+str(page_number)
        while reports_present_on_page:
            logging.info("Navigating to "+pahoreporturl)
            driver.get(pahoreporturl)
            # get all urls containing certain keywords on this page
            report_links_elements = driver.find_elements_by_partial_link_text("COVID-19 cases")
            # store all of the urls in each element
            report_links = []
            for report_link_element in report_links_elements:
                report_links.append(report_link_element.get_attribute('href'))
            # now go through each url in the list
            for report_link in report_links:
                # navigate to each url
                driver.get(report_link)
                # once the page has loaded, click the download link
                download_link = driver.find_element_by_link_text("DOWNLOAD")
                download_link.click()
                logging.info("File downloaded from: "+download_link.get_attribute('href'))
                        # check if we have any elements that we're interested in on this page, to control the loop
            if report_links_elements:
                reports_present_on_page = True
                page_number += 1
                pahoreporturl = "https://www.paho.org/en/technical-reports?topic=4922&d%5Bmin%5D=&d%5Bmax%5D=&page="+str(page_number)
            else:
                reports_present_on_page = False
                logging.info("No more reports on page. Breaking loop.")
        return 0
    except:
        logging.info("Encountered an issue while trying to download the pdfs.")
        raise
    finally:
            if 'driver' in locals() and driver is not None:
                # Always close the browser
                driver.quit()
                logging.info("Successfully closed web browser.")
                logging.info("Completed downloading of all COVID19 pdfs from PAHO website.")
                
def create_csvs(pdf_reports_data):
    """Take in a list of sublists and create csvs for each sublist
    
    :param pdf_reports_data: A list of sublists, with each sublist representing a pdf from PAHO
    :returns: 0 if successful. Also creates csv files at the paho_csv_reports_dir
    :raises : raises an exception
    """
    try:
        # create the csv output dir if it does not exist
        Path(paho_csv_reports_dir).mkdir(parents=False, exist_ok=True)
        for parsed_pdf_sublist in pdf_reports_data:
            # for each sublist, check if a csv file has been created for this date already
            csv_filename = "paho_covid19_report_"+parsed_pdf_sublist[0]['date'].strftime("%d-%m-%Y")+".csv"
            csv_full_path = os.path.join(paho_csv_reports_dir, csv_filename)
            # check if this csv file already exists
            try:
                with open (csv_full_path, 'x') as csv_file:
                    # if we don't throw an error, the file does not exist
                    # so write the pdf data to this new file
                    # create a pandas dataframe from the sublist
                    pd_dataframe = pd.DataFrame(parsed_pdf_sublist)
                    # write the data in the dataframe to the csv
                    pd_dataframe.to_csv(csv_file,index=False,header=True)
                    logging.info("Successfully created file "+csv_filename)
            except OSError:
                # error thrown if file exists
                # if this file already exists, we don't need to recreate as the reports are immutable
                logging.error("Could not create csv file "+csv_filename+". Maybe file already exists?")
    except:
        logging.error("Unable to create csvs.")
        raise
    else:
        logging.info("Successfully created csvs from PAHO pdfs.")
        return 0

def sync_git_repo():
    """Push new local csv files to the covid19_data_sources repo """
    # get the current dir of this script
    current_dir = os.path.dirname(os.path.realpath(sys.argv[0]))
    repo_path = os.path.join(current_dir,REPO_NAME)
    logging.info("Repository path is: "+repo_path)
    # check to see if a repo has been init already
    try: 
        repo = git.Repo(repo_path)
        logging.info("Git repo has already been created.")
    except (git.exc.InvalidGitRepositoryError,git.exc.NoSuchPathError):
        logging.info("No git repo has been initialized for this module. Cloning from github.com now.")
        repo_url = "https://"+REPO_USERNAME+":"+REPO_PERSONAL_ACCESS_TOKEN+"@github.com/"+REPO_USERNAME+"/"+REPO_NAME+".git"
        git.Repo.clone_from(repo_url,repo_path)
        logging.info("Repo cloned successfully.")
        repo = git.Repo(repo_path)
        # now we have a valid repo created 
    # pull the latest data from the repo
    origin = repo.remotes.origin
    origin.pull()
    # create the csv output dir if it does not exist
    Path(paho_csv_reports_dir).mkdir(parents=False, exist_ok=True)
    # get all csv files in this dir
    all_paho_csv_files = glob.glob(paho_csv_reports_dir+os.path.sep+"*.csv")
    # add all files in this dir to the repo index
    repo.index.add(all_paho_csv_files)
    logging.info("Added all .csv files from "+paho_csv_reports_dir+" to repo index.")
    # set the commit message
    repo.index.commit("Automatic commit by "+os.path.basename(__file__))
    # git push 
    origin.push()
    logging.info("All csv files pushed to github repo successfully.")

def write_data_to_db(parsed_pdf_data):
    """
    Write the data parsed from the pdfs to the mysql database
    """
    try:
        dbengine = None
        dbengine = create_engine("mysql://"+DBUSER+":"+DBPASS+"@"+DBADDRESS+"/"+
                                DBSCHEMA, echo=False)
        # Try to create a connection to the db
        with dbengine.connect() as dbcon:
            logging.info("Successfully connected to database")
            # Reflect the tables already created in our db
            covid19_paho_reports_table = Table('covid19_paho_reports', MetaData(), autoload=True, autoload_with=dbengine)
            # for each sublist in our list of lists, insert the list of dicts
            logging.info("Now trying to insert data from all pdfs into database.")
            for parsed_pdf_sublist in parsed_pdf_data:
                try:
                    insert_stmt = insert(covid19_paho_reports_table).values(parsed_pdf_sublist).prefix_with('IGNORE')
                    result = dbcon.execute(insert_stmt)
                    logging.info("Successfully inserted data for: "+parsed_pdf_sublist[0]['date'].strftime("%a %d %b %Y")+
                                 ". Number of rows affected in the covid19_paho_reports table was "+str(result.rowcount))
                except Exception as exc:
                    logging.error("Could not insert data for: "+parsed_pdf_sublist[0]['date'].strftime("%a %d %b %Y"))
                    raise
            logging.info("Successfully inserted all data into database.")
    except Exception:
        logging.error("Unable to write data to database.")
        raise

def main():
    """Download all pdf reports from PAHO on COVID19, parse them to get the useful data
    and create csvs from the data."""
    try:
        # Set up logging for this module
        logsetup = setuplogging(logfilestandardname='pullpahostats', 
                                logginglevel='logging.INFO', stdoutenabled=True)
        if logsetup == 0:
            logging.info("Logging set up successfully.")
        # construct the full path to the folders containing the pdf files and the output csv files
        global paho_raw_reports_dir
        global paho_csv_reports_dir
        currentdir = os.path.dirname(os.path.realpath(__file__))
        paho_raw_reports_dir = os.path.join(currentdir,PAHO_RAW_REPORTS_DIR_NAME)
        paho_csv_reports_dir = os.path.join(currentdir,REPO_NAME,PAHO_CSV_REPORTS_DIR_NAME)
        # set up the local repo if it is not already created
        sync_git_repo()
        # download all pdfs from the PAHO website
        download_pdfs()
        # parse all the pdfs in the paho_raw_reports_dir 
        pdf_reports_data = parse_pdfs()
        # create csvs in the output folder using the parsed content
        create_csvs(pdf_reports_data)
        # now resync the local repo to the remote repo to upload the files
        sync_git_repo() 
        # add the data from the pdfs to the database
        write_data_to_db(pdf_reports_data)
    except Exception as exc:
        logging.exception("Error encountered while running script "+os.path.basename(__file__))
        sys.exit(1)
    else:
        # Use 0 for normal exits, 1 for general errors and 2 for syntax errors (eg. bad input parameters)
        sys.exit(0)

if __name__ == "__main__":
    main()
