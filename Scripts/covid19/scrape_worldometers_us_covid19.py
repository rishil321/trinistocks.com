#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
This script collects all of the data from the URL
https://www.worldometers.info/coronavirus/#countries
and parses it into a Pandas dataframe for output to a .csv and database write
"""

# Put all your imports here, one per line.
# However multiple imports from the same lib are allowed on a line.
# Imports from Python standard libraries
import sys
import logging
import os
from datetime import datetime
from pathlib import Path
import tempfile

# Imports from the cheese factory
import pandas as pd
from pandas.compat.numpy import np
import requests
from bs4 import BeautifulSoup
from pid import PidFile
from sqlalchemy import create_engine, Table, select, MetaData, bindparam, text
from sqlalchemy.dialects.mysql import insert

# Imports from the local filesystem
from customlogging import customlogging
import trinistatsconfig

# Put your constants here. These should be named in CAPS.
DBUSER = trinistatsconfig.dbusername
DBPASS = trinistatsconfig.dbpassword
DBADDRESS = trinistatsconfig.dbaddress
DBSCHEMA = trinistatsconfig.schema

# Put your global variables here.

# Put your class definitions here. These should use the CapWords convention.

# Put your function definitions here. These should be lowercase, separated by underscores.


def scrape_url_to_dataframe(url: 'URL to search for tables',
                            table_position: 'Which table on the page to return as the dataframe, numbered from 0') \
        -> 'Pandas dataframe of chosen table from URL':
    logging.debug('Now getting creating HTTP GET request for '+url)
    raw_html = requests.get(
        "https://www.worldometers.info/coronavirus/country/us/").content
    # parse the html page using Beautiful Soup
    soup = BeautifulSoup(raw_html, 'lxml')
    tables_on_page = soup.find_all('table')
    logging.debug('We found '+str(len(tables_on_page))+' table on the page.')
    # load the table into a dataframe
    dataframe = (pd.read_html(str(tables_on_page[table_position])))[0]
    # create a datestamp
    report_date = datetime.now()
    # create a series containing the date
    date_series = pd.Series(report_date, index=dataframe.index)
    # set the name of the series
    date_series.rename("date")
    # add the series to the dateframe
    dataframe = dataframe.assign(date=date_series)
    # create a list of all the columns that we expect
    columns_expected = ['date', 'country_or_territory_name', 'total_cases', 'new_cases',
                        'total_deaths', 'new_deaths', 'total_recovered', 'active_cases',
                        'serious_critical', 'total_cases_1m_pop', 'deaths_1m_pop',
                        'total_tests', 'tests_1m_pop']
    # ensure that our dataframe only contains columns that we need
    # go through each column and rename or drop as needed
    for column in dataframe.columns:
        # standardize the column names that we want to keep
        if column in ["country_other", "country", "country_territory", "USAState"]:
            dataframe.rename(
                columns={column: "country_or_territory_name"}, inplace=True)
        elif column in ["totalcases", "cases", "TotalCases"]:
            dataframe.rename(columns={column: "total_cases"}, inplace=True)
        elif column in ["newcases", "change_today", "change_(cases)", "NewCases"]:
            dataframe.rename(columns={column: "new_cases"}, inplace=True)
        elif column in ["totaldeaths", "deaths", "TotalDeaths"]:
            dataframe.rename(columns={column: "total_deaths"}, inplace=True)
        elif column in ["newdeaths", "change_(deaths)", "NewDeaths"]:
            dataframe.rename(columns={column: "new_deaths"}, inplace=True)
        elif column in ["totalrecovered"]:
            dataframe.rename(columns={column: "total_recovered"}, inplace=True)
        elif column in ["activecases", "ActiveCases"]:
            dataframe.rename(columns={column: "active_cases"}, inplace=True)
        elif column in ["tot_cases_1m_pop", "Tot Cases/1M pop"]:
            dataframe.rename(
                columns={column: "total_cases_1m_pop"}, inplace=True)
        elif column in ["totaltests", "TotalTests"]:
            dataframe.rename(columns={column: "total_tests"}, inplace=True)
        elif column in ["serious__critical"]:
            dataframe.rename(
                columns={column: "serious_critical"}, inplace=True)
        # drop the columns that we don't need
        elif column not in columns_expected:
            dataframe.drop(column, axis=1, inplace=True)
    # ensure that the dataframe contains only ascii characters
    for column in dataframe.columns.values:
        # try replacing the string characters first
        try:
            dataframe[column].replace(
                {r'[^\x00-\x7F]+': ''}, regex=True, inplace=True)
        except TypeError:
            # leave the column as is if it is not a string column
            pass
    # \D is the same as [^\d] (anything that's not a digit) in REGEX
    for column in ['total_cases', 'new_cases', 'total_deaths', 'new_deaths',
                   'total_recovered', 'active_cases', 'total_cases_1m_pop',
                   'total_tests', 'serious_critical', 'deaths_1m_pop', 'tests_1m_pop']:
        try:
            dataframe[column] = dataframe[column].str.replace(r'\D', '')
            dataframe[column] = pd.to_numeric(
                dataframe[column], errors='coerce').fillna(0).astype(np.int64)
        except (AttributeError, KeyError) as exc:
            pass
    # remove unneeded rows
    dataframe.drop(
        dataframe[dataframe.country_or_territory_name == 'Total:'].index, inplace=True)
    dataframe.drop(
        dataframe[dataframe.country_or_territory_name == 'USA Total'].index, inplace=True)
    return dataframe


def write_dataframe_database(dataframe, table_name):
    """
    Write the data from this dataframe to the mysql table named table_name
    """
    try:
        dbengine = None
        dbengine = create_engine("mysql://"+DBUSER+":"+DBPASS+"@"+DBADDRESS+"/" +
                                 DBSCHEMA, echo=False)
        # Try to create a connection to the db
        with dbengine.connect() as dbcon:
            logging.info("Successfully connected to database")
            # Reflect the tables already created in our db
            sqlalchemy_table = Table(
                table_name, MetaData(), autoload=True, autoload_with=dbengine)
            logging.info(
                "Now trying to insert data from dataframe into database table "+table_name)
            # replace any NaN with None
            dataframe = dataframe.replace({np.nan: None})
            # convert dataframe to list of dicts for insert ignore statement
            dataframe_list_of_dicts = dataframe.to_dict('records')
            insert_stmt = insert(sqlalchemy_table).values(
                dataframe_list_of_dicts).prefix_with('IGNORE')
            result = dbcon.execute(insert_stmt)
            logging.info("Successfully inserted data for: "+dataframe_list_of_dicts[0]['date'].strftime("%Y-%m-%d") +
                         ". Number of rows affected in the table was "+str(result.rowcount))
    except Exception:
        logging.error("Unable to write data to database.")
        raise


def main():
    """Fetch the current worldometers statistics for the US states and store them in the db"""
    try:
        # Set up logging for this module
        logsetup = customlogging.setup_logging(
            logdirparent=str(os.path.dirname(os.path.realpath(__file__))),
            logfilestandardname=os.path.basename(__file__),
            smtploggingenabled=True,
            smtplogginglevel=logging.ERROR,
            smtpmailhost='localhost',
            smtpfromaddr='server1@trinistats.com',
            smtptoaddr=['latchmepersad@gmail.com'],
            smtpsubj='Automated report from Python script: '+os.path.basename(__file__))
        if logsetup == 0:
            logging.info("Logging set up successfully.")
        else:
            raise SystemError("Could not set up logging for this file.")
        with PidFile(piddir=tempfile.gettempdir()):
            us_covid_cases_df = scrape_url_to_dataframe(
                url="https://www.worldometers.info/coronavirus/country/us/", table_position=0)
            # now write this dataframe to the db
            write_dataframe_database(
                dataframe=us_covid_cases_df, table_name='covid19_worldometers_reports')
    except Exception as exc:
        logging.exception("Error in script "+os.path.basename(__file__))
        customlogging.flush_smtp_logger()
        sys.exit(1)
    else:
        logging.info(os.path.basename(__file__)+" executed successfully.")
        sys.exit(0)


# If this script is being run from the command-line, then run the main() function
if __name__ == "__main__":
    main()
