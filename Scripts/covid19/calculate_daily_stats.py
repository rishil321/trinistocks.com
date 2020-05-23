#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
This script reads the data from the worldometers table and computes the data per day for Trinidad
:returns: 0
:raises Exception if any issues are encountered
"""

# Put all your imports here, one per line. However multiple imports from the same lib are allowed on a line.
# Imports from the Python standard library
import sys
import logging
import traceback
import tempfile
import os
# Imports from the cheese factory
from pid import PidFile
from sqlalchemy import create_engine, Table, select, MetaData, bindparam
from sqlalchemy.dialects.mysql import insert
import pandas as pd
# Imports from the local machine
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


def calculate_worldometers_daily_tnt():
    """
    Use data from worldometers to calculate the daily tnt data
    """
    dbengine = None
    dbengine = create_engine("mysql://"+DBUSER+":"+DBPASS+"@"+DBADDRESS+"/" +
                             DBSCHEMA, echo=False)
    # Try to create a connection to the db
    with dbengine.connect() as dbcon:
        logging.info("Successfully connected to database")
        # Reflect the tables already created in our db
        covid19_worldometers_table = Table(
            'covid19_worldometers_reports', MetaData(), autoload=True, autoload_with=dbengine)
        covid19_daily_data_table = Table(
            'covid19_daily_data', MetaData(), autoload=True, autoload_with=dbengine)
        # Get the columns that we require from the main cases table
        selectstmt = select([covid19_worldometers_table.c.date, covid19_worldometers_table.c.total_tests,
                             covid19_worldometers_table.c.total_cases,
                             covid19_worldometers_table.c.total_deaths, covid19_worldometers_table.c.total_recovered]
                            ).where(covid19_worldometers_table.c.country_or_territory_name == "Trinidad and Tobago")
        db_results = dbcon.execute(selectstmt)
        logging.info("Fetched worldometers data from DB.")
        # create a dataframe from the results
        results_dataframe = pd.DataFrame(db_results)
        # set the column names
        results_dataframe.columns = [
            'date', 'total_tests', 'total_cases', 'total_deaths', 'total_recovered']
        # sort the dataframe by date
        results_dataframe.sort_values(['date'], ascending=[True], inplace=True)
        # calculate the differences between each row
        daily_results_dataframe = results_dataframe.set_index('date').diff()
        logging.debug("Calculated daily data from diffs.")
        # reset the index column
        daily_results_dataframe['index'] = range(
            1, len(daily_results_dataframe) + 1)
        daily_results_dataframe['date'] = daily_results_dataframe.index
        daily_results_dataframe = daily_results_dataframe.set_index('index')
        logging.debug("Reset index on new daily dataframe.")
        # set the column names for the daily table
        daily_results_dataframe.columns = [
            'daily_tests', 'daily_positive', 'daily_deaths', 'daily_recovered', 'date']
        # replace all nan with None
        daily_results_dataframe = daily_results_dataframe.where(
            daily_results_dataframe.notnull(), None)
        # now write our list to the db
        insert_stmt = insert(covid19_daily_data_table).values(
            daily_results_dataframe.to_dict('records')).prefix_with('IGNORE')
        result = dbcon.execute(insert_stmt)
        logging.info(
            "Number of rows affected in the covid19_daily_data table was "+str(result.rowcount))
        return 0


def main():
    """
    Main function to calculate COVID19 stats
    """
    try:
        # Set up logging for this module
        logsetup = customlogging.setup_logging(
            logdirparent=str(os.path.dirname(os.path.realpath(__file__))),
            logfilestandardname='calculate_daily_stats',
            smtploggingenabled=True,
            smtplogginglevel=logging.ERROR,
            smtpmailhost='localhost',
            smtpfromaddr='server1@trinistats.com',
            smtptoaddr=['latchmepersad@gmail.com'],
            smtpsubj='Automated report from Python script: '+os.path.basename(__file__))
        if logsetup == 0:
            logging.info("Logging set up successfully.")
        with PidFile(piddir=tempfile.gettempdir()):
            calculate_worldometers_daily_tnt()
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
