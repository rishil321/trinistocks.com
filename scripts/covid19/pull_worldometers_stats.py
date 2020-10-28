#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Description of this module/script goes here
:param -f OR --first_parameter: The description of your first input parameter
:param -s OR --second_parameter: The description of your second input parameter 
:returns: Whatever your script returns when called
:raises Exception if any issues are encountered
"""

# Put all your imports here, one per line. However multiple imports from the same lib are allowed on a line.
import sys
from customlogging import customlogging
from pathlib import Path
import os
import logging
import trinistatsconfig
import pandas as pd
from datetime import datetime
import github_actions
import glob
from sqlalchemy import create_engine, Table, select, MetaData, bindparam, text
from sqlalchemy.dialects.mysql import insert
from pandas.compat.numpy import np
import tempfile
from pid import PidFile

# Put your constants here. These should be named in CAPS.
WORLDOMETERS_REPO_DIR_NAME = "bin0x00_covid19"
COVID19_DATA_SOURCES_REPO_DIR_NAME = "covid19_data_sources"
COVID19_DATA_SOURCES_REPO_SUBDIR_NAME = "worldometers_csv_reports"
REPO_USERNAME = trinistatsconfig.githubusername
REPO_PERSONAL_ACCESS_TOKEN = trinistatsconfig.githubtoken
DBUSER = trinistatsconfig.dbusername
DBPASS = trinistatsconfig.dbpassword
DBADDRESS = trinistatsconfig.dbaddress
DBSCHEMA = trinistatsconfig.schema

# Put your global variables here.

# Put your class definitions here. These should use the CapWords convention.

# Put your function definitions here. These should be lowercase, separated by underscores.


def read_and_wrangle_json(json_file):
    """Get the data from each json file, wrangle the data 
    and return a pandas dataframe"""
    # read in the json file as a pandas dataframe
    dataframe = pd.read_json(json_file)
    # get the date
    # remove the prefix from the filename
    report_date = os.path.basename(json_file).replace("data-", "")
    # remove the suffix (filetype)
    report_date = report_date.replace(".json", "")
    report_date = datetime.strptime(report_date, "%d-%m-%Y")
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
    # go through each column and rename or drop as needed
    for column in dataframe.columns:
        # standardize the column names that we want to keep
        if column in ["country_other", "country", "country_territory"]:
            dataframe.rename(
                columns={column: "country_or_territory_name"}, inplace=True)
        elif column in ["totalcases", "cases"]:
            dataframe.rename(columns={column: "total_cases"}, inplace=True)
        elif column in ["newcases", "change_today", "change_(cases)"]:
            dataframe.rename(columns={column: "new_cases"}, inplace=True)
        elif column in ["totaldeaths", "deaths"]:
            dataframe.rename(columns={column: "total_deaths"}, inplace=True)
        elif column in ["newdeaths", "change_(deaths)"]:
            dataframe.rename(columns={column: "new_deaths"}, inplace=True)
        elif column in ["totalrecovered"]:
            dataframe.rename(columns={column: "total_recovered"}, inplace=True)
        elif column in ["activecases"]:
            dataframe.rename(columns={column: "active_cases"}, inplace=True)
        elif column in ["tot_cases_1m_pop"]:
            dataframe.rename(
                columns={column: "total_cases_1m_pop"}, inplace=True)
        elif column in ["totaltests"]:
            dataframe.rename(columns={column: "total_tests"}, inplace=True)
        elif column in ["serious__critical"]:
            dataframe.rename(
                columns={column: "serious_critical"}, inplace=True)
        # drop the columns that we don't need
        elif column not in columns_expected:
            dataframe.drop(column, axis=1, inplace=True)
    # ensure that all required columns are created
    for column in columns_expected:
        # if the required column is not in the dataframe, create the column
        if column not in dataframe.columns:
            blank_series = pd.Series(None, index=dataframe.index)
            # set the name of the series
            blank_series.rename(column)
            # add the series to the dateframe to make the new column
            dataframe[column] = blank_series
    # go through each column and remove the unneeded characters in the values (commas etc.)
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
    dataframe = dataframe.replace({np.nan: None})
    # return our fully wrangled daraframe
    return dataframe


def write_dataframe_csv(dataframe, directory_path, filename_prefix):
    dataframe = dataframe.reset_index()
    # try to create a csv file the dataframe sent in
    csv_filename = filename_prefix+"_" + \
        dataframe['date'][0].strftime("%Y-%m-%d")+".csv"
    csv_full_path = os.path.join(directory_path, csv_filename)
    # check if this csv file already exists
    try:
        with open(csv_full_path, 'x') as csv_file:
            # if we don't throw an error, the file does not exist
            # so write the dataframe to this new file
            dataframe.to_csv(csv_file, index=False,
                             header=True, encoding='utf-8')
            logging.info("Successfully created file "+csv_filename)
    except OSError:
        # error thrown if file exists
        # if this file already exists, we don't need to recreate as the reports are immutable
        logging.error("Could not create csv file " +
                      csv_filename+". Maybe file already exists?")
    except UnicodeEncodeError as exc:
        # error thrown if an invalid character was seen
        logging.error("Encoding error for file " +
                      csv_filename+str(exc))


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
            # convert dataframe to list of dicts for insert ignore statement
            temp_list = []
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
    """The main steps in pulling the worldometers data from an upstream repo, wrangling the data, to
    then push it back to the covid19_data_sources repo and write to a database as well"""
    try:
        # Set up logging for this module
        logsetup = customlogging.setup_logging(
            logdirparent=str(os.path.dirname(os.path.realpath(__file__))),
            logfilestandardname='pull_worldometers_stats',
            smtploggingenabled=True,
            smtplogginglevel=logging.ERROR,
            smtpmailhost='localhost',
            smtpfromaddr='server1@trinistats.com',
            smtptoaddr=['latchmepersad@gmail.com'],
            smtpsubj='Automated report from Python script: '+os.path.basename(__file__))
        if logsetup == 0:
            logging.info("Logging set up successfully.")
        with PidFile(piddir=tempfile.gettempdir()):
            # set up the directory to store the images
            currentdir = os.path.dirname(os.path.realpath(__file__))
            worldometers_upstream_repo_dir = os.path.join(
                currentdir, WORLDOMETERS_REPO_DIR_NAME)
            # create the folder if it does not exist already
            Path(worldometers_upstream_repo_dir).mkdir(
                parents=True, exist_ok=True)
            # sync to the remote repo
            github_actions.repo_pull(repo_remote_url="https://github.com/bin0x00/covid-19",
                                     repo_local_dir=worldometers_upstream_repo_dir)
            # get all json files in this dir
            worldometers_json_files = glob.glob(os.path.join(
                worldometers_upstream_repo_dir, "data", "*.json"))
            # do an initial pull, commit, push from the covid19_data_sources repo
            # to set up the subdir if it does not exist and get all existing files
            covid19_data_sources_dir = os.path.join(
                currentdir, COVID19_DATA_SOURCES_REPO_DIR_NAME)
            # create the dir if it does not exist already
            Path(covid19_data_sources_dir).mkdir(parents=True, exist_ok=True)
            covid19_data_sources_repo_url = "https://"+REPO_USERNAME+":"+REPO_PERSONAL_ACCESS_TOKEN + \
                "@github.com/"+REPO_USERNAME+"/covid19_data_sources"
            subdir_path = github_actions.repo_pull_commit_push(repo_remote_url=covid19_data_sources_repo_url,
                                                               repo_local_dir=covid19_data_sources_dir,
                                                               repo_subdir_name=COVID19_DATA_SOURCES_REPO_SUBDIR_NAME,
                                                               repo_subdir_files_extension=".csv")
            # process the new data from the upstream json files
            for json_file in worldometers_json_files:
                # wrangle the data from each json file
                json_dataframe = read_and_wrangle_json(json_file)
                # then write the dataframe to a csv
                write_dataframe_csv(dataframe=json_dataframe, directory_path=subdir_path,
                                    filename_prefix="worldometers_report")
                write_dataframe_database(
                    dataframe=json_dataframe, table_name='covid19_worldometers_reports')
            # pull, commit and push to the covid19_data_sources repo again to push the new csvs
            github_actions.repo_pull_commit_push(repo_remote_url=covid19_data_sources_repo_url,
                                                 repo_local_dir=covid19_data_sources_dir,
                                                 repo_subdir_name=COVID19_DATA_SOURCES_REPO_SUBDIR_NAME,
                                                 repo_subdir_files_extension=".csv")
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
