#!/usr/bin/env python3
# -*- coding: utf-8 -*-
 
"""
Description of this module/script goes here
:param -f OR --first_parameter: The description of your first input parameter
:returns: Whatever your script returns when called
:raises Exception if any issues are encountered
"""
 
# Put all your imports here, one per line. 
# However multiple imports from the same lib are allowed on a line.
# Imports from Python standard libraries
import sys
import logging
import os
import multiprocessing
import argparse
import re

# Imports from the cheese factory
from bs4 import BeautifulSoup
from bs4.element import Tag
from pid import PidFile
import tempfile
from sqlalchemy import create_engine, Table, select, MetaData, text, and_
from sqlalchemy.dialects.mysql import insert
import sqlalchemy.exc
import requests
import glob
from pathlib import Path
from datetime import datetime

# Imports from the local filesystem
from ... import custom_logging
from ...database_ops import DatabaseConnect
 
# Put your constants here. These should be named in CAPS.
TTSE_NEWS_CATEGORIES = {'annual_reports':56,'articles':57,'annual_statements':58,'quarterly_statements':59}
WEBPAGE_LOAD_TIMEOUT_SECS = 30

# Put your global variables here. 
 
# Put your class definitions here. These should use the CapWords convention.
 
# Put your function definitions here. These should be lowercase, separated by underscores.
def fetch_annual_statements():
    """Fetch the audited annual financial statements of each symbol from
    https://www.stockex.co.tt/news/xxxxxxx
    """
    # for some reason, ttse is using symbol ids instead of actual symbols here,
    # so we need to set up a list of these ids
    listed_symbol_data = []
    with DatabaseConnect() as db_obj:
        listed_equities_table = Table(
            'listed_equities', MetaData(), autoload=True, autoload_with=db_obj.dbengine)
        selectstmt = select(
            [listed_equities_table.c.symbol,listed_equities_table.c.symbol_id])
        result = db_obj.dbcon.execute(selectstmt)
        for row in result:
            listed_symbol_data.append({'symbol':row[0],'symbol_id':row[1]})
    # now go through the list of symbol ids and fetch the required reports for each
    for symbol_data in listed_symbol_data:
        # ensure the required directories are created
        current_dir = Path(os.path.realpath(__file__)).parent
        incoming_statements_dir = current_dir.joinpath('incoming_annual_statements').joinpath(symbol_data['symbol'])
        if incoming_statements_dir.exists() and incoming_statements_dir.is_dir():
            logging.info(f"Directory for {symbol_data['symbol']}'s annual statements found at {incoming_statements_dir}")
        else:
            logging.info(f"Directory for {symbol_data['symbol']}'s annual statements not found at {incoming_statements_dir}. Trying to create.")
            incoming_statements_dir.mkdir()
        # now load the page with the reports
        logging.info(f"Now trying to fetch annual statements for {symbol_data['symbol']}")
        annual_statements_url = f"https://www.stockex.co.tt/news/?symbol={symbol_data['symbol_id']}&category={TTSE_NEWS_CATEGORIES['annual_statements']}"
        logging.info(f"Navigating to {annual_statements_url}")
        annual_statements_page = requests.get(
            annual_statements_url, timeout=WEBPAGE_LOAD_TIMEOUT_SECS)
        if annual_statements_page.status_code != 200:
            raise requests.exceptions.HTTPError(
                "Could not load URL. "+annual_statements_url)
        logging.info("Successfully loaded webpage.")
        # and search the page for the links to all annual reports
        annual_statements_page_soup = BeautifulSoup(
            annual_statements_page.text, 'lxml')
        news_div =  annual_statements_page_soup.find(id='news')
        report_page_links = []
        for link in news_div.find_all('a', href=True):
            if 'audited' in link.text.lower() \
                and link.attrs['href']:
                report_page_links.append(link.attrs['href'])
        # now navigate to each link
        # and get the actual links of the pdf files
        pdf_reports = []
        for link in report_page_links:
            try:
                report_page = requests.get(
                    link, timeout=WEBPAGE_LOAD_TIMEOUT_SECS)
                if report_page.status_code != 200:
                    raise requests.exceptions.HTTPError(
                        "Could not load URL. "+annual_statements_url)
                logging.info("Successfully loaded webpage.")
                # and search the page for the link to the actual pdf report
                report_soup = BeautifulSoup(
                    report_page.text, 'lxml')
                a_links =  report_soup.find_all('a')
                pdf_link = None
                for a_link in a_links:
                    if not pdf_link and 'click here to download' in a_link.text: 
                        if a_link.attrs['href']:
                            pdf_link = a_link.attrs['href'].replace(" ","")
                if not pdf_link:
                    raise RuntimeError(f'Could not find a link for the pdf in {link}') 
                # and get the release date for this report
                pdf_release_date = None
                h2_texts = report_soup.find_all('h2',{"class": "elementor-heading-title elementor-size-default"})
                for text in h2_texts:
                    try:
                        # check if any of the texts can be parsed to a dates
                        if not pdf_release_date:
                            pdf_release_date = datetime.strptime(text.contents[0],'%d/%m/%Y')
                    except (ValueError,TypeError):
                        pass
                if not pdf_release_date:
                    raise RuntimeError(f'Could not find a release date for this report: {link}')
                # now append our data
                pdf_reports.append({'pdf_link':pdf_link,'release_date':pdf_release_date})
            except (requests.exceptions.HTTPError,RuntimeError) as exc:
                logging.warning(f"Ran into an error while trying to get the download link for {link}. Skipping statement."
                                ,exc_info=exc)
        # now actually download the pdf files
        for pdf in pdf_reports:
            # create the name to use for this downloaded file
            local_filename = f"{symbol_data['symbol']}_annual_statement__{pdf['release_date'].strftime('%Y-%m-%d')}.pdf"
            # check if file was already downloaded
            full_local_path = incoming_statements_dir.joinpath(local_filename)
            if full_local_path.is_file():
                logging.info(f"PDF file {local_filename} was already downloaded. Skipping.")
            else:
                # else download this new pdf report
                # get the http response 
                http_response_obj = requests.get(pdf['pdf_link'],stream=True)
                # save contents of response (pdf report) to the local file
                logging.info(f"Now downloading file {local_filename}. Please wait.")
                with open(full_local_path,"wb") as local_pdf_file:
                    for chunk in http_response_obj.iter_content(chunk_size=1024):
                        # save chunks of pdf to local file
                        if chunk:
                            local_pdf_file.write(chunk)
                logging.info("Finished download file.")
    logging.info("Finished downloading all annual statements.")
    return 0

def main():
    """Docstring description for each function"""
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
            # run all functions within a multiprocessing pool
            with multiprocessing.Pool(os.cpu_count(), custom_logging.logging_worker_init, [q]) as multipool:
                multipool.apply(
                fetch_annual_statements, ())
                multipool.close()
                multipool.join()
                logging.info(os.path.basename(__file__) +
                " executed successfully.")
                q_listener.stop()
                return 0
    except Exception:
        logging.error("Error in script "+os.path.basename(__file__))
        custom_logging.flush_smtp_logger()
        sys.exit(1)
    else:
        logging.info(os.path.basename(__file__)+" executed successfully.")
        sys.exit(0)

# If this script is being run from the command-line, then run the main() function
if __name__ == "__main__":
    # first check the arguements given to this script
    parser = argparse.ArgumentParser()
    args = parser.parse_args()
    main(args)