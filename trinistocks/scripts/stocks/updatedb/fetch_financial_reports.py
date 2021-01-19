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
from email.mime.text import MIMEText
from subprocess import Popen, PIPE

# Imports from the local filesystem
from ... import custom_logging
from ...database_ops import DatabaseConnect

# Put your constants here. These should be named in CAPS.
TTSE_NEWS_CATEGORIES = {'annual_reports': 56, 'articles': 57,
                        'annual_statements': 58, 'quarterly_statements': 59}
WEBPAGE_LOAD_TIMEOUT_SECS = 30
REPORTS_DIRECTORY = 'financial_reports'
IGNORE_SYMBOLS = ['CPFV', 'GMLP', 'LJWA', 'LJWP', 'MOV', 'PPMF', 'SFC']
QUARTERLY_STATEMENTS_START_DATE_STRING = '2020-10-01'
QUARTERLY_STATEMENTS_START_DATETIME = datetime.strptime(
    QUARTERLY_STATEMENTS_START_DATE_STRING, '%Y-%m-%d')
ANNUAL_STATEMENTS_START_DATE_STRING = '2020-01-01'
TODAY_DATE = datetime.strftime(datetime.now(), '%Y-%m-%d')
LOGGERNAME = 'fetch_financial_reports.py'
OUTSTANDINGREPORTEMAIL = 'latchmepersad@gmail.com'

# Put your global variables here.

# Put your class definitions here. These should use the CapWords convention.

# Put your function definitions here. These should be lowercase, separated by underscores.


def fetch_annual_reports():
    """Fetch the audited annual reports of each symbol from
    https://www.stockex.co.tt/news/xxxxxxx
    """
    # for some reason, ttse is using symbol ids instead of actual symbols here,
    # so we need to set up a list of these ids
    logger = logging.getLogger(LOGGERNAME)
    listed_symbol_data = []
    with DatabaseConnect() as db_obj:
        listed_equities_table = Table(
            'listed_equities', MetaData(), autoload=True, autoload_with=db_obj.dbengine)
        selectstmt = select(
            [listed_equities_table.c.symbol, listed_equities_table.c.symbol_id])
        result = db_obj.dbcon.execute(selectstmt)
        for row in result:
            if row[0] not in IGNORE_SYMBOLS:
                listed_symbol_data.append(
                    {'symbol': row[0], 'symbol_id': row[1]})
    # now go through the list of symbol ids and fetch the required reports for each
    for symbol_data in listed_symbol_data:
        # ensure the required directories are created
        current_dir = Path(os.path.realpath(__file__)).parent
        reports_dir = current_dir.joinpath(
            REPORTS_DIRECTORY).joinpath(symbol_data['symbol'])
        if reports_dir.exists() and reports_dir.is_dir():
            logger.info(
                f"Directory for {symbol_data['symbol']}'s annual reports found at {reports_dir}")
        else:
            logger.info(
                f"Directory for {symbol_data['symbol']}'s annual reports not found at {reports_dir}. Trying to create.")
            reports_dir.mkdir()
            if not reports_dir.exists():
                raise RuntimeError(
                    f"Could not create directory at {reports_dir}")
        # now load the page with the reports
        logger.info(
            f"Now trying to fetch annual reports for {symbol_data['symbol']}")
        annual_reports_url = f"https://www.stockex.co.tt/news/?symbol={symbol_data['symbol_id']}&category={TTSE_NEWS_CATEGORIES['annual_reports']}&date={ANNUAL_STATEMENTS_START_DATE_STRING}&date_to={TODAY_DATE}"
        logger.info(f"Navigating to {annual_reports_url}")
        annual_reports_page = requests.get(
            annual_reports_url, timeout=WEBPAGE_LOAD_TIMEOUT_SECS)
        if annual_reports_page.status_code != 200:
            raise requests.exceptions.HTTPError(
                "Could not load URL. "+annual_reports_url)
        logger.info("Successfully loaded webpage.")
        # and search the page for the links to all annual reports
        annual_reports_page_soup = BeautifulSoup(
            annual_reports_page.text, 'lxml')
        news_div = annual_reports_page_soup.find(id='news')
        report_page_links = []
        for link in news_div.find_all('a', href=True):
            if link.attrs['href'] and \
                    'annual-report' in link.attrs['href'].lower():
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
                        "Could not load URL. "+annual_reports_url)
                logger.info("Successfully loaded webpage.")
                # and search the page for the link to the actual pdf report
                report_soup = BeautifulSoup(
                    report_page.text, 'lxml')
                a_links = report_soup.find_all('a')
                pdf_link = None
                for a_link in a_links:
                    if not pdf_link and 'click here to download' in a_link.text:
                        if a_link.attrs['href']:
                            pdf_link = a_link.attrs['href'].replace(" ", "")
                if not pdf_link:
                    raise RuntimeError(
                        f'Could not find a link for the pdf in {link}')
                # and get the release date for this report
                pdf_release_date = None
                h2_texts = report_soup.find_all(
                    'h2', {"class": "elementor-heading-title elementor-size-default"})
                for text in h2_texts:
                    try:
                        # check if any of the texts can be parsed to a dates
                        if not pdf_release_date:
                            pdf_release_date = datetime.strptime(
                                text.contents[0], '%d/%m/%Y')
                    except (ValueError, TypeError):
                        pass
                if not pdf_release_date:
                    raise RuntimeError(
                        f'Could not find a release date for this report: {link}')
                # now append our data
                pdf_reports.append(
                    {'pdf_link': pdf_link, 'release_date': pdf_release_date})
            except (requests.exceptions.HTTPError, RuntimeError) as exc:
                logger.warning(
                    f"Ran into an error while trying to get the download link for {link}. Skipping statement.", exc_info=exc)
        # now actually download the pdf files
        for pdf in pdf_reports:
            # create the name to use for this downloaded file
            local_filename = f"{symbol_data['symbol']}_annual_report_{pdf['release_date'].strftime('%Y-%m-%d')}.pdf"
            # check if file was already downloaded
            full_local_path = reports_dir.joinpath(local_filename)
            if full_local_path.is_file():
                logger.info(
                    f"PDF file {local_filename} was already downloaded. Skipping.")
            else:
                # else download this new pdf report
                # get the http response
                http_response_obj = requests.get(pdf['pdf_link'], stream=True)
                # save contents of response (pdf report) to the local file
                logger.info(
                    f"Now downloading file {local_filename}. Please wait.")
                with open(full_local_path, "wb") as local_pdf_file:
                    for chunk in http_response_obj.iter_content(chunk_size=1024):
                        # save chunks of pdf to local file
                        if chunk:
                            local_pdf_file.write(chunk)
                logger.info("Finished download file.")
    logger.info("Finished downloading all annual reports.")
    return 0


def fetch_audited_statements():
    """Fetch the audited annual statements of each symbol from
    https://www.stockex.co.tt/news/xxxxxxx
    """
    # for some reason, ttse is using symbol ids instead of actual symbols here,
    # so we need to set up a list of these ids
    logger = logging.getLogger(LOGGERNAME)
    listed_symbol_data = []
    with DatabaseConnect() as db_obj:
        listed_equities_table = Table(
            'listed_equities', MetaData(), autoload=True, autoload_with=db_obj.dbengine)
        selectstmt = select(
            [listed_equities_table.c.symbol, listed_equities_table.c.symbol_id])
        result = db_obj.dbcon.execute(selectstmt)
        for row in result:
            if row[0] not in IGNORE_SYMBOLS:
                listed_symbol_data.append(
                    {'symbol': row[0], 'symbol_id': row[1]})
    # now go through the list of symbol ids and fetch the required reports for each
    for symbol_data in listed_symbol_data:
        # ensure the required directories are created
        current_dir = Path(os.path.realpath(__file__)).parent
        reports_dir = current_dir.joinpath(
            REPORTS_DIRECTORY).joinpath(symbol_data['symbol'])
        if reports_dir.exists() and reports_dir.is_dir():
            logger.info(
                f"Directory for {symbol_data['symbol']}'s annual statements found at {reports_dir}")
        else:
            logger.info(
                f"Directory for {symbol_data['symbol']}'s annual statements not found at {reports_dir}. Trying to create.")
            reports_dir.mkdir()
            if not reports_dir.exists():
                raise RuntimeError(
                    f"Could not create directory at {reports_dir}")
        # now load the page with the reports
        logger.info(
            f"Now trying to fetch annual audited statements for {symbol_data['symbol']}")
        annual_statements_url = f"https://www.stockex.co.tt/news/?symbol={symbol_data['symbol_id']}&category={TTSE_NEWS_CATEGORIES['annual_statements']}&date={ANNUAL_STATEMENTS_START_DATE_STRING}&date_to={TODAY_DATE}"
        logger.info(f"Navigating to {annual_statements_url}")
        annual_statements_page = requests.get(
            annual_statements_url, timeout=WEBPAGE_LOAD_TIMEOUT_SECS)
        if annual_statements_page.status_code != 200:
            raise requests.exceptions.HTTPError(
                "Could not load URL. "+annual_statements_url)
        logger.info("Successfully loaded webpage.")
        # and search the page for the links to all annual reports
        annual_statements_page_soup = BeautifulSoup(
            annual_statements_page.text, 'lxml')
        news_div = annual_statements_page_soup.find(id='news')
        report_page_links = []
        for link in news_div.find_all('a', href=True):
            if link.attrs['href'] and \
                    'audited' in link.attrs['href'].lower():
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
                logger.info("Successfully loaded webpage.")
                # and search the page for the link to the actual pdf report
                report_soup = BeautifulSoup(
                    report_page.text, 'lxml')
                a_links = report_soup.find_all('a')
                pdf_link = None
                for a_link in a_links:
                    if not pdf_link and 'click here to download' in a_link.text:
                        if a_link.attrs['href']:
                            pdf_link = a_link.attrs['href'].replace(" ", "")
                if not pdf_link:
                    raise RuntimeError(
                        f'Could not find a link for the pdf in {link}')
                # and get the release date for this report
                pdf_release_date = None
                h2_texts = report_soup.find_all(
                    'h2', {"class": "elementor-heading-title elementor-size-default"})
                for text in h2_texts:
                    try:
                        # check if any of the texts can be parsed to a dates
                        if not pdf_release_date:
                            pdf_release_date = datetime.strptime(
                                text.contents[0], '%d/%m/%Y')
                    except (ValueError, TypeError):
                        pass
                if not pdf_release_date:
                    raise RuntimeError(
                        f'Could not find a release date for this report: {link}')
                # now append our data
                pdf_reports.append(
                    {'pdf_link': pdf_link, 'release_date': pdf_release_date})
            except (requests.exceptions.HTTPError, RuntimeError) as exc:
                logger.warning(
                    f"Ran into an error while trying to get the download link for {link}. Skipping statement.", exc_info=exc)
        # now actually download the pdf files
        for pdf in pdf_reports:
            # create the name to use for this downloaded file
            local_filename = f"{symbol_data['symbol']}_audited_statement_{pdf['release_date'].strftime('%Y-%m-%d')}.pdf"
            # check if file was already downloaded
            full_local_path = reports_dir.joinpath(local_filename)
            if full_local_path.is_file():
                logger.info(
                    f"PDF file {local_filename} was already downloaded. Skipping.")
            else:
                # else download this new pdf report
                # get the http response
                http_response_obj = requests.get(pdf['pdf_link'], stream=True)
                # save contents of response (pdf report) to the local file
                logger.info(
                    f"Now downloading file {local_filename}. Please wait.")
                with open(full_local_path, "wb") as local_pdf_file:
                    for chunk in http_response_obj.iter_content(chunk_size=1024):
                        # save chunks of pdf to local file
                        if chunk:
                            local_pdf_file.write(chunk)
                logger.info("Finished download file.")
    logger.info("Finished downloading all annual audited statements.")
    return 0


def fetch_quarterly_statements():
    """Fetch the unaudited quarterly statements of each symbol from
    https://www.stockex.co.tt/news/xxxxxxx
    """
    # for some reason, ttse is using symbol ids instead of actual symbols here,
    # so we need to set up a list of these ids
    logger = logging.getLogger(LOGGERNAME)
    listed_symbol_data = []
    with DatabaseConnect() as db_obj:
        listed_equities_table = Table(
            'listed_equities', MetaData(), autoload=True, autoload_with=db_obj.dbengine)
        selectstmt = select(
            [listed_equities_table.c.symbol, listed_equities_table.c.symbol_id])
        result = db_obj.dbcon.execute(selectstmt)
        for row in result:
            if row[0] not in IGNORE_SYMBOLS:
                listed_symbol_data.append(
                    {'symbol': row[0], 'symbol_id': row[1]})
    # now go through the list of symbol ids and fetch the required reports for each
    for symbol_data in listed_symbol_data:
        # ensure the required directories are created
        current_dir = Path(os.path.realpath(__file__)).parent
        reports_dir = current_dir.joinpath(
            REPORTS_DIRECTORY).joinpath(symbol_data['symbol'])
        if reports_dir.exists() and reports_dir.is_dir():
            logger.info(
                f"Directory for {symbol_data['symbol']}'s quarterly statements found at {reports_dir}")
        else:
            logger.info(
                f"Directory for {symbol_data['symbol']}'s quarterly statements not found at {reports_dir}. Trying to create.")
            reports_dir.mkdir()
            if not reports_dir.exists():
                raise RuntimeError(
                    f"Could not create directory at {reports_dir}")
        # now load the page with the reports
        logger.info(
            f"Now trying to fetch quarterly unaudited statements for {symbol_data['symbol']}")
        quarterly_statements_url = f"https://www.stockex.co.tt/news/?symbol={symbol_data['symbol_id']}&category={TTSE_NEWS_CATEGORIES['quarterly_statements']}&date={QUARTERLY_STATEMENTS_START_DATE_STRING}&date_to={TODAY_DATE}"
        logger.info(f"Navigating to {quarterly_statements_url}")
        quarterly_statements_page = requests.get(
            quarterly_statements_url, timeout=WEBPAGE_LOAD_TIMEOUT_SECS)
        if quarterly_statements_page.status_code != 200:
            raise requests.exceptions.HTTPError(
                "Could not load URL. "+quarterly_statements_url)
        logger.info("Successfully loaded webpage.")
        # and search the page for the links to all annual reports
        quarterly_statements_page_soup = BeautifulSoup(
            quarterly_statements_page.text, 'lxml')
        news_div = quarterly_statements_page_soup.find(id='news')
        report_page_links = []
        for link in news_div.find_all('a', href=True):
            if link.attrs['href'] and \
                    ('unaudited' in link.attrs['href'].lower() or 'financial' in link.attrs['href'].lower()):
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
                        "Could not load URL. "+quarterly_statements_url)
                logger.info("Successfully loaded webpage.")
                # and search the page for the link to the actual pdf report
                report_soup = BeautifulSoup(
                    report_page.text, 'lxml')
                a_links = report_soup.find_all('a')
                pdf_link = None
                for a_link in a_links:
                    if not pdf_link and 'click here to download' in a_link.text:
                        if a_link.attrs['href']:
                            pdf_link = a_link.attrs['href'].replace(" ", "")
                if not pdf_link:
                    raise RuntimeError(
                        f'Could not find a link for the pdf in {link}')
                # and get the release date for this report
                pdf_release_date = None
                h2_texts = report_soup.find_all(
                    'h2', {"class": "elementor-heading-title elementor-size-default"})
                for text in h2_texts:
                    try:
                        # check if any of the texts can be parsed to a dates
                        if not pdf_release_date:
                            pdf_release_date = datetime.strptime(
                                text.contents[0], '%d/%m/%Y')
                    except (ValueError, TypeError):
                        pass
                if not pdf_release_date:
                    raise RuntimeError(
                        f'Could not find a release date for this report: {link}')
                # now append our data
                if pdf_release_date > QUARTERLY_STATEMENTS_START_DATETIME:
                    logger.info(
                        "Report is new enough. Adding to download list.")
                    pdf_reports.append(
                        {'pdf_link': pdf_link, 'release_date': pdf_release_date})
                else:
                    logger.info("Report is too old. Discarding.")
            except (requests.exceptions.HTTPError, RuntimeError) as exc:
                logger.warning(
                    f"Ran into an error while trying to get the download link for {link}. Skipping statement.", exc_info=exc)
        # now actually download the pdf files
        for pdf in pdf_reports:
            # create the name to use for this downloaded file
            local_filename = f"{symbol_data['symbol']}_quarterly_statement_{pdf['release_date'].strftime('%Y-%m-%d')}.pdf"
            # check if file was already downloaded
            full_local_path = reports_dir.joinpath(local_filename)
            if full_local_path.is_file():
                logger.info(
                    f"PDF file {local_filename} was already downloaded. Skipping.")
            else:
                # else download this new pdf report
                # get the http response
                http_response_obj = requests.get(pdf['pdf_link'], stream=True)
                # save contents of response (pdf report) to the local file
                logger.info(
                    f"Now downloading file {local_filename}. Please wait.")
                with open(full_local_path, "wb") as local_pdf_file:
                    for chunk in http_response_obj.iter_content(chunk_size=1024):
                        # save chunks of pdf to local file
                        if chunk:
                            local_pdf_file.write(chunk)
                logger.info("Finished download file.")
    logger.info("Finished downloading all quarterly unaudited statements.")
    return 0


def alert_me_new_annual_reports():
    """
    Send an email to latchmepersad@gmail.com if any new annual/audited/querterly reports are detected
    """
    logger = logging.getLogger(LOGGERNAME)
    with DatabaseConnect() as db_connect:
        # first get a list of all symbols currently stored
        listed_equities_table = Table(
            'listed_equities', MetaData(), autoload=True, autoload_with=db_connect.dbengine)
        raw_annual_reports_table = Table(
            'raw_annual_data', MetaData(), autoload=True, autoload_with=db_connect.dbengine)
        # get a list of stored symbols
        selectstmt = select([listed_equities_table.c.symbol,
                             listed_equities_table.c.symbol_id])
        results = db_connect.dbcon.execute(selectstmt)
        all_symbols = []
        for result in results:
            if result[0] not in IGNORE_SYMBOLS:
                all_symbols.append(
                    {'symbol': result[0], 'symbol_id': result[1]})
        # now go to the url for each symbol that we have listed, and collect the data we need
        # region ANNUAL REPORTS
        # create some sets to hold our unique report names
        all_new_annual_reports = set()
        for symbol_data in all_symbols:
            symbol_available_annual_reports = set()
            symbol_processed_annual_reports = set()
            annual_reports_url = f"https://www.stockex.co.tt/news/?symbol={symbol_data['symbol_id']}&category={TTSE_NEWS_CATEGORIES['annual_reports']}"
            logger.debug(
                f"Now fetching latest available annual reports for {symbol_data['symbol']}")
            logger.debug(f"Navigating to {annual_reports_url}")
            annual_reports_page = requests.get(
                annual_reports_url, timeout=WEBPAGE_LOAD_TIMEOUT_SECS)
            if annual_reports_page.status_code != 200:
                raise requests.exceptions.HTTPError(
                    "Could not load URL. "+annual_reports_url)
            logger.debug("Successfully loaded webpage.")
            # and search the page for the links to all annual reports
            annual_reports_page_soup = BeautifulSoup(
                annual_reports_page.text, 'lxml')
            news_div = annual_reports_page_soup.find(id='news')
            report_page_links = []
            for link in news_div.find_all('a', href=True):
                if link.attrs['href'] and \
                        'annual-report' in link.attrs['href'].lower():
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
                            "Could not load URL. "+annual_reports_url)
                    logger.debug("Successfully loaded webpage.")
                    # and search the page for the link to the actual pdf report
                    report_soup = BeautifulSoup(
                        report_page.text, 'lxml')
                    a_links = report_soup.find_all('a')
                    pdf_link = None
                    for a_link in a_links:
                        if not pdf_link and 'click here to download' in a_link.text:
                            if a_link.attrs['href']:
                                pdf_link = a_link.attrs['href'].replace(
                                    " ", "")
                    if not pdf_link:
                        raise RuntimeError(
                            f'Could not find a link for the pdf in {link}')
                    # and get the release date for this report
                    pdf_release_date = None
                    h2_texts = report_soup.find_all(
                        'h2', {"class": "elementor-heading-title elementor-size-default"})
                    for text in h2_texts:
                        try:
                            # check if any of the texts can be parsed to a dates
                            if not pdf_release_date:
                                pdf_release_date = datetime.strptime(
                                    text.contents[0], '%d/%m/%Y')
                        except (ValueError, TypeError):
                            pass
                    if not pdf_release_date:
                        raise RuntimeError(
                            f'Could not find a release date for this report: {link}')
                    # now append our data
                    pdf_reports.append(
                        {'pdf_link': pdf_link, 'release_date': pdf_release_date})
                except (requests.exceptions.HTTPError, RuntimeError) as exc:
                    logger.warning(
                        f"Ran into an error while trying to get the download link for {link}. Skipping statement.", exc_info=exc)
            # now create the names for all available reports
            for pdf in pdf_reports:
                local_filename = f"{symbol_data['symbol']}_annual_report_{pdf['release_date'].strftime('%Y-%m-%d')}.pdf"
                symbol_available_annual_reports.add(local_filename)
            # now get the names of all processed reports
            selectstmt = select(
                [raw_annual_reports_table.c.reports_and_statements_referenced]).where(raw_annual_reports_table.c.symbol == symbol_data['symbol'])
            results = db_connect.dbcon.execute(selectstmt)
            for result in results:
                if result[0]:
                    symbol_processed_annual_reports.add(result[0])
            # now compare both sets and see if any new reports were found
            for available_report in symbol_available_annual_reports:
                available_report_processed_already = False
                for processed_report in symbol_processed_annual_reports:
                    if available_report in processed_report:
                        available_report_processed_already = True
                if not available_report_processed_already:
                    all_new_annual_reports.add(available_report)
        # endregion
        return all_new_annual_reports


def alert_me_new_audited_statements():
    """
    Send an email to latchmepersad@gmail.com if any new annual/audited/querterly reports are detected
    """
    logger = logging.getLogger(LOGGERNAME)
    with DatabaseConnect() as db_connect:
        # first get a list of all symbols currently stored
        listed_equities_table = Table(
            'listed_equities', MetaData(), autoload=True, autoload_with=db_connect.dbengine)
        raw_annual_reports_table = Table(
            'raw_annual_data', MetaData(), autoload=True, autoload_with=db_connect.dbengine)
        # get a list of stored symbols
        selectstmt = select([listed_equities_table.c.symbol,
                             listed_equities_table.c.symbol_id])
        results = db_connect.dbcon.execute(selectstmt)
        all_symbols = []
        for result in results:
            if result[0] not in IGNORE_SYMBOLS:
                all_symbols.append(
                    {'symbol': result[0], 'symbol_id': result[1]})
        # now go to the url for each symbol that we have listed, and collect the data we need
        # region AUDITED STATEMENTS
        # now repeat the process for the audited statements
        all_new_audited_statements = set()
        for symbol_data in all_symbols:
            symbol_available_audited_statements = set()
            symbol_processed_audited_statements = set()
            logger.debug(
                f"Now trying to fetch annual audited statements for {symbol_data['symbol']}")
            annual_statements_url = f"https://www.stockex.co.tt/news/?symbol={symbol_data['symbol_id']}&category={TTSE_NEWS_CATEGORIES['annual_statements']}"
            logger.debug(f"Navigating to {annual_statements_url}")
            annual_statements_page = requests.get(
                annual_statements_url, timeout=WEBPAGE_LOAD_TIMEOUT_SECS)
            if annual_statements_page.status_code != 200:
                raise requests.exceptions.HTTPError(
                    "Could not load URL. "+annual_statements_url)
            logger.debug("Successfully loaded webpage.")
            # and search the page for the links to all annual reports
            annual_statements_page_soup = BeautifulSoup(
                annual_statements_page.text, 'lxml')
            news_div = annual_statements_page_soup.find(id='news')
            report_page_links = []
            for link in news_div.find_all('a', href=True):
                if link.attrs['href'] and \
                        'audited' in link.attrs['href'].lower():
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
                    logger.info("Successfully loaded webpage.")
                    # and search the page for the link to the actual pdf report
                    report_soup = BeautifulSoup(
                        report_page.text, 'lxml')
                    a_links = report_soup.find_all('a')
                    pdf_link = None
                    for a_link in a_links:
                        if not pdf_link and 'click here to download' in a_link.text:
                            if a_link.attrs['href']:
                                pdf_link = a_link.attrs['href'].replace(
                                    " ", "")
                    if not pdf_link:
                        raise RuntimeError(
                            f'Could not find a link for the pdf in {link}')
                    # and get the release date for this report
                    pdf_release_date = None
                    h2_texts = report_soup.find_all(
                        'h2', {"class": "elementor-heading-title elementor-size-default"})
                    for text in h2_texts:
                        try:
                            # check if any of the texts can be parsed to a dates
                            if not pdf_release_date:
                                pdf_release_date = datetime.strptime(
                                    text.contents[0], '%d/%m/%Y')
                        except (ValueError, TypeError):
                            pass
                    if not pdf_release_date:
                        raise RuntimeError(
                            f'Could not find a release date for this report: {link}')
                    # now append our data
                    pdf_reports.append(
                        {'pdf_link': pdf_link, 'release_date': pdf_release_date})
                except (requests.exceptions.HTTPError, RuntimeError) as exc:
                    logger.warning(
                        f"Ran into an error while trying to get the download link for {link}. Skipping statement.", exc_info=exc)
            # now create the names for all available reports
            for pdf in pdf_reports:
                local_filename = f"{symbol_data['symbol']}_audited_statement_{pdf['release_date'].strftime('%Y-%m-%d')}.pdf"
                symbol_available_audited_statements.add(local_filename)
            # now get the names of all processed reports
            selectstmt = select(
                [raw_annual_reports_table.c.reports_and_statements_referenced]).where(raw_annual_reports_table.c.symbol == symbol_data['symbol'])
            results = db_connect.dbcon.execute(selectstmt)
            for result in results:
                if result[0]:
                    symbol_processed_audited_statements.add(result[0])
            # now compare both sets and see if any new reports were found
            for available_report in symbol_available_audited_statements:
                available_report_processed_already = False
                for processed_report in symbol_processed_audited_statements:
                    if available_report in processed_report:
                        available_report_processed_already = True
                if not available_report_processed_already:
                    all_new_audited_statements.add(available_report)
            # endregion
        return all_new_audited_statements


def alert_me_new_quarterly_statements():
    """
    Send an email to latchmepersad@gmail.com if any new annual/audited/querterly reports are detected
    """
    logger = logging.getLogger(LOGGERNAME)
    with DatabaseConnect() as db_connect:
        # first get a list of all symbols currently stored
        listed_equities_table = Table(
            'listed_equities', MetaData(), autoload=True, autoload_with=db_connect.dbengine)
        raw_quarterly_reports_table = Table(
            'raw_quarterly_data', MetaData(), autoload=True, autoload_with=db_connect.dbengine)
        # get a list of stored symbols
        selectstmt = select([listed_equities_table.c.symbol,
                             listed_equities_table.c.symbol_id])
        results = db_connect.dbcon.execute(selectstmt)
        all_symbols = []
        for result in results:
            if result[0] not in IGNORE_SYMBOLS:
                all_symbols.append(
                    {'symbol': result[0], 'symbol_id': result[1]})
        # now go to the url for each symbol that we have listed, and collect the data we need
        # region QUARTERLY STATEMENTS
        # now repeat the process for the quarterly statements
        all_new_quarterly_statements = set()
        for symbol_data in all_symbols:
            symbol_available_quarterly_statements = set()
            symbol_processed_quarterly_statements = set()
            logger.debug(
                f"Now trying to fetch quarterly unaudited statements for {symbol_data['symbol']}")
            quarterly_statements_url = f"https://www.stockex.co.tt/news/?symbol={symbol_data['symbol_id']}&category={TTSE_NEWS_CATEGORIES['quarterly_statements']}"
            logger.debug(f"Navigating to {quarterly_statements_url}")
            quarterly_statements_page = requests.get(
                quarterly_statements_url, timeout=WEBPAGE_LOAD_TIMEOUT_SECS)
            if quarterly_statements_page.status_code != 200:
                raise requests.exceptions.HTTPError(
                    "Could not load URL. "+quarterly_statements_url)
            logger.debug("Successfully loaded webpage.")
            # and search the page for the links to all annual reports
            quarterly_statements_page_soup = BeautifulSoup(
                quarterly_statements_page.text, 'lxml')
            news_div = quarterly_statements_page_soup.find(id='news')
            report_page_links = []
            for link in news_div.find_all('a', href=True):
                if link.attrs['href'] and \
                        ('unaudited' in link.attrs['href'].lower() or 'financial' in link.attrs['href'].lower()):
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
                            "Could not load URL. "+quarterly_statements_url)
                    logger.debug("Successfully loaded webpage.")
                    # and search the page for the link to the actual pdf report
                    report_soup = BeautifulSoup(
                        report_page.text, 'lxml')
                    a_links = report_soup.find_all('a')
                    pdf_link = None
                    for a_link in a_links:
                        if not pdf_link and 'click here to download' in a_link.text:
                            if a_link.attrs['href']:
                                pdf_link = a_link.attrs['href'].replace(
                                    " ", "")
                    if not pdf_link:
                        raise RuntimeError(
                            f'Could not find a link for the pdf in {link}')
                    # and get the release date for this report
                    pdf_release_date = None
                    h2_texts = report_soup.find_all(
                        'h2', {"class": "elementor-heading-title elementor-size-default"})
                    for text in h2_texts:
                        try:
                            # check if any of the texts can be parsed to a dates
                            if not pdf_release_date:
                                pdf_release_date = datetime.strptime(
                                    text.contents[0], '%d/%m/%Y')
                        except (ValueError, TypeError):
                            pass
                    if not pdf_release_date:
                        raise RuntimeError(
                            f'Could not find a release date for this report: {link}')
                    # now append our data
                    if pdf_release_date > QUARTERLY_STATEMENTS_START_DATETIME:
                        logger.debug(
                            "Report is new enough. Adding to download list.")
                        pdf_reports.append(
                            {'pdf_link': pdf_link, 'release_date': pdf_release_date})
                    else:
                        logger.debug("Report is too old. Discarding.")
                except (requests.exceptions.HTTPError, RuntimeError) as exc:
                    logger.warning(
                        f"Ran into an error while trying to get the download link for {link}. Skipping statement.", exc_info=exc)
            # now create the names for all available reports
            for pdf in pdf_reports:
                local_filename = f"{symbol_data['symbol']}_quarterly_statement_{pdf['release_date'].strftime('%Y-%m-%d')}.pdf"
                symbol_available_quarterly_statements.add(local_filename)
            # now get the names of all processed reports
            selectstmt = select(
                [raw_quarterly_reports_table.c.reports_and_statements_referenced]).where(raw_quarterly_reports_table.c.symbol == symbol_data['symbol'])
            results = db_connect.dbcon.execute(selectstmt)
            for result in results:
                if result[0]:
                    symbol_processed_quarterly_statements.add(result[0])
            # now compare both sets and see if any new reports were found
            for available_report in symbol_available_quarterly_statements:
                available_report_processed_already = False
                for processed_report in symbol_processed_quarterly_statements:
                    if available_report in processed_report:
                        available_report_processed_already = True
                if not available_report_processed_already:
                    all_new_quarterly_statements.add(available_report)
        # endregion
        return all_new_quarterly_statements


def main(args):
    """The main function to coordinate the functions defined above"""
    # Set up logging for this module
    q_listener, q, logger = custom_logging.setup_logging(
        logdirparent=str(os.path.dirname(os.path.realpath(__file__))),
        loggername=LOGGERNAME,
        stdoutlogginglevel=logging.DEBUG,
        smtploggingenabled=True,
        smtplogginglevel=logging.ERROR,
        smtpmailhost='localhost',
        smtpfromaddr='server1@trinistats.com',
        smtptoaddr=['latchmepersad@gmail.com'],
        smtpsubj='Automated report from Python script: '+os.path.basename(__file__))
    try:
        logger = logging.getLogger(LOGGERNAME)
        # Set up a pidfile to ensure that only one instance of this script runs at a time
        with PidFile(piddir=tempfile.gettempdir()):
            # run all functions within a multiprocessing pool
            with multiprocessing.Pool(os.cpu_count(), custom_logging.logging_worker_init, [q]) as multipool:
                logger.debug(
                    "Downloading latest fundamental reports from the TTSE site.")
                annual_reports_code = multipool.apply_async(
                    fetch_annual_reports, ())
                audited_statements_code = multipool.apply_async(
                    fetch_audited_statements, ())
                quarterly_statements_code = multipool.apply_async(
                    fetch_quarterly_statements, ())
                # now wait and ensure that all reports are downloaded
                logger.debug(
                    f"Annual reports downloading function exited with code: {annual_reports_code.get()}")
                logger.debug(
                    f"Annual audited statements downloading function exited with code: {audited_statements_code.get()}")
                logger.debug(
                    f"Quarterly statements downloading function exited with code: {quarterly_statements_code.get()}")
                logging.debug(
                    'Downloads complete. Now checking if any reports have not been processed.')
                # now check if any reports are outstanding
                res_new_annual_reports = multipool.apply_async(
                    alert_me_new_annual_reports, ())
                res_new_audited_statements = multipool.apply_async(
                    alert_me_new_audited_statements, ())
                res_new_quarterly_statements = multipool.apply_async(
                    alert_me_new_quarterly_statements, ())
                # wait until the new report list is ready
                new_annual_reports = res_new_annual_reports.get()
                logger.debug(
                    f"Annual reports processing check function exited with code: {new_annual_reports}")
                new_audited_statements = res_new_audited_statements.get()
                logger.debug(
                    f"Audited statements processing check function exited with code: {new_audited_statements}")
                new_quarterly_statements = res_new_quarterly_statements.get()
                logger.debug(
                    f"Quarterly statements processing check function exited with code: {new_quarterly_statements}")
                multipool.close()
                multipool.join()
                # now send the email of outstanding reports
                msg = MIMEText(f'''
Hello,

These are the outstanding fundamental reports that we found on the TTSE.

New Annual Reports: {new_annual_reports}

New Audited Statements: {new_audited_statements}

New Quarterly Statements: {new_quarterly_statements}

Please add the data for these reports into the raw_quarterly_data and raw_annual_data tables on the server.

Sincerely,
trinistocks.com
                ''')
                msg["From"] = "trinistocks@gmail.com"
                msg["To"] = OUTSTANDINGREPORTEMAIL
                msg["Subject"] = "trinistocks: Outstanding Fundamental Reports"
                p = Popen(["sendmail", "-t", "-oi"],
                          stdin=PIPE, universal_newlines=True)
                p.communicate(msg.as_string())
                return_code = p.returncode
                if return_code == 0:
                    logger.debug(
                        f"Sent report to {OUTSTANDINGREPORTEMAIL} successfully.")
                else:
                    logger.error(
                        "Could not send report to {OUTSTANDINGREPORTEMAIL}")
            logger.info(os.path.basename(__file__) +
                        " executed successfully.")
        q_listener.stop()
        return 0
    except Exception:
        logger.exception("Error in script "+os.path.basename(__file__))
        custom_logging.flush_smtp_logger()


# If this script is being run from the command-line, then run the main() function
if __name__ == "__main__":
    # first check the arguements given to this script
    parser = argparse.ArgumentParser()
    args = parser.parse_args()
    main(args)
