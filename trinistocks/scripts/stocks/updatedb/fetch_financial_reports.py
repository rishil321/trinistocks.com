#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Description of this module/script goes here
:param -f OR --first_parameter: The description of your first input parameter
:returns: Whatever your script returns when called
:raises Exception if any issues are encountered
"""

import argparse
# Put all your imports here, one per line.
# However multiple imports from the same lib are allowed on a line.
# Imports from Python standard libraries
import logging
import multiprocessing
import os
from datetime import datetime
from email.mime.text import MIMEText
# Imports from the cheese factory
from logging.config import dictConfig
from pathlib import Path
from subprocess import Popen, PIPE
from typing import List
from typing_extensions import Self

import requests
from bs4 import BeautifulSoup
from dateutils import relativedelta
from dotenv import load_dotenv
from pid.decorator import pidfile
from sqlalchemy import Table, select, MetaData

from scripts.stocks.scraping_engine import ScrapingEngine

# Imports from the local filesystem
load_dotenv()
from ...database_ops import DatabaseConnect
from .. import logging_configs

dictConfig(logging_configs.LOGGING_CONFIG)
logger = logging.getLogger()
# Put your constants here. These should be named in CAPS.
TTSE_NEWS_CATEGORIES = {
    "annual_reports": 56,
    "articles": 57,
    "annual_statements": 58,
    "quarterly_statements": 59,
}
WEBPAGE_LOAD_TIMEOUT_SECS = 60
REPORTS_DIRECTORY = "financial_reports"
IGNORE_SYMBOLS = ["CPFV", "GMLP", "LJWA", "LJWP", "MOV", "PPMF", "SFC"]
QUARTERLY_STATEMENTS_START_DATE_STRING = "2020-10-01"
QUARTERLY_STATEMENTS_START_DATETIME = datetime.strptime(
    QUARTERLY_STATEMENTS_START_DATE_STRING, "%Y-%m-%d"
)
ANNUAL_STATEMENTS_START_DATE_STRING = "2020-01-01"
TOMORROW_DATE = datetime.strftime(datetime.now() + relativedelta(days=1), "%Y-%m-%d")
LOGGERNAME = "fetch_financial_reports.py"
LOGGER = logging.getLogger(LOGGERNAME)
OUTSTANDINGREPORTEMAIL = "latchmepersad@gmail.com"
HTTP_GET_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36"
}


# Put your global variables here.

# Put your class definitions here. These should use the CapWords convention.
class FinancialReportsScraper:
    def __init__(self):
        self.listed_symbol_data = fetch_listed_equities_data_from_db()
        self.symbol_report_directories = self.build_report_directories_for_each_symbol()

    def __del__(self):
        pass

    def alert_me_new_quarterly_statements(self: Self):
        """
        Send an email to latchmepersad@gmail.com if any new annual/audited/querterly reports are detected
        """
        with DatabaseConnect() as db_connect:
            scraping_engine: ScrapingEngine = ScrapingEngine()
            all_new_quarterly_statements = set()
            for symbol_data in self.listed_symbol_data:
                logger.info(
                    f"Now checking outstanding quarterly unaudited statements for {symbol_data['symbol']} in PID {os.getpid()}"
                )
                quarterly_statements_page_soup = self._scrape_quarterly_news_data_for_symbol(scraping_engine,
                                                                                             symbol_data)
                report_page_links = self._build_links_to_reports_for_quarterly_statements(
                    quarterly_statements_page_soup)
                pdf_reports = self._build_direct_links_to_pdfs_for_quarterly_statements(report_page_links,
                                                                                        scraping_engine)
                # now create the names for all available reports
                symbol_available_quarterly_statements = self._build_list_of_all_available_quarterly_statements(
                    pdf_reports, symbol_data)
                # now get the names of all processed reports
                symbol_processed_quarterly_statements = self._build_list_of_all_quarterly_statements_already_processed(
                    db_connect, symbol_data)
                # now compare both sets and see if any new reports were found
                self._calculate_new_available_reports(all_new_quarterly_statements,
                                                      symbol_available_quarterly_statements,
                                                      symbol_processed_quarterly_statements)
                logger.info(
                    f"Finished checking outstanding quarterly reports for {symbol_data['symbol']} in PID {os.getpid()}"
                )
            # endregion
            return all_new_quarterly_statements

    def _build_list_of_all_quarterly_statements_already_processed(self, db_connect, symbol_data):
        raw_quarterly_reports_table = Table(
            "raw_quarterly_data",
            MetaData(),
            autoload=True,
            autoload_with=db_connect.dbengine,
        )
        selectstmt = select(
            [raw_quarterly_reports_table.c.reports_and_statements_referenced]
        ).where(raw_quarterly_reports_table.c.symbol == symbol_data["symbol"])
        results = db_connect.dbcon.execute(selectstmt)
        symbol_processed_quarterly_statements = set()
        for result in results:
            if result[0]:
                symbol_processed_quarterly_statements.add(result[0])
        return symbol_processed_quarterly_statements

    def _build_list_of_all_available_quarterly_statements(self, pdf_reports, symbol_data):
        symbol_available_quarterly_statements = set()
        for pdf in pdf_reports:
            local_filename = f"{symbol_data['symbol']}_quarterly_statement_{pdf['release_date'].strftime('%Y-%m-%d')}.pdf"
            symbol_available_quarterly_statements.add(local_filename)
        return symbol_available_quarterly_statements

    def _build_links_to_reports_for_quarterly_statements(self, quarterly_statements_page_soup):
        news_div = quarterly_statements_page_soup.find(id="news")
        report_page_links = []
        for link in news_div.find_all("a", href=True):
            if link.attrs["href"] and (
                    "unaudited" in link.attrs["href"].lower()
                    or "financial" in link.attrs["href"].lower()
            ):
                report_page_links.append(link.attrs["href"])
        return report_page_links

    def _scrape_quarterly_news_data_for_symbol(self, scraping_engine, symbol_data):
        quarterly_statements_url = f"https://www.stockex.co.tt/news/?symbol={symbol_data['symbol_id']}&category={TTSE_NEWS_CATEGORIES['quarterly_statements']}"
        quarterly_statements_page = scraping_engine.get_url_and_return_html(url=quarterly_statements_url)
        # and search the page for the links to all annual reports
        quarterly_statements_page_soup = BeautifulSoup(
            quarterly_statements_page, "lxml"
        )
        return quarterly_statements_page_soup

    def alert_me_new_audited_statements(self: Self):
        """
        Send an email to latchmepersad@gmail.com if any new annual/audited/querterly reports are detected
        """
        with DatabaseConnect() as db_connect:
            scraping_engine: ScrapingEngine = ScrapingEngine()
            all_new_audited_statements = set()
            for symbol_data in self.listed_symbol_data:
                annual_statements_page_soup = self._scrape_audited_statement_news_data(scraping_engine, symbol_data)
                report_page_links = self._get_news_links_from_audited_statements_news_div(annual_statements_page_soup)
                pdf_reports = self._get_direct_pdf_links_for_audited_statements(report_page_links, scraping_engine)
                # now create the names for all available reports
                symbol_available_audited_statements = self._create_list_of_available_audited_statements(pdf_reports,
                                                                                                        symbol_data)
                symbol_processed_audited_statements = self._create_list_of_processed_audited_statements(db_connect,
                                                                                                        symbol_data)
                # now compare both sets and see if any new reports were found
                self._calculate_new_available_reports(all_new_audited_statements, symbol_available_audited_statements,
                                                      symbol_processed_audited_statements)
                logger.info(
                    f"Finished checking outstanding audited statements for {symbol_data['symbol']} in PID {os.getpid()}"
                )
            return all_new_audited_statements

    def _calculate_new_available_reports(self, all_new_available_reports: set, symbol_available_reports: set,
                                         symbol_processed_reports: set):
        for available_report in symbol_available_reports:
            available_report_processed_already = False
            for processed_report in symbol_processed_reports:
                if available_report in processed_report:
                    available_report_processed_already = True
            if not available_report_processed_already:
                all_new_available_reports.add(available_report)

    def _create_list_of_processed_audited_statements(self, db_connect, symbol_data):
        raw_annual_reports_table = Table(
            "raw_annual_data",
            MetaData(),
            autoload=True,
            autoload_with=db_connect.dbengine,
        )
        # now get the names of all processed reports
        selectstmt = select(
            [raw_annual_reports_table.c.reports_and_statements_referenced]
        ).where(raw_annual_reports_table.c.symbol == symbol_data["symbol"])
        results = db_connect.dbcon.execute(selectstmt)
        symbol_processed_audited_statements = set()
        for result in results:
            if result[0]:
                symbol_processed_audited_statements.add(result[0])
        return symbol_processed_audited_statements

    def _create_list_of_available_audited_statements(self, pdf_reports, symbol_data):
        symbol_available_audited_statements = set()
        for pdf in pdf_reports:
            local_filename = f"{symbol_data['symbol']}_audited_statement_{pdf['release_date'].strftime('%Y-%m-%d')}.pdf"
            symbol_available_audited_statements.add(local_filename)
        return symbol_available_audited_statements

    def _get_direct_pdf_links_for_audited_statements(self, report_page_links, scraping_engine):
        pdf_reports = []
        for link in report_page_links:
            try:
                report_page = scraping_engine.get_url_and_return_html(url=link)
                # and search the page for the link to the actual pdf report
                report_soup = BeautifulSoup(report_page, "lxml")
                a_links = report_soup.find_all("a")
                pdf_link = None
                for a_link in a_links:
                    if not pdf_link and "click here to download" in a_link.text:
                        if a_link.attrs["href"]:
                            pdf_link = a_link.attrs["href"].replace(" ", "")
                if not pdf_link:
                    raise RuntimeError(
                        f"Could not find a link for the pdf in {link}"
                    )
                # and get the release date for this report
                pdf_release_date = None
                h2_texts = report_soup.find_all(
                    "h2",
                    {"class": "elementor-heading-title elementor-size-default"},
                )
                for text in h2_texts:
                    try:
                        # check if any of the texts can be parsed to a dates
                        if not pdf_release_date:
                            pdf_release_date = datetime.strptime(
                                text.contents[0], "%d/%m/%Y"
                            )
                    except (ValueError, TypeError):
                        pass
                if not pdf_release_date:
                    raise RuntimeError(
                        f"Could not find a release date for this report: {link}"
                    )
                # now append our data
                pdf_reports.append(
                    {"pdf_link": pdf_link, "release_date": pdf_release_date}
                )
            except (requests.exceptions.HTTPError, RuntimeError) as exc:
                logger.warning(
                    f"Ran into an error while trying to get the download link for {link}. Skipping statement.",
                    exc_info=exc,
                )
        return pdf_reports

    def _get_news_links_from_audited_statements_news_div(self, annual_statements_page_soup):
        news_div = annual_statements_page_soup.find(id="news")
        report_page_links = []
        for link in news_div.find_all("a", href=True):
            if link.attrs["href"] and "audited" in link.attrs["href"].lower():
                report_page_links.append(link.attrs["href"])
        return report_page_links

    def _scrape_audited_statement_news_data(self, scraping_engine, symbol_data):
        logger.info(
            f"Now trying to fetch annual audited statements for {symbol_data['symbol']} in PID {os.getpid()}"
        )
        annual_statements_url = f"https://www.stockex.co.tt/news/?symbol={symbol_data['symbol_id']}&category={TTSE_NEWS_CATEGORIES['annual_statements']}"
        annual_statements_page = scraping_engine.get_url_and_return_html(url=annual_statements_url)
        # and search the page for the links to all annual reports
        annual_statements_page_soup = BeautifulSoup(
            annual_statements_page, "lxml"
        )
        return annual_statements_page_soup

    def alert_me_new_annual_reports(self: Self):
        """
        Send an email to latchmepersad@gmail.com if any new annual reports are detected
        """
        all_new_annual_reports = set()
        with DatabaseConnect() as db_connect:
            # go to the url for each symbol that we have listed, and check which new annual reports are available
            scraping_engine: ScrapingEngine = ScrapingEngine()
            for symbol_data in self.listed_symbol_data:
                annual_reports_page_soup = self._scrape_annual_reports_page_and_return_soup(
                    scraping_engine, symbol_data)
                report_page_links = self._build_links_to_annual_reports_from_news_page(annual_reports_page_soup)
                # now navigate to each link and get the actual links of the pdf files
                pdf_reports = self._create_list_of_direct_links_to_annual_reports(report_page_links, scraping_engine)
                # now create the names for all available reports
                symbol_available_annual_reports = self._create_list_of_all_available_annual_reports(pdf_reports,
                                                                                                    symbol_data)
                # now get the names of all processed reports
                symbol_processed_annual_reports = self._create_list_of_all_processed_annual_reports(db_connect,
                                                                                                    symbol_data)
                # now compare both sets and see if any new reports were found
                self._check_for_new_available_annual_reports(all_new_annual_reports, symbol_available_annual_reports,
                                                             symbol_data, symbol_processed_annual_reports)
            return all_new_annual_reports

    def _check_for_new_available_annual_reports(self, all_new_annual_reports, symbol_available_annual_reports,
                                                symbol_data, symbol_processed_annual_reports):
        logger.debug(
            "Now comparing downloaded and processed reports for all symbols."
        )
        self._calculate_new_available_reports(all_new_annual_reports, symbol_available_annual_reports,
                                              symbol_processed_annual_reports)
        logger.info(
            f"Finished checks for {symbol_data['symbol']} in PID {os.getpid()}"
        )

    def _create_list_of_all_processed_annual_reports(self, db_connect, symbol_data):
        symbol_processed_annual_reports = set()
        raw_annual_reports_table = Table(
            "raw_annual_data",
            MetaData(),
            autoload=True,
            autoload_with=db_connect.dbengine,
        )
        selectstmt = select(
            [raw_annual_reports_table.c.reports_and_statements_referenced]
        ).where(raw_annual_reports_table.c.symbol == symbol_data["symbol"])
        results = db_connect.dbcon.execute(selectstmt)
        for result in results:
            if result[0]:
                symbol_processed_annual_reports.add(result[0])
        return symbol_processed_annual_reports

    def _create_list_of_all_available_annual_reports(self, pdf_reports, symbol_data):
        symbol_available_annual_reports = set()
        for pdf in pdf_reports:
            local_filename = f"{symbol_data['symbol']}_annual_report_{pdf['release_date'].strftime('%Y-%m-%d')}.pdf"
            symbol_available_annual_reports.add(local_filename)
        return symbol_available_annual_reports

    def _create_list_of_direct_links_to_annual_reports(self, report_page_links, scraping_engine):
        pdf_reports = []
        for link in report_page_links:
            try:
                report_page = scraping_engine.get_url_and_return_html(url=link)
                # and search the page for the link to the actual pdf report
                report_soup = BeautifulSoup(report_page, "lxml")
                a_links = report_soup.find_all("a")
                pdf_link = None
                for a_link in a_links:
                    if not pdf_link and "click here to download" in a_link.text:
                        if a_link.attrs["href"]:
                            pdf_link = a_link.attrs["href"].replace(" ", "")
                if not pdf_link:
                    raise RuntimeError(
                        f"Could not find a link for the pdf in {link}"
                    )
                # and get the release date for this report
                pdf_release_date = None
                h2_texts = report_soup.find_all(
                    "h2",
                    {"class": "elementor-heading-title elementor-size-default"},
                )
                for text in h2_texts:
                    try:
                        # check if any of the texts can be parsed to a dates
                        if not pdf_release_date:
                            pdf_release_date = datetime.strptime(
                                text.contents[0], "%d/%m/%Y"
                            )
                    except (ValueError, TypeError):
                        pass
                if not pdf_release_date:
                    raise RuntimeError(
                        f"Could not find a release date for this report: {link}"
                    )
                # now append our data
                pdf_reports.append(
                    {"pdf_link": pdf_link, "release_date": pdf_release_date}
                )
            except (requests.exceptions.HTTPError, RuntimeError) as exc:
                logger.warning(
                    f"Ran into an error while trying to get the download link for {link}. Skipping statement.",
                    exc_info=exc,
                )
        return pdf_reports

    def _build_links_to_annual_reports_from_news_page(self, annual_reports_page_soup):
        news_div = annual_reports_page_soup.find(id="news")
        report_page_links = []
        for link in news_div.find_all("a", href=True):
            if link.attrs["href"] and "annual-report" in link.attrs["href"].lower():
                report_page_links.append(link.attrs["href"])
        return report_page_links

    def _scrape_annual_reports_page_and_return_soup(self, scraping_engine, symbol_data) -> BeautifulSoup:
        annual_reports_url = f"https://www.stockex.co.tt/news/?symbol={symbol_data['symbol_id']}&category={TTSE_NEWS_CATEGORIES['annual_reports']}"
        logger.info(
            f"Now checking latest available annual reports for {symbol_data['symbol']} in PID {os.getpid()}"
        )
        annual_reports_page = scraping_engine.get_url_and_return_html(url=annual_reports_url)
        # and search the page for the links to all annual reports
        annual_reports_page_soup = BeautifulSoup(annual_reports_page, "lxml")
        return annual_reports_page_soup

    def fetch_annual_reports(self: Self) -> int:
        """Fetch the audited annual reports of each symbol from
        https://www.stockex.co.tt/news/xxxxxxx
        """
        scraping_engine: ScrapingEngine = ScrapingEngine()
        # now go through the list of symbol ids and fetch the required reports for each
        for symbol_data in self.listed_symbol_data:
            # now load the page with the reports
            LOGGER.info(
                f"Now trying to fetch annual reports for {symbol_data['symbol']} in PID {os.getpid()}"
            )
            annual_reports_url = f"https://www.stockex.co.tt/news/?symbol={symbol_data['symbol_id']}&category={TTSE_NEWS_CATEGORIES['annual_reports']}&date={ANNUAL_STATEMENTS_START_DATE_STRING}&date_to={TOMORROW_DATE}"
            html: str = scraping_engine.get_url_and_return_html(url=annual_reports_url)
            # and search the page for the links to all annual reports
            report_page_links = self.build_list_of_annual_report_news_links_for_symbol(html)
            # now navigate to each link
            # and get the actual links of the pdf files
            all_pdf_reports_for_symbol = self.build_list_of_direct_pdf_news_report_for_each_symbol(report_page_links)
            # now actually download the pdf files
            self.download_all_pdf_annual_reports_for_symbol(all_pdf_reports_for_symbol, symbol_data)
            LOGGER.info(
                f"Finished downloading all annual reports for {symbol_data['symbol']} in PID {os.getpid()}"
            )
        return 0

    def download_all_pdf_annual_reports_for_symbol(self, all_pdf_reports_for_symbol, symbol_data):
        for pdf in all_pdf_reports_for_symbol:
            # create the name to use for this downloaded file
            local_filename = f"{symbol_data['symbol']}_annual_report_{pdf['release_date'].strftime('%Y-%m-%d')}.pdf"
            # check if file was already downloaded
            symbol_report_directory: Path = self.symbol_report_directories[symbol_data['symbol']]
            full_local_path = symbol_report_directory.joinpath(local_filename)
            if full_local_path.is_file():
                LOGGER.debug(
                    f"PDF file {local_filename} was already downloaded. Skipping."
                )
            else:
                # else download this new pdf report
                # get the http response
                http_response_obj = requests.get(
                    pdf["pdf_link"], stream=True, headers=HTTP_GET_HEADERS
                )
                # save contents of response (pdf report) to the local file
                LOGGER.debug(f"Now downloading file {local_filename}. Please wait.")
                with open(full_local_path, "wb", encoding="utf-8") as local_pdf_file:
                    for chunk in http_response_obj.iter_content(chunk_size=1024):
                        # save chunks of pdf to local file
                        if chunk:
                            local_pdf_file.write(chunk)
                LOGGER.debug("Finished download file.")

    def build_list_of_direct_pdf_news_report_for_each_symbol(self, report_page_links):
        pdf_reports = []
        for link in report_page_links:
            try:
                report_page = requests.get(
                    link, timeout=WEBPAGE_LOAD_TIMEOUT_SECS, headers=HTTP_GET_HEADERS
                )
                if report_page.status_code != 200:
                    raise requests.exceptions.HTTPError(
                        "Could not load URL. " + link
                    )
                LOGGER.debug("Successfully loaded webpage.")
                # and search the page for the link to the actual pdf report
                report_soup = BeautifulSoup(report_page.text, "lxml")
                a_links = report_soup.find_all("a")
                pdf_link = None
                for a_link in a_links:
                    if not pdf_link and "click here to download" in a_link.text:
                        if a_link.attrs["href"]:
                            pdf_link = a_link.attrs["href"].replace(" ", "")
                if not pdf_link:
                    raise RuntimeError(f"Could not find a link for the pdf in {link}")
                # and get the release date for this report
                pdf_release_date = None
                h2_texts = report_soup.find_all(
                    "h2", {"class": "elementor-heading-title elementor-size-default"}
                )
                for text in h2_texts:
                    try:
                        # check if any of the texts can be parsed to a dates
                        if not pdf_release_date:
                            pdf_release_date = datetime.strptime(
                                text.contents[0], "%d/%m/%Y"
                            )
                    except (ValueError, TypeError):
                        pass
                if not pdf_release_date:
                    raise RuntimeError(
                        f"Could not find a release date for this report: {link}"
                    )
                # now append our data
                pdf_reports.append(
                    {"pdf_link": pdf_link, "release_date": pdf_release_date}
                )
            except (requests.exceptions.HTTPError, RuntimeError) as exc:
                LOGGER.warning(
                    f"Ran into an error while trying to get the download link for {link}. Skipping statement.",
                    exc_info=exc,
                )
        return pdf_reports

    def build_list_of_annual_report_news_links_for_symbol(self, html):
        page_soup: BeautifulSoup = BeautifulSoup(html, "lxml")
        report_page_links = self._build_links_to_annual_reports_from_news_page(page_soup)
        return report_page_links

    def build_report_directories_for_each_symbol(self: Self) -> dict[str, Path]:
        # ensure the required directories are created for each symbol listed
        # and return a dictonary of the report directories in the format symbol:report_directory
        report_directories_for_all_symbols: dict = {}
        for symbol_data in self.listed_symbol_data:
            current_dir = Path(os.path.realpath(__file__)).parent
            reports_dir: Path = current_dir.joinpath(REPORTS_DIRECTORY).joinpath(
                symbol_data["symbol"]
            )
            if reports_dir.exists() and reports_dir.is_dir():
                LOGGER.debug(
                    f"Directory for {symbol_data['symbol']}'s annual reports found at {reports_dir}"
                )
            else:
                LOGGER.debug(
                    f"Directory for {symbol_data['symbol']}'s annual reports not found at {reports_dir}. Trying to create."
                )
                reports_dir.mkdir()
                if not reports_dir.exists():
                    raise RuntimeError(f"Could not create directory at {reports_dir}")
            report_directories_for_all_symbols[symbol_data["symbol"]] = reports_dir
        return report_directories_for_all_symbols

    def fetch_audited_statements(self: Self):
        """Fetch the audited annual statements of each symbol from
        https://www.stockex.co.tt/news/xxxxxxx
        """
        scraping_engine: ScrapingEngine = ScrapingEngine()
        # now go through the list of symbol ids and fetch the required reports for each
        for symbol_data in self.listed_symbol_data:
            # now load the page with the reports
            logger.info(
                f"Now trying to fetch annual audited statements for {symbol_data['symbol']} in PID {os.getpid()}"
            )
            annual_statements_url = f"https://www.stockex.co.tt/news/?symbol={symbol_data['symbol_id']}&category={TTSE_NEWS_CATEGORIES['annual_statements']}&date={ANNUAL_STATEMENTS_START_DATE_STRING}&date_to={TOMORROW_DATE}"
            annual_statements_page: str = scraping_engine.get_url_and_return_html(url=annual_statements_url)
            # and search the page for the links to all annual reports
            report_page_links = self.build_list_of_audited_statements_news_pages_for_symbol(annual_statements_page)
            # now navigate to each link
            # and get the actual links of the pdf files
            pdf_reports = self.build_list_of_direct_pdf_reports_for_audited_statements(annual_statements_url,
                                                                                       report_page_links)
            self.download_all_pdf_audited_statements_for_symbol(pdf_reports, symbol_data)
            logger.info(
                f"Finished downloading all annual audited statements for {symbol_data['symbol']} in PID {os.getpid()}"
            )
        return 0

    def fetch_quarterly_statements(self: Self):
        """Fetch the unaudited quarterly statements of each symbol from
        https://www.stockex.co.tt/news/xxxxxxx
        """
        # now go through the list of symbol ids and fetch the required reports for each
        scraping_engine: ScrapingEngine = ScrapingEngine()
        for symbol_data in self.listed_symbol_data:
            # now load the page with the reports
            logger.info(
                f"Now trying to fetch quarterly unaudited statements for {symbol_data['symbol']} in PID {os.getpid()}"
            )
            quarterly_statements_url = f"https://www.stockex.co.tt/news/?symbol={symbol_data['symbol_id']}&category={TTSE_NEWS_CATEGORIES['quarterly_statements']}&date={QUARTERLY_STATEMENTS_START_DATE_STRING}&date_to={TOMORROW_DATE}"
            quarterly_statements_page: str = scraping_engine.get_url_and_return_html(url=quarterly_statements_url)
            # and search the page for the links to all annual reports
            quarterly_statements_page_soup = BeautifulSoup(
                quarterly_statements_page, "lxml"
            )
            report_page_links = self._build_list_of_quarterly_statements_links_from_news_page(
                quarterly_statements_page_soup)
            # now navigate to each link
            # and get the actual links of the pdf files
            pdf_reports = self._build_direct_links_to_pdfs_for_quarterly_statements(report_page_links, scraping_engine)
            # now actually download the pdf files
            self._download_pdf_reports_for_quarterly_statements(pdf_reports, symbol_data)
        return 0

    def _download_pdf_reports_for_quarterly_statements(self, pdf_reports, symbol_data):
        for pdf in pdf_reports:
            # create the name to use for this downloaded file
            local_filename = f"{symbol_data['symbol']}_quarterly_statement_{pdf['release_date'].strftime('%Y-%m-%d')}.pdf"
            # check if file was already downloaded
            symbol_report_directory: Path = self.symbol_report_directories[symbol_data['symbol']]
            full_local_path = symbol_report_directory.joinpath(local_filename)
            if full_local_path.is_file():
                logger.debug(
                    f"PDF file {local_filename} was already downloaded. Skipping."
                )
            else:
                # else download this new pdf report
                # get the http response
                http_response_obj = requests.get(
                    pdf["pdf_link"], stream=True, headers=HTTP_GET_HEADERS
                )
                # save contents of response (pdf report) to the local file
                logger.debug(f"Now downloading file {local_filename}. Please wait.")
                with open(full_local_path, "wb") as local_pdf_file:
                    for chunk in http_response_obj.iter_content(chunk_size=1024):
                        # save chunks of pdf to local file
                        if chunk:
                            local_pdf_file.write(chunk)
                logger.debug("Finished download file.")
        logger.info(
            f"Finished downloading all quarterly unaudited statements for {symbol_data['symbol']} in PID {os.getpid()}"
        )

    def _build_direct_links_to_pdfs_for_quarterly_statements(self, report_page_links,
                                                             scraping_engine: ScrapingEngine):
        pdf_reports = []
        for link in report_page_links:
            try:
                report_page = scraping_engine.get_url_and_return_html(url=link)
                # and search the page for the link to the actual pdf report
                report_soup = BeautifulSoup(report_page, "lxml")
                a_links = report_soup.find_all("a")
                pdf_link = None
                for a_link in a_links:
                    if not pdf_link and "click here to download" in a_link.text:
                        if a_link.attrs["href"]:
                            pdf_link = a_link.attrs["href"].replace(" ", "")
                if not pdf_link:
                    raise RuntimeError(f"Could not find a link for the pdf in {link}")
                # and get the release date for this report
                pdf_release_date = None
                h2_texts = report_soup.find_all(
                    "h2", {"class": "elementor-heading-title elementor-size-default"}
                )
                for text in h2_texts:
                    try:
                        # check if any of the texts can be parsed to a dates
                        if not pdf_release_date:
                            pdf_release_date = datetime.strptime(
                                text.contents[0], "%d/%m/%Y"
                            )
                    except (ValueError, TypeError):
                        pass
                if not pdf_release_date:
                    raise RuntimeError(
                        f"Could not find a release date for this report: {link}"
                    )
                # now append our data
                if pdf_release_date > QUARTERLY_STATEMENTS_START_DATETIME:
                    logger.debug("Report is new enough. Adding to download list.")
                    pdf_reports.append(
                        {"pdf_link": pdf_link, "release_date": pdf_release_date}
                    )
                else:
                    logger.debug("Report is too old. Discarding.")
            except (requests.exceptions.HTTPError, RuntimeError) as exc:
                logger.warning(
                    f"Ran into an error while trying to get the download link for {link}. Skipping statement.",
                    exc_info=exc,
                )
        return pdf_reports

    def _build_list_of_quarterly_statements_links_from_news_page(self, quarterly_statements_page_soup):
        report_page_links = self._build_links_to_reports_for_quarterly_statements(
            quarterly_statements_page_soup)
        return report_page_links

    def download_all_pdf_audited_statements_for_symbol(self, pdf_reports, symbol_data):
        # now actually download the pdf files
        for pdf in pdf_reports:
            # create the name to use for this downloaded file
            local_filename = f"{symbol_data['symbol']}_audited_statement_{pdf['release_date'].strftime('%Y-%m-%d')}.pdf"
            symbol_report_directory: Path = self.symbol_report_directories[symbol_data['symbol']]
            # check if file was already downloaded
            full_local_path = symbol_report_directory.joinpath(local_filename)
            if full_local_path.is_file():
                logger.debug(
                    f"PDF file {local_filename} was already downloaded. Skipping."
                )
            else:
                # else download this new pdf report
                # get the http response
                http_response_obj = requests.get(
                    pdf["pdf_link"], stream=True, headers=HTTP_GET_HEADERS
                )
                # save contents of response (pdf report) to the local file
                logger.debug(f"Now downloading file {local_filename}. Please wait.")
                with open(full_local_path, "wb") as local_pdf_file:
                    for chunk in http_response_obj.iter_content(chunk_size=1024):
                        # save chunks of pdf to local file
                        if chunk:
                            local_pdf_file.write(chunk)
                logger.debug("Finished download file.")

    def build_list_of_direct_pdf_reports_for_audited_statements(self, annual_statements_url, report_page_links):
        pdf_reports = []
        for link in report_page_links:
            try:
                report_page = requests.get(
                    link, timeout=WEBPAGE_LOAD_TIMEOUT_SECS, headers=HTTP_GET_HEADERS
                )
                if report_page.status_code != 200:
                    raise requests.exceptions.HTTPError(
                        "Could not load URL. " + annual_statements_url
                    )
                logger.debug("Successfully loaded webpage.")
                # and search the page for the link to the actual pdf report
                report_soup = BeautifulSoup(report_page.text, "lxml")
                a_links = report_soup.find_all("a")
                pdf_link = None
                for a_link in a_links:
                    if not pdf_link and "click here to download" in a_link.text:
                        if a_link.attrs["href"]:
                            pdf_link = a_link.attrs["href"].replace(" ", "")
                if not pdf_link:
                    raise RuntimeError(f"Could not find a link for the pdf in {link}")
                # and get the release date for this report
                pdf_release_date = None
                h2_texts = report_soup.find_all(
                    "h2", {"class": "elementor-heading-title elementor-size-default"}
                )
                for text in h2_texts:
                    try:
                        # check if any of the texts can be parsed to a dates
                        if not pdf_release_date:
                            pdf_release_date = datetime.strptime(
                                text.contents[0], "%d/%m/%Y"
                            )
                    except (ValueError, TypeError):
                        pass
                if not pdf_release_date:
                    raise RuntimeError(
                        f"Could not find a release date for this report: {link}"
                    )
                # now append our data
                pdf_reports.append(
                    {"pdf_link": pdf_link, "release_date": pdf_release_date}
                )
            except (requests.exceptions.HTTPError, RuntimeError) as exc:
                logger.warning(
                    f"Ran into an error while trying to get the download link for {link}. Skipping statement.",
                    exc_info=exc,
                )
        return pdf_reports

    def build_list_of_audited_statements_news_pages_for_symbol(self, annual_statements_page):
        annual_statements_page_soup = BeautifulSoup(annual_statements_page, "lxml")
        report_page_links = self._get_news_links_from_audited_statements_news_div(annual_statements_page_soup)
        return report_page_links


# Put your function definitions here. These should be lowercase, separated by underscores
def fetch_listed_equities_data_from_db() -> List:
    # for some reason, ttse is using symbol ids instead of actual symbols here,
    # so we need to set up a list of these ids
    listed_symbol_data = []
    with DatabaseConnect() as db_obj:
        listed_equities_table = Table(
            "listed_equities", MetaData(), autoload=True, autoload_with=db_obj.dbengine
        )
        selectstmt = select(
            [listed_equities_table.c.symbol, listed_equities_table.c.symbol_id]
        )
        result = db_obj.dbcon.execute(selectstmt)
        for row in result:
            if row[0] not in IGNORE_SYMBOLS:
                listed_symbol_data.append({"symbol": row[0], "symbol_id": row[1]})
    return listed_symbol_data


@pidfile()
def main(args):
    """The main function to coordinate the functions defined above"""
    try:
        with multiprocessing.Pool(
                os.cpu_count()
        ) as multipool:
            # now check if any reports are outstanding
            res_new_annual_reports = multipool.apply_async(
                alert_me_new_annual_reports, ()
            )
            res_new_audited_statements = multipool.apply_async(
                alert_me_new_audited_statements, ()
            )
            res_new_quarterly_statements = multipool.apply_async(
                alert_me_new_quarterly_statements, ()
            )
            # wait until the new report list is ready
            new_annual_reports = res_new_annual_reports.get()
            logger.debug(f"Annual reports processing check function complete.")
            new_audited_statements = res_new_audited_statements.get()
            logger.debug(f"Audited statements processing check function complete.")
            new_quarterly_statements = res_new_quarterly_statements.get()
            logger.debug(
                f"Quarterly statements processing check function complete."
            )
            multipool.close()
            multipool.join()
            # now send the email of outstanding reports
            _email_new_reports_to_me(logger, new_annual_reports, new_audited_statements, new_quarterly_statements)
        logger.info(os.path.basename(__file__) + " executed successfully.")
        return 0
    except Exception as exc:
        logger.exception("Error in script " + os.path.basename(__file__), exc_info=exc)


def alert_me_new_annual_reports() -> set:
    financial_reports_scraper: FinancialReportsScraper = FinancialReportsScraper()
    new_annual_reports: set = financial_reports_scraper.alert_me_new_annual_reports()
    return new_annual_reports


def alert_me_new_audited_statements() -> set:
    financial_reports_scraper: FinancialReportsScraper = FinancialReportsScraper()
    new_audited_statements: set = financial_reports_scraper.alert_me_new_audited_statements()
    return new_audited_statements


def alert_me_new_quarterly_statements() -> set:
    financial_reports_scraper: FinancialReportsScraper = FinancialReportsScraper()
    new_quarterly_statements: set = financial_reports_scraper.alert_me_new_quarterly_statements()
    return new_quarterly_statements


def _email_new_reports_to_me(logger, new_annual_reports, new_audited_statements, new_quarterly_statements):
    msg = MIMEText(
        f"""
Hello,

These are the outstanding fundamental reports that we found on the TTSE.

New Annual Reports: {new_annual_reports}

New Audited Statements: {new_audited_statements}

New Quarterly Statements: {new_quarterly_statements}

Please add the data for these reports into the raw_quarterly_data and raw_annual_data tables on the server.

Sincerely,
trinistocks.com
                """
    )
    msg["From"] = "admin@trinistocks.com"
    msg["To"] = OUTSTANDINGREPORTEMAIL
    msg["Subject"] = "trinistocks: Outstanding Fundamental Reports"
    p = Popen(
        ["/usr/sbin/sendmail", "-t", "-oi"],
        stdin=PIPE,
        universal_newlines=True,
    )
    p.communicate(msg.as_string())
    return_code = p.returncode
    if return_code == 0:
        logger.debug(
            f"Sent report to {OUTSTANDINGREPORTEMAIL} successfully."
        )
    else:
        logger.error("Could not send report to {OUTSTANDINGREPORTEMAIL}")


# If this script is being run from the command-line, then run the main() function
if __name__ == "__main__":
    # first check the arguements given to this script
    parser = argparse.ArgumentParser()
    args = parser.parse_args()
    main(args)
