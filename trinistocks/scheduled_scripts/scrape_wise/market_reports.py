import os

from scheduled_scripts.setup_django_orm import setup_django_orm

setup_django_orm()

from datetime import date, datetime, timedelta
from stocks.models import DailyStockSummary
from typing_extensions import Self
from typing import List, TypedDict
import requests
from bs4 import BeautifulSoup
from lxml import etree
from pathlib import Path
from lxml.etree import _Element
import tempfile
import logging
from dateutil.relativedelta import relativedelta

from scheduled_scripts import logging_configs
from logging.config import dictConfig

dictConfig(logging_configs.LOGGING_CONFIG)
logger = logging.getLogger(__name__)


class MarketReportLinks(TypedDict):
    date: date
    url: str


class MissingMarketReportMonthAndYear(TypedDict):
    month: str
    year: str


class MarketReportsScraper:

    def __init__(self):
        tempfile_directory: str = tempfile.gettempdir()
        self.market_reports_directory = tempfile_directory + "/wise_market_reports"
        Path(self.market_reports_directory).mkdir(parents=True, exist_ok=True)
        pass

    def __del__(self):
        pass

    def get_date_of_latest_daily_report_in_db(self: Self) -> date:
        latest_daily_stock_summary: DailyStockSummary = DailyStockSummary.objects.latest("date")
        return latest_daily_stock_summary.date

    def build_list_of_missing_market_reports_month_and_year(self: Self) -> List[MissingMarketReportMonthAndYear]:
        all_missing_market_reports_month_and_year: List[MissingMarketReportMonthAndYear] = []
        date_of_latest_report_in_db: date = self.get_date_of_latest_daily_report_in_db()
        current_date: date = datetime.now().date()
        date_counter: date = date_of_latest_report_in_db
        while date_counter.year <= current_date.year and date_counter.month <= current_date.month:
            all_missing_market_reports_month_and_year.append(
                MissingMarketReportMonthAndYear(month=str(date_counter.month), year=str(date_counter.year)))
            date_counter = date_counter + relativedelta(months=1)
        return all_missing_market_reports_month_and_year

    def scrape_all_market_report_links_for_month_and_year(self: Self, month: str, year: str) -> List[
        MarketReportLinks]:
        market_reports_webpage: requests.Response = requests.get(
            f"https://wiseequities.com/home/market-reports.php?month={month}&year={year}")
        soup: BeautifulSoup = BeautifulSoup(market_reports_webpage.content, "html.parser")
        html_dom: _Element = etree.HTML(str(soup))
        daily_market_reports_container: _Element = html_dom.xpath('/html/body/div[2]/div[3]/div[1]/div[2]/div[1]/div')
        if not len(daily_market_reports_container):
            raise RuntimeError("No daily market reports found for this month and year.")
        all_market_reports_pdf_links = daily_market_reports_container[0].findall('.//a')
        base_url: str = "https://wiseequities.com"
        all_market_report_links: List[MarketReportLinks] = []
        for pdf_link in all_market_reports_pdf_links:
            report_date: date = datetime.strptime(pdf_link.text, "%B %d, %Y").date()
            url: str = base_url + pdf_link.get("href")
            all_market_report_links.append(MarketReportLinks(date=report_date, url=url))
        return all_market_report_links

    def build_list_of_missing_market_reports_full_dates(self: Self) -> List[date]:
        all_missing_market_report_dates: List[date] = []
        date_of_latest_report_in_db: date = self.get_date_of_latest_daily_report_in_db()
        current_date: date = datetime.now().date()
        date_counter: date = date_of_latest_report_in_db + relativedelta(days=1)
        while date_counter <= current_date:
            all_missing_market_report_dates.append(
                date_counter)
            date_counter = date_counter + relativedelta(days=1)
        return all_missing_market_report_dates

    def download_all_missing_market_reports(self: Self, all_market_report_links: List[MarketReportLinks],
                                            all_missing_market_report_dates: List[date]) -> List[
        Path]:
        all_downloaded_market_reports: List[Path] = []
        for market_report_link in all_market_report_links:
            if not market_report_link['date'] in all_missing_market_report_dates:
                continue
            os.chdir(self.market_reports_directory)
            filename: str = "market_report_" + str(market_report_link['date']) + ".pdf"
            with open(filename, "wb") as file:
                # get request
                response = requests.get(market_report_link['url'])
                # write to file
                file.write(response.content)
                logger.info("Downloaded WISE market report for " + str(market_report_link['date']))
                all_downloaded_market_reports.append(Path(self.market_reports_directory).joinpath(filename))
        return all_downloaded_market_reports
