from scheduled_scripts.setup_django_orm import setup_django_orm

setup_django_orm()

from datetime import date, datetime, timedelta
from stocks.models import DailyStockSummary, HistoricalIndicesInfo, ListedEquities
from typing_extensions import Self
from typing import List, TypedDict, Tuple, Optional
import requests
from bs4 import BeautifulSoup
from lxml import etree
from pathlib import Path
from lxml.etree import _Element
import tempfile
import logging
from dateutil.relativedelta import relativedelta
import camelot
import os
from camelot.core import TableList, Table
from django.core.exceptions import ObjectDoesNotExist
from django.db.models import QuerySet
from pandas import DataFrame, Series

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

    def _get_date_of_latest_daily_report_in_db(self: Self) -> date:
        latest_daily_stock_summary: DailyStockSummary = DailyStockSummary.objects.latest("date")
        return latest_daily_stock_summary.date

    def build_list_of_missing_market_reports_month_and_year(self: Self) -> List[MissingMarketReportMonthAndYear]:
        all_missing_market_reports_month_and_year: List[MissingMarketReportMonthAndYear] = []
        date_of_latest_report_in_db: date = self._get_date_of_latest_daily_report_in_db()
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
        date_of_latest_report_in_db: date = self._get_date_of_latest_daily_report_in_db()
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

    def download_specific_market_report(self: Self, market_report_link: MarketReportLinks) -> Path:
        os.chdir(self.market_reports_directory)
        filename: str = "market_report_" + str(market_report_link['date']) + ".pdf"
        with open(filename, "wb") as file:
            # get request
            response = requests.get(market_report_link['url'])
            # write to file
            file.write(response.content)
            logger.info("Downloaded WISE market report for " + str(market_report_link['date']))
            downloaded_market_report = Path(self.market_reports_directory).joinpath(filename)
        return downloaded_market_report

    def parse_all_missing_market_report_data(self: Self, all_downloaded_market_reports: List[Path]) -> bool:
        for market_report in all_downloaded_market_reports:
            logger.info(f"Now parsing data from {market_report.name}")
            date_str: str = market_report.name.replace("market_report_", "").replace(".pdf", "")
            report_date: datetime.date = datetime.strptime(date_str, "%Y-%m-%d").date()
            market_summary_table: DataFrame = \
                camelot.read_pdf(str(market_report), pages='all', flavor="stream", edge_tol=500,
                                 table_areas=['0,800,250,700'])[0].df
            self._parse_data_from_market_summary_table(report_date, market_summary_table)
            # reduce from 250 down to 0 in case table gets longer (in table areas)
            raw_daily_trading_report_table = \
                camelot.read_pdf(str(market_report), pages='all', flavor="stream", edge_tol=5000,
                                 table_areas=['0,680,600,250'])[0]
            daily_trading_report_table: DataFrame = raw_daily_trading_report_table.df
            self._parse_data_from_daily_trading_report_table(report_date, daily_trading_report_table)
        return True

    def parse_specific_market_report_data(self: Self, downloaded_market_report: Path) -> bool:
        logger.info(f"Now parsing data from {downloaded_market_report.name}")
        date_str: str = downloaded_market_report.name.replace("market_report_", "").replace(".pdf", "")
        report_date: datetime.date = datetime.strptime(date_str, "%Y-%m-%d").date()
        try:
            market_summary_table: DataFrame = \
                camelot.read_pdf(str(downloaded_market_report), pages='all', flavor="stream", edge_tol=5000,line_scale=30,
                                 table_areas=['0,800,250,700'])[0].df
            self._parse_data_from_market_summary_table(report_date, market_summary_table)
        except ValueError:
            logger.error("Could not read market summary table. Maybe inserted as image?")
        # reduce from 250 down to 0 in case table gets longer (in table areas)
        raw_daily_trading_report_table = \
            camelot.read_pdf(str(downloaded_market_report), pages='all', flavor="stream", edge_tol=5000,
                             table_areas=['0,680,600,250'])[0]
        daily_trading_report_table: DataFrame = raw_daily_trading_report_table.df
        self._parse_data_from_daily_trading_report_table(report_date, daily_trading_report_table)
        return True

    def _parse_data_from_market_summary_table(self: Self, report_date: datetime.date, market_report_table: DataFrame):
        for index, row in market_report_table.iterrows():
            row_zero_text: str = row[0].lower()
            match row_zero_text:
                case "composite index":
                    historical_indices_info_queryset = HistoricalIndicesInfo.objects.filter(
                        date=report_date, index_name="Composite Totals")
                    if historical_indices_info_queryset.exists():
                        historical_indices_info = historical_indices_info_queryset.first()
                    else:
                        historical_indices_info = HistoricalIndicesInfo(date=report_date, index_name="Composite Totals")
                    historical_indices_info.index_value = row[1].replace(",", "")
                    historical_indices_info.save()
                case "all t&t index":
                    historical_indices_info_queryset = HistoricalIndicesInfo.objects.filter(
                        date=report_date, index_name="All T&T Totals")
                    if historical_indices_info_queryset.exists():
                        historical_indices_info = historical_indices_info_queryset.first()
                    else:
                        historical_indices_info = HistoricalIndicesInfo(date=report_date, index_name="All T&T Totals")
                    historical_indices_info.index_value = row[1].replace(",", "")
                    historical_indices_info.save()
                case "cross listed index":
                    historical_indices_info_queryset = HistoricalIndicesInfo.objects.filter(
                        date=report_date, index_name="Cross-Listed Totals")
                    if historical_indices_info_queryset.exists():
                        historical_indices_info = historical_indices_info_queryset.first()
                    else:
                        historical_indices_info = HistoricalIndicesInfo(date=report_date,
                                                                        index_name="Cross-Listed Totals")
                    historical_indices_info.index_value = row[1].replace(",", "")
                    historical_indices_info.save()
                case "small & medium enterprise index":
                    historical_indices_info_queryset = HistoricalIndicesInfo.objects.filter(
                        date=report_date, index_name="Sme Totals")
                    if historical_indices_info_queryset.exists():
                        historical_indices_info = historical_indices_info_queryset.first()
                    else:
                        historical_indices_info = HistoricalIndicesInfo(date=report_date, index_name="Sme Totals")
                    historical_indices_info.index_value = row[1].replace(",", "")
                    historical_indices_info.save()

    def _parse_data_from_daily_trading_report_table(self: Self, report_date: datetime.date,
                                                    daily_trading_report_table: DataFrame):
        for index, row in daily_trading_report_table.iterrows():
            security_name: str = row[0]
            if not security_name or security_name in ['Security', 'Banking', 'Conglomerates', 'Energy', 'Manufacturing',
                                                      'Non-Banking Finance', 'Property', 'Trading', 'Preference',
                                                      'Second Tier Market', 'Mutual Fund Market',
                                                      'Small & Medium Enterprise Market', 'USD Equity Market',
                                                      'Corporate Bond Market']:
                continue
            try:
                listed_equity: ListedEquities = ListedEquities.objects.get(wise_equity_name=security_name)
            except ObjectDoesNotExist:
                logger.debug(f"No equity found with name {security_name} in listed equity table. Skipping")
                continue
            # check if we already have daily summary data for this date and equity
            daily_stock_summary_queryset: QuerySet[DailyStockSummary] = DailyStockSummary.objects.filter(
                date=report_date, symbol=listed_equity)
            if daily_stock_summary_queryset.exists():
                daily_stock_summary: DailyStockSummary = daily_stock_summary_queryset.first()
            else:
                daily_stock_summary: DailyStockSummary = DailyStockSummary(date=report_date, symbol=listed_equity)
            open_quote, close_quote, high, low, volume_traded, value_traded, os_bid, os_bid_volume, os_offer, os_offer_volume, was_traded_today = self._parse_prices_for_symbol(
                row)
            daily_stock_summary.open_price = open_quote
            daily_stock_summary.close_price = close_quote
            daily_stock_summary.high = high
            daily_stock_summary.low = low
            daily_stock_summary.volume_traded = volume_traded
            daily_stock_summary.value_traded = value_traded
            daily_stock_summary.os_bid = os_bid
            daily_stock_summary.os_bid_vol = os_bid_volume
            daily_stock_summary.os_offer = os_offer
            daily_stock_summary.os_offer_vol = os_offer_volume
            daily_stock_summary.was_traded_today = was_traded_today
            daily_stock_summary.last_sale_price = close_quote
            daily_stock_summary.change_dollars = close_quote - open_quote
            daily_stock_summary.save()

    def _parse_prices_for_symbol(self: Self, row: Series) -> Tuple[
        float, float, float, float, int, float, float, int, float, int, bool]:
        try:
            open_quote: float = float(row[1].replace(",", ""))
        except ValueError as exc:
            open_quote: Optional[float] = None
        try:
            high: float = float(row[2].replace(",", ""))
        except ValueError as exc:
            high: Optional[float] = None
        try:
            low: float = float(row[3].replace(",", ""))
        except ValueError as exc:
            low: Optional[float] = None
        try:
            close_quote: float = float(row[4].replace(",", ""))
        except ValueError as exc:
            close_quote: Optional[float] = None
        try:
            volume_traded: int = int(row[6].replace(",", ""))
        except ValueError as exc:
            volume_traded: Optional[int] = None
        try:
            os_bid_volume: int = int(row[7].replace(",", ""))
        except ValueError as exc:
            os_bid_volume: Optional[int] = None
        try:
            os_bid: float = float(row[8].replace(",", ""))
        except ValueError as exc:
            os_bid: Optional[float] = None
        try:
            os_offer: float = float(row[9].replace(",", ""))
        except ValueError as exc:
            os_offer: Optional[float] = None
        try:
            os_offer_volume: int = int(row[10].replace(",", ""))
        except ValueError as exc:
            os_offer_volume: Optional[int] = None
        value_traded: float = float(volume_traded) * close_quote
        was_traded_today: bool = False
        if value_traded > float(0):
            was_traded_today: bool = True
        return open_quote, close_quote, high, low, volume_traded, value_traded, os_bid, os_bid_volume, os_offer, os_offer_volume, was_traded_today
