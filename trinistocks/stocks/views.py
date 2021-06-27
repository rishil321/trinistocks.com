# region IMPORTS
# Imports from standard Python lib
import logging
from datetime import datetime, timedelta

# Imports from cheese factory
from django.core.exceptions import ValidationError
import django_tables2 as tables2
from django_tables2.export.views import ExportMixin
from django_filters.views import FilterView
from django.views.generic import TemplateView
from django import forms
from django.db.models import F
from django.utils.datastructures import MultiValueDictKeyError
from urllib.parse import urlencode
from django.shortcuts import redirect
from django.urls import reverse
from django.urls import reverse_lazy
from django.views.generic.edit import FormView
from django.contrib.auth import login, logout, get_user_model, authenticate
from django.db.utils import IntegrityError
import pandas as pd
from django.core.mail import send_mail
from django.contrib.auth.mixins import LoginRequiredMixin
from django.template.loader import render_to_string
from django.utils.http import urlsafe_base64_encode
from django.contrib.auth.tokens import default_token_generator
from django.utils.encoding import force_bytes
from rest_framework import generics, permissions, views, response, status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.authtoken.views import ObtainAuthToken
from rest_framework.authtoken.models import Token


# Imports from local machine
from . import serializers
from . import models, filters
from . import tables as stocks_tables
from .templatetags import stocks_template_tags
from . import forms
from scripts.stocks.updatedb import updater

# endregion

# CONSTANTS
ALERTMESSAGE = "Sorry! An error was encountered while processing your request."
# Set up logging
LOGGER = logging.getLogger("root")

# Class definitions

# Regular Page Views


class HomePageView(ExportMixin, tables2.MultiTableMixin, FilterView):
    """
    Our homepage for trinistocks.com
    """

    template_name = "stocks/base_homepage.html"
    tables = []
    filterset_class = filters.DailyTradingSummaryFilter

    def get(self, request, *args, **kwargs):
        # get the filters included in the URL.
        # If the required filters are not present, return a redirect
        required_parameters = ["date", "was_traded_today", "sort"]
        for parameter in required_parameters:
            try:
                # check that each parameter has a value
                if self.request.GET[parameter]:
                    pass
            except MultiValueDictKeyError:
                LOGGER.warning(
                    "Homepage requested without all parameters. Sending redirect."
                )
                # if we are missing any parameters, return a redirect
                base_url = reverse("stocks:homepage", current_app="stocks")
                query_string = urlencode(
                    {
                        "date": stocks_template_tags.get_latest_date_dailytradingsummary(),
                        "was_traded_today": 1,
                        "sort": "-value_traded",
                    }
                )
                url = "{}?{}".format(base_url, query_string)
                return redirect(url)
        return super(HomePageView, self).get(request)

    def get_context_data(self, *args, **kwargs):
        try:
            errors = ""
            LOGGER.info("Home page was called")
            # get the current context
            context = super().get_context_data(*args, **kwargs)
            selected_date = datetime.strptime(self.request.GET.get("date"), "%Y-%m-%d")
            # get the date 4 weeks back for the market indexes
            trailing_30d_date = selected_date - timedelta(days=30)
            if not selected_date:
                raise RuntimeError("Could not get a valid date for this query.")
            # Now select the records corresponding to the selected date
            # as well as their symbols, and order by the highest volume traded
            daily_trading_summary_records = (
                models.DailyStockSummary.objects.filter(was_traded_today=1)
                .filter(date=selected_date)
                .select_related("symbol")
                .order_by("-value_traded")
            )
            if not daily_trading_summary_records:
                raise ValueError("No data available for the date selected.")
            # rename the symbol field properly and select only required fields
            selected_records = daily_trading_summary_records.values(
                "symbol", "value_traded"
            )
            # check if an export request was received
            # Set up the graph
            # get the top 10 records by value traded
            graph_symbols = [record["symbol"] for record in selected_records[:10]]
            graph_value_traded = [
                record["value_traded"] for record in selected_records[:10]
            ]
            # create a category for the sum of all other symbols (not in the top 10)
            others = dict(symbol="Others", value_traded=0)
            for record in selected_records:
                if (record["symbol"] not in graph_symbols) and record["value_traded"]:
                    others["value_traded"] += record["value_traded"]
            # add the 'other' category to the graph
            graph_symbols.append(others["symbol"])
            graph_value_traded.append(others["value_traded"])
            # get a human readable date
            selected_date_parsed = selected_date.strftime("%Y-%m-%d")
            # Now set up the data for the market indexes
            market_indexes_records = (
                models.HistoricalIndicesInfo.objects.filter(date__gt=trailing_30d_date)
                .filter(date__lt=selected_date)
                .order_by("date")
            )
            tnt_data = market_indexes_records.filter(index_name="All T&T Totals")
            tnt_values = [obj.index_value for obj in tnt_data]
            tnt_dates = [datetime.strftime(obj.date, "%d-%m-%Y") for obj in tnt_data]
            composite_data = market_indexes_records.filter(
                index_name="Composite Totals"
            )
            composite_dates = [
                datetime.strftime(obj.date, "%d-%m-%Y") for obj in composite_data
            ]
            composite_values = [obj.index_value for obj in composite_data]
            cross_listed_data = market_indexes_records.filter(
                index_name="Cross-Listed Totals"
            )
            cross_listed_dates = [
                datetime.strftime(obj.date, "%d-%m-%Y") for obj in cross_listed_data
            ]
            cross_listed_values = [obj.index_value for obj in cross_listed_data]
            sme_data = market_indexes_records.filter(index_name="Sme Totals")
            sme_dates = [datetime.strftime(obj.date, "%d-%m-%Y") for obj in sme_data]
            sme_values = [obj.index_value for obj in sme_data]
            # set up the news data
            stock_news_records = models.StockNewsData.objects.select_related(
                "symbol"
            ).order_by("-date")[:10]
            # Now add our context data and return a response
            context["errors"] = errors
            context["selected_date"] = selected_date.date()
            context["selected_date_parsed"] = selected_date_parsed
            context["graph_symbols"] = graph_symbols
            context["graph_value_traded"] = graph_value_traded
            context["tnt_dates"] = tnt_dates
            context["tnt_values"] = tnt_values
            context["composite_dates"] = composite_dates
            context["composite_values"] = composite_values
            context["cross_listed_dates"] = cross_listed_dates
            context["cross_listed_values"] = cross_listed_values
            context["sme_dates"] = sme_dates
            context["sme_values"] = sme_values
            context["daily_traded_table"] = stocks_tables.DailyTradingSummaryTable(
                daily_trading_summary_records
            )
            context["stock_news_table"] = stocks_tables.StockNewsTable(
                stock_news_records
            )
            LOGGER.info("Successfully loaded page.")
        except ValueError as verr:
            context["errors"] = ALERTMESSAGE + str(verr)
            LOGGER.warning("Got a value error while loading this page" + str(verr))
        except Exception as ex:
            LOGGER.exception(
                "Sorry. Ran into a problem while attempting to load the page: "
                + self.template_name
            )
            context["errors"] = ALERTMESSAGE + str(ex)
        return context


class DailyTradingSummaryView(ExportMixin, tables2.views.SingleTableMixin, FilterView):
    """
    Set up the data for the Daily Equity Summary page
    """

    template_name = "stocks/base_dailytradingsummary.html"
    model = models.DailyStockSummary
    table_class = stocks_tables.DailyTradingSummaryTable
    filterset_class = filters.DailyTradingSummaryFilter

    def get(self, request, *args, **kwargs):
        # get the filters included in the URL.
        # If the required filters are not present, return a redirect
        required_parameters = ["date", "was_traded_today", "sort"]
        for parameter in required_parameters:
            try:
                # check that each parameter has a value
                if self.request.GET[parameter]:
                    pass
            except MultiValueDictKeyError:
                LOGGER.warning(
                    "Daily trading summary page requested without all parameters. Sending redirect."
                )
                # if we are missing any parameters, return a redirect
                base_url = reverse("stocks:dailytradingsummary", current_app="stocks")
                query_string = urlencode(
                    {
                        "date": stocks_template_tags.get_latest_date_dailytradingsummary(),
                        "was_traded_today": 1,
                        "sort": "-value_traded",
                    }
                )
                url = "{}?{}".format(base_url, query_string)
                return redirect(url)
        return super(DailyTradingSummaryView, self).get(request)

    def get_context_data(self, *args, **kwargs):
        try:
            errors = ""
            LOGGER.info("Daily trading summary page was called")
            # get the current context
            context = super().get_context_data(*args, **kwargs)
            selected_date = datetime.strptime(self.request.GET.get("date"), "%Y-%m-%d")
            # Now select the records corresponding to the selected date
            # as well as their symbols, and order by the highest volume traded
            daily_trading_summary_records = (
                models.DailyStockSummary.objects.exclude(was_traded_today=0)
                .filter(date=selected_date)
                .select_related("symbol")
                .order_by("-value_traded")
            )
            if not daily_trading_summary_records:
                raise ValueError(
                    "No data available for the date selected. Please press the back button and choose another date."
                )
            # rename the symbol field properly and select only required fields
            selected_records = daily_trading_summary_records.values(
                "symbol", "value_traded"
            )
            # check if an export request was received
            # Set up the graph
            # get the top 10 records by value traded
            graph_symbols = [record["symbol"] for record in selected_records[:10]]
            graph_value_traded = [
                record["value_traded"] for record in selected_records[:10]
            ]
            # create a category for the sum of all other symbols (not in the top 10)
            others = dict(symbol="Others", value_traded=0)
            for record in selected_records:
                if (record["symbol"] not in graph_symbols) and record["value_traded"]:
                    others["value_traded"] += record["value_traded"]
            # add the 'other' category to the graph
            graph_symbols.append(others["symbol"])
            graph_value_traded.append(others["value_traded"])
            # get a human readable date
            selected_date_parsed = selected_date.strftime("%Y-%m-%d")
            # Now add our context data and return a response
            context["errors"] = errors
            context["selected_date"] = selected_date.date()
            context["selected_date_parsed"] = selected_date_parsed
            context["graph_symbols"] = graph_symbols
            context["graph_value_traded"] = graph_value_traded
            LOGGER.info("Successfully loaded page.")
        except ValueError as verr:
            context["errors"] = ALERTMESSAGE + str(verr)
            LOGGER.warning("Got a value error while loading this page" + str(verr))
        except Exception as ex:
            LOGGER.exception(
                "Sorry. Ran into a problem while attempting to load the page: "
                + self.template_name
            )
            context["errors"] = ALERTMESSAGE + str(ex)
        return context


class ListedStocksView(ExportMixin, tables2.MultiTableMixin, FilterView):
    """
    Set up the data for the Listed Stocks page
    """

    template_name = "stocks/base_listedstocks.html"
    model1 = models.ListedEquities
    qs1 = model1.objects.all()
    model2 = models.ListedEquitiesPerSector
    qs2 = model2.objects.all()
    tables = [
        stocks_tables.ListedStocksTable(qs1),
        stocks_tables.ListedStocksPerSectorTable(qs2),
    ]
    table_pagination = False
    filterset_class = filters.ListedStocksFilter

    def get(self, request, *args, **kwargs):
        # get the filters included in the URL.
        # If the required filters are not present, return a redirect
        required_parameters = ["table_0-sort", "table_1-sort"]
        for parameter in required_parameters:
            try:
                # check that each parameter has a value
                if self.request.GET[parameter]:
                    pass
            except MultiValueDictKeyError:
                LOGGER.warning(
                    "Listed stocks page requested without all parameters. Sending redirect."
                )
                # if we are missing any parameters, return a redirect
                base_url = reverse("stocks:listedstocks", current_app="stocks")
                query_string = urlencode(
                    {"table_0-sort": "symbol", "table_1-sort": "-num_listed"}
                )
                url = "{}?{}".format(base_url, query_string)
                return redirect(url)
        return super(ListedStocksView, self).get(request)

    def get_context_data(self, *args, **kwargs):
        try:
            errors = ""
            LOGGER.info("Listed stocks page was called")
            # get the current context
            context = super().get_context_data(*args, **kwargs)
            LOGGER.info("Successfully loaded page.")
        except ValueError as verr:
            context["errors"] = ALERTMESSAGE + str(verr)
            LOGGER.warning("Got a valueerror while loading this page" + str(verr))
        except Exception as ex:
            LOGGER.exception(
                "Sorry. Ran into a problem while attempting to load the page: "
                + self.template_name
            )
            context["errors"] = ALERTMESSAGE + str(ex)
        return context


class TechnicalAnalysisSummary(ExportMixin, tables2.views.SingleTableMixin, FilterView):
    """
    Set up the data for the technical analysis summary page
    """

    template_name = "stocks/base_technicalanalysissummary.html"
    model = models.TechnicalAnalysisSummary
    table_class = stocks_tables.TechnicalAnalysisSummaryTable
    table_pagination = False
    filterset_class = filters.TechnicalAnalysisSummaryFilter

    def get_context_data(self, *args, **kwargs):
        try:
            errors = ""
            LOGGER.info("Technical Analysis Summary Page was called")
            # get the current context
            context = super().get_context_data(*args, **kwargs)
            LOGGER.info("Successfully loaded page.")
        except ValueError as verr:
            context["errors"] = ALERTMESSAGE + str(verr)
            LOGGER.warning("Got a valueerror while loading this page" + str(verr))
        except Exception as ex:
            LOGGER.exception(
                "Sorry. Ran into a problem while attempting to load the page: "
                + self.template_name
            )
            context["errors"] = ALERTMESSAGE + str(ex)
        return context


class FundamentalAnalysisSummary(
    ExportMixin, tables2.views.MultiTableMixin, TemplateView
):
    """
    Set up the data for the technical analysis summary page
    """

    template_name = "stocks/base_fundamentalanalysissummary.html"
    model = models.FundamentalAnalysisSummary
    qs1 = model.objects.raw(
        """
        SELECT * 
        FROM calculated_fundamental_ratios WHERE (symbol,date) IN (
        SELECT symbol, MAX(date)
        FROM calculated_fundamental_ratios
        WHERE report_type='annual'
        GROUP BY symbol
        )
        AND report_type='annual'
        """
    )
    qs2 = model.objects.raw(
        """
        SELECT * 
        FROM calculated_fundamental_ratios WHERE (symbol,date) IN (
        SELECT symbol, MAX(date)
        FROM calculated_fundamental_ratios
        WHERE report_type='quarterly'
        GROUP BY symbol
        )
        AND report_type='quarterly'
        """
    )
    tables = [
        stocks_tables.FundamentalAnalysisSummaryTable(qs1),
        stocks_tables.FundamentalAnalysisSummaryTable(qs2),
    ]
    table_pagination = False

    def get(self, request, *args, **kwargs):
        # get the filters included in the URL.
        # If the required filters are not present, return a redirect
        required_parameters = [
            "sort",
        ]
        for parameter in required_parameters:
            try:
                # check that each parameter has a value
                if self.request.GET[parameter]:
                    pass
            except MultiValueDictKeyError:
                LOGGER.warning(
                    "Fundamental analysis page requested without all parameters. Sending redirect."
                )
                # if we are missing any parameters, return a redirect
                base_url = reverse("stocks:fundamentalanalysis", current_app="stocks")
                query_string = urlencode({"sort": "symbol"})
                url = "{}?{}".format(base_url, query_string)
                return redirect(url)
        return super(FundamentalAnalysisSummary, self).get(request)

    def get_context_data(self, *args, **kwargs):
        try:
            LOGGER.info("Fundamental Analysis Summary Page was called")
            # get the current context
            context = super().get_context_data(*args, **kwargs)
            LOGGER.info("Successfully loaded page.")
        except Exception as ex:
            LOGGER.exception(
                "Sorry. Ran into a problem while attempting to load the page: "
                + self.template_name
            )
            context["errors"] = ALERTMESSAGE + str(ex)
        return context


class StockHistoryView(ExportMixin, tables2.views.SingleTableMixin, FilterView):
    """
    The class for displaying the stock history view website
    """

    template_name = "base_stockhistory.html"
    model = models.DailyStockSummary  # models.something
    table_class = stocks_tables.HistoricalStockInfoTable  # tables.something
    filterset_class = filters.StockHistoryFilter  # filters.something
    page_name = "Stock History"  # a string representing the name of the page
    selected_chart_type = "candlestick"
    request = None

    def get(self, request, *args, **kwargs):
        LOGGER.debug(f"GET request submitted for: {request.build_absolute_uri()}")
        # get the filters included in the URL.
        # If the required filters are not present, return a redirect
        required_parameters = ["symbol", "date__gte", "date__lte", "chart_type", "sort"]
        for parameter in required_parameters:
            try:
                # check that each parameter has a value
                if self.request.GET[parameter]:
                    pass
                # check that the sort parameter is valid
                if self.request.GET["sort"].replace("-", "") not in [
                    "date",
                    "open_price",
                    "close_price",
                    "high",
                    "low",
                    "volume_traded",
                    "change_price",
                ]:
                    raise MultiValueDictKeyError(
                        "Incorrect value submitted for sorting."
                    )
            except MultiValueDictKeyError:
                LOGGER.warning(
                    "Stock history page requested without all proper parameters. Sending redirect."
                )
                # if we are missing any parameters, return a redirect
                base_url = reverse("stocks:stockhistory", current_app="stocks")
                query_string = urlencode(
                    {
                        "symbol": stocks_template_tags.get_session_symbol_or_default(
                            self
                        ),
                        "date__gte": stocks_template_tags.get_1_yr_back(),
                        "date__lte": stocks_template_tags.get_today(),
                        "chart_type": "candlestick",
                        "sort": "-date",
                    }
                )
                url = "{}?{}".format(base_url, query_string)
                return redirect(url)
        return super(StockHistoryView, self).get(request)

    def get_context_data(self, *args, **kwargs):
        # get the current context
        context = super().get_context_data(*args, **kwargs)
        try:
            listed_stocks = models.ListedEquities.objects.all().order_by("symbol")
            # check the chart type selected if the configure button was clicked
            self.selected_chart_type = self.request.GET.get("chart_type")
            # store the session variable
            self.request.session["chart_type"] = self.selected_chart_type
            if "symbol" in self.request.GET:
                self.selected_symbol = self.request.GET.get("symbol")
                self.request.session["selected_symbol"] = self.selected_symbol
            if self.request.GET.get("configure_button"):
                self.entered_start_date = datetime.strptime(
                    self.request.GET.get("date__gte"), "%Y-%m-%d"
                )
                # store the date as a session variable to be reused
                self.request.session[
                    "entered_start_date"
                ] = self.entered_start_date.strftime("%Y-%m-%d")
            # else look for the starting date in the GET variables
            elif self.request.GET.get("date__gte"):
                self.entered_start_date = datetime.strptime(
                    self.request.GET.get("date__gte"), "%Y-%m-%d"
                )
                self.request.session[
                    "entered_start_date"
                ] = self.entered_start_date.strftime("%Y-%m-%d")
            else:
                # else raise an error
                raise ValueError(
                    " Please ensure that you have included a starting date in the URL! For example: ?date__gte=2019-05-12"
                )
            # else look for the ending date in the GET variables
            if "date__lte" in self.request.GET:
                self.entered_end_date = datetime.strptime(
                    self.request.GET.get("date__lte"), "%Y-%m-%d"
                )
            else:
                raise ValueError(
                    " Please ensure that you have included an ending date in the URL! For example: ?date__lte=2020-05-12"
                )
            # check if the configuration button was clicked
            if "sort" in self.request.GET:
                self.order_by = self.request.GET.get("sort")
            else:
                raise ValueError(
                    " Please ensure that you have included a sort order in the URL! For example: ?sort=date"
                )
            self.selected_stock = models.ListedEquities.objects.get(
                symbol=self.selected_symbol
            )
            # store the context variable
            context["chart_type"] = self.selected_chart_type
            context["listed_stocks"] = listed_stocks
            context["selected_symbol"] = self.selected_symbol
            context["selected_stock_name"] = self.selected_stock.security_name
            context["selected_stock_symbol"] = self.selected_stock.symbol
            context["entered_start_date"] = self.entered_start_date.strftime("%Y-%m-%d")
            context["entered_end_date"] = self.entered_end_date.strftime("%Y-%m-%d")
            context["chart_dates"] = []
            context["open_prices"] = []
            context["close_prices"] = []
            context["highs"] = []
            context["lows"] = []
            if self.selected_chart_type == "line":
                self.historical_records = (
                    self.model.objects.filter(symbol=self.selected_symbol)
                    .filter(date__gte=self.entered_start_date)
                    .filter(date__lte=self.entered_end_date)
                    .order_by(self.order_by)
                )
                context["graph_labels"] = [obj.date for obj in self.historical_records]
                context["graph_dataset_1"] = [
                    float(obj.close_price) for obj in self.historical_records
                ]
            elif self.selected_chart_type == "candlestick":
                # query and filter the records from the db
                selected_records = (
                    models.DailyStockSummary.objects.filter(symbol=self.selected_symbol)
                    .filter(date__gte=self.entered_start_date)
                    .filter(date__lte=self.entered_end_date)
                    .order_by(self.order_by)
                )
                # store the required values for the chart
                context["chart_dates"] = [
                    d.strftime("%Y-%m-%d")
                    for d in selected_records.values_list("date", flat=True)
                ]
                context["open_prices"] = [
                    float(num)
                    for num in selected_records.values_list("open_price", flat=True)
                ]
                context["close_prices"] = [
                    float(num)
                    for num in selected_records.values_list("close_price", flat=True)
                ]
                context["lows"] = [
                    float(num) for num in selected_records.values_list("low", flat=True)
                ]
                context["highs"] = [
                    float(num)
                    for num in selected_records.values_list("high", flat=True)
                ]
        except ValueError as verr:
            context["errors"] = ALERTMESSAGE + str(verr)
            LOGGER.warning("Got a value error while loading this page: " + str(verr))
        except Exception as ex:
            LOGGER.exception(
                "Sorry. Ran into a problem while attempting to load the page: "
                + self.template_name
            )
            context["errors"] = ALERTMESSAGE + str(ex)
        return context


class DividendHistoryView(FilterView):
    """
    The class for displaying the dividend history view website
    """

    template_name = "base_dividendhistory.html"
    filterset_class = filters.DividendHistoryFilter  # filters.something
    page_name = "Dividend History"  # a string representing the name of the page

    def get(self, request, *args, **kwargs):
        # get the filters included in the URL.
        # If the required filters are not present, return a redirect
        required_parameters = ["symbol", "record_date__gte", "record_date__lte", "sort"]
        for parameter in required_parameters:
            try:
                # check that each parameter has a value
                if self.request.GET[parameter]:
                    pass
                logging.debug(
                    "All required parameters included in dividend history URL."
                )
            except MultiValueDictKeyError:
                LOGGER.warning(
                    "Dividend history page requested without all parameters. Sending redirect."
                )
                # if we are missing any parameters, return a redirect
                base_url = reverse("stocks:dividendhistory", current_app="stocks")
                query_string = urlencode(
                    {
                        "symbol": stocks_template_tags.get_session_symbol_or_default(
                            self
                        ),
                        "record_date__gte": stocks_template_tags.get_5_yr_back(),
                        "record_date__lte": stocks_template_tags.get_today(),
                        "sort": "-record_date",
                    }
                )
                url = "{}?{}".format(base_url, query_string)
                return redirect(url)
        return super(DividendHistoryView, self).get(request)

    def get_context_data(self, *args, **kwargs):
        context = super().get_context_data(*args, **kwargs)
        try:
            logging.debug("Now loading context data.")
            LOGGER.debug("Now loading all listed equities.")
            listed_stocks = models.ListedEquities.objects.all().order_by("symbol")
            # else look for the starting date in the GET variables
            if "record_date__gte" in self.request.GET:
                self.entered_start_date = datetime.strptime(
                    self.request.GET.get("record_date__gte"), "%Y-%m-%d"
                )
                self.request.session[
                    "entered_start_date"
                ] = self.entered_start_date.strftime("%Y-%m-%d")
            else:
                # else raise an error
                raise ValueError(
                    " Please ensure that you have included a starting date in the URL! For example: ?record_date__gte=2019-05-12"
                )
            # else look for the ending date in the GET variables
            if "record_date__lte" in self.request.GET:
                self.entered_end_date = datetime.strptime(
                    self.request.GET.get("record_date__lte"), "%Y-%m-%d"
                )
            else:
                raise ValueError(
                    " Please ensure that you have included an ending date in the URL! For example: ?record_date__lte=2020-05-12"
                )
            if "symbol" in self.request.GET:
                self.selected_symbol = self.request.GET.get("symbol")
                self.request.session["selected_symbol"] = self.selected_symbol
            else:
                raise ValueError(
                    " Please ensure that you have included a symbol in the URL! For example: ?symbol=ACL"
                )
            if "sort" in self.request.GET:
                self.order_by = self.request.GET.get("sort")
            else:
                self.order_by = "-record_date"
            # validate input data
            if self.entered_start_date >= self.entered_end_date:
                raise ValueError(
                    "Your starting date must be before your ending date. Please recheck."
                )
            # Fetch the records
            self.selected_stock = models.ListedEquities.objects.get(
                symbol=self.selected_symbol
            )
            self.historical_dividends_paid = (
                models.HistoricalDividendInfo.objects.filter(
                    symbol=self.selected_symbol
                )
                .filter(record_date__gte=self.entered_start_date)
                .filter(record_date__lte=self.entered_end_date)
                .order_by(self.order_by)
            )
            self.historical_dividend_yields = (
                models.HistoricalDividendYield.objects.filter(
                    symbol=self.selected_symbol
                )
                .filter(date__gte=self.entered_start_date)
                .filter(date__lte=self.entered_end_date)
                .order_by("-date")
            )
            # Set up our graph
            self.graph_labels_1 = [
                obj.record_date for obj in self.historical_dividends_paid
            ]
            self.graph_dataset_1 = [
                float(obj.dividend_amount) for obj in self.historical_dividends_paid
            ]
            self.graph_labels_2 = [obj.date for obj in self.historical_dividend_yields]
            self.graph_dataset_2 = [
                float(obj.dividend_yield) for obj in self.historical_dividend_yields
            ]
            # add the context keys
            LOGGER.debug("Loading context keys.")
            context["listed_stocks"] = listed_stocks
            context["selected_symbol"] = self.selected_symbol
            context["selected_stock_name"] = self.selected_stock.security_name
            context["selected_stock_symbol"] = self.selected_stock.symbol
            context["entered_start_date"] = self.entered_start_date.strftime("%Y-%m-%d")
            context["entered_end_date"] = self.entered_end_date.strftime("%Y-%m-%d")
            context["graph_labels_1"] = self.graph_labels_1
            context["graph_dataset_1"] = self.graph_dataset_1
            context["graph_labels_2"] = self.graph_labels_2
            context["graph_dataset_2"] = self.graph_dataset_2
            LOGGER.info("Successfully loaded page.")
        except ValueError as verr:
            context["errors"] = ALERTMESSAGE + str(verr)
            LOGGER.warning("Got a value error while loading this page" + str(verr))
        except Exception as ex:
            LOGGER.exception(
                "Sorry. Ran into a problem while attempting to load the page: "
                + self.template_name
            )
            context["errors"] = ALERTMESSAGE + str(ex)
        return context


class MarketIndexHistoryView(ExportMixin, tables2.views.SingleTableMixin, FilterView):
    """
    Set up the data for the market indices history page
    """

    template_name = "stocks/base_marketindexhistory.html"
    model = models.HistoricalIndicesInfo
    table_class = stocks_tables.HistoricalIndicesSummaryTable
    filterset_class = filters.MarketIndexHistoryFilter

    def __init__(self):
        super(MarketIndexHistoryView, self).__init__()
        self.symbol_needed = False
        self.index_name_needed = True

    def get(self, request, *args, **kwargs):
        # get the filters included in the URL.
        # If the required filters are not present, return a redirect
        required_parameters = [
            "index_name",
            "index_parameter",
            "date__gte",
            "date__lte",
            "sort",
        ]
        for parameter in required_parameters:
            try:
                # check that each parameter has a value
                if self.request.GET[parameter]:
                    pass
            except MultiValueDictKeyError:
                LOGGER.warning(
                    "Market history page requested without all parameters. Sending redirect."
                )
                # if we are missing any parameters, return a redirect
                base_url = reverse("stocks:marketindexhistory", current_app="stocks")
                query_string = urlencode(
                    {
                        "index_name": "Composite Totals",
                        "index_parameter": "index_value",
                        "date__gte": stocks_template_tags.get_1_yr_back(),
                        "date__lte": stocks_template_tags.get_today(),
                        "sort": "-date",
                    }
                )
                url = "{}?{}".format(base_url, query_string)
                return redirect(url)
        return super(MarketIndexHistoryView, self).get(request)

    def get_context_data(self, *args, **kwargs):
        try:
            errors = ""
            # get the current context
            context = super().get_context_data(*args, **kwargs)
            LOGGER.debug("Now loading all listed equities.")
            listed_stocks = models.ListedEquities.objects.all().order_by("symbol")
            # now load all the data for the subclasses (pages)
            # note that different pages require different data, so we check which data is needed for the page
            # check if the configuration button was clicked
            LOGGER.debug("Checking which GET parameters were included in the request.")
            if self.request.GET.get("configure_button"):
                entered_start_date = datetime.strptime(
                    self.request.GET.get("date__gte"), "%Y-%m-%d"
                )
                # store the date as a session variable to be reused
                self.request.session[
                    "entered_start_date"
                ] = entered_start_date.strftime("%Y-%m-%d")
                self.entered_start_date = entered_start_date
            # else look for the starting date in the GET variables
            elif self.request.GET.get("date__gte"):
                entered_start_date = datetime.strptime(
                    self.request.GET.get("date__gte"), "%Y-%m-%d"
                )
                self.request.session[
                    "entered_start_date"
                ] = entered_start_date.strftime("%Y-%m-%d")
                self.entered_start_date = entered_start_date
            else:
                # else raise an error
                raise ValueError(
                    " Please ensure that you have included a starting date in the URL! For example: ?date__gte=2019-05-12"
                )
            # check if the configuration button was clicked
            if self.request.GET.get("configure_button"):
                entered_end_date = datetime.strptime(
                    self.request.GET.get("date__lte"), "%Y-%m-%d"
                )
                self.request.session["entered_end_date"] = entered_end_date.strftime(
                    "%Y-%m-%d"
                )
                self.entered_end_date = entered_end_date
            # else look for the ending date in the GET variables
            elif self.request.GET.get("date__lte"):
                entered_end_date = datetime.strptime(
                    self.request.GET.get("date__lte"), "%Y-%m-%d"
                )
                self.request.session["entered_end_date"] = entered_end_date.strftime(
                    "%Y-%m-%d"
                )
                self.entered_end_date = entered_end_date
            else:
                raise ValueError(
                    " Please ensure that you have included an ending date in the URL! For example: ?date__lte=2020-05-12"
                )
            # check if the configuration button was clicked
            if self.request.GET.get("sort"):
                self.order_by = self.request.GET.get("sort")
            else:
                raise ValueError(
                    " Please ensure that you have included a sort order in the URL! For example: ?sort=date"
                )
            if self.index_name_needed:
                if self.request.GET.get("index_name"):
                    self.index_name = self.request.GET.get("index_name")
                else:
                    raise ValueError(
                        "Please ensure that you have an index_name included in your URL! eg. &index_name=Composite Totals"
                    )
                if self.request.GET.get("index_parameter"):
                    self.index_parameter = self.request.GET.get("index_parameter")
                    self.index_parameter_string = (
                        models.HistoricalIndicesInfo._meta.get_field(
                            self.index_parameter
                        ).verbose_name
                    )
                else:
                    raise ValueError(
                        "Please ensure that you have an index_parameter included in your URL! eg. &index_parameter=index_value"
                    )
            # validate input data
            if entered_start_date >= entered_end_date:
                errors += "Your starting date must be before your ending date. Please recheck."
            # Fetch the records
            if self.index_name_needed:
                self.historical_records = (
                    self.model.objects.filter(date__gt=self.entered_start_date)
                    .filter(date__lte=self.entered_end_date)
                    .filter(index_name=self.index_name)
                    .order_by(self.order_by)
                )
            else:
                self.historical_records = (
                    self.model.objects.filter(date__gt=self.entered_start_date)
                    .filter(date__lte=self.entered_end_date)
                    .order_by(self.order_by)
                )
            LOGGER.debug("Finished parsing GET parameters. Now loading graph data.")
            # Set up our graph
            graph_labels = [obj.date for obj in self.historical_records]
            # Store the variables for the subclasses to calculate the required dict
            self.set_graph_dataset()
            # add the context keys
            LOGGER.debug("Loading context keys.")
            context["errors"] = errors
            context["listed_stocks"] = listed_stocks
            if self.index_name_needed:
                context["index_parameter"] = self.index_parameter
                context["index_parameter_string"] = self.index_parameter_string
                context["index_name"] = self.index_name
            context["entered_start_date"] = entered_start_date.strftime("%Y-%m-%d")
            context["entered_end_date"] = entered_end_date.strftime("%Y-%m-%d")
            context["graph_labels"] = graph_labels
            context["graph_dataset"] = self.graph_dataset
            LOGGER.info("Successfully loaded page.")
        except ValueError as verr:
            context["errors"] = ALERTMESSAGE + str(verr)
            LOGGER.warning("Got a value error while loading this page" + str(verr))
        except Exception as ex:
            LOGGER.exception(
                "Sorry. Ran into a problem while attempting to load the page: "
                + self.template_name
            )
            context["errors"] = ALERTMESSAGE + str(ex)
        return context

    def set_graph_dataset(
        self,
    ):
        self.graph_dataset = [
            float(obj[self.index_parameter]) for obj in self.historical_records.values()
        ]


class OSTradesHistoryView(ExportMixin, tables2.views.SingleTableMixin, FilterView):
    """
    Set up the data for the outstanding trades history page
    """

    template_name = "stocks/base_ostradeshistory.html"
    model = models.DailyStockSummary
    table_class = stocks_tables.OSTradesHistoryTable
    filterset_class = filters.OSTradesHistoryFilter

    def __init__(self):
        super(OSTradesHistoryView, self).__init__()
        self.symbol_needed = True
        self.index_name_needed = False
        self.os_parameter_needed = True

    def get(self, request, *args, **kwargs):
        # get the filters included in the URL.
        # If the required filters are not present, return a redirect
        required_parameters = [
            "symbol",
            "date__gte",
            "date__lte",
            "os_parameter",
            "sort",
        ]
        for parameter in required_parameters:
            try:
                # check that each parameter has a value
                if self.request.GET[parameter]:
                    pass
            except MultiValueDictKeyError:
                LOGGER.warning(
                    "Outstanding history page requested without all parameters. Sending redirect."
                )
                # if we are missing any parameters, return a redirect
                base_url = reverse("stocks:ostradeshistory", current_app="stocks")
                query_string = urlencode(
                    {
                        "symbol": stocks_template_tags.get_session_symbol_or_default(
                            self
                        ),
                        "date__gte": stocks_template_tags.get_1_yr_back(),
                        "date__lte": stocks_template_tags.get_today(),
                        "os_parameter": "os_offer_vol",
                        "sort": "-date",
                    }
                )
                url = "{}?{}".format(base_url, query_string)
                return redirect(url)
        return super(OSTradesHistoryView, self).get(request)

    def get_context_data(self, *args, **kwargs):
        try:
            errors = ""
            # get the current context
            context = super().get_context_data(*args, **kwargs)
            LOGGER.debug("Now loading all listed equities.")
            listed_stocks = models.ListedEquities.objects.all().order_by("symbol")
            # now load all the data for the subclasses (pages)
            # note that different pages require different data, so we check which data is needed for the page
            # check if the configuration button was clicked
            LOGGER.debug("Checking which GET parameters were included in the request.")
            if self.request.GET.get("configure_button"):
                entered_start_date = datetime.strptime(
                    self.request.GET.get("date__gte"), "%Y-%m-%d"
                )
                # store the date as a session variable to be reused
                self.request.session[
                    "entered_start_date"
                ] = entered_start_date.strftime("%Y-%m-%d")
                self.entered_start_date = entered_start_date
            # else look for the starting date in the GET variables
            elif self.request.GET.get("date__gte"):
                entered_start_date = datetime.strptime(
                    self.request.GET.get("date__gte"), "%Y-%m-%d"
                )
                self.request.session[
                    "entered_start_date"
                ] = entered_start_date.strftime("%Y-%m-%d")
                self.entered_start_date = entered_start_date
            else:
                # else raise an error
                raise ValueError(
                    " Please ensure that you have included a starting date in the URL! For example: ?date__gte=2019-05-12"
                )
            # check if the configuration button was clicked
            if self.request.GET.get("configure_button"):
                entered_end_date = datetime.strptime(
                    self.request.GET.get("date__lte"), "%Y-%m-%d"
                )
                self.request.session["entered_end_date"] = entered_end_date.strftime(
                    "%Y-%m-%d"
                )
                self.entered_end_date = entered_end_date
            # else look for the ending date in the GET variables
            elif self.request.GET.get("date__lte"):
                entered_end_date = datetime.strptime(
                    self.request.GET.get("date__lte"), "%Y-%m-%d"
                )
                self.request.session["entered_end_date"] = entered_end_date.strftime(
                    "%Y-%m-%d"
                )
                self.entered_end_date = entered_end_date
            else:
                raise ValueError(
                    " Please ensure that you have included an ending date in the URL! For example: ?date__lte=2020-05-12"
                )
            # check if the configuration button was clicked
            if self.symbol_needed:
                if self.request.GET.get("configure_button"):
                    selected_symbol = self.request.GET.get("symbol")
                    self.selected_symbol = selected_symbol
                    self.request.session["selected_symbol"] = selected_symbol
                # else look for the stock code in the GET variables
                elif self.request.GET.get("symbol"):
                    selected_symbol = self.request.GET.get("symbol")
                    self.selected_symbol = selected_symbol
                    self.request.session["selected_symbol"] = selected_symbol
                else:
                    raise ValueError(
                        " Please ensure that you have included a symbol in the URL! For example: ?symbol=ACL"
                    )
            if self.request.GET.get("sort"):
                self.order_by = self.request.GET.get("sort")
            else:
                raise ValueError(
                    " Please ensure that you have included a sort order in the URL! For example: ?sort=date"
                )
            if self.os_parameter_needed:
                if self.request.GET.get("os_parameter"):
                    self.os_parameter = self.request.GET.get("os_parameter")
                    self.os_parameter_string = models.DailyStockSummary._meta.get_field(
                        self.os_parameter
                    ).verbose_name
            if self.index_name_needed:
                if self.request.GET.get("index_name"):
                    self.index_name = self.request.GET.get("index_name")
                else:
                    raise ValueError(
                        "Please ensure that you have an index_name included in your URL! eg. &index_name=Composite Totals"
                    )
                if self.request.GET.get("index_parameter"):
                    self.index_parameter = self.request.GET.get("index_parameter")
                    self.index_parameter_string = (
                        models.HistoricalIndicesInfo._meta.get_field(
                            self.index_parameter
                        ).verbose_name
                    )
                else:
                    raise ValueError(
                        "Please ensure that you have an index_parameter included in your URL! eg. &index_parameter=index_value"
                    )
            # validate input data
            if entered_start_date >= entered_end_date:
                errors += "Your starting date must be before your ending date. Please recheck."
            # Fetch the records
            if self.symbol_needed:
                self.selected_stock = models.ListedEquities.objects.get(
                    symbol=self.selected_symbol
                )
                self.historical_records = (
                    self.model.objects.filter(symbol=self.selected_symbol)
                    .filter(date__gte=self.entered_start_date)
                    .filter(date__lte=self.entered_end_date)
                    .order_by(self.order_by)
                )
            elif self.index_name_needed:
                self.historical_records = (
                    self.model.objects.filter(date__gt=self.entered_start_date)
                    .filter(date__lte=self.entered_end_date)
                    .filter(index_name=self.index_name)
                    .order_by(self.order_by)
                )
            else:
                self.historical_records = (
                    self.model.objects.filter(date__gt=self.entered_start_date)
                    .filter(date__lte=self.entered_end_date)
                    .order_by(self.order_by)
                )
            LOGGER.debug("Finished parsing GET parameters. Now loading graph data.")
            # Set up our graph
            graph_labels = [obj.date for obj in self.historical_records]
            # Store the variables for the subclasses to calculate the required dict
            self.set_graph_dataset()
            # add the context keys
            LOGGER.debug("Loading context keys.")
            context["errors"] = errors
            context["listed_stocks"] = listed_stocks
            if self.symbol_needed:
                context["selected_symbol"] = selected_symbol
                context["selected_stock_name"] = self.selected_stock.security_name
                context["selected_stock_symbol"] = self.selected_stock.symbol
            if self.index_name_needed:
                context["index_parameter"] = self.index_parameter
                context["index_parameter_string"] = self.index_parameter_string
                context["index_name"] = self.index_name
            if self.os_parameter_needed:
                context["os_parameter"] = self.os_parameter
                context["os_parameter_string"] = self.os_parameter_string
            context["entered_start_date"] = entered_start_date.strftime("%Y-%m-%d")
            context["entered_end_date"] = entered_end_date.strftime("%Y-%m-%d")
            context["graph_labels"] = graph_labels
            context["graph_dataset"] = self.graph_dataset
            LOGGER.info("Successfully loaded page.")
        except ValueError as verr:
            context["errors"] = ALERTMESSAGE + str(verr)
            LOGGER.warning("Got a value error while loading this page" + str(verr))
        except Exception as ex:
            LOGGER.exception(
                "Sorry. Ran into a problem while attempting to load the page: "
                + self.template_name
            )
            context["errors"] = ALERTMESSAGE + str(ex)
        return context

    def set_graph_dataset(
        self,
    ):
        self.graph_dataset = [
            obj[self.os_parameter] for obj in self.historical_records.values()
        ]


class FundamentalHistoryView(ExportMixin, tables2.views.SingleTableMixin, FilterView):
    """
    Set up the data for the fundamental history page
    """

    template_name = "stocks/base_fundamentalhistory.html"
    model = models.FundamentalAnalysisSummary
    table_class = stocks_tables.FundamentalAnalysisSummaryTable
    indicator = None

    def __init__(self):
        super(FundamentalHistoryView, self).__init__()
        self.symbol_needed = False
        self.index_name_needed = False
        self.os_parameter_needed = False

    def get(self, request, *args, **kwargs):
        # get the filters included in the URL.
        # If the required filters are not present, return a redirect
        required_parameters = [
            "symbol1",
            "symbol2",
            "indicator",
            "date__gte",
            "date__lte",
        ]
        for parameter in required_parameters:
            try:
                # check that each parameter has a value
                if self.request.GET[parameter]:
                    pass
            except MultiValueDictKeyError:
                LOGGER.warning(
                    "Fundamental indicators history page requested without all parameters. Sending redirect."
                )
                # if we are missing any parameters, return a redirect
                base_url = reverse("stocks:fundamentalhistory", current_app="stocks")
                query_string = urlencode(
                    {
                        "symbol1": stocks_template_tags.get_session_symbol_or_default(
                            self
                        ),
                        "symbol2": "WCO",
                        "indicator": "EPS",
                        "date__gte": stocks_template_tags.get_5_yr_back(),
                        "date__lte": stocks_template_tags.get_today(),
                    }
                )
                url = "{}?{}".format(base_url, query_string)
                return redirect(url)
        return super(FundamentalHistoryView, self).get(request)

    def get_context_data(self, *args, **kwargs):
        try:
            errors = ""
            # get the current context
            context = super().get_context_data(*args, **kwargs)
            LOGGER.debug("Checking which GET parameters were included in the request.")
            if self.request.GET.get("symbol1"):
                self.symbol1 = self.request.GET.get("symbol1")
                # store the session variable
                self.request.session["symbol1"] = self.symbol1
            if self.request.GET.get("symbol2"):
                self.symbol2 = self.request.GET.get("symbol2")
                # store the session variable
                self.request.session["symbol2"] = self.symbol2
            if self.request.GET.get("indicator"):
                self.selected_indicator = self.request.GET.get("indicator")
                # store the session variable
                self.request.session["selected_indicator"] = self.selected_indicator
            if self.request.GET.get("date__gte"):
                entered_start_date = datetime.strptime(
                    self.request.GET.get("date__gte"), "%Y-%m-%d"
                )
                self.request.session[
                    "entered_start_date"
                ] = entered_start_date.strftime("%Y-%m-%d")
                self.entered_start_date = entered_start_date
            if self.request.GET.get("date__lte"):
                entered_end_date = datetime.strptime(
                    self.request.GET.get("date__lte"), "%Y-%m-%d"
                )
                self.request.session["entered_end_date"] = entered_end_date.strftime(
                    "%Y-%m-%d"
                )
                self.entered_end_date = entered_end_date
            # validate input data
            if entered_start_date >= entered_end_date:
                errors += "Your starting date must be before your ending date. Please recheck."
            LOGGER.debug("Finished parsing GET parameters. Now loading graph data.")
            # fetch data from the db
            listed_stocks = models.ListedEquities.objects.all().order_by("symbol")
            historical_records_1 = (
                self.model.objects.filter(symbol=self.symbol1)
                .filter(date__gte=self.entered_start_date)
                .filter(date__lte=self.entered_end_date)
                .filter(report_type="annual")
            )
            historical_records_2 = (
                self.model.objects.filter(symbol=self.symbol2)
                .filter(date__gte=self.entered_start_date)
                .filter(date__lte=self.entered_end_date)
                .filter(report_type="annual")
            )
            historical_quarterly_records_1 = (
                self.model.objects.filter(symbol=self.symbol1)
                .filter(date__gte=self.entered_start_date)
                .filter(date__lte=self.entered_end_date)
                .filter(report_type="quarterly")
            )
            historical_quarterly_records_2 = (
                self.model.objects.filter(symbol=self.symbol2)
                .filter(date__gte=self.entered_start_date)
                .filter(date__lte=self.entered_end_date)
                .filter(report_type="quarterly")
            )
            historical_close_prices_1 = (
                models.DailyStockSummary.objects.filter(symbol=self.symbol1)
                .filter(date__gte=self.entered_start_date)
                .filter(date__lte=self.entered_end_date)
            )
            historical_close_prices_2 = (
                models.DailyStockSummary.objects.filter(symbol=self.symbol2)
                .filter(date__gte=self.entered_start_date)
                .filter(date__lte=self.entered_end_date)
            )
            # set up a list of all the valid indicators
            all_indicators = []
            for field in self.model._meta.fields:
                if field.column not in ["id", "symbol", "date", "report_type"]:
                    temp_indicator = dict()
                    temp_indicator["field_name"] = field.column
                    temp_indicator["verbose_name"] = field.verbose_name
                    all_indicators.append(temp_indicator)
                    if field.column == self.selected_indicator:
                        self.selected_indicator_verbose_name = field.verbose_name
            # Set up our graph
            # set up the annual data
            graph_labels_1 = [obj.date for obj in historical_records_1]
            graph_labels_2 = [obj.date for obj in historical_records_2]
            graph_dataset_1 = [
                obj[self.selected_indicator] for obj in historical_records_1.values()
            ]
            graph_dataset_2 = [
                obj[self.selected_indicator] for obj in historical_records_2.values()
            ]
            # set up the quarterly data
            quarterly_dates_1 = [obj.date for obj in historical_quarterly_records_1]
            quarterly_dates_2 = [obj.date for obj in historical_quarterly_records_2]
            quarterly_dataset_1 = [
                obj[self.selected_indicator]
                for obj in historical_quarterly_records_1.values()
            ]
            quarterly_dataset_2 = [
                obj[self.selected_indicator]
                for obj in historical_quarterly_records_2.values()
            ]
            # also include the stock price below
            graph_labels_3 = [obj.date for obj in historical_close_prices_1]
            graph_labels_4 = [obj.date for obj in historical_close_prices_2]
            graph_close_prices_1 = [
                obj["close_price"] for obj in historical_close_prices_1.values()
            ]
            graph_close_prices_2 = [
                obj["close_price"] for obj in historical_close_prices_2.values()
            ]
            # add the context keys
            LOGGER.debug("Loading context keys.")
            context["errors"] = errors
            context["listed_stocks"] = listed_stocks
            context["entered_start_date"] = entered_start_date.strftime("%Y-%m-%d")
            context["entered_end_date"] = entered_end_date.strftime("%Y-%m-%d")
            context["graph_labels_1"] = graph_labels_1
            context["graph_labels_2"] = graph_labels_2
            context["graph_labels_3"] = graph_labels_3
            context["graph_labels_4"] = graph_labels_4
            context["quarterly_dates_1"] = quarterly_dates_1
            context["quarterly_dates_2"] = quarterly_dates_2
            context["symbol1"] = self.symbol1
            context["symbol2"] = self.symbol2
            context["all_indicators"] = all_indicators
            context["selected_indicator"] = self.selected_indicator
            context[
                "selected_indicator_verbose_name"
            ] = self.selected_indicator_verbose_name
            context["graph_dataset_1"] = graph_dataset_1
            context["graph_dataset_2"] = graph_dataset_2
            context["quarterly_dataset_1"] = quarterly_dataset_1
            context["quarterly_dataset_2"] = quarterly_dataset_2
            context["graph_close_prices_1"] = graph_close_prices_1
            context["graph_close_prices_2"] = graph_close_prices_2
            LOGGER.info("Successfully loaded page.")
        except ValueError as verr:
            context["errors"] = ALERTMESSAGE + str(verr)
            LOGGER.warning("Got a value error while loading this page" + str(verr))
        except Exception as ex:
            LOGGER.exception(
                "Sorry. Ran into a problem while attempting to load the page: "
                + self.template_name
            )
            context["errors"] = ALERTMESSAGE + str(ex)
        return context


class StockNewsHistoryView(ExportMixin, tables2.views.SingleTableMixin, FilterView):
    """
    Set up the data for the news on each stock
    """

    template_name = "stocks/base_stocknewshistory.html"
    model = models.StockNewsData
    table_class = stocks_tables.StockNewsHistoryTable
    table_pagination = {"per_page": 10}
    filterset_class = filters.StockNewsHistoryFilter

    def get_context_data(self, *args, **kwargs):
        try:
            errors = ""
            LOGGER.info("Stock news history page was called")
            # get the current context
            context = super().get_context_data(*args, **kwargs)
            LOGGER.info("Successfully loaded page.")
            listed_stocks = models.ListedEquities.objects.all().order_by("symbol")
            context["listed_stocks"] = listed_stocks
        except ValueError as verr:
            context["errors"] = ALERTMESSAGE + str(verr)
            LOGGER.warning("Got a valueerror while loading this page" + str(verr))
        except Exception as ex:
            LOGGER.exception(
                "Sorry. Ran into a problem while attempting to load the page: "
                + self.template_name
            )
            context["errors"] = ALERTMESSAGE + str(ex)
        return context


class AboutPageView(TemplateView):
    """
    Set up the data for the technical analysis summary page
    """

    template_name = "stocks/base_about.html"


class LoginPageView(FormView):
    """
    User login
    """

    template_name = "stocks/account/base_login.html"
    form_class = forms.LoginForm
    success_url = reverse_lazy("stocks:login", current_app="stocks")

    def get(self, request, *args, **kwargs):
        context = super(LoginPageView, self).get_context_data(**kwargs)
        # store a next URL if one is included
        if "next" in self.request.GET:
            self.request.session["next_URL"] = self.request.GET.get("next")
            context["next_URL_included"] = True
        return self.render_to_response(context)

    def form_invalid(self, form, **kwargs):
        """This is called when the form is submitted with invalid data"""
        context = super(LoginPageView, self).get_context_data(**kwargs)
        context["form_submit_fail"] = True
        return self.render_to_response(context)

    def form_valid(self, form, **kwargs):
        """This is called when the form is submitted with all data valid"""
        # set up our context variables
        context = super(LoginPageView, self).get_context_data(**kwargs)
        context["form_submit_success"] = True
        # login the user
        user = authenticate(
            self.request,
            username=form.cleaned_data["username"],
            password=form.cleaned_data["password"],
        )
        if user is not None:
            login(self.request, user)
            LOGGER.info(f"{form.cleaned_data['username']} logged in successfully.")
            # if there is a 'next' URL included, move to it
            if "next_URL" in self.request.session:
                return redirect(self.request.session["next_URL"])
        return self.render_to_response(context)


class LogoutPageView(LoginRequiredMixin, TemplateView):
    """
    User logout
    """

    template_name = "stocks/account/base_logout.html"

    def get_context_data(self, **kwargs):
        """This is called when the form is called for the first time"""
        context = super(LogoutPageView, self).get_context_data(**kwargs)
        try:
            if "logout" in self.request.GET:
                logout(self.request)
                context["logout_success"] = True
            elif "deleteaccount" in self.request.GET:
                current_user = get_user_model().objects.get(
                    username=self.request.user.username
                )
                current_user.delete()
                context["delete_account_success"] = True
        except Exception as exc:
            context["errors"] = exc.message
        return context


class RegisterPageView(FormView):
    """
    Register new user
    """

    template_name = "stocks/account/base_register.html"
    form_class = forms.RegisterForm
    success_url = reverse_lazy("stocks:register", current_app="stocks")

    def get_context_data(self, **kwargs):
        """This is called when the form is called for the first time"""
        context = super(RegisterPageView, self).get_context_data(**kwargs)
        context["initial_form"] = True
        return context

    def form_valid(self, form, **kwargs):
        """This is called when the form is submitted with all data valid"""
        # set up our context variables
        context = super(RegisterPageView, self).get_context_data(**kwargs)
        try:
            context["form_submit_success"] = True
            context["initial_form"] = False
            # create the user
            new_username = form.cleaned_data["username"]
            new_email = form.cleaned_data["email"]
            new_password = form.cleaned_data["password"]
            get_user_model().objects.create_user(new_username, new_email, new_password)
            LOGGER.info(f"Successfully created user: {new_username}.")
            # send an email to the user
            send_mail(
                "trinistocks: Account Creation",
                f"You have created a new account at www.trinistocks.com! Your username is {new_username}.Please login and start monitoring and growing your portfolio with us today.",
                "admin@trinistocks.com",
                [f"{new_email}"],
                fail_silently=False,
            )
            LOGGER.info(f"Sent email to {new_email}.")
        except Exception as exc:
            logging.exception("We could not create the user.")
        return self.render_to_response(context)

    def form_invalid(self, form, **kwargs):
        """This is called when the form is submitted with invalid data"""
        context = super(RegisterPageView, self).get_context_data(**kwargs)
        context["form_submit_success"] = False
        context["initial_form"] = False
        return self.render_to_response(context)


class PortfolioTransactionsView(LoginRequiredMixin, FormView):
    """
    Allow user to add new market transactions for their portfolio
    """

    template_name = "stocks/base_portfoliotransactions.html"
    form_class = forms.PortfolioTransactionForm
    success_url = reverse_lazy("stocks:portfoliotransactions", current_app="stocks")

    def get_context_data(self, **kwargs):
        """This is called when the form is called for the first time"""
        context = super(PortfolioTransactionsView, self).get_context_data(**kwargs)
        context["initial_form"] = True
        return context

    def form_valid(self, form, **kwargs):
        context = super(PortfolioTransactionsView, self).get_context_data(**kwargs)
        try:
            """This is called when the form is submitted with all data valid"""
            # set up our context variables
            context["form_submit_success"] = True
            context["initial_form"] = False
            # try to repopulate data
            if "symbol" in form.data:
                context["selected_symbol"] = form.data["symbol"]
            if "date" in form.data:
                context["date"] = form.data["date"]
            if "bought_or_sold" in form.data:
                context["bought_or_sold"] = form.data["bought_or_sold"]
            # insert the record into the db
            current_user = get_user_model().objects.get(
                username=self.request.user.username
            )
            # if the user is inserting a sell transaction, check that they have bought enough shares previously
            if form.data["bought_or_sold"] == "Sold":
                remaining_shares = (
                    models.PortfolioSummary.objects.all()
                    .filter(user_id=current_user.id, symbol_id=form.data["symbol"])[0]
                    .shares_remaining
                )
                if remaining_shares < int(form.data["num_shares"]):
                    raise ValidationError(
                        "You are trying to sell more shares than you have remaining! Did you forget to add some share purchases?"
                    )
            transaction = models.PortfolioTransactions.objects.create(
                user=current_user,
                date=form.data["date"],
                symbol=models.ListedEquities.objects.get(symbol=form.data["symbol"]),
                bought_or_sold=form.data["bought_or_sold"],
                share_price=form.data["price"],
                num_shares=form.data["num_shares"],
            )
            # update the book values with this new transaction
            updater.update_portfolio_summary_book_costs()
            # update the market values
            updater.update_portfolio_summary_market_values()
        except IntegrityError as exc:
            context[
                "general_error"
            ] = "Sorry. It seems like that's a duplicate entry. Did you already add this transaction?"
        except ValidationError as exc:
            context["general_error"] = exc.message
        except Exception as exc:
            context[
                "general_error"
            ] = f"Sorry. We ran into an error with your submission. Here's what we know: {exc}"
        return self.render_to_response(context)

    def form_invalid(self, form, **kwargs):
        """This is called when the form is submitted with invalid data"""
        context = super(PortfolioTransactionsView, self).get_context_data(**kwargs)
        context["form_submit_success"] = False
        context["initial_form"] = False
        # try to repopulate data
        if "symbol" in form.data:
            context["selected_symbol"] = form.data["symbol"]
        if "date" in form.data:
            context["date"] = form.data["date"]
        if "bought_or_sold" in form.data:
            context["bought_or_sold"] = form.data["bought_or_sold"]
        return self.render_to_response(context)


class PortfolioSummaryView(
    LoginRequiredMixin, ExportMixin, tables2.views.SingleTableMixin, FilterView
):
    """
    A page showing an overview of stocks in each user's portfolio, and allowing the user to delete stocks as required.
    """

    template_name = "stocks/base_portfoliosummary.html"
    model = models.PortfolioSummary
    table_class = stocks_tables.PortfolioSummaryTable
    table_pagination = False
    filterset_class = filters.PortfolioSummaryFilter

    def get_context_data(self, *args, **kwargs):
        LOGGER.info(f"{self.template_name} was called")
        # get the current context
        context = super().get_context_data(*args, **kwargs)
        try:
            LOGGER.info("Successfully loaded page.")
            current_user = get_user_model().objects.get(
                username=self.request.user.username
            )
            current_data = self.model.objects.filter(user=current_user).order_by(
                "-market_value"
            )
            context["current_username"] = self.request.user.username
            # check which GET variables were included in the request
            delete_request_message = None
            if "delete" in self.request.GET:
                delete_request = self.request.GET["delete"]
                if delete_request == "ALL":
                    current_data.delete()
                    models.PortfolioTransactions.objects.filter(
                        user=current_user
                    ).delete()
                    delete_request_message = (
                        "All stocks deleted from portfolio successfully."
                    )
                else:
                    current_data.filter(symbol_id=delete_request).delete()
                    models.PortfolioTransactions.objects.filter(
                        user=current_user
                    ).filter(symbol_id=delete_request).delete()
                    delete_request_message = (
                        f"Stocks for {delete_request} deleted successfully."
                    )
            # set up the data for the graphs
            # first get the symbols and their market values
            symbols = list(current_data.values_list("symbol_id", flat=True))
            # if we have no symbols, send an error message to the user
            if not symbols:
                raise RuntimeError("No symbols in portfolio")
            market_values = list(current_data.values_list("market_value", flat=True))
            # convert the market values to float
            symbol_market_values = [float(x) for x in market_values]
            # then get the sectors and their market values
            sectors_df = pd.DataFrame.from_records(
                current_data.values_list("symbol_id__sector", "market_value")
            )
            sectors_df.rename(columns={0: "sector", 1: "market_value"}, inplace=True)
            # sum the market values for all symbols in the same sector
            sectors_df = sectors_df.groupby("sector").sum().reset_index()
            sectors = sectors_df["sector"].to_list()
            market_values = sectors_df["market_value"].to_list()
            sector_market_values = [float(x) for x in market_values]
            sector_market_values.sort(reverse=True)
            # set up our context variables to graph
            context["symbols"] = symbols
            context["symbol_market_values"] = symbol_market_values
            context["sectors"] = sectors
            context["sector_market_values"] = sector_market_values
            # set up the context variables for the deletion
            context["delete_request_message"] = delete_request_message
        except RuntimeError:
            context["no_symbols"] = True
        except Exception as ex:
            LOGGER.exception(
                "Sorry. Ran into a problem while attempting to load the page: "
                + self.template_name,
                exc_info=ex,
            )
            context["errors"] = ALERTMESSAGE + str(ex)
        return context


class UserProfileView(LoginRequiredMixin, TemplateView):
    """
    A summary of the user's profile on trinistats. Provides normal
    options to logout, delete account etc.
    """

    template_name = "stocks/account/base_userprofile.html"

    def get_context_data(self, **kwargs):
        """Add context data for the page"""
        context = super(UserProfileView, self).get_context_data(**kwargs)
        LOGGER.debug("Now loading context data for UserProfileView...")
        current_user = get_user_model().objects.get(username=self.request.user.username)
        context["current_username"] = self.request.user.username
        context["date_created"] = current_user.date_joined.strftime("%d-%m-%Y")
        context["last_login"] = current_user.last_login.strftime("%d-%m-%Y")
        context["email"] = current_user.email
        return context


class PasswordResetRequestView(FormView):
    """
    Account password reset
    """

    template_name = "stocks/account/base_passwordreset.html"
    form_class = forms.PasswordResetForm
    success_url = reverse_lazy("stocks:password_reset_request", current_app="stocks")

    def get(self, request, *args, **kwargs):
        context = super(PasswordResetRequestView, self).get_context_data(**kwargs)
        return self.render_to_response(context)

    def form_invalid(self, form, **kwargs):
        """This is called when the form is submitted with invalid data"""
        context = super(PasswordResetRequestView, self).get_context_data(**kwargs)
        context["form_submit_fail"] = True
        return self.render_to_response(context)

    def form_valid(self, form, **kwargs):
        """This is called when the form is submitted with all data valid"""
        # set up our context variables
        context = super(PasswordResetRequestView, self).get_context_data(**kwargs)
        context["form_submit_success"] = True
        # login the user
        user = models.User.objects.get(email=form.cleaned_data["email"])
        if user is not None:
            # if we find a user with this email address, send them a password reset email
            subject = "trinistocks: Password Reset"
            email_template_name = "stocks/account/password_reset_email.txt"
            email_body = {
                "email": user.email,
                "domain": "trinistocks.com",
                "site_name": "stocks",
                "uid": urlsafe_base64_encode(force_bytes(user.pk)),
                "user": user,
                "token": default_token_generator.make_token(user),
                "protocol": "https",
            }
            email = render_to_string(email_template_name, email_body)
            send_mail(
                subject,
                email,
                "admin@trinistocks.com",
                [user.email],
                fail_silently=False,
            )
            LOGGER.info(f"Sent password reset email to {user.email}.")
            context["reset_sent"] = True
        return self.render_to_response(context)


# API Views


class DailyStocksTradedApiView(generics.ListCreateAPIView):
    serializer_class = serializers.DailyStockSummarySerializer
    # require a token to access the api
    permission_classes = (permissions.IsAuthenticated,)

    def get_queryset(self):
        """
        Return only the objects for the last trading day
        """
        queryset = models.DailyStockSummary.objects.all()
        # check if a date was included in the request
        filter_date = self.request.query_params.get("date")
        # if no date was included, send data for the last date
        if not filter_date:
            filter_date = stocks_template_tags.get_latest_date_dailytradingsummary()
        return (
            models.DailyStockSummary.objects.exclude(was_traded_today=0)
            .filter(date=filter_date)
            .select_related("symbol")
            .order_by("-value_traded")
        )


class StockNewsApiView(generics.ListCreateAPIView):
    serializer_class = serializers.StockNewsDataSerializer
    # require a token to access the api
    permission_classes = (permissions.IsAuthenticated,)

    def get_queryset(self):
        """
        check the url for any filters applied and return the filtered queryset
        """
        queryset = models.StockNewsData.objects.all()
        filter_symbol = self.request.query_params.get("symbol")
        filter_start_date = self.request.query_params.get("start_date")
        filter_category = self.request.query_params.get("category")
        if filter_symbol is not None:
            queryset = queryset.filter(symbol=filter_symbol)
        if filter_start_date is not None:
            queryset = queryset.filter(date__gte=filter_start_date)
        if filter_category is not None:
            queryset = queryset.filter(category=filter_category)
        return queryset


class ListedStocksApiView(generics.ListCreateAPIView):
    serializer_class = serializers.ListedStocksSerializer
    # require a token to access the api
    permission_classes = (permissions.IsAuthenticated,)

    def get_queryset(self):
        """
        check the url for any filters applied and return the filtered queryset
        """
        queryset = models.ListedEquities.objects.all()
        filter_symbol = self.request.query_params.get("symbol")
        filter_status = self.request.query_params.get("status")
        filter_sector = self.request.query_params.get("sector")
        filter_currency = self.request.query_params.get("currency")
        if filter_symbol is not None:
            queryset = queryset.filter(symbol=filter_symbol)
        if filter_status is not None:
            queryset = queryset.filter(status=filter_status)
        if filter_sector is not None:
            queryset = queryset.filter(sector=filter_sector)
        if filter_currency is not None:
            queryset = queryset.filter(currency=filter_currency)
        return queryset


class TechnicalAnalysisApiView(generics.ListCreateAPIView):
    serializer_class = serializers.TechnicalAnalysisSerializer
    # require a token to access the api
    permission_classes = (permissions.IsAuthenticated,)

    def get_queryset(self):
        """
        check the url for any filters applied and return the filtered queryset
        """
        queryset = models.TechnicalAnalysisSummary.objects.all()
        return queryset


class FundamentalAnalysisApiView(generics.ListCreateAPIView):
    serializer_class = serializers.FundamentalAnalysisSerializer
    # require a token to access the api
    permission_classes = (permissions.IsAuthenticated,)

    def get_queryset(self):
        """
        check the url for any filters applied and return the filtered queryset
        """
        queryset = models.FundamentalAnalysisSummary.objects.all()
        # check for any url filters applied
        filter_symbol = self.request.query_params.get("symbol")
        filter_report_type = self.request.query_params.get("report_type")
        filter_start_date = self.request.query_params.get("start_date")
        if filter_symbol is not None:
            queryset = queryset.filter(symbol=filter_symbol)
        if filter_start_date is not None:
            queryset = queryset.filter(date__gte=filter_start_date)
        if filter_report_type is not None:
            queryset = queryset.filter(report_type=filter_report_type)
        return queryset


class StockPriceApiView(generics.ListCreateAPIView):
    serializer_class = serializers.StockPriceSerializer
    # require a token to access the api
    permission_classes = (permissions.IsAuthenticated,)

    def get_queryset(self):
        """
        Return only the objects for the last trading day
        """
        queryset = models.DailyStockSummary.objects.all()
        filter_symbol = self.request.query_params.get("symbol")
        # check if a date was included in the request
        filter_start_date = self.request.query_params.get("start_date")
        if filter_symbol is not None:
            queryset = queryset.filter(symbol=filter_symbol)
        if filter_start_date is not None:
            queryset = queryset.filter(date__gte=filter_start_date)
        return queryset


class DividendPaymentsApiView(generics.ListCreateAPIView):
    serializer_class = serializers.DividendPaymentsSerializer
    # require a token to access the api
    permission_classes = (permissions.IsAuthenticated,)

    def get_queryset(self):
        """
        Return only the objects for the last trading day
        """
        queryset = models.HistoricalDividendInfo.objects.all()
        filter_symbol = self.request.query_params.get("symbol")
        # check if a date was included in the request
        filter_start_date = self.request.query_params.get("start_date")
        if filter_symbol is not None:
            queryset = queryset.filter(symbol=filter_symbol)
        if filter_start_date is not None:
            queryset = queryset.filter(record_date__gte=filter_start_date)
        return queryset


class DividendYieldsApiView(generics.ListCreateAPIView):
    serializer_class = serializers.DividendYieldSerializer
    # require a token to access the api
    permission_classes = (permissions.IsAuthenticated,)

    def get_queryset(self):
        """
        Return only the objects for the last trading day
        """
        queryset = models.HistoricalDividendYield.objects.all()
        filter_symbol = self.request.query_params.get("symbol")
        # check if a date was included in the request
        filter_start_date = self.request.query_params.get("start_date")
        if filter_symbol is not None:
            queryset = queryset.filter(symbol=filter_symbol)
        if filter_start_date is not None:
            queryset = queryset.filter(date__gte=filter_start_date)
        return queryset


class MarketIndicesApiView(generics.ListCreateAPIView):
    serializer_class = serializers.MarketIndicesSerializer
    # require a token to access the api
    permission_classes = (permissions.IsAuthenticated,)

    def get_queryset(self):
        """
        Return only the objects for the last trading day
        """
        queryset = models.HistoricalIndicesInfo.objects.all()
        filter_index_name = self.request.query_params.get("index_name")
        # check if a date was included in the request
        filter_start_date = self.request.query_params.get("start_date")
        if filter_index_name is not None:
            queryset = queryset.filter(index_name=filter_index_name)
        if filter_start_date is not None:
            queryset = queryset.filter(date__gte=filter_start_date)
        return queryset


class OutstandingTradesApiView(generics.ListCreateAPIView):
    serializer_class = serializers.OutstandingTradesSerializer
    # require a token to access the api
    permission_classes = (permissions.IsAuthenticated,)

    def get_queryset(self):
        """
        Return only the objects for the last trading day
        """
        queryset = models.DailyStockSummary.objects.all()
        filter_symbol = self.request.query_params.get("symbol")
        # check if a date was included in the request
        filter_start_date = self.request.query_params.get("start_date")
        if filter_symbol is not None:
            queryset = queryset.filter(symbol=filter_symbol)
        if filter_start_date is not None:
            queryset = queryset.filter(date__gte=filter_start_date)
        return queryset


class PortfolioSummaryApiView(generics.ListCreateAPIView):
    serializer_class = serializers.PortfolioSummarySerializer
    # require a token to access the api
    permission_classes = (permissions.IsAuthenticated,)

    def get_queryset(self):
        """
        Return all objects in the portfolio for the current authorized user
        """
        queryset = models.PortfolioSummary.objects.all().filter(user=self.request.user)
        return queryset


class PortfolioSectorsApiView(generics.ListCreateAPIView):
    serializer_class = serializers.PortfolioSectorsSerializer
    # require a token to access the api
    permission_classes = (permissions.IsAuthenticated,)

    def get_queryset(self):
        """
        Return all objects in the portfolio for the current authorized user
        """
        queryset = models.PortfolioSectors.objects.all().filter(user=self.request.user)
        return queryset


class PortfolioTransactionsApiView(generics.UpdateAPIView):
    serializer_class = serializers.PortfolioTransactionsSerializer
    # require a token to access the api
    permission_classes = (permissions.IsAuthenticated,)

    def put(self, request):
        """
        Accept put requests for new transactions
        """
        try:
            # do some prechecks
            if self.request.POST["bought_or_sold"] == "Sold":
                # check that the user has enough shares to sell
                shares_remaining = models.PortfolioSummary.objects.filter(
                    user=self.request.user,
                    symbol=models.ListedEquities(symbol=self.request.POST["symbol"]),
                ).first()
                if not shares_remaining:
                    raise RuntimeError(
                        "You don't have any shares of this company remaining."
                    )
                else:
                    # if they have shares in the company, ensure that they have enough
                    if shares_remaining.shares_remaining < int(
                        self.request.POST["num_shares"]
                    ):
                        raise RuntimeError(
                            "You are selling more shares than you have left."
                        )
            queryset = models.PortfolioTransactions.objects.create(
                user=self.request.user,
                symbol=models.ListedEquities(symbol=self.request.POST["symbol"]),
                date=self.request.POST["date"],
                bought_or_sold=self.request.POST["bought_or_sold"],
                share_price=self.request.POST["share_price"],
                num_shares=self.request.POST["num_shares"],
            )
            queryset.save()
        except Exception as exc:
            LOGGER.error(
                "Ran into an error during Portfolio transaction addition", exc_info=exc
            )
            return Response(
                data="Failed to add transaction. " + str(exc),
                status=status.HTTP_400_BAD_REQUEST,
            )
        else:
            return Response(data="Success", status=status.HTTP_201_CREATED)


class CustomAuthToken(ObtainAuthToken):
    serializer_class = serializers.CustomAuthTokenSerializer

    def post(self, request, *args, **kwargs):
        serializer = self.serializer_class(
            data=request.data, context={"request": request}
        )
        serializer.is_valid(raise_exception=True)
        user = serializer.validated_data["user"]
        token, created = Token.objects.get_or_create(user=user)
        return Response(
            {
                "token": token.key,
                "user_id": user.pk,
                "email": user.email,
                "username": user.username,
            }
        )


class UserCreate(generics.CreateAPIView):
    queryset = models.User.objects.all()
    serializer_class = serializers.UserSerializer
    permission_classes = (permissions.AllowAny,)

    def post(self, request, format=None):
        serializer = serializers.UserSerializer(data=request.data)
        # first check if user has already created an account
        username_user = models.User.objects.get(username=request.data["username"])
        email_user = models.User.objects.get(email=request.data["email"])
        if username_user and email_user and username_user == email_user:
            if username_user.is_active:
                return Response(
                    data={
                        "error": "Are you sure this account hasn't been created already?"
                    },
                    status=status.HTTP_304_NOT_MODIFIED,
                )
            else:
                # if the username and email match an inactive account, reactivate the account
                username_user.is_active = True
                username_user.save()
                return Response(
                    data={"message": "Success. Account reactivated."},
                    status=status.HTTP_201_CREATED,
                )
        else:
            if serializer.is_valid():
                serializer.save()
                return Response(serializer.data, status=status.HTTP_201_CREATED)
            else:
                return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)


class UserDelete(generics.DestroyAPIView):
    queryset = models.User.objects.all()
    permission_classes = (permissions.IsAuthenticated,)

    def delete(self, request, format=None):
        try:
            check_user = request.user
            if check_user:
                check_user.is_active = False
                check_user.save()
                return Response(
                    data=f"Successfully deleted user {check_user.username}.",
                    status=status.HTTP_200_OK,
                )
            else:
                raise RuntimeError("No user logged in.")
        except Exception as exc:
            return Response(
                data="Error: " + exc.args[0], status=status.HTTP_400_BAD_REQUEST
            )


class ChangePasswordView(generics.UpdateAPIView):
    """
    An endpoint for changing the password using the app
    """

    serializer_class = serializers.ChangePasswordSerializer
    model = models.User
    permission_classes = (IsAuthenticated,)

    def get_object(self, queryset=None):
        obj = self.request.user
        return obj

    def update(self, request, *args, **kwargs):
        self.object = self.get_object()
        serializer = self.get_serializer(data=request.data)

        if serializer.is_valid():
            # if both required passwords were provided, check the old password first
            if not self.object.check_password(serializer.data.get("old_password")):
                response = {
                    "status": "failed",
                    "code": status.HTTP_401_UNAUTHORIZED,
                    "message": "Your provided credentials do not seem to match any existing record.",
                    "data": [],
                }
            else:
                # else update the password with the new password
                # set_password also hashes the password that the user will get
                self.object.set_password(serializer.data.get("new_password"))
                self.object.save()
                response = {
                    "status": "success",
                    "code": status.HTTP_200_OK,
                    "message": "Your password was updated successfully!",
                    "data": [],
                }
            return Response(response)
        else:
            # else the serializer may be missing some required parameter
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)


class SimulatorGamesApiView(generics.ListCreateAPIView):
    serializer_class = serializers.SimulatorGamesSerializer
    # require a token to access the api
    permission_classes = (permissions.IsAuthenticated,)

    def get_queryset(self):
        """
        Return all objects in the portfolio for the current authorized user
        """
        queryset = models.SimulatorGames.objects.all().filter(
            simulatorplayers=models.SimulatorPlayers.objects.get(user=self.request.user)
        )
        return queryset

    def post(self, request):
        try:
            serializer = serializers.SimulatorGamesSerializer(data=request.data)
            # check if game code was provided if private game is chosen
            provided_game_code = request.data["game_code"]
            is_private = request.data["private"]
            if is_private and not provided_game_code:
                return Response(
                    data={
                        "error": "Please ensure a game code is entered for private games."
                    },
                    status=status.HTTP_400_BAD_REQUEST,
                )
            # check if the game name was used already
            check_game_name = models.SimulatorGames.objects.filter(
                game_name=request.data["game_name"]
            )
            if check_game_name.count() > 0:
                return Response(
                    data={"error": "Game name already in use."},
                    status=status.HTTP_400_BAD_REQUEST,
                )
            if serializer.is_valid():
                LOGGER.debug(
                    f"New simulator game created! Name:{request.data['game_name']}"
                )
                serializer.save()
                return Response(serializer.data, status=status.HTTP_201_CREATED)
            else:
                return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
        except Exception as exc:
            LOGGER.error("Problem with Simulator Games POST request", exc_info=exc)
            return Response(data=str(exc), status=status.HTTP_400_BAD_REQUEST)


class SimulatorPlayersApiView(generics.ListCreateAPIView):
    serializer_class = serializers.SimulatorPlayersSerializer
    # require a token to access the api
    permission_classes = (permissions.IsAuthenticated,)

    def get_queryset(self):
        """
        Return all objects in the portfolio for the current authorized user
        """
        queryset = models.SimulatorPlayers.objects.all().filter(user=self.request.user)
        return queryset

    def post(self, request):
        try:
            # check if liquid cash is not null
            if not request.data["liquid_cash"]:
                return Response(
                    data={"error": "Please ensure that you added some starting cash!"},
                    status=status.HTTP_400_BAD_REQUEST,
                )
            # first build our serializer data
            simulator_player_data = {}
            simulator_player_data["user"] = request.user.id
            simulator_player_data["liquid_cash"] = request.data["liquid_cash"]
            # get the simulator game
            simulator_player_data["simulator_game"] = models.SimulatorGames.objects.get(
                game_name=request.data["game_name"]
            ).pk
            serializer = serializers.SimulatorPlayersSerializer(
                data=simulator_player_data
            )
            if serializer.is_valid():
                serializer.save()
                return Response(serializer.data, status=status.HTTP_201_CREATED)
            else:
                return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
        except Exception as exc:
            LOGGER.error("Problem with Simulator Players POST request", exc_info=exc)
            return Response(data=str(exc), status=status.HTTP_400_BAD_REQUEST)


class SimulatorTransactionsApiView(generics.UpdateAPIView):
    serializer_class = serializers.SimulatorTransactionsSerializer
    # require a token to access the api
    permission_classes = (permissions.IsAuthenticated,)

    def put(self, request):
        """
        Accept put requests for new transactions
        """
        try:
            # do some prechecks
            # check that all keys are in dict
            required_fields = [
                "bought_or_sold",
                "symbol",
                "num_shares",
                "game_name",
                "date",
                "share_price",
            ]
            for field in required_fields:
                if field not in request.POST:
                    raise RuntimeError(
                        f"Please ensure that a value for {field} is included in the PUT request."
                    )
            if self.request.POST["bought_or_sold"] == "Sold":
                # check that the user has enough shares to sell
                shares_remaining = models.SimulatorPortfolios.objects.filter(
                    user=self.request.user,
                    symbol=models.ListedEquities(symbol=self.request.POST["symbol"]),
                ).first()
                if not shares_remaining:
                    raise RuntimeError(
                        "You don't have any shares of this company remaining."
                    )
                else:
                    # if they have shares in the company, ensure that they have enough
                    if shares_remaining.shares_remaining < int(
                        self.request.POST["num_shares"]
                    ):
                        raise RuntimeError(
                            "You are selling more shares than you have left."
                        )
            # first build our serializer data
            simulator_transaction_data = {}
            simulator_transaction_data[
                "simulator_player"
            ] = models.SimulatorPlayers.objects.get(
                user=self.request.user,
                simulator_game=models.SimulatorGames.objects.get(
                    game_name=self.request.POST["game_name"]
                ),
            ).pk
            simulator_transaction_data["symbol"] = models.ListedEquities.objects.get(
                symbol=self.request.POST["symbol"]
            ).pk
            simulator_transaction_data["date"] = self.request.POST["date"]
            simulator_transaction_data["bought_or_sold"] = self.request.POST[
                "bought_or_sold"
            ]
            simulator_transaction_data["share_price"] = self.request.POST["share_price"]
            simulator_transaction_data["num_shares"] = self.request.POST["num_shares"]
            serializer = serializers.SimulatorTransactionsSerializer(
                data=simulator_transaction_data
            )
            if serializer.is_valid():
                serializer.save()
                return Response(serializer.data, status=status.HTTP_201_CREATED)
            else:
                return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
        except Exception as exc:
            LOGGER.error(
                "Ran into an error during simulator portfolio transaction addition",
                exc_info=exc,
            )
            return Response(
                data="Failed to add transaction. "
                + type(exc).__name__
                + ": "
                + str(exc),
                status=status.HTTP_400_BAD_REQUEST,
            )
        else:
            return Response(data="Success", status=status.HTTP_201_CREATED)


class SimulatorPortfoliosApiView(generics.ListCreateAPIView):
    serializer_class = serializers.SimulatorPortfolioSerializer
    # require a token to access the api
    permission_classes = (permissions.IsAuthenticated,)

    def get_queryset(self):
        """
        Return all objects in the portfolio for the current authorized user
        """
        queryset = models.SimulatorPortfolios.objects.all().filter(
            simulator_player_id=models.SimulatorPlayers.objects.get(
                user=self.request.user,
                simulator_game=models.SimulatorGames.objects.get(
                    game_name=self.request.GET["game_name"]
                ),
            )
        )
        return queryset