# Imports from standard Python lib
import csv
import json
import logging
import traceback
from datetime import datetime
# Imports from cheese factory
import dateutil
import django_tables2 as tables2
from django_tables2.export.views import ExportMixin
from django_filters.views import FilterView
from django.views.generic.base import RedirectView
from django.http import HttpResponse
from django.shortcuts import render
from django.views.generic import TemplateView
from chartjs.views.lines import BaseLineChartView
from django.views.generic import ListView
from dateutil.parser import parse
from django.core import serializers
from django.http.response import JsonResponse
from django import forms
from django.db.models import F
from django.utils.datastructures import MultiValueDictKeyError
from urllib.parse import urlencode
from django.shortcuts import redirect
from django.urls import reverse
from django.db.models import Max
# Imports from local machine
from stocks import models, filters
from stocks import tables as stocks_tables
from .templatetags import stocks_template_tags

# Set up logging
logger = logging.getLogger('root')

# Class definitions


class LandingPageView(RedirectView):

    def get_redirect_url(self, *args, **kwargs):
        base_url = reverse('stocks:dailytradingsummary', current_app="stocks")
        query_string = urlencode({'date': stocks_template_tags.get_latest_date_dailytradingsummary(),
                                  'wastradedtoday': 1, 'sort': '-valuetraded'})
        url = '{}?{}'.format(base_url, query_string)
        return url


class DailyTradingSummaryView(ExportMixin, tables2.views.SingleTableMixin, FilterView):
    """
    Set up the data for the Daily Equity Summary page
    """
    template_name = 'stocks/base_dailytradingsummary.html'
    model = models.DailyStockSummary
    table_class = stocks_tables.DailyTradingSummaryTable
    filterset_class = filters.DailyTradingSummaryFilter

    def get(self, request, *args, **kwargs):
        # get the filters included in the URL.
        # If the required filters are not present, return a redirect
        required_parameters = ['date', 'was_traded_today', 'sort']
        for parameter in required_parameters:
            try:
                # check that each parameter has a value
                if self.request.GET[parameter]:
                    pass
            except MultiValueDictKeyError:
                logger.warning(
                    "Daily trading summary page requested without all parameters. Sending redirect.")
                # if we are missing any parameters, return a redirect
                base_url = reverse(
                    'stocks:dailytradingsummary', current_app="stocks")
                query_string = urlencode({'date': stocks_template_tags.get_latest_date_dailytradingsummary(),
                                          'was_traded_today': 1, 'sort': '-value_traded'})
                url = '{}?{}'.format(base_url, query_string)
                return redirect(url)
        return super(DailyTradingSummaryView, self).get(request)

    def get_context_data(self, *args, **kwargs):
        try:
            errors = ""
            logger.info("Daily trading summary page was called")
            # get the current context
            context = super().get_context_data(
                *args, **kwargs)
            selected_date = datetime.strptime(
                self.request.GET.get('date'), "%Y-%m-%d")
            # Now select the records corresponding to the selected date
            # as well as their symbols, and order by the highest volume traded
            daily_trading_summary_records = models.DailyStockSummary.objects.exclude(was_traded_today=0).filter(
                date=selected_date).select_related('symbol').order_by('-value_traded')
            if not daily_trading_summary_records:
                raise ValueError(
                    "No data available for the date selected. Please press the back button and choose another date.")
            # rename the symbol field properly and select only required fields
            selected_records = daily_trading_summary_records.values(
                'symbol', 'value_traded')
            # check if an export request was received
            # Set up the graph
            # get the top 10 records by value traded
            graph_symbols = [record['symbol']
                             for record in selected_records[:10]]
            graph_value_traded = [record['value_traded']
                                  for record in selected_records[:10]]
            # create a category for the sum of all other symbols (not in the top 10)
            others = dict(symbol='Others', value_traded=0)
            for record in selected_records:
                if (record['symbol'] not in graph_symbols) and record['value_traded']:
                    others['value_traded'] += record['value_traded']
            # add the 'other' category to the graph
            graph_symbols.append(others['symbol'])
            graph_value_traded.append(others['value_traded'])
            # get a human readable date
            selected_date_parsed = selected_date.strftime('%Y-%m-%d')
            # Now add our context data and return a response
            context['errors'] = errors
            context['selected_date'] = selected_date.date()
            context['selected_date_parsed'] = selected_date_parsed
            context['graph_symbols'] = graph_symbols
            context['graph_value_traded'] = graph_value_traded
            logger.info("Successfully loaded page.")
        except ValueError as verr:
            context['errors'] = ALERTMESSAGE+str(verr)
            logger.warning(
                "Got a value error while loading this page"+str(verr))
        except Exception as ex:
            logger.exception(
                "Sorry. Ran into a problem while attempting to load the page: "+self.template_name)
            context['errors'] = ALERTMESSAGE+str(ex)
        return context


class ListedStocksView(ExportMixin, tables2.MultiTableMixin, FilterView):
    """
    Set up the data for the Listed Stocks page
    """
    template_name = 'stocks/base_listedstocks.html'
    model1 = models.ListedEquities
    qs1 = model1.objects.all()
    model2 = models.ListedEquitiesPerSector
    qs2 = model2.objects.all()
    tables = [stocks_tables.ListedStocksTable(
        qs1), stocks_tables.ListedStocksPerSectorTable(qs2)]
    table_pagination = False
    filterset_class = filters.ListedStocksFilter

    def get(self, request, *args, **kwargs):
        # get the filters included in the URL.
        # If the required filters are not present, return a redirect
        required_parameters = ['table_0-sort', 'table_1-sort']
        for parameter in required_parameters:
            try:
                # check that each parameter has a value
                if self.request.GET[parameter]:
                    pass
            except MultiValueDictKeyError:
                logger.warning(
                    "Listed stocks page requested without all parameters. Sending redirect.")
                # if we are missing any parameters, return a redirect
                base_url = reverse(
                    'stocks:listedstocks', current_app="stocks")
                query_string = urlencode(
                    {'table_0-sort': 'symbol', 'table_1-sort': '-num_listed'})
                url = '{}?{}'.format(base_url, query_string)
                return redirect(url)
        return super(ListedStocksView, self).get(request)

    def get_context_data(self, *args, **kwargs):
        try:
            errors = ""
            logger.info("Listed stocks page was called")
            # get the current context
            context = super().get_context_data(
                *args, **kwargs)
            logger.info("Successfully loaded page.")
        except ValueError as verr:
            context['errors'] = ALERTMESSAGE+str(verr)
            logger.warning(
                "Got a valueerror while loading this page"+str(verr))
        except Exception as ex:
            logger.exception(
                "Sorry. Ran into a problem while attempting to load the page: "+self.template_name)
            context['errors'] = ALERTMESSAGE+str(ex)
        return context


class TechnicalAnalysisSummary(ExportMixin, tables2.views.SingleTableMixin, FilterView):
    """
    Set up the data for the technical analysis summary page
    """
    template_name = 'stocks/base_technicalanalysissummary.html'
    model = models.TechnicalAnalysisSummary
    table_class = stocks_tables.TechnicalAnalysisSummaryTable
    table_pagination = False
    filterset_class = filters.TechnicalAnalysisSummaryFilter

    def get_context_data(self, *args, **kwargs):
        try:
            errors = ""
            logger.info("Technical Analysis Summary Page was called")
            # get the current context
            context = super().get_context_data(
                *args, **kwargs)
            logger.info("Successfully loaded page.")
        except ValueError as verr:
            context['errors'] = ALERTMESSAGE+str(verr)
            logger.warning(
                "Got a valueerror while loading this page"+str(verr))
        except Exception as ex:
            logger.exception(
                "Sorry. Ran into a problem while attempting to load the page: "+self.template_name)
            context['errors'] = ALERTMESSAGE+str(ex)
        return context


class FundamentalAnalysisSummary( ExportMixin, tables2.views.SingleTableMixin, TemplateView):
    """
    Set up the data for the technical analysis summary page
    """
    template_name = 'stocks/base_fundamentalanalysissummary.html'
    model = models.FundamentalAnalysisSummary
    table_class = stocks_tables.FundamentalAnalysisSummaryTable
    table_pagination = False

    def get(self, request, *args, **kwargs):
        # get the filters included in the URL.
        # If the required filters are not present, return a redirect
        required_parameters = ['sort',]
        for parameter in required_parameters:
            try:
                # check that each parameter has a value
                if self.request.GET[parameter]:
                    pass
            except MultiValueDictKeyError:
                logger.warning(
                    "Fundamental analysis page requested without all parameters. Sending redirect.")
                # if we are missing any parameters, return a redirect
                base_url = reverse(
                    'stocks:fundamentalanalysis', current_app="stocks")
                query_string = urlencode({'sort': '-sector'})
                url = '{}?{}'.format(base_url, query_string)
                return redirect(url)
        return super(FundamentalAnalysisSummary, self).get(request)


    def get_queryset(self):
        return models.FundamentalAnalysisSummary.objects.raw(
            '''
            SELECT id, latest_id
            FROM (
                SELECT id, symbol, MAX(date) AS latest_id
                FROM audited_fundamental_calculated_data
                GROUP BY symbol DESC) as ids
            ORDER BY symbol;
            ''')

    def get_context_data(self, *args, **kwargs):
        try:
            logger.info("Fundamental Analysis Summary Page was called")
            # get the current context
            context = super().get_context_data(
                *args, **kwargs)
            logger.info("Successfully loaded page.")
        except ValueError as verr:
            context['errors'] = ALERTMESSAGE+str(verr)
            logger.warning(
                "Got a valueerror while loading this page"+str(verr))
        except Exception as ex:
            logger.exception(
                "Sorry. Ran into a problem while attempting to load the page: "+self.template_name)
            context['errors'] = ALERTMESSAGE+str(ex)
        return context

class BasicLineChartAndTableView(ExportMixin, tables2.views.SingleTableMixin, FilterView):
    """
    A generic class for displaying a line chart using ChartJS and a table using djangotables2
    """

    template_name = ''
    model = None  # models.something
    table_class = None  # tables.something
    filterset_class = None  # filters.something
    page_name = ''  # a string representing the name of the page
    historical_records = None
    selected_symbol = None
    selected_stock = None
    graph_dataset = []  # the list of dictionary of objects to display in the graph
    symbol_needed = True
    os_parameter_needed = False
    os_parameter = None
    os_parameter_string = None
    index_name = None
    index_name_needed = False
    index_parameter = None
    index_parameter_string = None
    entered_start_date = None
    entered_end_date = None
    order_by = None

    def __init__(self):
        super(BasicLineChartAndTableView, self).__init__()
        logger.debug("Now loading template: "+self.template_name)

    def set_graph_dataset(self,):
        self.graph_dataset = []

    def get_context_data(self, *args, **kwargs):
        try:
            errors = ""
            # get the current context
            context = super().get_context_data(
                *args, **kwargs)
            logger.debug("Now loading all listed equities.")
            listed_stocks = models.ListedEquities.objects.all().order_by('symbol')
            # now load all the data for the subclasses (pages)
            # note that different pages require different data, so we check which data is needed for the page
            # check if the configuration button was clicked
            logger.debug(
                "Checking which GET parameters were included in the request.")
            if self.request.GET.get('configure_button'):
                entered_start_date = datetime.strptime(
                    self.request.GET.get('date__gte'), "%Y-%m-%d")
                # store the date as a session variable to be reused
                self.request.session['entered_start_date'] = entered_start_date.strftime(
                    '%Y-%m-%d')
                self.entered_start_date = entered_start_date
            # else look for the starting date in the GET variables
            elif self.request.GET.get('date__gte'):
                entered_start_date = datetime.strptime(
                    self.request.GET.get('date__gte'), "%Y-%m-%d")
                self.request.session['entered_start_date'] = entered_start_date.strftime(
                    '%Y-%m-%d')
                self.entered_start_date = entered_start_date
            else:
                # else raise an error
                raise ValueError(
                    " Please ensure that you have included a starting date in the URL! For example: ?date__gte=2019-05-12")
            # check if the configuration button was clicked
            if self.request.GET.get("configure_button"):
                entered_end_date = datetime.strptime(
                    self.request.GET.get('date__lte'), "%Y-%m-%d")
                self.request.session['entered_end_date'] = entered_end_date.strftime(
                    '%Y-%m-%d')
                self.entered_end_date = entered_end_date
            # else look for the ending date in the GET variables
            elif self.request.GET.get('date__lte'):
                entered_end_date = datetime.strptime(
                    self.request.GET.get('date__lte'), "%Y-%m-%d")
                self.request.session['entered_end_date'] = entered_end_date.strftime(
                    '%Y-%m-%d')
                self.entered_end_date = entered_end_date
            else:
                raise ValueError(
                    " Please ensure that you have included an ending date in the URL! For example: ?date__lte=2020-05-12")
            # check if the configuration button was clicked
            if self.symbol_needed:
                if self.request.GET.get("configure_button"):
                    selected_symbol = self.request.GET.get('symbol')
                    self.selected_symbol = selected_symbol
                    self.request.session['selected_symbol'] = selected_symbol
                # else look for the stock code in the GET variables
                elif self.request.GET.get('symbol'):
                    selected_symbol = self.request.GET.get('symbol')
                    self.selected_symbol = selected_symbol
                    self.request.session['selected_symbol'] = selected_symbol
                else:
                    raise ValueError(
                        " Please ensure that you have included a symbol in the URL! For example: ?symbol=ACL")
            if self.request.GET.get('sort'):
                self.order_by = self.request.GET.get('sort')
            else:
                raise ValueError(
                    " Please ensure that you have included a sort order in the URL! For example: ?sort=date")
            if self.os_parameter_needed:
                if self.request.GET.get('os_parameter'):
                    self.os_parameter = self.request.GET.get('os_parameter')
                    self.os_parameter_string = models.DailyStockSummary._meta.get_field(
                        self.os_parameter).verbose_name
            if self.index_name_needed:
                if self.request.GET.get('index_name'):
                    self.index_name = self.request.GET.get('index_name')
                else:
                    raise ValueError(
                        "Please ensure that you have an index_name included in your URL! eg. &index_name=Composite Totals")
                if self.request.GET.get('index_parameter'):
                    self.index_parameter = self.request.GET.get(
                        'index_parameter')
                    self.index_parameter_string = models.HistoricalIndicesInfo._meta.get_field(
                        self.index_parameter).verbose_name
                else:
                    raise ValueError(
                        "Please ensure that you have an index_parameter included in your URL! eg. &index_parameter=index_value")
            # validate input data
            if entered_start_date >= entered_end_date:
                errors += "Your starting date must be before your ending date. Please recheck."
            # Fetch the records
            if self.symbol_needed:
                self.selected_stock = models.ListedEquities.objects.get(
                    symbol=self.selected_symbol)
                self.historical_records = self.model.objects.filter(
                    symbol=self.selected_symbol).filter(date__gte=self.entered_start_date).filter(date__lte=self.entered_end_date).order_by(self.order_by)
            elif self.index_name_needed:
                self.historical_records = self.model.objects.filter(
                    date__gt=self.entered_start_date).filter(date__lte=self.entered_end_date).filter(index_name=self.index_name).order_by(self.order_by)
            else:
                self.historical_records = self.model.objects.filter(
                    date__gt=self.entered_start_date).filter(date__lte=self.entered_end_date).order_by(self.order_by)
            logger.debug(
                "Finished parsing GET parameters. Now loading graph data.")
            # Set up our graph
            graph_labels = [obj.date
                            for obj in self.historical_records]
            # Store the variables for the subclasses to calculate the required dict
            self.set_graph_dataset()
            # add the context keys
            logger.debug("Loading context keys.")
            context['errors'] = errors
            context['listed_stocks'] = listed_stocks
            if self.symbol_needed:
                context['selected_symbol'] = selected_symbol
                context['selected_stock_name'] = self.selected_stock.security_name.title()
                context['selected_stock_symbol'] = self.selected_stock.symbol
            if self.index_name_needed:
                context['index_parameter'] = self.index_parameter
                context['index_parameter_string'] = self.index_parameter_string
                context['index_name'] = self.index_name
            if self.os_parameter_needed:
                context['os_parameter'] = self.os_parameter
                context['os_parameter_string'] = self.os_parameter_string
            context['entered_start_date'] = entered_start_date.strftime(
                '%Y-%m-%d')
            context['entered_end_date'] = entered_end_date.strftime('%Y-%m-%d')
            context['graph_labels'] = graph_labels
            context['graph_dataset'] = self.graph_dataset
            logger.info("Successfully loaded page.")
        except ValueError as verr:
            context['errors'] = ALERTMESSAGE+str(verr)
            logger.warning(
                "Got a value error while loading this page"+str(verr))
        except Exception as ex:
            logger.exception(
                "Sorry. Ran into a problem while attempting to load the page: "+self.template_name)
            context['errors'] = ALERTMESSAGE+str(ex)
        return context


class StockHistoryView(BasicLineChartAndTableView):
    """
    The class for displaying the stock history view website
    """
    template_name = 'base_stockhistory.html'
    model = models.DailyStockSummary  # models.something
    table_class = stocks_tables.HistoricalStockInfoTable  # tables.something
    filterset_class = filters.StockHistoryFilter  # filters.something
    page_name = 'Stock History'  # a string representing the name of the page
    selected_chart_type = 'candlestick'
    request = None

    def get(self, request, *args, **kwargs):
        # get the filters included in the URL.
        # If the required filters are not present, return a redirect
        required_parameters = ['symbol', 'date__gte',
                               'date__lte', 'chart_type', 'sort']
        for parameter in required_parameters:
            try:
                # check that each parameter has a value
                if self.request.GET[parameter]:
                    pass
            except MultiValueDictKeyError:
                logger.warning(
                    "Stock history page requested without all parameters. Sending redirect.")
                # if we are missing any parameters, return a redirect
                base_url = reverse(
                    'stocks:stockhistory', current_app="stocks")
                query_string = urlencode({'symbol': stocks_template_tags.get_session_symbol_or_default(self),
                                          'date__gte': stocks_template_tags.get_session_start_date_or_1_yr_back(self),
                                          'date__lte': stocks_template_tags.get_session_end_date_or_today(self),
                                          'chart_type': 'candlestick', 'sort': '-date'})
                url = '{}?{}'.format(base_url, query_string)
                return redirect(url)
        return super(StockHistoryView, self).get(request)

    def get_context_data(self, *args, **kwargs):
        try:
            # get the current context
            context = super().get_context_data(
                *args, **kwargs)
            # check the chart type selected if the configure button was clicked
            self.selected_chart_type = self.request.GET.get('chart_type')
            # store the session variable
            self.request.session['chart_type'] = self.selected_chart_type
            # store the context variable
            context['chart_type'] = self.selected_chart_type
            context['chart_dates'] = []
            context['open_prices'] = []
            context['close_prices'] = []
            context['highs'] = []
            context['lows'] = []
            if self.selected_chart_type == 'candlestick':
                # query and filter the records from the db
                selected_records = models.DailyStockSummary.objects.filter(
                    symbol=self.selected_symbol).filter(date__gte=self.entered_start_date)\
                    .filter(date__lte=self.entered_end_date).order_by(self.order_by)
                # store the required values for the chart
                context['chart_dates'] = [d.strftime('%Y-%m-%d') for d in selected_records.values_list(
                    'date', flat=True)]
                context['open_prices'] = [float(num) for num in selected_records.values_list(
                    'open_price', flat=True)]
                context['close_prices'] = [float(num) for num in selected_records.values_list(
                    'close_price', flat=True)]
                context['lows'] = [float(num) for num in selected_records.values_list(
                    'low', flat=True)]
                context['highs'] = [float(num) for num in selected_records.values_list(
                    'high', flat=True)]
        except ValueError as verr:
            context['errors'] = ALERTMESSAGE+str(verr)
            logger.warning(
                "Got a value error while loading this page: "+str(verr))
        except Exception as ex:
            logger.exception(
                "Sorry. Ran into a problem while attempting to load the page: "+self.template_name)
            context['errors'] = ALERTMESSAGE+str(ex)
        return context

    def set_symbol_needed(self, bool_input_arg):
        self.symbol = True

    def set_graph_dataset(self,):
        self.graph_dataset = [float(obj.close_price)
                              for obj in self.historical_records]


class DividendHistoryView(BasicLineChartAndTableView):
    """
    The class for displaying the dividend history view website
    """
    template_name = 'base_dividendhistory.html'
    model = models.HistoricalDividendInfo  # models.something
    table_class = stocks_tables.HistoricalDividendInfoTable  # tables.something
    filterset_class = filters.DividendHistoryFilter  # filters.something
    page_name = 'Dividend History'  # a string representing the name of the page

    def get(self, request, *args, **kwargs):
        # get the filters included in the URL.
        # If the required filters are not present, return a redirect
        required_parameters = ['symbol', 'record_date__gte',
                               'record_date__lte', 'sort']
        for parameter in required_parameters:
            try:
                # check that each parameter has a value
                if self.request.GET[parameter]:
                    pass
            except MultiValueDictKeyError:
                logger.warning(
                    "Dividend history page requested without all parameters. Sending redirect.")
                # if we are missing any parameters, return a redirect
                base_url = reverse(
                    'stocks:dividendhistory', current_app="stocks")
                query_string = urlencode({'symbol': stocks_template_tags.get_session_symbol_or_default(self),
                                          'record_date__gte': stocks_template_tags.get_5_yr_back(),
                                          'record_date__lte': stocks_template_tags.get_today(),
                                          'sort': '-record_date'})
                url = '{}?{}'.format(base_url, query_string)
                return redirect(url)
        return super(DividendHistoryView, self).get(request)

    def get_context_data(self, *args, **kwargs):
        try:
            errors = ""
            context = super().get_context_data(**kwargs)
            logger.debug("Now loading all listed equities.")
            listed_stocks = models.ListedEquities.objects.all().order_by('symbol')
            # now load all the data for the subclasses (pages)
            # note that different pages require different data, so we check which data is needed for the page
            # check if the configuration button was clicked
            logger.debug(
                "Checking which GET parameters were included in the request.")
            if self.request.GET.get("configure_button"):
                entered_start_date = datetime.strptime(
                    self.request.GET.get('record_date__gte'), "%Y-%m-%d")
                # store the date as a session variable to be reused
                self.request.session['entered_start_date'] = entered_start_date.strftime(
                    '%Y-%m-%d')
                self.entered_start_date = entered_start_date
            # else look for the starting date in the GET variables
            elif self.request.GET.get('record_date__gte'):
                entered_start_date = datetime.strptime(
                    self.request.GET.get('record_date__gte'), "%Y-%m-%d")
                self.request.session['entered_start_date'] = entered_start_date.strftime(
                    '%Y-%m-%d')
                self.entered_start_date = entered_start_date
            else:
                # else raise an error
                raise ValueError(
                    " Please ensure that you have included a starting date in the URL! For example: ?record_date__gte=2019-05-12")
            # check if the configuration button was clicked
            if self.request.GET.get("configure_button"):
                entered_end_date = datetime.strptime(
                    self.request.GET.get('record_date__lte'), "%Y-%m-%d")
                self.request.session['entered_end_date'] = entered_end_date.strftime(
                    '%Y-%m-%d')
                self.entered_end_date = entered_end_date
            # else look for the ending date in the GET variables
            elif self.request.GET.get('record_date__lte'):
                entered_end_date = datetime.strptime(
                    self.request.GET.get('record_date__lte'), "%Y-%m-%d")
                self.request.session['entered_end_date'] = entered_end_date.strftime(
                    '%Y-%m-%d')
                self.entered_end_date = entered_end_date
            else:
                raise ValueError(
                    " Please ensure that you have included an ending date in the URL! For example: ?record_date__lte=2020-05-12")
            # check if the configuration button was clicked
            if self.symbol_needed:
                if self.request.GET.get("configure_button"):
                    selected_symbol = self.request.GET.get('symbol')
                    self.selected_symbol = selected_symbol
                    self.request.session['selected_symbol'] = selected_symbol
                # else look for the stock code in the GET variables
                elif self.request.GET.get('symbol'):
                    selected_symbol = self.request.GET.get('symbol')
                    self.selected_symbol = selected_symbol
                    self.request.session['selected_symbol'] = selected_symbol
                else:
                    raise ValueError(
                        " Please ensure that you have included a symbol in the URL! For example: ?symbol=ACL")
            if self.request.GET.get('sort'):
                self.order_by = self.request.GET.get('sort')
            else:
                raise ValueError(
                    " Please ensure that you have included a sort order in the URL! For example: ?sort=date")
            # validate input data
            if entered_start_date >= entered_end_date:
                errors += "Your starting date must be before your ending date. Please recheck."
            # Fetch the records
            if self.symbol_needed:
                self.selected_stock = models.ListedEquities.objects.get(
                    symbol=self.selected_symbol)
                self.historical_records = self.model.objects.filter(
                    symbol=self.selected_symbol).filter(record_date__gte=self.entered_start_date).filter(record_date__lte=self.entered_end_date).order_by(self.order_by)
            logger.debug(
                "Finished parsing GET parameters. Now loading graph data.")
            # Set up our graph
            graph_labels = [obj.record_date
                            for obj in self.historical_records]
            # Store the variables for the subclasses to calculate the required dict
            self.set_graph_dataset()
            # add the context keys
            logger.debug("Loading context keys.")
            context['errors'] = errors
            context['listed_stocks'] = listed_stocks
            if self.symbol_needed:
                context['selected_symbol'] = selected_symbol
                context['selected_stock_name'] = self.selected_stock.security_name.title()
                context['selected_stock_symbol'] = self.selected_stock.symbol
            context['entered_start_date'] = entered_start_date.strftime(
                '%Y-%m-%d')
            context['entered_end_date'] = entered_end_date.strftime('%Y-%m-%d')
            context['graph_labels'] = graph_labels
            context['graph_dataset'] = self.graph_dataset
            logger.info("Successfully loaded page.")
        except ValueError as verr:
            context['errors'] = ALERTMESSAGE+str(verr)
            logger.warning(
                "Got a value error while loading this page"+str(verr))
        except Exception as ex:
            logger.exception(
                "Sorry. Ran into a problem while attempting to load the page: "+self.template_name)
            context['errors'] = ALERTMESSAGE+str(ex)
        return context

    def set_symbol_needed(self, bool_input_arg):
        self.symbol_needed = True

    def set_graph_dataset(self,):
        self.graph_dataset = [
            float(obj.dividend_amount) for obj in self.historical_records]


class DividendYieldHistoryView(BasicLineChartAndTableView):
    """
    The class for displaying the dividend history view website
    """
    template_name = 'base_dividendyieldhistory.html'
    model = models.DividendYield  # models.something
    table_class = stocks_tables.HistoricalDividendYieldTable  # tables.something
    filterset_class = filters.DividendYieldFilter  # filters.something
    # a string representing the name of the page
    page_name = 'Dividend Yield History'

    def get(self, request, *args, **kwargs):
        # get the filters included in the URL.
        # If the required filters are not present, return a redirect
        required_parameters = ['symbol', 'date__gte',
                               'date__lte', 'sort']
        for parameter in required_parameters:
            try:
                # check that each parameter has a value
                if self.request.GET[parameter]:
                    pass
            except MultiValueDictKeyError:
                logger.warning(
                    "Dividend yield history page requested without all parameters. Sending redirect.")
                # if we are missing any parameters, return a redirect
                base_url = reverse(
                    'stocks:dividendyieldhistory', current_app="stocks")
                query_string = urlencode({'symbol': stocks_template_tags.get_session_symbol_or_default(self),
                                          'date__gte': stocks_template_tags.get_session_start_date_or_1_yr_back(self),
                                          'date__lte': stocks_template_tags.get_session_end_date_or_today(self),
                                          'sort': 'date'})
                url = '{}?{}'.format(base_url, query_string)
                return redirect(url)
        return super(DividendYieldHistoryView, self).get(request)

    def set_symbol_needed(self, bool_input_arg):
        self.symbol_needed = True

    def set_graph_dataset(self,):
        self.graph_dataset = [float(obj.yield_percent)
                              for obj in self.historical_records]


class MarketIndexHistoryView(BasicLineChartAndTableView):
    """
    Set up the data for the market indices history page
    """
    template_name = 'stocks/base_marketindexhistory.html'
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
        required_parameters = ['index_name', 'index_parameter', 'date__gte',
                               'date__lte', 'sort']
        for parameter in required_parameters:
            try:
                # check that each parameter has a value
                if self.request.GET[parameter]:
                    pass
            except MultiValueDictKeyError:
                logger.warning(
                    "Market history page requested without all parameters. Sending redirect.")
                # if we are missing any parameters, return a redirect
                base_url = reverse(
                    'stocks:marketindexhistory', current_app="stocks")
                query_string = urlencode({'index_name': 'Composite Totals', 'index_parameter': 'index_value',
                                          'date__gte': stocks_template_tags.get_session_start_date_or_1_yr_back(self),
                                          'date__lte': stocks_template_tags.get_session_end_date_or_today(self),
                                          'sort': '-date'})
                url = '{}?{}'.format(base_url, query_string)
                return redirect(url)
        return super(MarketIndexHistoryView, self).get(request)

    def set_graph_dataset(self,):
        self.graph_dataset = [float(obj[self.index_parameter])
                              for obj in self.historical_records.values()]


class OSTradesHistoryView(BasicLineChartAndTableView):
    """
    Set up the data for the outstanding trades history page
    """
    template_name = 'stocks/base_ostradeshistory.html'
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
        required_parameters = ['symbol', 'date__gte',
                               'date__lte', 'os_parameter', 'sort']
        for parameter in required_parameters:
            try:
                # check that each parameter has a value
                if self.request.GET[parameter]:
                    pass
            except MultiValueDictKeyError:
                logger.warning(
                    "Outstanding history page requested without all parameters. Sending redirect.")
                # if we are missing any parameters, return a redirect
                base_url = reverse(
                    'stocks:ostradeshistory', current_app="stocks")
                query_string = urlencode({'symbol': stocks_template_tags.get_session_symbol_or_default(self),
                                          'date__gte': stocks_template_tags.get_session_start_date_or_1_yr_back(self),
                                          'date__lte': stocks_template_tags.get_session_end_date_or_today(self),
                                          'os_parameter': 'os_offer_vol', 'sort': '-date'})
                url = '{}?{}'.format(base_url, query_string)
                return redirect(url)
        return super(OSTradesHistoryView, self).get(request)

    def set_graph_dataset(self,):
        self.graph_dataset = [obj[self.os_parameter]
                              for obj in self.historical_records.values()]


class FundamentalHistoryView(BasicLineChartAndTableView):
    """
    Set up the data for the fundamental history page
    """
    template_name = 'stocks/base_fundamentalhistory.html'
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
        required_parameters = ['symbol1', 'symbol2','indicator', 'date__gte',
                               'date__lte']
        for parameter in required_parameters:
            try:
                # check that each parameter has a value
                if self.request.GET[parameter]:
                    pass
            except MultiValueDictKeyError:
                logger.warning(
                    "Fundamental indicators history page requested without all parameters. Sending redirect.")
                # if we are missing any parameters, return a redirect
                base_url = reverse(
                    'stocks:fundamentalhistory', current_app="stocks")
                query_string = urlencode({'symbol1': stocks_template_tags.get_session_symbol_or_default(self),
                                        'symbol2': 'WCO',
                                        'indicator':'EPS',
                                        'date__gte': stocks_template_tags.get_5_yr_back(),
                                        'date__lte': stocks_template_tags.get_today()})
                url = '{}?{}'.format(base_url, query_string)
                return redirect(url)
        return super(FundamentalHistoryView, self).get(request)

    def get_context_data(self, *args, **kwargs):
        try:
            errors = ""
            # get the current context
            context = super().get_context_data(
                *args, **kwargs)
            logger.debug(
                "Checking which GET parameters were included in the request.")
            if self.request.GET.get('symbol1'):
                self.symbol1 = self.request.GET.get('symbol1')
                # store the session variable
                self.request.session['symbol1'] = self.symbol1
            if self.request.GET.get('symbol2'):
                self.symbol2 = self.request.GET.get('symbol2')
                # store the session variable
                self.request.session['symbol2'] = self.symbol2
            if self.request.GET.get('indicator'):
                self.selected_indicator = self.request.GET.get('indicator')
                # store the session variable
                self.request.session['selected_indicator'] = self.selected_indicator
            if self.request.GET.get('date__gte'):
                entered_start_date = datetime.strptime(
                    self.request.GET.get('date__gte'), "%Y-%m-%d")
                self.request.session['entered_start_date'] = entered_start_date.strftime(
                    '%Y-%m-%d')
                self.entered_start_date = entered_start_date
            if self.request.GET.get('date__lte'):
                entered_end_date = datetime.strptime(
                    self.request.GET.get('date__lte'), "%Y-%m-%d")
                self.request.session['entered_end_date'] = entered_end_date.strftime(
                    '%Y-%m-%d')
                self.entered_end_date = entered_end_date
            # validate input data
            if entered_start_date >= entered_end_date:
                errors += "Your starting date must be before your ending date. Please recheck."
            logger.debug(
                "Finished parsing GET parameters. Now loading graph data.")
            # fetch data from the db
            listed_stocks = models.ListedEquities.objects.all().order_by('symbol')
            historical_records_1 = self.model.objects.filter(
                    symbol=self.symbol1).filter(date__gte=self.entered_start_date).filter(date__lte=self.entered_end_date)
            historical_records_2 = self.model.objects.filter(
                    symbol=self.symbol2).filter(date__gte=self.entered_start_date).filter(date__lte=self.entered_end_date)
            historical_close_prices_1 = models.DailyStockSummary.objects.filter(symbol=self.symbol1).filter(date__gte=self.entered_start_date).filter(date__lte=self.entered_end_date)
            historical_close_prices_2 = models.DailyStockSummary.objects.filter(symbol=self.symbol2).filter(date__gte=self.entered_start_date).filter(date__lte=self.entered_end_date)
            # set up a list of all the valid indicators
            all_indicators = []
            for field in self.model._meta.fields:
                if field.column not in ['id','symbol','date']:
                    temp_indicator = dict()
                    temp_indicator['field_name'] = field.column
                    temp_indicator['verbose_name'] = field.verbose_name
                    all_indicators.append(temp_indicator)
                    if field.column == self.selected_indicator:
                        self.selected_indicator_verbose_name = field.verbose_name
            # Set up our graph
            graph_labels_1 = [obj.date
                            for obj in historical_records_1]
            graph_labels_2 = [obj.date
                            for obj in historical_records_2]
            graph_dataset_1 = [obj[self.selected_indicator]
                              for obj in historical_records_1.values()]
            graph_dataset_2 = [obj[self.selected_indicator]
                              for obj in historical_records_2.values()]
            graph_labels_3 = [obj.date
                            for obj in historical_close_prices_1]
            graph_close_prices_1 = [obj['close_price']
                              for obj in historical_close_prices_1.values()]
            graph_close_prices_2 = [obj['close_price']
                              for obj in historical_close_prices_2.values()]
            # add the context keys
            logger.debug("Loading context keys.")
            context['errors'] = errors
            context['listed_stocks'] = listed_stocks
            context['entered_start_date'] = entered_start_date.strftime(
                '%Y-%m-%d')
            context['entered_end_date'] = entered_end_date.strftime('%Y-%m-%d')
            context['graph_labels_1'] = graph_labels_1
            context['graph_labels_2'] = graph_labels_2
            context['graph_labels_3'] = graph_labels_3
            context['symbol1'] = self.symbol1
            context['symbol2'] = self.symbol2
            context['all_indicators'] = all_indicators
            context['selected_indicator'] = self.selected_indicator
            context['selected_indicator_verbose_name'] = self.selected_indicator_verbose_name
            context['graph_dataset_1'] = graph_dataset_1
            context['graph_dataset_2'] = graph_dataset_2
            context['graph_close_prices_1'] = graph_close_prices_1
            context['graph_close_prices_2'] = graph_close_prices_2
            logger.info("Successfully loaded page.")
        except ValueError as verr:
            context['errors'] = ALERTMESSAGE+str(verr)
            logger.warning(
                "Got a value error while loading this page"+str(verr))
        except Exception as ex:
            logger.exception(
                "Sorry. Ran into a problem while attempting to load the page: "+self.template_name)
            context['errors'] = ALERTMESSAGE+str(ex)
        return context

class AboutPageView(TemplateView):
    """
    Set up the data for the technical analysis summary page
    """
    template_name = 'stocks/base_about.html'


# CONSTANTS
ALERTMESSAGE = "Sorry! An error was encountered while processing your request."

# Global variables

# Create functions used by the views here

# Create your view functions here.
