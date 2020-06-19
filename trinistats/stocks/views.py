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
# Imports from local machine
from stocks import models, tables, filters

# Set up logging
logger = logging.getLogger('root')

# Class definitions


class DailyTradingSummaryView(ExportMixin, tables2.views.SingleTableMixin, FilterView):
    """
    Set up the data for the Daily Equity Summary page
    """
    template_name = 'stocks/base_dailytradingsummary.html'
    model = models.DailyTradingSummary
    table_class = tables.DailyTradingSummaryTable
    filterset_class = filters.DailyTradingSummaryFilter

    def get_context_data(self, *args, **kwargs):
        try:
            errors = ""
            logger.info("Daily trading summary page was called")
            # get the current context
            context = super().get_context_data(
                *args, **kwargs)
            # get the filters included in the URL. If the required filters are not present, raise an error
            if self.request.GET.get('date'):
                selecteddate = datetime.strptime(
                    self.request.GET.get('date'), "%Y-%m-%d")
            else:
                raise ValueError(
                    " Please ensure that you have set a date in the URL! For example: stocks/dailyequitysummary?date=2020-05-12 ")
            # Now select the records corresponding to the selected date
            # as well as their symbols, and order by the highest volume traded
            dailytradingsummaryrecords = models.DailyTradingSummary.objects.exclude(wastradedtoday=0).filter(
                date=selecteddate).select_related('stockcode').order_by('-valuetraded')
            if not dailytradingsummaryrecords:
                raise ValueError(
                    "No data available for the date selected. Please press the back button and choose another date.")
            # rename the symbol field properly and select only required fields
            selectedrecords = dailytradingsummaryrecords.annotate(
                symbol=F('stockcode__symbol')).values('symbol', 'valuetraded')
            # check if an export request was received
            # Set the graph
            # get the top 10 records by value traded
            graphsymbols = [record['symbol']
                            for record in selectedrecords[:10]]
            graphvaluetraded = [record['valuetraded']
                                for record in selectedrecords[:10]]
            # create a category for the sum of all other symbols (not in the top 10)
            others = dict(symbol='Others', valuetraded=0)
            for record in selectedrecords:
                if (record['symbol'] not in graphsymbols) and record['valuetraded']:
                    others['valuetraded'] += record['valuetraded']
            # add the 'other' category to the graph
            graphsymbols.append(others['symbol'])
            graphvaluetraded.append(others['valuetraded'])
            # get a human readable date
            selecteddateparsed = selecteddate.strftime('%Y-%m-%d')
            # Now add our context data and return a response
            context['errors'] = errors
            context['selecteddate'] = selecteddate.date()
            context['selecteddateparsed'] = selecteddateparsed
            context['graphsymbols'] = graphsymbols
            context['graphvaluetraded'] = graphvaluetraded
            logger.info("Successfully loaded page.")
        except ValueError as verr:
            context['errors'] = ALERTMESSAGE+str(verr)
            logger.warning(
                "Got a valueerror while loading this page"+str(verr))
        except Exception as ex:
            logger.exception(
                "Sorry. Ran into a problem while attempting to load the page: "+template_name)
            context['errors'] = ALERTMESSAGE+str(ex)
        return context


class ListedStocksView(ExportMixin, tables2.MultiTableMixin, FilterView):
    """
    Set up the data for the Listed Stocks page
    """
    template_name = 'stocks/base_listedstocks.html'
    model = models.ListedEquities
    tables = [tables.ListedStocksTable]
    table_pagination = False
    filterset_class = filters.ListedStocksFilter

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
                "Sorry. Ran into a problem while attempting to load the page: "+template_name)
            context['errors'] = ALERTMESSAGE+str(ex)
        return context


class TechnicalAnalysisSummary(ExportMixin, tables2.views.SingleTableMixin, FilterView):
    """
    Set up the data for the technical analysis summary page
    """
    template_name = 'stocks/base_technicalanalysissummary.html'
    model = models.DailyTradingSummary
    table_class = tables.DailyTradingSummaryTable
    filterset_class = filters.DailyTradingSummaryFilter

    def get_context_data(self, *args, **kwargs):
        try:
            errors = ""
            logger.info("Daily equity summary page was called")
            # get the current context
            context = super().get_context_data(
                *args, **kwargs)
            # get the filters included in the URL. If the required filters are not present, raise an error
            if self.request.GET.get('date'):
                selecteddate = datetime.strptime(
                    self.request.GET.get('date'), "%Y-%m-%d")
            else:
                raise ValueError(
                    " Please ensure that you have set a date in the URL! For example: stocks/dailyequitysummary?date=2020-05-12 ")
            # Now select the records corresponding to the selected date
            # as well as their symbols, and order by the highest volume traded
            dailyequitysummaryrecords = models.DailyTradingSummary.objects.exclude(wastradedtoday=0).filter(
                date=selecteddate).select_related('stockcode').order_by('-valuetraded')
            if not dailyequitysummaryrecords:
                raise ValueError(
                    "No data available for the date selected. Please press the back button and choose another date.")
            # rename the symbol field properly and select only required fields
            selectedrecords = dailyequitysummaryrecords.annotate(
                symbol=F('stockcode__symbol')).values('symbol', 'valuetraded')
            # check if an export request was received
            # Set the graph
            # get the top 10 records by value traded
            graphsymbols = [record['symbol']
                            for record in selectedrecords[:10]]
            graphvaluetraded = [record['valuetraded']
                                for record in selectedrecords[:10]]
            # create a category for the sum of all other symbols (not in the top 10)
            others = dict(symbol='Others', valuetraded=0)
            for record in selectedrecords:
                if (record['symbol'] not in graphsymbols) and record['valuetraded']:
                    others['valuetraded'] += record['valuetraded']
            # add the 'other' category to the graph
            graphsymbols.append(others['symbol'])
            graphvaluetraded.append(others['valuetraded'])
            # get a human readable date
            selecteddateparsed = selecteddate.strftime('%Y-%m-%d')
            # Now add our context data and return a response
            context['errors'] = errors
            context['selecteddate'] = selecteddate.date()
            context['selecteddateparsed'] = selecteddateparsed
            context['graphsymbols'] = graphsymbols
            context['graphvaluetraded'] = graphvaluetraded
            logger.info("Successfully loaded page.")
        except ValueError as verr:
            context['errors'] = ALERTMESSAGE+str(verr)
            logger.warning(
                "Got a valueerror while loading this page"+str(verr))
        except Exception as ex:
            logger.exception(
                "Sorry. Ran into a problem while attempting to load the page: "+template_name)
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
    historicalrecords = None
    selectedstockcode = None
    selectedstock = None
    graph_dataset = []  # the list of dictionary of objects to display in the graph
    stock_code_needed = True
    os_parameter_needed = False
    osparameter = None
    os_parameter_string = None
    indexname = None
    index_name_needed = False
    indexparameter = None
    index_parameter_string = None
    enteredstartdate = None
    enteredenddate = None
    orderby = None

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
            logger.debug("Now loading all listedequities.")
            listedstocks = models.ListedEquities.objects.all().order_by('symbol')
            # now load all the data for the subclasses (pages)
            # note that different pages require different data, so we check which data is needed for the page
            # check if the configuration button was clicked
            logger.debug(
                "Checking which GET parameters were included in the request.")
            if self.request.GET.get("configure_button"):
                enteredstartdate = datetime.strptime(
                    self.request.GET.get('date__gte'), "%Y-%m-%d")
                # store the date as a session variable to be reused
                self.request.session['enteredstartdate'] = enteredstartdate.strftime(
                    '%Y-%m-%d')
                self.enteredstartdate = enteredstartdate
            # else look for the starting date in the GET variables
            elif self.request.GET.get('date__gte'):
                enteredstartdate = datetime.strptime(
                    self.request.GET.get('date__gte'), "%Y-%m-%d")
                self.request.session['enteredstartdate'] = enteredstartdate.strftime(
                    '%Y-%m-%d')
                self.enteredstartdate = enteredstartdate
            else:
                # else raise an error
                raise ValueError(
                    " Please ensure that you have included a starting date in the URL! For example: ?date__gte=2019-05-12")
            # check if the configuration button was clicked
            if self.request.GET.get("configure_button"):
                enteredenddate = datetime.strptime(
                    self.request.GET.get('date__lte'), "%Y-%m-%d")
                self.request.session['enteredenddate'] = enteredenddate.strftime(
                    '%Y-%m-%d')
                self.enteredenddate = enteredenddate
            # else look for the ending date in the GET variables
            elif self.request.GET.get('date__lte'):
                enteredenddate = datetime.strptime(
                    self.request.GET.get('date__lte'), "%Y-%m-%d")
                self.request.session['enteredenddate'] = enteredenddate.strftime(
                    '%Y-%m-%d')
                self.enteredenddate = enteredenddate
            else:
                raise ValueError(
                    " Please ensure that you have included an ending date in the URL! For example: ?date__lte=2020-05-12")
            # check if the configuration button was clicked
            if self.stock_code_needed:
                if self.request.GET.get("configure_button"):
                    selectedstockcode = int(self.request.GET.get('stockcode'))
                    self.selectedstockcode = selectedstockcode
                    self.request.session['selectedstockcode'] = selectedstockcode
                # else look for the stock code in the GET variables
                elif self.request.GET.get('stockcode'):
                    selectedstockcode = int(self.request.GET.get('stockcode'))
                    self.selectedstockcode = selectedstockcode
                    self.request.session['selectedstockcode'] = selectedstockcode
                else:
                    raise ValueError(
                        " Please ensure that you have included a stockcode in the URL! For example: ?stockcode=169")
            if self.request.GET.get('sort'):
                self.orderby = self.request.GET.get('sort')
            else:
                raise ValueError(
                    " Please ensure that you have included a sort order in the URL! For example: ?sort=date")
            if self.os_parameter_needed:
                if self.request.GET.get('osparameter'):
                    self.osparameter = self.request.GET.get('osparameter')
                    self.os_parameter_string = models.DailyTradingSummary._meta.get_field(
                        self.osparameter).verbose_name
            if self.index_name_needed:
                if self.request.GET.get('indexname'):
                    self.indexname = self.request.GET.get('indexname')
                else:
                    raise ValueError(
                        "Please ensure that you have an indexname included in your URL! eg. &indexname=Composite Totals")
                if self.request.GET.get('indexparameter'):
                    self.indexparameter = self.request.GET.get(
                        'indexparameter')
                    self.index_parameter_string = models.HistoricalMarketSummary._meta.get_field(
                        self.indexparameter).verbose_name
                else:
                    raise ValueError(
                        "Please ensure that you have an indexparameter included in your URL! eg. &indexparameter=indexvalue")
            # validate input data
            if enteredstartdate >= enteredenddate:
                errors += "Your starting date must be before your ending date. Please recheck."
            # Fetch the records
            if self.stock_code_needed:
                self.selectedstock = models.ListedEquities.objects.get(
                    stockcode=self.selectedstockcode)
                self.historicalrecords = self.model.objects.filter(
                    stockcode=self.selectedstockcode).filter(date__gte=self.enteredstartdate).filter(date__lte=self.enteredenddate).order_by(self.orderby)
            elif self.index_name_needed:
                self.historicalrecords = self.model.objects.filter(
                    date__gt=self.enteredstartdate).filter(date__lte=self.enteredenddate).filter(indexname=self.indexname).order_by(self.orderby)
            else:
                self.historicalrecords = self.model.objects.filter(
                    date__gt=self.enteredstartdate).filter(date__lte=self.enteredenddate).order_by(self.orderby)
            logger.debug(
                "Finished parsing GET parameters. Now loading graph data.")
            # Set up our graph
            graphlabels = [obj.date.strftime('%Y-%m-%d')
                           for obj in self.historicalrecords]
            # Store the variables for the subclasses to calculate the required dict
            self.set_graph_dataset()
            # add the context keys
            logger.debug("Loading context keys.")
            context['errors'] = errors
            context['listedstocks'] = listedstocks
            if self.stock_code_needed:
                context['selectedstockcode'] = selectedstockcode
                context['selectedstockname'] = self.selectedstock.securityname.title()
                context['selectedstocksymbol'] = self.selectedstock.symbol
            if self.index_name_needed:
                context['indexparameter'] = self.indexparameter
                context['index_parameter_string'] = self.index_parameter_string
                context['indexname'] = self.indexname
            if self.os_parameter_needed:
                context['osparameter'] = self.osparameter
                context['os_parameter_string'] = self.os_parameter_string
            context['enteredstartdate'] = enteredstartdate.strftime('%Y-%m-%d')
            context['enteredenddate'] = enteredenddate.strftime('%Y-%m-%d')
            context['graphlabels'] = graphlabels
            context['graphdataset'] = self.graph_dataset
            logger.info("Successfully loaded page.")
        except ValueError as verr:
            context['errors'] = ALERTMESSAGE+str(verr)
            logger.warning(
                "Got a valueerror while loading this page"+str(verr))
        except Exception as ex:
            logger.exception(
                "Sorry. Ran into a problem while attempting to load the page: "+template_name)
            context['errors'] = ALERTMESSAGE+str(ex)
        return context


class StockHistoryView(BasicLineChartAndTableView):
    """
    The class for displaying the stock history view website
    """
    template_name = 'base_stockhistory.html'
    model = models.DailyTradingSummary  # models.something
    table_class = tables.HistoricalStockInfoTable  # tables.something
    filterset_class = filters.StockHistoryFilter  # filters.something
    page_name = 'Stock History'  # a string representing the name of the page
    selected_chart_type = 'candlestick'

    def get_context_data(self, *args, **kwargs):
        try:
            errors = ""
            # get the current context
            context = super().get_context_data(
                *args, **kwargs)
            if self.request.GET.get("charttype"):
                # check the chart type selected if the configure button was clicked
                self.selected_chart_type = self.request.GET.get('charttype')
                # store the session variable
                self.request.session['charttype'] = self.selected_chart_type
                # store the context variable
                context['chart_type'] = self.selected_chart_type
            context['dates'] = []
            context['open_prices'] = []
            context['close_prices'] = []
            context['highs'] = []
            context['lows'] = []
            if self.selected_chart_type == 'candlestick':
                # query and filter the records from the db
                selected_records = models.DailyTradingSummary.objects.filter(
                    stockcode=self.selectedstockcode).filter(date__gte=self.enteredstartdate)\
                    .filter(date__lte=self.enteredenddate).order_by(self.orderby)
                # store the required values for the chart
                context['dates'] = [d.strftime('%Y-%m-%d') for d in selected_records.values_list(
                    'date', flat=True)]
                context['open_prices'] = [float(num) for num in selected_records.values_list(
                    'openprice', flat=True)]
                context['close_prices'] = [float(num) for num in selected_records.values_list(
                    'closeprice', flat=True)]
                context['lows'] = [float(num) for num in selected_records.values_list(
                    'low', flat=True)]
                context['highs'] = [float(num) for num in selected_records.values_list(
                    'high', flat=True)]
        except ValueError as verr:
            context['errors'] = ALERTMESSAGE+str(verr)
            logger.warning(
                "Got a valueerror while loading this page"+str(verr))
        except Exception as ex:
            logger.exception(
                "Sorry. Ran into a problem while attempting to load the page: "+template_name)
            context['errors'] = ALERTMESSAGE+str(ex)
        return context

    def set_stock_code_needed(self, bool_input_arg):
        self.stock_code_needed = True

    def set_graph_dataset(self,):
        self.graph_dataset = [dict(data=[float(obj.closeprice) for obj in self.historicalrecords],
                                   borderColor='rgb(0, 0, 0)',
                                   backgroundColor='rgb(255, 0, 0)',
                                   label=self.selectedstock.securityname.title()+'('+self.selectedstock.symbol+')')]


class DividendHistoryView(BasicLineChartAndTableView):
    """
    The class for displaying the dividend history view website
    """
    template_name = 'base_dividendhistory.html'
    model = models.HistoricalDividendInfo  # models.something
    table_class = tables.HistoricalDividendInfoTable  # tables.something
    filterset_class = filters.DividendHistoryFilter  # filters.something
    page_name = 'Dividend History'  # a string representing the name of the page

    def set_stock_code_needed(self, bool_input_arg):
        self.stock_code_needed = True

    def set_graph_dataset(self,):
        self.graph_dataset = [dict(data=[float(obj.dividendamount) for obj in self.historicalrecords],
                                   borderColor='rgb(0, 0, 0)',
                                   backgroundColor='rgb(255, 0, 0)',
                                   label=self.selectedstock.securityname+'('+self.selectedstock.symbol+')')]


class DividendYieldHistoryView(BasicLineChartAndTableView):
    """
    The class for displaying the dividend history view website
    """
    template_name = 'base_dividendyieldhistory.html'
    model = models.DividendYield  # models.something
    table_class = tables.HistoricalDividendYieldTable  # tables.something
    filterset_class = filters.DividendYieldHistoryFilter  # filters.something
    # a string representing the name of the page
    page_name = 'Dividend Yield History'

    def set_stock_code_needed(self, bool_input_arg):
        self.stock_code_needed = True

    def set_graph_dataset(self,):
        self.graph_dataset = [dict(data=[float(obj.yieldpercent) for obj in self.historicalrecords],
                                   borderColor='rgb(0, 0, 0)',
                                   backgroundColor='rgb(255, 0, 0)',
                                   label=self.selectedstock.securityname+'('+self.selectedstock.symbol+')')]


class MarketIndexHistoryView(BasicLineChartAndTableView):
    """
    Set up the data for the market indices history page
    """
    template_name = 'stocks/base_marketindexhistory.html'
    model = models.HistoricalMarketSummary
    table_class = tables.HistoricalMarketSummaryTable
    filterset_class = filters.MarketIndexHistoryFilter

    def __init__(self):
        super(MarketIndexHistoryView, self).__init__()
        self.stock_code_needed = False
        self.index_name_needed = True

    def set_graph_dataset(self,):
        self.graph_dataset = []
        graphdict = dict(data=[float(obj[self.indexparameter]) for obj in self.historicalrecords.values()],
                         borderColor='rgb(0, 0, 0)',
                         backgroundColor='rgb(255, 0, 0)',
                         label=self.indexname)
        self.graph_dataset.append(graphdict)


class OSTradesHistoryView(BasicLineChartAndTableView):
    """
    Set up the data for the outstanding trades history page
    """
    template_name = 'stocks/base_ostradeshistory.html'
    model = models.DailyTradingSummary
    table_class = tables.OSTradesHistoryTable
    filterset_class = filters.OSTradesHistoryFilter

    def __init__(self):
        super(OSTradesHistoryView, self).__init__()
        self.stock_code_needed = True
        self.index_name_needed = False
        self.os_parameter_needed = True

    def set_graph_dataset(self,):
        self.graph_dataset = [dict(data=[obj[self.osparameter] for obj in self.historicalrecords.values()],
                                   borderColor='rgb(0, 0, 0)',
                                   backgroundColor='rgb(255, 0, 0)',
                                   label=self.selectedstock.securityname+'('+self.selectedstock.symbol+')')]


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
