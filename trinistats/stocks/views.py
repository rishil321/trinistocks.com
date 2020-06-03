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

# Class definitions


class DailyEquitySummaryView(ExportMixin, tables2.views.SingleTableMixin, FilterView):
    """
    Set up the data for the Daily Equity Summary page
    """
    template_name = 'stocks/base_dailyequitysummary.html'
    model = models.DailyEquitySummary
    table_class = tables.DailyEquitySummaryTable
    filterset_class = filters.DailyEquitySummaryFilter

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
            dailyequitysummaryrecords = models.DailyEquitySummary.objects.filter(
                date=selecteddate).select_related('stockcode').order_by('-valuetraded')
            if not dailyequitysummaryrecords:
                errors += "No data available for the date selected. Please choose another date."
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
        except Exception as ex:
            logging.exception(
                "Sorry. Ran into a problem while attempting to load this page.")
            logger.exception(errors)
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
    selectedstock = None
    graph_dataset = []  # the list of dictionary of objects to display in the graph
    stock_code_needed = None
    osparameter = None

    def set_graph_dataset(self,):
        self.graph_dataset = []

    def set_stock_code_needed(self, bool_input_arg):
        self.stock_code_needed = bool_input_arg

    def get_context_data(self, *args, **kwargs):
        try:
            errors = ""
            # get the current context
            context = super().get_context_data(
                *args, **kwargs)
            logger.info(self.page_name+" page was called.")
            listedstocks = models.ListedEquities.objects.all().order_by('symbol')
            # get the filters included in the URL.
            # If the required filters are not present, raise an error
            # check if the configuration button was clicked
            if self.request.GET.get("configure_button"):
                enteredstartdate = datetime.strptime(
                    self.request.GET.get('date__gte'), "%Y-%m-%d")
                self.request.session['enteredstartdate'] = enteredstartdate.strftime(
                    '%Y-%m-%d')
            # check if we have a starting date stored in the session variable
            elif self.request.session['enteredstartdate']:
                enteredstartdate = datetime.strptime(
                    self.request.session['enteredstartdate'], "%Y-%m-%d")
            # else look for the starting date in the GET variables
            elif self.request.GET.get('date__gte'):
                enteredstartdate = datetime.strptime(
                    self.request.GET.get('date__gte'), "%Y-%m-%d")
                self.request.session['enteredstartdate'] = enteredstartdate.strftime(
                    '%Y-%m-%d')
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
            # check if we have a ending date stored in the session variable
            elif self.request.session['enteredenddate']:
                enteredenddate = datetime.strptime(
                    self.request.session['enteredenddate'], "%Y-%m-%d")
            # else look for the ending date in the GET variables
            elif self.request.GET.get('date__lte'):
                enteredenddate = datetime.strptime(
                    self.request.GET.get('date__lte'), "%Y-%m-%d")
                self.request.session['enteredenddate'] = enteredenddate.strftime(
                    '%Y-%m-%d')
            else:
                raise ValueError(
                    " Please ensure that you have included an ending date in the URL! For example: ?date__lte=2020-05-12")
            # check if the configuration button was clicked
            if self.request.GET.get("configure_button"):
                selectedstockcode = int(self.request.GET.get('stockcode'))
                self.request.session['selectedstockcode'] = selectedstockcode
            # check if we have a stock code stored in the session
            elif self.request.session['selectedstockcode']:
                selectedstockcode = int(
                    self.request.session['selectedstockcode'])
            # else look for the stock code in the GET variables
            elif self.request.GET.get('stockcode'):
                selectedstockcode = int(self.request.GET.get('stockcode'))
                self.request.session['selectedstockcode'] = selectedstockcode
            else:
                if self.stock_code_needed:
                    raise ValueError(
                        " Please ensure that you have included a stockcode in the URL! For example: ?stockcode=169")
            if self.request.GET.get('sort'):
                orderby = self.request.GET.get('sort')
            else:
                raise ValueError(
                    " Please ensure that you have included a sort order in the URL! For example: ?sort=date")
            if self.request.GET.get('osparameter'):
                self.osparameter = self.request.GET.get('osparameter')
            # validate input data
            if enteredstartdate >= enteredenddate:
                errors += "Your starting date must be before your ending date. Please recheck."
            # Fetch the records
            if 'selectedstockcode' in locals():
                self.selectedstock = models.ListedEquities.objects.get(
                    stockcode=selectedstockcode)
                self.historicalrecords = self.model.objects.filter(
                    stockcode=selectedstockcode).filter(date__gt=enteredstartdate).filter(date__lt=enteredenddate).order_by(orderby)
            else:
                self.historicalrecords = self.model.objects.filter(
                    date__gt=enteredstartdate).filter(date__lt=enteredenddate).order_by(orderby)
            # Set up our graph
            graphlabels = [obj.date.strftime('%Y-%m-%d')
                           for obj in self.historicalrecords]
            # Store the variables for the subclasses to calculate the required dict
            self.set_graph_dataset()
            # add the context keys
            context['errors'] = errors
            context['listedstocks'] = listedstocks
            if 'selectedstockcode' in locals():
                context['selectedstockcode'] = selectedstockcode
                context['selectedstockname'] = self.selectedstock.securityname
            context['enteredstartdate'] = enteredstartdate.strftime('%Y-%m-%d')
            context['enteredenddate'] = enteredenddate.strftime('%Y-%m-%d')
            context['graphlabels'] = graphlabels
            context['graphdataset'] = self.graph_dataset
        except Exception as ex:
            logging.exception(
                "Sorry. Ran into a problem while attempting to load this page.")
            logger.exception(errors)
            context['errors'] = ALERTMESSAGE+str(ex)
        return context


class StockHistoryView(BasicLineChartAndTableView):
    """
    The class for displaying the stock history view website
    """
    template_name = 'base_stockhistory.html'
    model = models.HistoricalStockInfo  # models.something
    table_class = tables.HistoricalStockInfoTable  # tables.something
    filterset_class = filters.StockHistoryFilter  # filters.something
    page_name = 'Stock History'  # a string representing the name of the page

    def set_stock_code_needed(self, bool_input_arg):
        self.stock_code_needed = True

    def set_graph_dataset(self,):
        self.graph_dataset = [dict(data=[float(obj.closingquote) for obj in self.historicalrecords],
                                   borderColor='rgb(0, 0, 0)',
                                   backgroundColor='rgb(255, 0, 0)',
                                   label=self.selectedstock.securityname+'('+self.selectedstock.symbol+')')]


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

    def set_stock_code_needed(self, bool_input_arg):
        self.stock_code_needed = False

    def set_graph_dataset(self,):
        self.graph_dataset = []
        # Add the composite totals
        composite_totals_data = []
        all_tnt_totals_data = []
        crosslisted_totals_data = []
        sme_totals_data = []
        for historical_market_record in self.historicalrecords:
            try:
                composite_totals_data.append(
                    float(historical_market_record.compositetotalsindexvalue))
            except TypeError as exc:
                pass
            try:
                all_tnt_totals_data.append(
                    float(historical_market_record.alltnttotalsindexvalue))
            except TypeError as exc:
                pass
            try:
                crosslisted_totals_data.append(
                    float(historical_market_record.crosslistedtotalsindexvalue))
            except TypeError as exc:
                pass
            try:
                sme_totals_data.append(
                    float(historical_market_record.smetotalsindexvalue))
            except TypeError as exc:
                pass
        graphdict = dict(data=composite_totals_data,
                         borderColor='rgb(255, 0, 0)',
                         backgroundColor='transparent',
                         label='Composite Totals Index')
        self.graph_dataset.append(graphdict)
        # Add the TnT totals
        graphdict = dict(data=all_tnt_totals_data,
                         borderColor='rgb(0,255, 0)',
                         backgroundColor='transparent',
                         label='All TnT Totals Index')
        self.graph_dataset.append(graphdict)
        # Cross-listed totals
        graphdict = dict(data=crosslisted_totals_data,
                         borderColor='rgb(0,0,255)',
                         backgroundColor='transparent',
                         label='Cross-listed Totals Index')
        self.graph_dataset.append(graphdict)
        # SME totals
        graphdict = dict(data=sme_totals_data,
                         borderColor='rgb(0,255, 128)',
                         backgroundColor='transparent',
                         label='SME Totals Index')
        self.graph_dataset.append(graphdict)


class OSTradesHistoryView(BasicLineChartAndTableView):
    """
    Set up the data for the outstanding trades history page
    """
    template_name = 'stocks/base_ostradeshistory.html'
    model = models.DailyEquitySummary
    table_class = tables.OSTradesHistoryTable
    filterset_class = filters.OSTradesHistoryFilter

    def set_stock_code_needed(self, bool_input_arg):
        self.stock_code_needed = True

    def set_graph_dataset(self,):
        self.graph_dataset = [dict(data=[obj[self.osparameter] for obj in self.historicalrecords.values()],
                                   borderColor='rgb(0, 0, 0)',
                                   backgroundColor='rgb(255, 0, 0)',
                                   label=self.selectedstock.securityname+'('+self.selectedstock.symbol+')')]


# CONSTANTS
ALERTMESSAGE = "Sorry! An error was encountered while processing your request."

# Global variables
logger = logging.getLogger(__name__)

# Create functions used by the views here

# Create your view functions here.


def ostradeshistory(request):
    try:
        errors = ""
        logger.info("O/S trades history page was called")
        listedstocks = models.ListedEquities.objects.all().order_by('symbol')
        # Check if our request contains any GET variables
        # search the database by those parameters if they are present in the request, and store them for the session
        # Else check if we have any variables stored for the current sessions
        # else provide some defaults as fallback
        if request.GET.get('startdate'):
            enteredstartdate = parse(request.GET.get(
                'startdate')).strftime('%Y-%m-%d')
            request.session['enteredstartdate'] = enteredstartdate
        else:
            enteredstartdate = request.session.get(
                'enteredstartdate', (datetime.now(
                )+dateutil.relativedelta.relativedelta(months=-3)).strftime('%Y-%m-%d'))
        if request.GET.get('enddate'):
            enteredenddate = parse(request.GET.get(
                'enddate')).strftime('%Y-%m-%d')
            request.session['enteredenddate'] = enteredenddate
        else:
            enteredenddate = request.session.get(
                'enteredenddate', datetime.now().strftime('%Y-%m-%d'))
        if request.GET.get('stockcode'):
            selectedstockcode = int(request.GET.get('stockcode'))
            request.session['selectedstockcode'] = selectedstockcode
        else:
            selectedstockcode = request.session.get(
                'selectedstockcode', models.ListedEquities.objects.order_by('symbol').first().stockcode)
        if request.GET.get('sort'):
            orderby = request.GET.get('sort')
        else:
            orderby = 'date'
        if request.GET.get('osparameter'):
            osparameter = request.GET.get('osparameter')
        else:
            osparameter = 'osoffervol'
        selectedstock = models.ListedEquities.objects.get(
            stockcode=selectedstockcode)
        # validate input data
        if datetime.strptime(enteredstartdate, '%Y-%m-%d') >= datetime.strptime(enteredenddate, '%Y-%m-%d'):
            errors += "Your starting date must be before your ending date. Please recheck."
        # Fetch the records
        selectedrecords = models.DailyEquitySummary.objects.filter(stockcode=selectedstockcode).filter(date__gt=enteredstartdate).filter(
            date__lt=enteredenddate).values('date', 'osbid', 'osbidvol', 'osoffer', 'osoffervol').order_by(orderby)
        # Set up our table
        tabledata = tables.OSTradesHistoryTable(
            selectedrecords, order_by=orderby)
        tabledata.paginate(page=request.GET.get("page", 1), per_page=25)
        # Set up our graph
        graphlabels = [obj['date'].strftime(
            '%Y-%m-%d') for obj in selectedrecords]
        graphdataset = []
        # Add the composite totals
        graphdict = dict(data=[obj[osparameter] for obj in selectedrecords],
                         borderColor='rgb(0, 0, 0)',
                         backgroundColor='rgb(255, 0, 0)',
                         label=selectedstock.securityname+'('+selectedstock.symbol+')')
        graphdataset.append(graphdict)
    except Exception as ex:
        errors = ALERTMESSAGE+str(ex)
        logging.critical(traceback.format_exc())
        logger.error(errors)
    # Now add our context data and return a response
    context = {
        'errors': errors,
        'listedstocks': listedstocks,
        'selectedstockcode': selectedstockcode,
        'osparameterstr': osparameter,
        'selectedstockname': selectedstock.securityname,
        'table': tabledata,
        'enteredstartdate': enteredstartdate,
        'enteredenddate': enteredenddate,
        'graphlabels': graphlabels,
        'graphdataset': graphdataset,
    }
    return render(request, "stocks/base_ostradeshistory.html", context)
