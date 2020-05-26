# Imports
from django.shortcuts import render
from django.http import HttpResponse
from random import randint
from django.views.generic import TemplateView
from chartjs.views.lines import BaseLineChartView
from stocks import models
from django.views.generic import ListView
import django_tables2 as tables
from dateutil.parser import parse
import json
from django.core import serializers
from django.http.response import JsonResponse
import logging
from django import forms
from django.db.models import F
from datetime import datetime
import dateutil
import traceback

# Class definitions


class HistoricalStockInfoTable(tables.Table):
    class Meta:
        model = models.Historicalstockinfo
        attrs = {"class": "djangotables"}
        fields = ('date', 'closingquote', 'changedollars',
                  'currency', 'volumetraded')


class HistoricalDividendInfoTable(tables.Table):
    class Meta:
        model = models.Historicaldividendinfo
        attrs = {"class": "djangotables"}
        fields = ('recorddate', 'dividendamount', 'currency')


class HistoricalDividendYieldTable(tables.Table):
    class Meta:
        model = models.Dividendyield
        attrs = {"class": "djangotables"}
        fields = ('yielddate', 'yieldpercent')


class DailyEquitySummaryTable(tables.Table):
    securityname = tables.Column(verbose_name="Equity Name")
    symbol = tables.Column(verbose_name="Symbol")
    volumetraded = tables.Column(verbose_name="Volume Traded")
    saleprice = tables.Column(verbose_name="Sale Price ($)")
    valuetraded = tables.Column(verbose_name="Value Traded ($)")
    low = tables.Column(verbose_name="Low ($)")
    high = tables.Column(verbose_name="High ($)")
    changedollars = tables.Column(verbose_name="Change ($)")

    class Meta:
        attrs = {"class": "djangotables"}


class HistoricalMarketSummaryTable(tables.Table):
    class Meta:
        model = models.Historicalmarketsummary
        attrs = {"class": "djangotables"}
        fields = ('date', 'compositetotalsindexvalue', 'alltnttotalsindexvalue',
                  'crosslistedtotalsindexvalue', 'smetotalsindexvalue')


class OSTradesHistoryTable(tables.Table):
    class Meta:
        model = models.Dailyequitysummary
        attrs = {"class": "djangotables"}
        fields = ('date', 'osbid', 'osbidvol', 'osoffer', 'osoffervol')


# CONSTANTS
alertmessage = "Sorry! An error was encountered while processing your request."

# Global variables?
logger = logging.getLogger(__name__)

# Create functions used by the views here

# Create your views here.


def dailyequitysummary(request):
    try:
        errors = ""
        logger.info("Daily equity summary page was called")
        # Check if our request contains our GET variables
        if request.method == 'GET' and all(x in request.GET for x in ['selecteddate']):
            logger.info("All required GET parameters were found")
            # Validate all input fields
            selecteddate = parse(request.GET.get('selecteddate'))
            if not selecteddate:
                errors += "Please enter a valid value for your date."
        else:
            logger.info("All required GET parameters were not found")
            # The default date for our form will be the latest date available
            # This will be overridden later on if the user has submitted input data
            # Put the latest date as the selected date by default
            selecteddate = models.Dailyequitysummary.objects.latest(
                'date').date
        if request.GET.get('sort'):
            orderby = request.GET.get('sort')
        else:
            orderby = 'date'
        # Now select the records corresponding to this date, as well as their symbols, and order by the highest volume traded
        dailyequitysummaryrecords = models.Dailyequitysummary.objects.filter(
            date=selecteddate).select_related('equityid').order_by('-valuetraded')
        if not dailyequitysummaryrecords:
            errors += "No data available for the date selected. Please choose another date."
        # rename the symbol field properly and select only required fields
        selectedrecords = dailyequitysummaryrecords.annotate(symbol=F('equityid__symbol'), securityname=F('equityid__securityname')).values(
            'securityname', 'symbol', 'valuetraded', 'volumetraded', 'saleprice', 'low', 'high', 'changedollars')
        # Set up our table
        tabledata = DailyEquitySummaryTable(selectedrecords, order_by=orderby)
        # Set the graph
        # get the top 10 records by value traded
        graphsymbols = [record['symbol'] for record in selectedrecords[:10]]
        graphvaluetraded = [record['valuetraded']
                            for record in selectedrecords[:10]]
        # create a category for the sum of all other symbols (not in the top 10)
        others = dict(symbol='Others', valuetraded=0)
        for record in selectedrecords:
            if record['symbol'] not in graphsymbols:
                others['valuetraded'] += record['valuetraded']
        # add the 'other' category to the graph
        graphsymbols.append(others['symbol'])
        graphvaluetraded.append(others['valuetraded'])
        # get a human readable date
        selecteddateparsed = selecteddate.strftime('%Y-%m-%d')
    except Exception as ex:
        errors = alertmessage+str(ex)
        logging.critical(traceback.format_exc())
        logger.error(errors)
    # Now add our context data and return a response
    context = {
        'errors': errors,
        'table': tabledata,
        'selecteddate': selecteddate,
        'selecteddateparsed': selecteddateparsed,
        'graphsymbols': graphsymbols,
        'graphvaluetraded': graphvaluetraded,
    }
    return render(request, "stocks/base_dailyequitysummary.html", context)


def stockhistory(request):
    try:
        errors = ""
        logger.info("Stock history page was called")
        listedstocks = models.Listedequities.objects.all()
        # Check if our request contains our GET variables
        if request.method == 'GET' and all(x in request.GET for x in ['startdate', 'enddate', 'equityid']):
            logger.info("All required GET parameters were found")
            # Validate all input fields
            startdate = parse(request.GET.get('startdate'))
            if not startdate:
                errors += "Please enter a valid value for your start date."
            enddate = parse(request.GET.get('enddate'))
            if not enddate:
                errors += "Please enter a valid value for your end date."
            # Store values for all fields to repopulate the form
            enteredstartdate = startdate.strftime('%Y-%m-%d')
            enteredenddate = enddate.strftime('%Y-%m-%d')
            # Raise an error if the start date is after the end date
            if startdate > enddate:
                errors += "Your starting date must be before your ending date. Please recheck."
            # Fetch the selected equityid
            selectedequityid = int(request.GET.get('equityid'))
        else:
            logger.info("All required GET parameters were not found")
            # Put some default values into our form.
            # These will be overridden later on if the user has submitted input data
            selectedequityid = models.Listedequities.objects.first().equityid
            enteredstartdate = (datetime.now(
            )+dateutil.relativedelta.relativedelta(months=-3)).strftime('%Y-%m-%d')
            enteredenddate = datetime.now().strftime('%Y-%m-%d')
        if request.GET.get('sort'):
            orderby = request.GET.get('sort')
        else:
            orderby = 'date'
        selectedstock = models.Listedequities.objects.get(
            equityid=selectedequityid)
        # Fetch the records
        historicalrecords = models.Historicalstockinfo.objects.filter(
            equityid=selectedequityid).filter(date__gt=enteredstartdate).filter(date__lt=enteredenddate)
        # Set up our table
        tabledata = HistoricalStockInfoTable(
            historicalrecords, order_by=orderby)
        tabledata.paginate(page=request.GET.get("page", 1), per_page=25)
        # Set up our graph
        graphlabels = [obj.date.strftime('%Y-%m-%d')
                       for obj in historicalrecords]
        graphdataset = []
        # Add the composite totals
        graphdict = dict(data=[float(obj.closingquote) for obj in historicalrecords],
                         borderColor='rgb(0, 0, 0)',
                         backgroundColor='rgb(255, 0, 0)',
                         label=selectedstock.securityname+'('+selectedstock.symbol+')')
        graphdataset.append(graphdict)
    except Exception as ex:
        errors = alertmessage+str(ex)
        logging.critical(traceback.format_exc())
        logger.error(errors)
    # Now add our context data and return a response
    context = {
        'errors': errors,
        'listedstocks': listedstocks,
        'selectedequityid': selectedequityid,
        'selectedstockname': selectedstock.securityname,
        'table': tabledata,
        'enteredstartdate': enteredstartdate,
        'enteredenddate': enteredenddate,
        'graphlabels': graphlabels,
        'graphdataset': graphdataset,
    }
    return render(request, "stocks/base_stockhistory.html", context)


def dividendhistory(request):
    try:
        errors = ""
        logger.info("Dividend history page was called")
        listedstocks = models.Listedequities.objects.all()
        # Check if our request contains our GET variables
        if request.method == 'GET' and all(x in request.GET for x in ['startdate', 'enddate', 'equityid']):
            logger.info("All required GET parameters were found")
            # Validate all input fields
            startdate = parse(request.GET.get('startdate'))
            if not startdate:
                errors += "Please enter a valid value for your start date."
            enddate = parse(request.GET.get('enddate'))
            if not enddate:
                errors += "Please enter a valid value for your end date."
            # Store values for all fields to repopulate the form
            enteredstartdate = startdate.strftime('%Y-%m-%d')
            enteredenddate = enddate.strftime('%Y-%m-%d')
            # Raise an error if the start date is after the end date
            if startdate > enddate:
                errors += "Your starting date must be before your ending date. Please recheck."
            # Fetch the selected equityid
            selectedequityid = int(request.GET.get('equityid'))
        else:
            logger.info("All required GET parameters were not found")
            # Put some default values into our form.
            # These will be overridden later on if the user has submitted input data
            selectedequityid = models.Listedequities.objects.first().equityid
            enteredstartdate = (datetime.now(
            )+dateutil.relativedelta.relativedelta(years=-10)).strftime('%Y-%m-%d')
            enteredenddate = datetime.now().strftime('%Y-%m-%d')
        if request.GET.get('sort'):
            orderby = request.GET.get('sort')
        else:
            orderby = 'recorddate'
        selectedstock = models.Listedequities.objects.get(
            equityid=selectedequityid)
        # Fetch some default records
        historicalrecords = models.Historicaldividendinfo.objects.filter(equityid=selectedequityid).filter(
            recorddate__gt=enteredstartdate).filter(recorddate__lt=enteredenddate)
        if not historicalrecords:
            errors += "No data found for equity and date range selected."
        # Set up our table
        tabledata = HistoricalDividendInfoTable(
            historicalrecords, order_by=orderby)
        tabledata.paginate(page=request.GET.get("page", 1), per_page=25)
        # Set up our graph
        graphlabels = ""
        graphdataset = []
        graphlabels = [obj.recorddate.strftime(
            '%Y-%m-%d') for obj in historicalrecords]
        # Add the composite totals
        graphdict = dict(data=[float(obj.dividendamount) for obj in historicalrecords],
                         borderColor='rgb(0, 0, 0)',
                         backgroundColor='rgb(255, 0, 0)',
                         label=selectedstock.securityname+'('+selectedstock.symbol+')')
        graphdataset.append(graphdict)
    except Exception as ex:
        errors = alertmessage+str(ex)
        logging.critical(traceback.format_exc())
        logger.error(errors)
    # Now add our context data and return a response
    context = {
        'errors': errors,
        'listedstocks': listedstocks,
        'selectedequityid': selectedequityid,
        'selectedstockname': selectedstock.securityname,
        'table': tabledata,
        'enteredstartdate': enteredstartdate,
        'enteredenddate': enteredenddate,
        'graphlabels': graphlabels,
        'graphdataset': graphdataset,
    }
    return render(request, "stocks/base_dividendhistory.html", context)


def dividendyieldhistory(request):
    try:
        errors = ""
        logger.info("Dividend yield history page was called")
        listedstocks = models.Listedequities.objects.all()
        # Check if our request contains our GET variables
        if request.method == 'GET' and all(x in request.GET for x in ['startdate', 'enddate', 'equityid']):
            logger.info("All required GET parameters were found")
            # Validate all input fields
            startdate = parse(request.GET.get('startdate'))
            if not startdate:
                errors += "Please enter a valid value for your start date."
            enddate = parse(request.GET.get('enddate'))
            if not enddate:
                errors += "Please enter a valid value for your end date."
            # Store values for all fields to repopulate the form
            enteredstartdate = startdate.strftime('%Y-%m-%d')
            enteredenddate = enddate.strftime('%Y-%m-%d')
            # Raise an error if the start date is after the end date
            if startdate > enddate:
                errors += "Your starting date must be before your ending date. Please recheck."
            # Fetch the selected equityid
            selectedequityid = int(request.GET.get('equityid'))
        else:
            logger.info("All required GET parameters were not found")
            # Put some default values into our form.
            # These will be overridden later on if the user has submitted input data
            selectedequityid = models.Listedequities.objects.first().equityid
            enteredstartdate = (datetime.now(
            )+dateutil.relativedelta.relativedelta(years=-10)).strftime('%Y-%m-%d')
            enteredenddate = datetime.now().strftime('%Y-%m-%d')
        if request.GET.get('sort'):
            orderby = request.GET.get('sort')
        else:
            orderby = 'yielddate'
        selectedstock = models.Listedequities.objects.get(
            equityid=selectedequityid)
        # Fetch some default records
        historicalrecords = models.Dividendyield.objects.filter(equityid=selectedequityid).filter(
            yielddate__gt=enteredstartdate).filter(yielddate__lt=enteredenddate)
        if not historicalrecords:
            errors += "No data found for equity and date range selected."
        # Set up our table
        tabledata = HistoricalDividendYieldTable(
            historicalrecords, order_by=orderby)
        tabledata.paginate(page=request.GET.get("page", 1), per_page=10)
        # Set up our graph
        graphlabels = ""
        graphdataset = []
        graphlabels = [obj.yielddate.strftime(
            '%Y-%m-%d') for obj in historicalrecords]
        # Add the composite totals
        graphdict = dict(data=[float(obj.yieldpercent) for obj in historicalrecords],
                         borderColor='rgb(0, 0, 0)',
                         backgroundColor='rgb(255, 0, 0)',
                         label=selectedstock.securityname+'('+selectedstock.symbol+')')
        graphdataset.append(graphdict)
    except Exception as ex:
        errors = alertmessage+str(ex)
        logging.critical(traceback.format_exc())
        logger.error(errors)
    # Now add our context data and return a response
    context = {
        'errors': errors,
        'listedstocks': listedstocks,
        'selectedequityid': selectedequityid,
        'selectedstockname': selectedstock.securityname,
        'table': tabledata,
        'enteredstartdate': enteredstartdate,
        'enteredenddate': enteredenddate,
        'graphlabels': graphlabels,
        'graphdataset': graphdataset,
    }
    return render(request, "stocks/base_dividendyieldhistory.html", context)


def markethistory(request):
    try:
        errors = ""
        logger.info("Market history page was called")
        # Check if our request contains our GET variables
        if request.method == 'GET' and all(x in request.GET for x in ['startdate', 'enddate']):
            logger.info("All required GET parameters were found")
            # Validate all input fields
            startdate = parse(request.GET.get('startdate'))
            if not startdate:
                errors += "Please enter a valid value for your start date."
            enddate = parse(request.GET.get('enddate'))
            if not enddate:
                errors += "Please enter a valid value for your end date."
            # Store values for all fields to repopulate the form
            enteredstartdate = startdate.strftime('%Y-%m-%d')
            enteredenddate = enddate.strftime('%Y-%m-%d')
            # Raise an error if the start date is after the end date
            if startdate > enddate:
                raise ValueError(
                    "Your starting date must be before your ending date. Please recheck.")
        else:
            logger.info("All required GET parameters were not found")
            # Put some default values into our form.
            # These will be overridden later on if the user has submitted input data
            enteredstartdate = (datetime.now(
            )+dateutil.relativedelta.relativedelta(months=-3)).strftime('%Y-%m-%d')
            enteredenddate = datetime.now().strftime('%Y-%m-%d')
        if request.GET.get('sort'):
            orderby = request.GET.get('sort')
        else:
            orderby = 'date'
        # Fetch the records
        historicalrecords = models.Historicalmarketsummary.objects.filter(
            date__gt=enteredstartdate).filter(date__lt=enteredenddate)
        # Set up our table
        tabledata = HistoricalMarketSummaryTable(
            historicalrecords, order_by=orderby)
        tabledata.paginate(page=request.GET.get("page", 1), per_page=25)
        # Set up our graph
        graphlabels = [obj.date.strftime('%Y-%m-%d')
                       for obj in historicalrecords]
        graphdataset = []
        # Add the composite totals
        graphdict = dict(data=[float(obj.compositetotalsindexvalue) for obj in historicalrecords],
                         borderColor='rgb(255, 0, 0)',
                         backgroundColor='transparent',
                         label='Composite Totals Index')
        graphdataset.append(graphdict)
        # Add the TnT totals
        graphdict = dict(data=[float(obj.alltnttotalsindexvalue) for obj in historicalrecords],
                         borderColor='rgb(0,255, 0)',
                         backgroundColor='transparent',
                         label='All TnT Totals Index')
        graphdataset.append(graphdict)
        # Cross-listed totals
        graphdict = dict(data=[float(obj.crosslistedtotalsindexvalue) for obj in historicalrecords],
                         borderColor='rgb(0,0,255)',
                         backgroundColor='transparent',
                         label='Cross-listed Totals Index')
        graphdataset.append(graphdict)
        # SME totals
        graphdict = dict(data=[float(obj.smetotalsindexvalue) for obj in historicalrecords],
                         borderColor='rgb(0,255, 128)',
                         backgroundColor='transparent',
                         label='SME Totals Index')
        graphdataset.append(graphdict)
    except Exception as ex:
        errors = alertmessage+str(ex)
        logging.critical(traceback.format_exc())
        logger.error(errors)
    # Now add our context data and return a response
    context = {
        'errors': errors,
        'table': tabledata,
        'enteredstartdate': enteredstartdate,
        'enteredenddate': enteredenddate,
        'graphlabels': graphlabels,
        'graphdataset': graphdataset,
    }
    return render(request, "stocks/base_markethistory.html", context)


def ostradeshistory(request):
    try:
        errors = ""
        logger.info("O/S trades history page was called")
        listedstocks = models.Listedequities.objects.all()
        # Check if our request contains our GET variables
        if request.method == 'GET' and all(x in request.GET for x in ['startdate', 'enddate', 'equityid']):
            logger.info("All required GET parameters were found")
            # Validate all input fields
            startdate = parse(request.GET.get('startdate'))
            if not startdate:
                errors += "Please enter a valid value for your start date."
            enddate = parse(request.GET.get('enddate'))
            if not enddate:
                errors += "Please enter a valid value for your end date."
            # Store values for all fields to repopulate the form
            enteredstartdate = startdate.strftime('%Y-%m-%d')
            enteredenddate = enddate.strftime('%Y-%m-%d')
            # Raise an error if the start date is after the end date
            if startdate > enddate:
                errors += "Your starting date must be before your ending date. Please recheck."
            # Fetch the selected equityid
            selectedequityid = int(request.GET.get('equityid'))
        else:
            logger.info("All required GET parameters were not found")
            # Put some default values into our form.
            # These will be overridden later on if the user has submitted input data
            selectedequityid = models.Listedequities.objects.first().equityid
            enteredstartdate = (datetime.now(
            )+dateutil.relativedelta.relativedelta(months=-1)).strftime('%Y-%m-%d')
            enteredenddate = datetime.now().strftime('%Y-%m-%d')
        if request.GET.get('sort'):
            orderby = request.GET.get('sort')
        else:
            orderby = 'date'
        if request.GET.get('osparameter'):
            osparameter = request.GET.get('osparameter')
        else:
            osparameter = 'osoffervol'
        selectedstock = models.Listedequities.objects.get(
            equityid=selectedequityid)
        # Fetch the records
        selectedrecords = models.Dailyequitysummary.objects.filter(equityid=selectedequityid).filter(date__gt=enteredstartdate).filter(
            date__lt=enteredenddate).values('date', 'osbid', 'osbidvol', 'osoffer', 'osoffervol').order_by('date')
        # Set up our table
        tabledata = OSTradesHistoryTable(selectedrecords, order_by=orderby)
        tabledata.paginate(page=request.GET.get("page", 1), per_page=10)
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
        errors = alertmessage+str(ex)
        logging.critical(traceback.format_exc())
        logger.error(errors)
    # Now add our context data and return a response
    context = {
        'errors': errors,
        'listedstocks': listedstocks,
        'selectedequityid': selectedequityid,
        'osparameterstr': osparameter,
        'selectedstockname': selectedstock.securityname,
        'table': tabledata,
        'enteredstartdate': enteredstartdate,
        'enteredenddate': enteredenddate,
        'graphlabels': graphlabels,
        'graphdataset': graphdataset,
    }
    return render(request, "stocks/base_ostradeshistory.html", context)
