from django.shortcuts import render
from django.http import HttpResponse
from random import randint
from django.views.generic import TemplateView
from chartjs.views.lines import BaseLineChartView
from covid19 import models
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
from _datetime import timedelta
import pytz

class Covid19CasesTable(tables.Table):
    class Meta:
        model = models.Covid19Cases
        attrs = {"class":"djangotables"}
        fields = ('date','numtested','numpositive','numdeaths','numrecovered')

class Covid19DailyTable(tables.Table):
    class Meta:
        model = models.Covid19DailyData
        attrs = {"class":"djangotables"}
        fields = ('date','dailytests','dailypositive','dailydeaths','dailyrecovered')
    
#CONSTANTS
ALERTMESSAGE = "Sorry! An error was encountered while processing your request."

# Global variables?
logger = logging.getLogger(__name__)

# Create functions used by the views here

# Create your views here.
def totals(request):
    try:
        errors = ""
        logger.info("Totals page was called")
        recordedcases = models.Covid19Cases.objects.all()
        # Check if our request contains our GET dates
        if request.method == 'GET' and all(x in request.GET for x in ['startdate','enddate']):
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
        else:
            # Put some default dates into our form.
            enteredstartdate = (datetime.now()+dateutil.relativedelta.relativedelta(months=-1)).strftime('%Y-%m-%d')
            enteredenddate = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
        if request.GET.get('sort'):
            orderby=request.GET.get('sort')
        else:
            # default table order
            orderby = '-date'
        if request.GET.get('selectedcasetypeleft'):
            selectedcasetypeleft = request.GET.get('selectedcasetypeleft')
        else:
            selectedcasetypeleft = 'numtested'
        if request.GET.get('selectedcasetyperight'):
            selectedcasetyperight = request.GET.get('selectedcasetyperight')
        else:
            selectedcasetyperight = 'numpositive'
        # Get the full names for the case types selected
        selectedfieldleftverbosename = models.Covid19Cases._meta.get_field(selectedcasetypeleft).verbose_name
        selectedfieldrightverbosename = models.Covid19Cases._meta.get_field(selectedcasetyperight).verbose_name
        # Fetch the records from the db
        selectedrecords = models.Covid19Cases.objects.filter(date__gt=enteredstartdate).filter(date__lt=enteredenddate).values('date','numtested','numpositive','numdeaths','numrecovered').order_by('date')
        # Set up our table
        tabledata = Covid19CasesTable(selectedrecords, order_by=orderby)
        tabledata.paginate(page=request.GET.get("page", 1), per_page=10)
        # Set up our graph
        graphlabels = [obj['date'] for obj in selectedrecords]
        graphdataset = []
        # Add data for the first dataset
        graphdict = dict(data = [obj[selectedcasetypeleft] for obj in selectedrecords],
                         yAxisID = 'A',
                         borderColor = 'rgb(0, 0, 255)',
                         backgroundColor = 'rgba(255, 255, 255,0)',
                         label = selectedfieldleftverbosename)
        graphdataset.append(graphdict)
        # Add data for the second dataset
        graphdict = dict(data = [obj[selectedcasetyperight] for obj in selectedrecords],
                         yAxisID = 'B',
                         borderColor = 'rgb(0, 255, 0)',
                         backgroundColor = 'rgba(255, 255, 255,0)',
                         label = selectedfieldrightverbosename)
        graphdataset.append(graphdict)
        # Set up the case type options for the dropdown select
        validcasetypes = [models.Covid19Cases._meta.get_field('numtested'),
                          models.Covid19Cases._meta.get_field('numpositive'),
                          models.Covid19Cases._meta.get_field('numdeaths'),
                          models.Covid19Cases._meta.get_field('numrecovered')]
    except Exception as ex:
        errors = ALERTMESSAGE+str(ex)
        logging.critical(traceback.format_exc())
        logger.error(errors)    
    # Now add our context data and return a response
    context = {
        'errors':errors,
        'validcasetypes':validcasetypes,
        'selectedcasetypeleftstr':selectedcasetypeleft,
        'selectedcasetypeleftverbose':selectedfieldleftverbosename,
        'selectedcasetyperightstr':selectedcasetyperight,
        'selectedcasetyperightverbose':selectedfieldrightverbosename,
        'table':tabledata,
        'enteredstartdate':enteredstartdate,
        'enteredenddate':enteredenddate,
        'graphlabels':graphlabels,
        'graphdataset':graphdataset,
    }
    return render(request, "covid19/base_totals.html", context)

def daily(request):
    try:
        errors = ""
        logger.info("Daily page was called")
        dailycases = models.Covid19DailyData.objects.all()
        # Check if our request contains our GET dates
        if request.method == 'GET' and all(x in request.GET for x in ['startdate','enddate']):
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
        else:
            # Put some default dates into our form.
            enteredstartdate = (datetime.now()+dateutil.relativedelta.relativedelta(months=-1)).strftime('%Y-%m-%d')
            enteredenddate = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
        if request.GET.get('sort'):
            orderby=request.GET.get('sort')
        else:
            # default table order
            orderby = '-date'
        if request.GET.get('selectedcasetype'):
            selectedcasetype = request.GET.get('selectedcasetype')
        else:
            selectedcasetype = 'dailypositive'
        # Get the full names for the case types selected
        selectedfieldverbosename = models.Covid19DailyData._meta.get_field(selectedcasetype).verbose_name
        # Fetch the records from the db
        selectedrecords = models.Covid19DailyData.objects.filter(date__gt=enteredstartdate).filter(date__lt=enteredenddate).values('date','dailytests','dailypositive','dailydeaths','dailyrecovered').order_by('date')
        # Set up our table
        tabledata = Covid19DailyTable(selectedrecords, order_by=orderby)
        tabledata.paginate(page=request.GET.get("page", 1), per_page=10)
        # Set up our graph
        graphlabels = [obj['date'] for obj in selectedrecords]
        graphdataset = []
        # Add data for the first dataset
        graphdict = dict(
                        label = selectedfieldverbosename,
                        backgroundColor= "rgb(255,0,0)",
                        data = [obj[selectedcasetype] for obj in selectedrecords],)
        graphdataset.append(graphdict)
        # Set up the case type options for the dropdown select
        validcasetypes = [models.Covid19DailyData._meta.get_field('dailytests'),
                          models.Covid19DailyData._meta.get_field('dailypositive'),
                          models.Covid19DailyData._meta.get_field('dailydeaths'),
                          models.Covid19DailyData._meta.get_field('dailyrecovered')]
    except Exception as ex:
        errors = ALERTMESSAGE+str(ex)
        logging.critical(traceback.format_exc())
        logger.error(errors)    
    # Now add our context data and return a response
    context = {
        'errors':errors,
        'validcasetypes':validcasetypes,
        'selectedcasetypestr':selectedcasetype,
        'selectedcasetypeverbose':selectedfieldverbosename,
        'table':tabledata,
        'enteredstartdate':enteredstartdate,
        'enteredenddate':enteredenddate,
        'graphlabels':graphlabels,
        'graphdataset':graphdataset,
    }
    return render(request, "covid19/base_daily.html", context)

def about(request):
    try:
        errors = ""
        logger.info("About page was called")
    except Exception as ex:
        errors = ALERTMESSAGE+str(ex)
        logging.critical(traceback.format_exc())
        logger.error(errors)    
    # Now add our context data and return a response
    context = {
        'errors':errors,
    }
    return render(request, "covid19/base_about.html", context)