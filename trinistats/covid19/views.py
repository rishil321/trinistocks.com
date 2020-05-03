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
from rest_framework import viewsets
from .models import Covid19_Paho_Reports
from .serializers import *

# Django Tables
class Covid19DailyTable(tables.Table):
    class Meta:
        model = models.Covid19DailyData
        attrs = {"class":"djangotables"}
        fields = ('date','dailytests','dailypositive','dailydeaths','dailyrecovered')
        
# Django REST Framework Views
class Covid19_Paho_Reports_List(viewsets.ReadOnlyModelViewSet):
    serializer_class = Covid19_Paho_Reports_Serializer
    queryset = Covid19_Paho_Reports.objects.all()
    filterset_fields = ['country_or_territory_name','date','transmission_type']
    
class Covid19_Worldometers_Reports_List(viewsets.ReadOnlyModelViewSet):
    serializer_class = Covid19_Worldometers_Reports_Serializer
    queryset = Covid19_Worldometers_Reports.objects.all()
    filterset_fields = ['country_or_territory_name','date']
  
#CONSTANTS
ALERTMESSAGE = "Sorry! An error was encountered while processing your request."

# Global variables?
logger = logging.getLogger(__name__)

# Create functions used by the views here

# Django Regular HTTP(s) Views
def totals(request):
    try:
        errors = ""
        logger.info("Totals page was called")
        # check whether this is the first page load
        recordedcases = models.Covid19Cases.objects.all()
        # Validate all input fields
        try:
            # check whether each GET variable was submitted with the request
            if request.GET.get('startdate'):
                startdateentered = True
            else:
                startdateentered = False
            startdate = parse(request.GET.get('startdate'))
        except:
            if startdateentered:
                errors += "Please enter a valid value for your start date."
            startdate =datetime.now()+dateutil.relativedelta.relativedelta(weeks=-4)
        try:
            # check whether each GET variable was submitted with the request
            if request.GET.get('enddate'):
                enddateentered = True
            else:
                enddateentered = False
            enddate = parse(request.GET.get('enddate'))
        except:
            if enddateentered:
                errors += "Please enter a valid value for your end date."
            enddate = datetime.now() + timedelta(days=1)
        # Store values to repopulate the form
        enteredstartdate = startdate.strftime('%Y-%m-%d')
        enteredenddate = enddate.strftime('%Y-%m-%d')
        # Raise an error if the start date is after the end date
        if startdate > enddate:
            errors += "Your starting date must be before your ending date. Please recheck."
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
        # Fetch the selected records from the db
        selectedrecords = models.Covid19Cases.objects.filter(date__gt=enteredstartdate).filter(date__lt=enteredenddate).values('date','numtested','numpositive','numdeaths','numrecovered').order_by('date')
        # Set up our summary data
        latestrecord = models.Covid19Cases.objects.latest('date')
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
        'enteredstartdate':enteredstartdate,
        'enteredenddate':enteredenddate,
        'graphlabels':graphlabels,
        'graphdataset':graphdataset,
        'latestrecord':latestrecord,
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
        selectedfieldverbosename = 'N/A'
        selectedfieldverbosename = models.Covid19DailyData._meta.get_field(selectedcasetype).verbose_name
        # Fetch the records from the db
        selectedrecords = models.Covid19DailyData.objects.filter(date__gt=enteredstartdate).filter(date__lt=enteredenddate).values('date','dailytests','dailypositive','dailydeaths','dailyrecovered').order_by('date')
        # Get the date for yesterday
        yesterdaydate = (datetime.now() + timedelta(days=-1)).strftime('%Y-%m-%d')
        # Get the record from the daily table for yesterday data
        yesterdaydata = models.Covid19DailyData.objects.filter(date = yesterdaydate)
        if not yesterdaydata:
            errors += "No daily data found for yesterday ("+yesterdaydate+")"
            yesterdaydata = ['N/A']
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
        'yesterdaydata':yesterdaydata[0],
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