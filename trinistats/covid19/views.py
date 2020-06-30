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


class Covid19_Daily_Table(tables.Table):
    class Meta:
        model = models.Covid19_Daily_Data
        attrs = {"class": "djangotables"}
        fields = ('date', 'daily_tests', 'daily_positive',
                  'daily_deaths', 'daily_recovered')

# Django REST Framework Views


class Covid19_Paho_Reports_List(viewsets.ReadOnlyModelViewSet):
    serializer_class = Covid19_Paho_Reports_Serializer
    queryset = Covid19_Paho_Reports.objects.all()
    filterset_fields = ['country_or_territory_name',
                        'date', 'transmission_type']


class Covid19_Worldometers_Reports_List(viewsets.ReadOnlyModelViewSet):
    serializer_class = Covid19_Worldometers_Reports_Serializer
    queryset = Covid19_Worldometers_Reports.objects.all()
    filterset_fields = ['country_or_territory_name', 'date']


# CONSTANTS
ALERTMESSAGE = "Sorry! This page seems to be full of bugs :( We'll call an exterminator soon! Here's what we know: "

# Global variables
logger = logging.getLogger(__name__)

# Create functions used by the views here

# Django Regular HTTP(s) Views


def totals(request):
    try:
        errors = ""
        logger.info("Covid19 totals page was called")
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
            startdate = datetime.now()+dateutil.relativedelta.relativedelta(weeks=-4)
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
            orderby = request.GET.get('sort')
        else:
            # default table order
            orderby = '-date'
        if request.GET.get('selectedcasetypeleft'):
            selectedcasetypeleft = request.GET.get('selectedcasetypeleft')
        else:
            selectedcasetypeleft = 'total_cases'
        if request.GET.get('selectedcasetyperight'):
            selectedcasetyperight = request.GET.get('selectedcasetyperight')
        else:
            selectedcasetyperight = 'total_tests'
        # Get the full names for the case types selected
        selectedfieldleftverbosename = models.Covid19_Worldometers_Reports._meta.get_field(
            selectedcasetypeleft).verbose_name
        selectedfieldrightverbosename = models.Covid19_Worldometers_Reports._meta.get_field(
            selectedcasetyperight).verbose_name
        # Fetch the selected records from the db
        selectedrecords = models.Covid19_Worldometers_Reports.objects.filter(date__gt=enteredstartdate)\
            .filter(country_or_territory_name="Trinidad and Tobago")\
            .filter(date__lt=enteredenddate)\
            .values('date', 'total_tests', 'total_cases', 'total_deaths', 'total_recovered')\
            .order_by('date')
        # Set up our summary data
        latestrecord = models.Covid19_Worldometers_Reports.objects\
            .filter(country_or_territory_name="Trinidad and Tobago")\
            .latest('date')
        # Set up our graph
        graphlabels = [obj['date'] for obj in selectedrecords]
        graphdataset1 = [obj[selectedcasetypeleft] for obj in selectedrecords]
        # Add data for the second dataset
        graphdataset2 = [obj[selectedcasetyperight] for obj in selectedrecords]
        # Set up the case type options for the dropdown select
        validcasetypes = [models.Covid19_Worldometers_Reports._meta.get_field('total_tests'),
                          models.Covid19_Worldometers_Reports._meta.get_field(
                              'total_cases'),
                          models.Covid19_Worldometers_Reports._meta.get_field(
                              'total_deaths'),
                          models.Covid19_Worldometers_Reports._meta.get_field('total_recovered')]
    except Exception as ex:
        errors = ALERTMESSAGE+str(ex)
        logging.critical(traceback.format_exc())
        logger.error(errors)
    # Now add our context data and return a response
    context = {
        'errors': errors,
        'validcasetypes': validcasetypes,
        'selectedcasetypeleftstr': selectedcasetypeleft,
        'selectedcasetypeleftverbose': selectedfieldleftverbosename,
        'selectedcasetyperightstr': selectedcasetyperight,
        'selectedcasetyperightverbose': selectedfieldrightverbosename,
        'enteredstartdate': enteredstartdate,
        'enteredenddate': enteredenddate,
        'graphlabels': graphlabels,
        'graphdataset1': graphdataset1,
        'graphdataset2': graphdataset2,
        'latestrecord': latestrecord,
    }
    return render(request, "covid19/base_totals.html", context)


def daily(request):
    try:
        errors = ""
        logger.info("Daily page was called")
        daily_cases = models.Covid19_Daily_Data.objects.all()
        # Check if our request contains our GET dates
        if request.method == 'GET' and all(x in request.GET for x in ['startdate', 'enddate']):
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
            enteredstartdate = (datetime.now(
            )+dateutil.relativedelta.relativedelta(months=-1)).strftime('%Y-%m-%d')
            enteredenddate = (
                datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
        if request.GET.get('sort'):
            orderby = request.GET.get('sort')
        else:
            # default table order
            orderby = '-date'
        if request.GET.get('selectedcasetype'):
            selectedcasetype = request.GET.get('selectedcasetype')
        else:
            selectedcasetype = 'daily_positive'
        # Get the full names for the case types selected
        selectedfieldverbosename = 'N/A'
        selectedfieldverbosename = models.Covid19_Daily_Data._meta.get_field(
            selectedcasetype).verbose_name
        # Fetch the records from the db
        selectedrecords = models.Covid19_Daily_Data.objects.filter(date__gt=enteredstartdate)\
            .filter(date__lt=enteredenddate)\
            .values('date', 'daily_tests', 'daily_positive', 'daily_deaths', 'daily_recovered')\
            .order_by('date')
        # Get the date for yesterday
        yesterdaydate = (datetime.now() + timedelta(days=-1)
                         ).strftime('%Y-%m-%d')
        # Get the record from the daily table for yesterday data
        yesterdaydata = models.Covid19_Daily_Data.objects.filter(
            date=yesterdaydate)
        if not yesterdaydata:
            errors += "No daily data found for yesterday ("+yesterdaydate+")"
            yesterdaydata = ['N/A']
        # Set up our graph
        graphlabels = [obj['date'] for obj in selectedrecords]
        graphdataset = [obj[selectedcasetype] for obj in selectedrecords]
        # Set up the case type options for the dropdown select
        validcasetypes = [models.Covid19_Daily_Data._meta.get_field('daily_tests'),
                          models.Covid19_Daily_Data._meta.get_field(
                              'daily_positive'),
                          models.Covid19_Daily_Data._meta.get_field(
                              'daily_deaths'),
                          models.Covid19_Daily_Data._meta.get_field('daily_recovered')]
    except Exception as ex:
        errors = ALERTMESSAGE+str(ex)
        logging.critical(traceback.format_exc())
        logger.error(errors)
    # Now add our context data and return a response
    context = {
        'errors': errors,
        'validcasetypes': validcasetypes,
        'selectedcasetypestr': selectedcasetype,
        'selectedcasetypeverbose': selectedfieldverbosename,
        'yesterdaydata': yesterdaydata[0],
        'enteredstartdate': enteredstartdate,
        'enteredenddate': enteredenddate,
        'graphlabels': graphlabels,
        'graphdataset': graphdataset,
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
        'errors': errors,
    }
    return render(request, "covid19/base_about.html", context)
