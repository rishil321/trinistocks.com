"""
Custom template tags for the trinistats stocks app
"""
from .. import models
from django import template
from datetime import datetime
from dateutil.relativedelta import relativedelta
from django.contrib.sessions.backends.db import SessionStore
from django.conf import settings
from importlib import import_module
import logging

register = template.Library()
logger = logging.getLogger('root')


@register.simple_tag
def get_latest_date_dailytradingsummary():
    latest_date = models.DailyStockSummary.objects.latest(
        'date').date.strftime("%Y-%m-%d")
    return latest_date


@register.simple_tag(takes_context=True)
def get_session_end_date_or_today(context):
    session_data = context.request.session
    if 'entered_end_date' in session_data:
        return session_data['entered_end_date']
    else:
        return datetime.now().strftime("%Y-%m-%d")

@register.simple_tag(takes_context=False)
def get_today():
    return datetime.now().strftime("%Y-%m-%d")


@register.simple_tag(takes_context=True)
def get_session_start_date_or_1_yr_back(context):
    session_data = context.request.session
    if 'entered_start_date' in session_data:
        return session_data['entered_start_date']
    else:
        return (datetime.now()+relativedelta(years=-1)).strftime('%Y-%m-%d')

@register.simple_tag(takes_context=False)
def get_1_yr_back():
    return (datetime.now()+relativedelta(years=-1)).strftime('%Y-%m-%d')


@register.simple_tag(takes_context=True)
def get_session_start_date_or_5_yr_back(context):
    session_data = context.request.session
    if 'entered_start_date' in session_data:
        return session_data['entered_start_date']
    else:
        return (datetime.now()+relativedelta(years=-5)).strftime('%Y-%m-%d')

@register.simple_tag(takes_context=False)
def get_5_yr_back():
    return (datetime.now()+relativedelta(years=-5)).strftime('%Y-%m-%d')


@register.simple_tag(takes_context=True)
def get_session_symbol_or_default(context):
    session_data = context.request.session
    if 'selected_symbol' in session_data:
        return session_data['selected_symbol']
    else:
        return 'AGL'
