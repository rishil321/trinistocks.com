"""
Custom template tags for the trinistats stocks app
"""
from .. import models
from django import template
from datetime import datetime
from dateutil.relativedelta import relativedelta
from django.contrib.sessions.backends.db import SessionStore

register = template.Library()


@register.simple_tag
def get_latest_date_dailyequitysummary():
    return models.DailyEquitySummary.objects.latest('date').date.strftime("%Y-%m-%d")


@register.simple_tag
def get_session_end_date_or_today():
    session_data = SessionStore()
    if 'enteredenddate' in session_data:
        return session_data['enteredenddate'].strftime('%Y-%m-%d')
    else:
        return datetime.now().strftime("%Y-%m-%d")


@register.simple_tag
def get_session_start_date_or_1_yr_back():
    session_data = SessionStore()
    if 'enteredstartdate' in session_data:
        return session_data['enteredstartdate'].strftime('%Y-%m-%d')
    else:
        return (datetime.now()+relativedelta(years=-1)).strftime('%Y-%m-%d')


@register.simple_tag
def get_session_stockcode_or_default():
    session_data = SessionStore()
    if 'selectedstockcode' in session_data:
        return session_data['selectedstockcode']
    else:
        return 89
