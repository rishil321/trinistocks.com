"""
Custom template tags for the trinistats stocks app
"""
from .. import models
from django import template
from datetime import datetime
from dateutil.relativedelta import relativedelta

register = template.Library()


@register.simple_tag
def get_latest_date_dailyequitysummary():
    return models.DailyEquitySummary.objects.latest('date').date.strftime("%Y-%m-%d")


@register.simple_tag
def get_today_date_iso_8601():
    return datetime.now().strftime("%Y-%m-%d")


@register.simple_tag
def get_1_yr_ago_date_iso_8601():
    return (datetime.now()+relativedelta(years=-1)).strftime('%Y-%m-%d')


@register.simple_tag
def get_5_yr_ago_date_iso_8601():
    return (datetime.now()+relativedelta(years=-5)).strftime('%Y-%m-%d')
