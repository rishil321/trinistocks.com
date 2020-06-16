from django.urls import path
from django.views.generic.base import RedirectView
from . import views
from .templatetags import stocks_template_tags
import urllib.parse

# Functions for URLS


app_name = 'stocks'

urlpatterns = [
    path('', RedirectView.as_view(
        url=f'dailytradingsummary?date={stocks_template_tags.get_latest_date_dailytradingsummary()}&wastradedtoday=1&sort=-valuetraded', permanent=False), name="landingpage"),
    path('dailytradingsummary', views.DailyTradingSummaryView.as_view(),
         name='dailytradingsummary'),
    path('marketindexhistory', views.MarketIndexHistoryView.as_view(),
         name='marketindexhistory'),
    path('stockhistory', views.StockHistoryView.as_view(), name='stockhistory'),
    path('dividendhistory', views.DividendHistoryView.as_view(),
         name='dividendhistory'),
    path('dividendyieldhistory', views.DividendYieldHistoryView.as_view(),
         name='dividendyieldhistory'),
    path('ostradeshistory', views.OSTradesHistoryView.as_view(),
         name='ostradeshistory'),
    path('about', views.AboutPageView.as_view(),
         name='about'),
]
