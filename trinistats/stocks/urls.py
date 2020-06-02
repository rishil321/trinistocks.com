from django.urls import path

from . import views
from . import models

# Functions for URLS


app_name = 'stocks'

urlpatterns = [
    path('dailyequitysummary', views.DailyEquitySummaryView.as_view(),
         name='dailyequitysummary'),
    path('marketindexhistory', views.MarketIndexHistoryView.as_view(),
         name='marketindexhistory'),
    path('stockhistory', views.StockHistoryView.as_view(), name='stockhistory'),
    path('dividendhistory', views.DividendHistoryView.as_view(),
         name='dividendhistory'),
    path('dividendyieldhistory', views.DividendYieldHistoryView.as_view(),
         name='dividendyieldhistory'),
    path('ostradeshistory', views.OSTradesHistoryView.as_view(),
         name='ostradeshistory'),
]
