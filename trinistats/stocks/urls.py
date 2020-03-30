from django.urls import path

from . import views

# app_name = 'ttseanalytics'

urlpatterns = [
    path('', views.dailyequitysummary, name='dailyequitysummary'),
    path('dailyequitysummary', views.dailyequitysummary, name='dailyequitysummary'),
    path('markethistory', views.markethistory, name='markethistory'),
    path('stockhistory', views.stockhistory, name='stockhistory'),
    path('dividendhistory', views.dividendhistory, name='dividendhistory'),
    path('dividendyieldhistory', views.dividendyieldhistory, name='dividendyieldhistory'),
    path('ostradeshistory', views.ostradeshistory, name='ostradeshistory'),
]