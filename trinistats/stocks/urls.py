from django.urls import path
from . import views
from django.contrib.sitemaps.views import sitemap
from . import sitemaps

sitemaps = {
    "static": sitemaps.StaticViewSitemap,
}
# Functions for URLS
app_name = 'stocks'

urlpatterns = [
    path("sitemap.xml", sitemap, {"sitemaps": sitemaps}, name="sitemap"),
    path('', views.LandingPageView.as_view(), name="landingpage"),
    path('dailytradingsummary', views.DailyTradingSummaryView.as_view(),
         name='dailytradingsummary'),
    path('listedstocks', views.ListedStocksView.as_view(),
         name='listedstocks'),
    path('technicalanalysis', views.TechnicalAnalysisSummary.as_view(),
         name='technicalanalysis'),
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
