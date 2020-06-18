import django_tables2 as tables
from stocks import models
from django_tables2.export.views import ExportMixin


class HistoricalStockInfoTable(tables.Table):
    date = tables.Column(verbose_name="Date")
    currency = tables.Column(
        accessor="stockcode__currency", verbose_name="Currency")
    openprice = tables.Column(verbose_name="Open Price ($)")
    high = tables.Column(verbose_name="High ($)")
    low = tables.Column(verbose_name="Low ($)")
    closeprice = tables.Column(verbose_name="Close Price ($)")
    volumetraded = tables.Column(verbose_name="Volume Traded (Shares)")
    changedollars = tables.Column(verbose_name="Price Change ($)")

    class Meta:
        attrs = {"class": "djangotables"}
        export_formats = ['csv', 'xlsx']


class HistoricalDividendInfoTable(tables.Table):
    class Meta:
        model = models.HistoricalDividendInfo
        attrs = {"class": "djangotables"}
        fields = ('date', 'dividendamount', 'currency')
        export_formats = ['csv', 'xlsx']


class HistoricalDividendYieldTable(tables.Table):
    class Meta:
        model = models.DividendYield
        attrs = {"class": "djangotables"}
        fields = ('date', 'yieldpercent')


class DailyTradingSummaryTable(tables.Table):

    securityname = tables.Column(
        accessor="stockcode__securityname", verbose_name="Stock Name")
    symbol = tables.Column(
        accessor="stockcode__symbol", verbose_name="Symbol")
    volumetraded = tables.Column(verbose_name="Volume Traded (Shares)")
    lastsaleprice = tables.Column(verbose_name="Sale Price ($)")
    currency = tables.Column(
        accessor="stockcode__currency", verbose_name="Currency")
    valuetraded = tables.Column(verbose_name="Dollar Volume ($)")
    low = tables.Column(verbose_name="Low ($)")
    high = tables.Column(verbose_name="High ($)")
    changedollars = tables.Column(verbose_name="Price Change ($)")

    class Meta:
        attrs = {"class": "djangotables"}
        export_formats = ['csv', 'xlsx']


class HistoricalMarketSummaryTable(tables.Table):
    class Meta:
        model = models.HistoricalMarketSummary
        attrs = {"class": "djangotables"}
        fields = ('date', 'indexvalue', 'changepercent',
                  'volumetraded', 'valuetraded', 'numtrades')
        export_formats = ['csv', 'xlsx']


class OSTradesHistoryTable(tables.Table):
    class Meta:
        model = models.DailyTradingSummary
        attrs = {"class": "djangotables"}
        fields = ('date', 'osbid', 'osbidvol', 'osoffer', 'osoffervol')
        export_formats = ['csv', 'xlsx']
