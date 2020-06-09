import django_tables2 as tables
from stocks import models
from django_tables2.export.views import ExportMixin


class HistoricalStockInfoTable(tables.Table):
    class Meta:
        model = models.HistoricalStockInfo
        attrs = {"class": "djangotables"}
        fields = ('date', 'closingquote', 'changedollars',
                  'currency', 'volumetraded')
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


class DailyEquitySummaryTable(tables.Table):

    securityname = tables.Column(
        accessor="stockcode__securityname", verbose_name="Equity Name")
    symbol = tables.Column(
        accessor="stockcode__symbol")
    volumetraded = tables.Column(verbose_name="Volume Traded")
    lastsaleprice = tables.Column(verbose_name="Sale Price ($)")
    valuetraded = tables.Column(verbose_name="Value Traded ($)")
    low = tables.Column(verbose_name="Low ($)")
    high = tables.Column(verbose_name="High ($)")
    changedollars = tables.Column(verbose_name="Change ($)")

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
        model = models.DailyEquitySummary
        attrs = {"class": "djangotables"}
        fields = ('date', 'osbid', 'osbidvol', 'osoffer', 'osoffervol')
        export_formats = ['csv', 'xlsx']
