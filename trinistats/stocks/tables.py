import django_tables2 as tables
from stocks import models
from django_tables2.export.views import ExportMixin


class HistoricalStockInfoTable(tables.Table):
    date = tables.DateColumn(verbose_name="Date")
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
    symbol = tables.Column(
        accessor="stockcode__symbol", verbose_name="Symbol", attrs={"th": {"class": "headcol"},
                                                                    "td": {"class": "headcol"}})
    volumetraded = tables.Column(verbose_name="Volume Traded (Shares)")
    lastsaleprice = tables.Column(verbose_name="Sale Price ($)")
    currency = tables.Column(
        accessor="stockcode__currency", verbose_name="Currency")
    valuetraded = tables.Column(verbose_name="Dollar Volume ($)")
    low = tables.Column(verbose_name="Low ($)")
    high = tables.Column(verbose_name="High ($)")
    changedollars = tables.Column(verbose_name="Price Change ($)")

    # make the cells green if prices are up, and red if prices are down
    def render_changedollars(self, value, column):
        if value < 0:
            column.attrs = {'td': {'bgcolor': '#ff9999'}}
        elif value > 0:
            column.attrs = {'td': {'bgcolor': '#80ff80'}}
        else:
            column.attrs = {'td': {}}
        return value

    # freeze the first column of the table
    def render_symbol(self, value, column):
        return value

    class Meta:
        attrs = {"class": "djangotables"}
        export_formats = ['csv', 'xlsx']


class ListedStocksTable(tables.Table):
    symbol = tables.Column(attrs={"th": {"class": "headcol"},
                                  "td": {"class": "headcol"}})
    securityname = tables.Column()
    status = tables.Column()
    sector = tables.Column()
    issuedsharecapital = tables.Column()
    marketcapitalization = tables.Column()
    currency = tables.Column()

    def render_status(self, value, column):
        if value == 'SUSPENDED':
            column.attrs = {'td': {'bgcolor': '#ff8080'}}
        else:
            column.attrs = {'td': {'bgcolor': '#80ff80'}}
        return value

    class Meta:
        attrs = {"class": "djangotables"}
        export_formats = ['csv', 'xlsx']


class ListedStocksPerSectorTable(tables.Table):
    sector = tables.Column()
    num_listed = tables.Column()

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


class TechnicalAnalysisSummaryTable(tables.Table):
    symbol = tables.Column(
        accessor="stockcode__symbol", verbose_name="Symbol", attrs={"th": {"class": "headcol"},
                                                                    "td": {"class": "headcol"}})
    sma200 = tables.Column()
    sma20 = tables.Column()
    lastcloseprice = tables.Column()
    high52w = tables.Column()
    low52w = tables.Column()
    ytd = tables.Column()
    mtd = tables.Column()
    wtd = tables.Column()
    beta = tables.Column()
    adtv = tables.Column()

    class Meta:
        attrs = {'class': 'djangotables'}
        export_formats = ['csv', 'xlsx']
