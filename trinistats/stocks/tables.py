import django_tables2 as tables
from stocks import models
from django_tables2.export.views import ExportMixin


class HistoricalStockInfoTable(tables.Table):
    date = tables.DateColumn(verbose_name="Date")
    currency = tables.Column(
        accessor="symbol__currency", verbose_name="Currency")
    open_price = tables.Column(verbose_name="Open Price ($)")
    high = tables.Column(verbose_name="High ($)")
    low = tables.Column(verbose_name="Low ($)")
    close_price = tables.Column(verbose_name="Close Price ($)")
    volume_traded = tables.Column(verbose_name="Volume Traded (Shares)")
    change_dollars = tables.Column(verbose_name="Price Change ($)")

    class Meta:
        attrs = {"class": "djangotables"}
        export_formats = ['csv', 'xlsx']


class HistoricalDividendInfoTable(tables.Table):
    class Meta:
        model = models.HistoricalDividendInfo
        attrs = {"class": "djangotables"}
        fields = ('record_date', 'dividend_amount', 'currency')
        export_formats = ['csv', 'xlsx']


class HistoricalDividendYieldTable(tables.Table):
    class Meta:
        model = models.DividendYield
        attrs = {"class": "djangotables"}
        fields = ('date', 'yield_percent')


class DailyTradingSummaryTable(tables.Table):
    symbol = tables.Column(accessor="symbol__symbol", verbose_name="Symbol", attrs={"th": {"class": "headcol"},
                                                                                    "td": {"class": "headcol"}})
    volume_traded = tables.Column(verbose_name="Volume Traded (Shares)")
    last_sale_price = tables.Column(verbose_name="Last Sale Price ($)")
    currency = tables.Column(
        accessor="symbol__currency", verbose_name="Currency")
    value_traded = tables.Column(verbose_name="Dollar Volume ($)")
    low = tables.Column(verbose_name="Low ($)")
    high = tables.Column(verbose_name="High ($)")
    change_dollars = tables.Column(verbose_name="Price Change ($)")

    # make the cells green if prices are up, and red if prices are down
    def render_change_dollars(self, value, column):
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
    security_name = tables.Column()
    status = tables.Column()
    sector = tables.Column()
    issued_share_capital = tables.Column()
    market_capitalization = tables.Column()
    currency = tables.Column()

    def render_status(self, value, column):
        if value == 'Suspended':
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


class HistoricalIndicesSummaryTable(tables.Table):
    class Meta:
        model = models.HistoricalIndicesInfo
        attrs = {"class": "djangotables"}
        fields = ('date', 'index_value', 'change_percent',
                  'volume_traded', 'value_traded', 'num_trades')
        export_formats = ['csv', 'xlsx']


class OSTradesHistoryTable(tables.Table):
    class Meta:
        model = models.DailyStockSummary
        attrs = {"class": "djangotables"}
        fields = ('date', 'os_bid', 'os_bid_vol', 'os_offer', 'os_offer_vol')
        export_formats = ['csv', 'xlsx']


class TechnicalAnalysisSummaryTable(tables.Table):
    symbol = tables.Column(accessor="symbol__symbol", verbose_name="Symbol", attrs={"th": {"class": "headcol"},
                                                                                    "td": {"class": "headcol"}})
    sma_200 = tables.Column()
    sma_20 = tables.Column()
    last_close_price = tables.Column()
    high_52w = tables.Column()
    low_52w = tables.Column()
    ytd = tables.Column()
    mtd = tables.Column()
    wtd = tables.Column()
    beta = tables.Column()
    adtv = tables.Column()

    def render_ytd(self, value, column):
        if value < 0:
            column.attrs = {'td': {'bgcolor': '#ff8080'}}
        elif value > 0:
            column.attrs = {'td': {'bgcolor': '#80ff80'}}
        else:
            column.attrs = {'td': {}}
        return value

    def render_mtd(self, value, column):
        if value < 0:
            column.attrs = {'td': {'bgcolor': '#ff8080'}}
        elif value > 0:
            column.attrs = {'td': {'bgcolor': '#80ff80'}}
        else:
            column.attrs = {'td': {}}
        return value

    def render_wtd(self, value, column):
        if value < 0:
            column.attrs = {'td': {'bgcolor': '#ff8080'}}
        elif value > 0:
            column.attrs = {'td': {'bgcolor': '#80ff80'}}
        else:
            column.attrs = {'td': {}}
        return value

    class Meta:
        attrs = {'class': 'djangotables'}
        export_formats = ['csv', 'xlsx']
