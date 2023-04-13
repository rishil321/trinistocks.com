from urllib.parse import urlencode

import django_tables2 as tables
from django.urls import reverse
from django.utils.html import escape
from django.utils.safestring import mark_safe
from django_tables2.export.views import ExportMixin

from stocks import models

from .templatetags import stocks_template_tags


class HistoricalStockInfoTable(tables.Table):
    date = tables.DateColumn(verbose_name="Date")
    currency = tables.Column(
        accessor="symbol__currency", verbose_name="Currency")
    open_price = tables.Column(verbose_name="Open Price")
    high = tables.Column(verbose_name="High")
    low = tables.Column(verbose_name="Low")
    close_price = tables.Column(verbose_name="Close Price")
    volume_traded = tables.Column(verbose_name="Volume Traded")
    change_dollars = tables.Column(verbose_name="Price Change")

    def render_date(self, value, column):
        column.attrs = {'td': {'data-label': column.verbose_name}}
        return value

    def render_currency(self, value, column):
        column.attrs = {'td': {'data-label': column.verbose_name}}
        return value

    def render_open_price(self, value, column):
        column.attrs = {'td': {'data-label': column.verbose_name}}
        return "${:,.2f}".format(value)

    def render_high(self, value, column):
        column.attrs = {'td': {'data-label': column.verbose_name}}
        return "${:,.2f}".format(value)

    def render_low(self, value, column):
        column.attrs = {'td': {'data-label': column.verbose_name}}
        return "${:,.2f}".format(value)

    def render_close_price(self, value, column):
        column.attrs = {'td': {'data-label': column.verbose_name}}
        return "${:,.2f}".format(value)

    def render_volume_traded(self, value, column):
        column.attrs = {'td': {'data-label': column.verbose_name}}
        return "{:,} shares".format(value)

    def render_change_dollars(self, value, column):
        if value < 0:
            column.attrs = {'td': {'style': 'color:red',
                                   'data-label': column.verbose_name}}
            return '-'+'$'+str(abs(value))
        elif value > 0:
            column.attrs = {'td': {'style': 'color:green',
                                   'data-label': column.verbose_name}}
        else:
            column.attrs = {'td': {'data-label': column.verbose_name}}
        return "${:,.2f}".format(value)

    class Meta:
        attrs = {"class": "djangotables"}
        export_formats = ['csv', 'xlsx']


class HistoricalDividendInfoTable(tables.Table):
    class Meta:
        model = models.HistoricalDividendInfo
        attrs = {"class": "djangotables"}
        fields = ('record_date', 'dividend_amount', 'currency')
        export_formats = ['csv', 'xlsx']


class DailyTradingSummaryTable(tables.Table):
    symbol = tables.Column(accessor="symbol__symbol", verbose_name="Symbol", linkify=(
        lambda record: render_symbol_id_link(value=record)), orderable=False)
    volume_traded = tables.Column(
        verbose_name="Volume Traded", orderable=False)
    last_sale_price = tables.Column(
        verbose_name="Last Sale Price", orderable=False)
    currency = tables.Column(
        accessor="symbol__currency", verbose_name="Currency", orderable=False)
    value_traded = tables.Column(
        verbose_name="Dollar Volume Traded", orderable=False)
    low = tables.Column(verbose_name="Low", orderable=False)
    high = tables.Column(verbose_name="High", orderable=False)
    change_dollars = tables.Column(
        verbose_name="Price Change", orderable=False)

    # add the symbols to all other columns
    def render_volume_traded(self, value, column):
        column.attrs = {'td': {'data-label': column.verbose_name}}
        return "{:,} shares".format(value)

    def render_last_sale_price(self, value, column):
        column.attrs = {'td': {'data-label': column.verbose_name}}
        return "${:,.2f}".format(value)

    def render_currency(self, value, column):
        column.attrs = {'td': {'data-label': column.verbose_name}}
        return value

    def render_value_traded(self, value, column):
        column.attrs = {'td': {'data-label': column.verbose_name}}
        return "${:,.2f}".format(value)

    def render_low(self, value, column):
        column.attrs = {'td': {'data-label': column.verbose_name}}
        return "${:,.2f}".format(value)

    def render_high(self, value, column):
        column.attrs = {'td': {'data-label': column.verbose_name}}
        return "${:,.2f}".format(value)

    def render_symbol(self, value, column):
        column.attrs = {'td': {'data-label': column.verbose_name}}
        return value

    # make the cells green if prices are up, and red if prices are down
    def render_change_dollars(self, value, column):
        if value < 0:
            column.attrs = {'td': {'style': 'color:red',
                                   'data-label': column.verbose_name}}
            return '-'+'$'+str(abs(value))
        elif value > 0:
            column.attrs = {'td': {'style': 'color:green',
                                   'data-label': column.verbose_name}}
        else:
            column.attrs = {'td': {'data-label': column.verbose_name}}
        return "${:,.2f}".format(value)

    class Meta:
        attrs = {"class": "djangotables"}
        export_formats = ['csv', 'xlsx']


class ListedStocksTable(tables.Table):
    symbol = tables.Column(verbose_name="Symbol", linkify=(
        lambda record: render_symbol_link(value=record)))
    security_name = tables.Column(verbose_name="Security Name")
    status = tables.Column(verbose_name="Status")
    sector = tables.Column(verbose_name="Sector")
    issued_share_capital = tables.Column(verbose_name="Issued Share Capital")
    market_capitalization = tables.Column(verbose_name="Market Capitalization")
    currency = tables.Column(verbose_name="Currency")

    def render_symbol(self, value, column):
        column.attrs = {'td': {'data-label': column.verbose_name}}
        return value

    def render_security_name(self, value, column):
        column.attrs = {
            'td': {'data-label': column.verbose_name, 'class': 'limited_width_col'}}
        return value

    def render_status(self, value, column):
        if value == 'Suspended':
            column.attrs = {'td': {'style': 'color:red'},
                            'data-label': column.verbose_name}
        else:
            column.attrs = {'td': {'style': 'color:green',
                                   'data-label': column.verbose_name}}
        return value

    def render_sector(self, value, column):
        column.attrs = {'td': {'data-label': column.verbose_name}}
        return value

    def render_issued_share_capital(self, value, column):
        column.attrs = {'td': {'data-label': column.verbose_name}}
        return "{:,} shares".format(value)

    def render_market_capitalization(self, value, column):
        column.attrs = {'td': {'data-label': column.verbose_name}}
        return "${:,.2f}".format(value)

    def render_currency(self, value, column):
        column.attrs = {'td': {'data-label': column.verbose_name}}
        return value

    class Meta:
        attrs = {"class": "djangotables"}
        export_formats = ['csv', 'xlsx']


class ListedStocksPerSectorTable(tables.Table):
    sector = tables.Column(verbose_name="Sector")
    num_listed = tables.Column(verbose_name="Number Listed")

    def render_sector(self, value, column):
        column.attrs = {'td': {'data-label': column.verbose_name}}
        return value

    def render_num_listed(self, value, column):
        column.attrs = {
            'td': {'data-label': column.verbose_name, 'class': 'limited_width_col'}}
        return value

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

    date = tables.Column(verbose_name="Date")
    volume_traded = tables.Column(verbose_name="Volume Traded")
    os_bid = tables.Column(verbose_name="O/S Bid Price")
    os_bid_vol = tables.Column(verbose_name="O/S Bid Volume")
    os_offer = tables.Column(verbose_name="O/S Offer Price")
    os_offer_vol = tables.Column("O/S Offer Volume")

    def render_date(self, value, column):
        column.attrs = {'td': {'data-label': column.verbose_name}}
        return value

    def render_volume_traded(self, value, column):
        column.attrs = {'td': {'data-label': column.verbose_name}}
        return "{:,} shares".format(value)

    def render_os_bid(self, value, column):
        column.attrs = {'td': {'data-label': column.verbose_name}}
        return "${:,.2f}".format(value)

    def render_os_bid_vol(self, value, column):
        column.attrs = {'td': {'data-label': column.verbose_name}}
        return "{:,} shares".format(value)

    def render_os_offer(self, value, column):
        column.attrs = {'td': {'data-label': column.verbose_name}}
        return "${:,.2f}".format(value)

    def render_os_offer_vol(self, value, column):
        column.attrs = {'td': {'data-label': column.verbose_name}}
        return "{:,} shares".format(value)

    class Meta:
        model = models.DailyStockSummary
        attrs = {"class": "djangotables"}
        fields = ('date', 'volume_traded', 'os_bid', 'os_bid_vol', 'os_offer',
                  'os_offer_vol')
        export_formats = ['csv', 'xlsx']


class TechnicalAnalysisSummaryTable(tables.Table):
    symbol = tables.Column(accessor="symbol__symbol", verbose_name="Symbol", linkify=(
        lambda record: render_symbol_id_link(value=record)))
    sma_200 = tables.Column(verbose_name="SMA200")
    sma_20 = tables.Column(verbose_name="SMA20")
    last_close_price = tables.Column(verbose_name="Latest Close Price")
    high_52w = tables.Column(verbose_name="52W High")
    low_52w = tables.Column(verbose_name="52W Low")
    ytd = tables.Column(verbose_name="YTD")
    mtd = tables.Column(verbose_name="MTD")
    wtd = tables.Column(verbose_name="WTD")
    beta = tables.Column(verbose_name="TTM Beta")
    adtv = tables.Column(verbose_name="30d ADTV")

    def render_symbol(self, value, column):
        column.attrs = {'td': {'data-label': column.verbose_name}}
        return value

    def render_sma_200(self, value, column):
        column.attrs = {'td': {'data-label': column.verbose_name}}
        return "${:,.2f}".format(value)

    def render_sma_20(self, value, column):
        column.attrs = {'td': {'data-label': column.verbose_name}}
        return "${:,.2f}".format(value)

    def render_last_close_price(self, value, column):
        column.attrs = {'td': {'data-label': column.verbose_name}}
        return "${:,.2f}".format(value)

    def render_high_52w(self, value, column):
        column.attrs = {'td': {'data-label': column.verbose_name}}
        return "${:,.2f}".format(value)

    def render_low_52w(self, value, column):
        column.attrs = {'td': {'data-label': column.verbose_name}}
        return "${:,.2f}".format(value)

    def render_ytd(self, value, column):
        if value < 0:
            column.attrs = {'td': {'style': 'color:red',
                                   'data-label': column.verbose_name}}
        elif value > 0:
            column.attrs = {'td': {'style': 'color:green',
                                   'data-label': column.verbose_name}}
        else:
            column.attrs = {'td': {'data-label': column.verbose_name}}
        return "{:,.2f}%".format(value)

    def render_mtd(self, value, column):
        if value < 0:
            column.attrs = {'td': {'style': 'color:red',
                                   'data-label': column.verbose_name}}
        elif value > 0:
            column.attrs = {'td': {'style': 'color:green',
                                   'data-label': column.verbose_name}}
        else:
            column.attrs = {'td': {'data-label': column.verbose_name}}
        return "{:,.2f}%".format(value)

    def render_wtd(self, value, column):
        if value < 0:
            column.attrs = {'td': {'style': 'color:red',
                                   'data-label': column.verbose_name}}
        elif value > 0:
            column.attrs = {'td': {'style': 'color:green',
                                   'data-label': column.verbose_name}}
        else:
            column.attrs = {'td': {'data-label': column.verbose_name}}
        return "{:,.2f}%".format(value)

    def render_beta(self, value, column):
        column.attrs = {'td': {'data-label': column.verbose_name}}
        return "{:,.2f}".format(value)

    def render_adtv(self, value, column):
        column.attrs = {'td': {'data-label': column.verbose_name}}
        return "{:,} shares".format(value)

    class Meta:
        attrs = {'class': 'djangotables'}
        export_formats = ['csv', 'xlsx']


class FundamentalAnalysisSummaryTable(tables.Table):

    symbol = tables.Column(accessor="symbol__symbol", verbose_name="Symbol", linkify=(
        lambda record: render_fundamental_history_symbol_link(value=record)))
    sector = tables.Column(accessor="symbol__sector", verbose_name="Sector")
    date = tables.Column(verbose_name="Last Updated")
    price_to_earnings_ratio = tables.Column(verbose_name="P/E")
    RoE = tables.Column(verbose_name="RoE")
    price_to_book_ratio = tables.Column(verbose_name="P/B")
    current_ratio = tables.Column(verbose_name="Current Ratio")
    dividend_yield = tables.Column(verbose_name="Dividend Yield")
    dividend_payout_ratio = tables.Column(
        verbose_name="Payout Ratio")
    cash_per_share = tables.Column(verbose_name="Cash Per Share")

    def render_symbol(self, value, column):
        column.attrs = {'td': {'data-label': column.verbose_name}}
        return value

    def render_sector(self, value, column):
        column.attrs = {'td': {'data-label': column.verbose_name}}
        return value

    def render_date(self, value, column):
        column.attrs = {'td': {'data-label': column.verbose_name}}
        return value

    def render_price_to_earnings_ratio(self, value, column):
        column.attrs = {'td': {'data-label': column.verbose_name}}
        return "{:,.2f}".format(value)

    def render_RoE(self, value, column):
        column.attrs = {'td': {'data-label': column.verbose_name}}
        return "{:,.2f}".format(value)

    def render_price_to_book_ratio(self, value, column):
        column.attrs = {'td': {'data-label': column.verbose_name}}
        return "{:,.2f}".format(value)

    def render_current_ratio(self, value, column):
        column.attrs = {'td': {'data-label': column.verbose_name}}
        return "{:,.2f}".format(value)

    def render_dividend_yield(self, value, column):
        column.attrs = {'td': {'data-label': column.verbose_name}}
        return "{:,.2f}%".format(value)

    def render_dividend_payout_ratio(self, value, column):
        column.attrs = {'td': {'data-label': column.verbose_name}}
        return "{:,.2f}%".format(value)

    def render_cash_per_share(self, value, column):
        column.attrs = {'td': {'data-label': column.verbose_name}}
        return "{:,.2f}".format(value)+" $/share"

    class Meta:
        attrs = {'class': 'djangotables'}
        export_formats = ['csv', 'xlsx']


class PortfolioSummaryTable(tables.Table):

    symbol_id = tables.Column(verbose_name="Symbol")
    sector = tables.Column(accessor="symbol__sector", verbose_name="Sector")
    shares_remaining = tables.Column(verbose_name='Number of Shares')
    average_cost = tables.Column(verbose_name="Average Cost")
    current_market_price = tables.Column(verbose_name='Market Price')
    book_cost = tables.Column(verbose_name="Book Cost")
    market_value = tables.Column(verbose_name="Market Value")
    total_gain_loss = tables.Column(verbose_name='Overall Gain/Loss')

    def render_symbol_id(self, value, column):
        column.attrs = {'td': {'data-label': column.verbose_name}}
        return value

    def render_sector(self, value, column):
        column.attrs = {'td': {'data-label': column.verbose_name}}
        return value

    def render_shares_remaining(self, value, column):
        column.attrs = {'td': {'data-label': column.verbose_name}}
        return "{:,} shares".format(value)

    def render_average_cost(self, value, column):
        column.attrs = {'td': {'data-label': column.verbose_name}}
        return "${:,.2f}".format(value)

    def render_current_market_price(self, value, column):
        column.attrs = {'td': {'data-label': column.verbose_name}}
        return "${:,.2f}".format(value)

    def render_book_cost(self, value, column):
        column.attrs = {'td': {'data-label': column.verbose_name}}
        return "${:,.2f}".format(value)

    def render_market_value(self, value, column):
        column.attrs = {'td': {'data-label': column.verbose_name}}
        return "${:,.2f}".format(value)

    def render_total_gain_loss(self, value, column):
        if value < 0:
            column.attrs = {'td': {'style': 'color:red',
                                   'data-label': column.verbose_name}}
            return '-'+'$'+str(abs(value))
        elif value > 0:
            column.attrs = {'td': {'style': 'color:green',
                                   'data-label': column.verbose_name}}
        else:
            column.attrs = {'td': {'data-label': column.verbose_name}}
        return "${:,.2f}".format(value)

    class Meta:
        attrs = {'class': 'djangotables'}
        export_formats = ['csv', 'xlsx']


class StockNewsTable(tables.Table):

    symbol = tables.Column(accessor="symbol__symbol", verbose_name="Symbol", linkify=(
        lambda record: render_fundamental_history_symbol_link(value=record)), orderable=False)
    date = tables.Column(verbose_name="Date Published", orderable=False)
    category = tables.Column(verbose_name="Category", orderable=False)
    title = tables.Column(verbose_name="Title", orderable=False)

    def render_symbol(self, value, column):
        column.attrs = {'td': {'data-label': column.verbose_name}}
        return value

    def render_date(self, value, column):
        column.attrs = {'td': {'data-label': column.verbose_name}}
        return value

    def render_title(self, value, column, record):
        column.attrs = {
            'td': {'data-label': column.verbose_name, 'class': 'title_width_col'}}
        return mark_safe(f'<a href="{escape(record.link)}">{escape(value)}</a>')

    def render_link(self, value, column):
        column.attrs = {'td': {'data-label': column.verbose_name}}
        return value

    def render_category(self, value, column):
        column.attrs = {'td': {'data-label': column.verbose_name}}
        return value

    class Meta:
        attrs = {'class': 'djangotables'}
        export_formats = ['csv', 'xlsx']


class StockNewsHistoryTable(tables.Table):

    symbol = tables.Column(accessor="symbol__symbol", verbose_name="Symbol", linkify=(
        lambda record: render_fundamental_history_symbol_link(value=record)), orderable=True)
    date = tables.Column(verbose_name="Date Published", orderable=True)
    category = tables.Column(verbose_name="Category", orderable=True)
    title = tables.Column(verbose_name="Title", orderable=False)

    def render_symbol(self, value, column):
        column.attrs = {'td': {'data-label': column.verbose_name}}
        return value

    def render_date(self, value, column):
        column.attrs = {'td': {'data-label': column.verbose_name}}
        return value

    def render_title(self, value, column, record):
        column.attrs = {
            'td': {'data-label': column.verbose_name, 'class': 'title_width_col'}}
        return mark_safe(f'<a href="{escape(record.link)}">{escape(value)}</a>')

    def render_link(self, value, column):
        column.attrs = {'td': {'data-label': column.verbose_name}}
        return value

    def render_category(self, value, column):
        column.attrs = {'td': {'data-label': column.verbose_name}}
        return value

    class Meta:
        attrs = {'class': 'djangotables'}
        export_formats = ['csv', 'xlsx']

# methods
# render the URLs for the symbol column


def render_fundamental_history_symbol_link(value):
    base_url = reverse(
        'stocks:fundamentalhistory', current_app="stocks")
    query_string = urlencode({'symbol1': value.symbol_id,
                              'symbol2': 'WCO',
                              'indicator': 'EPS',
                              'date__gte': stocks_template_tags.get_5_yr_back(),
                              'date__lte': stocks_template_tags.get_today()})
    url = '{}?{}'.format(base_url, query_string)
    return url


def render_symbol_id_link(value):
    base_url = reverse(
        'stocks:stockhistory', current_app="stocks")
    query_string = urlencode({'symbol': value.symbol_id,
                              'date__gte': stocks_template_tags.get_1_yr_back(),
                              'date__lte': stocks_template_tags.get_today(),
                              'chart_type': 'candlestick', 'sort': 'date'})
    url = '{}?{}'.format(base_url, query_string)
    return url


def render_symbol_link(value):
    base_url = reverse(
        'stocks:stockhistory', current_app="stocks")
    query_string = urlencode({'symbol': value.symbol,
                              'date__gte': stocks_template_tags.get_1_yr_back(),
                              'date__lte': stocks_template_tags.get_today(),
                              'chart_type': 'candlestick', 'sort': 'date'})
    url = '{}?{}'.format(base_url, query_string)
    return url
