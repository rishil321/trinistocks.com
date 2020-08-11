from django.db import models
from django.urls import reverse
from .templatetags import stocks_template_tags
from urllib.parse import urlencode

# Create your models here.

# These classes are created manually in the database for the output of the scraping script


class ListedEquities(models.Model):
    symbol = models.CharField(
        primary_key=True, unique=True, max_length=20, verbose_name="Symbol")
    security_name = models.CharField(
        max_length=100, verbose_name="Security Name")
    status = models.CharField(max_length=20, blank=True, null=True)
    sector = models.CharField(max_length=100, blank=True, null=True)
    issued_share_capital = models.BigIntegerField(
        blank=True, null=True, verbose_name="Issued Share Capital (shares)")
    market_capitalization = models.DecimalField(
        max_digits=23, decimal_places=2, blank=True, null=True, verbose_name="Market Capitalization ($)")
    currency = models.CharField(max_length=3, blank=False, null=False)
    financial_year_end = models.CharField(max_length=45, blank=True, null=True)
    website_url = models.CharField(max_length=2083, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'listed_equities'


class ListedEquitiesPerSector(models.Model):
    sector_id = models.SmallAutoField(primary_key=True)
    sector = models.CharField(max_length=100, verbose_name="Sector")
    num_listed = models.SmallIntegerField(
        null=False, verbose_name="Number of Listed Stocks")

    class Meta:
        managed = False
        db_table = 'listed_equities_per_sector'


class DailyStockSummary(models.Model):
    daily_share_id = models.AutoField(primary_key=True)
    symbol = models.ForeignKey(
        ListedEquities, models.CASCADE, db_column='symbol')
    date = models.DateField()
    open_price = models.DecimalField(
        max_digits=12, decimal_places=2, blank=True, null=True, verbose_name="Open Price ($)")
    high = models.DecimalField(
        max_digits=12, decimal_places=2, blank=True, null=True, verbose_name="High ($)")
    low = models.DecimalField(
        max_digits=12, decimal_places=2, blank=True, null=True, verbose_name="Low ($)")
    os_bid = models.DecimalField(max_digits=12, decimal_places=2,
                                 blank=True, null=True, verbose_name="O/S Bid Price($)")
    os_bid_vol = models.PositiveIntegerField(
        blank=True, null=True, verbose_name="O/S Bid Volume")
    os_offer = models.DecimalField(
        max_digits=12, decimal_places=2, blank=True, null=True, verbose_name="O/S Offer Price($)")
    os_offer_vol = models.PositiveIntegerField(
        blank=True, null=True, verbose_name="O/S Offer Volume")
    last_sale_price = models.DecimalField(
        max_digits=12, decimal_places=2, blank=True, null=True, verbose_name="Last Sale Price($)")
    was_traded_today = models.SmallIntegerField(
        blank=True, null=True, verbose_name="Was Traded Today")
    volume_traded = models.PositiveIntegerField(blank=True, null=True)
    close_price = models.DecimalField(
        max_digits=12, decimal_places=2, blank=True, null=True, verbose_name="Close Price ($)")
    change_dollars = models.DecimalField(
        max_digits=7, decimal_places=2, blank=True, null=True, verbose_name="Daily Change ($)")
    value_traded = models.DecimalField(
        max_digits=20, decimal_places=2, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'dailyequitysummary'
        unique_together = (('date', 'symbol'),)
        ordering = ["-valuetraded"]

    def get_absolute_url(self):
        base_url = reverse('stocks:dailytradingsummary', current_app="stocks")
        query_string = urlencode({'date': stocks_template_tags.get_latest_date_dailytradingsummary(),
                                  'wastradedtoday': 1, 'sort': '-valuetraded'})
        url = '{}?{}'.format(base_url, query_string)
        return url


class HistoricalDividendInfo(models.Model):
    historicaldividendid = models.AutoField(primary_key=True)
    date = models.DateField(verbose_name='Record Date')
    dividendamount = models.DecimalField(
        max_digits=14, decimal_places=2, verbose_name='Dividend ($/share)')
    stockcode = models.ForeignKey(
        'Listedequities', models.CASCADE, db_column='stockcode')
    currency = models.CharField(max_length=6, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'historicaldividendinfo'
        unique_together = (('date', 'stockcode'),)


class DividendYield(models.Model):
    dividendyieldid = models.AutoField(primary_key=True)
    date = models.DateField(verbose_name='Date Yield Calculated')
    yieldpercent = models.DecimalField(
        max_digits=20, decimal_places=5, verbose_name='Yield %')
    stockcode = models.ForeignKey(
        'Listedequities', models.CASCADE, db_column='stockcode')

    class Meta:
        managed = False
        db_table = 'dividendyield'
        unique_together = (('date', 'stockcode'),)


class HistoricalStockInfo(models.Model):
    historicalstockid = models.AutoField(primary_key=True)
    date = models.DateField(verbose_name="Date Recorded")
    stockcode = models.ForeignKey(
        'Listedequities', models.CASCADE, db_column='stockcode')
    closingquote = models.DecimalField(
        max_digits=14, decimal_places=2, blank=True, null=True, verbose_name="Stock Closing Quote ($)")
    changedollars = models.DecimalField(
        max_digits=14, decimal_places=2, blank=True, null=True, verbose_name="Change ($)")
    volumetraded = models.PositiveIntegerField(
        blank=True, null=True, verbose_name="Volume Traded")
    currency = models.CharField(
        max_length=6, blank=True, null=True, verbose_name="Currency")

    class Meta:
        managed = False
        db_table = 'historicalstockinfo'
        unique_together = (('date', 'stockcode'),)


class HistoricalMarketSummary(models.Model):
    summaryid = models.AutoField(primary_key=True)
    date = models.DateField(verbose_name="Date Recorded", unique=True)
    indexname = models.CharField(
        verbose_name="Market Index Name", null=False, blank=False, max_length=100)
    indexvalue = models.DecimalField(
        max_digits=20, decimal_places=2, blank=True, null=True, verbose_name="Index Value")
    indexchange = models.DecimalField(
        max_digits=10, decimal_places=2, blank=True, null=True, verbose_name="Index Change")
    changepercent = models.DecimalField(
        max_digits=7, decimal_places=2, blank=True, null=True, verbose_name="Change (%)")
    volumetraded = models.PositiveIntegerField(
        blank=True, null=True, verbose_name="Volume Traded (Shares)")
    valuetraded = models.DecimalField(
        max_digits=23, decimal_places=2, blank=True, null=True, verbose_name="Value Traded ($)")
    numtrades = models.PositiveIntegerField(
        blank=True, null=True, verbose_name="Number of Trades")

    class Meta:
        managed = False
        db_table = 'historicalmarketsummary'


class TechnicalAnalysisSummary(models.Model):
    technicalanalysisid = models.AutoField(primary_key=True)
    stockcode = models.ForeignKey(
        'ListedEquities', models.CASCADE, db_column='stockcode')
    lastcloseprice = models.DecimalField(
        max_digits=20, decimal_places=2, blank=True, null=True, verbose_name="Last Close Quote($)")
    sma20 = models.DecimalField(
        max_digits=20, decimal_places=2, blank=True, null=True, verbose_name="SMA20($)")
    sma200 = models.DecimalField(
        max_digits=20, decimal_places=2, blank=True, null=True, verbose_name="SMA200($)")
    beta = models.DecimalField(
        max_digits=4, decimal_places=2, blank=True, null=True, verbose_name="Beta(TTM)")
    adtv = models.PositiveIntegerField(
        blank=True, null=True, verbose_name="ADTV(shares)(Trailing 30d)")
    high52w = models.DecimalField(
        max_digits=20, decimal_places=2, blank=True, null=True, verbose_name="52W-high($)")
    low52w = models.DecimalField(
        max_digits=20, decimal_places=2, blank=True, null=True, verbose_name="52W-low($)")
    wtd = models.DecimalField(
        max_digits=6, decimal_places=2, blank=True, null=True, verbose_name="WTD(%)")
    mtd = models.DecimalField(
        max_digits=6, decimal_places=2, blank=True, null=True, verbose_name="MTD(%)")
    ytd = models.DecimalField(
        max_digits=6, decimal_places=2, blank=True, null=True, verbose_name="YTD(%)")

    class Meta:
        managed = False
        db_table = 'technical_analysis_summary'
