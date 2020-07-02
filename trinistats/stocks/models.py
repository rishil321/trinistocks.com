from django.db import models
from django.urls import reverse
from .templatetags import stocks_template_tags
from urllib.parse import urlencode

# Create your models here.

# These classes are created manually in the database for the output of the scraping script


class ListedEquities(models.Model):
    stockcode = models.SmallAutoField(primary_key=True)
    securityname = models.CharField(
        max_length=100, verbose_name="Security Name")
    symbol = models.CharField(
        unique=True, max_length=20, verbose_name="Symbol")
    status = models.CharField(max_length=20, blank=True, null=True)
    sector = models.CharField(max_length=100, blank=True, null=True)
    issuedsharecapital = models.BigIntegerField(
        blank=True, null=True, verbose_name="Issued Share Capital (shares)")
    marketcapitalization = models.DecimalField(
        max_digits=23, decimal_places=2, blank=True, null=True, verbose_name="Market Capitalization ($)")
    currency = models.CharField(max_length=3, blank=False, null=False)

    class Meta:
        managed = False
        db_table = 'listedequities'


class ListedEquitiesPerSector(models.Model):
    sector_id = models.SmallAutoField(primary_key=True)
    sector = models.CharField(max_length=100, verbose_name="Sector Name")
    num_listed = models.SmallIntegerField(
        null=False, verbose_name="Number of Listed Stocks")

    class Meta:
        managed = False
        db_table = 'listedequities_per_sector'


class DailyTradingSummary(models.Model):
    equitytradeid = models.AutoField(primary_key=True)
    date = models.DateField(unique=True)
    stockcode = models.ForeignKey(
        ListedEquities, models.CASCADE, db_column='stockcode')
    openprice = models.DecimalField(
        max_digits=12, decimal_places=2, blank=True, null=True, verbose_name="Open Price ($)")
    high = models.DecimalField(
        max_digits=12, decimal_places=2, blank=True, null=True, verbose_name="High ($)")
    low = models.DecimalField(
        max_digits=12, decimal_places=2, blank=True, null=True, verbose_name="Low ($)")
    osbid = models.DecimalField(max_digits=12, decimal_places=2,
                                blank=True, null=True, verbose_name="O/S Bid Price($)")
    osbidvol = models.PositiveIntegerField(
        blank=True, null=True, verbose_name="O/S Bid Volume")
    osoffer = models.DecimalField(
        max_digits=12, decimal_places=2, blank=True, null=True, verbose_name="O/S Offer Price($)")
    osoffervol = models.PositiveIntegerField(
        blank=True, null=True, verbose_name="O/S Offer Volume")
    lastsaleprice = models.DecimalField(
        max_digits=12, decimal_places=2, blank=True, null=True, verbose_name="Last Sale Price($)")
    wastradedtoday = models.SmallIntegerField(
        blank=True, null=True, verbose_name="Was Traded Today")
    volumetraded = models.PositiveIntegerField(blank=True, null=True)
    closeprice = models.DecimalField(
        max_digits=12, decimal_places=2, blank=True, null=True, verbose_name="Close Price ($)")
    changedollars = models.DecimalField(
        max_digits=7, decimal_places=2, blank=True, null=True, verbose_name="Daily Change ($)")
    valuetraded = models.DecimalField(
        max_digits=20, decimal_places=2, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'dailyequitysummary'
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
