from django.db import models

# Create your models here.


class ListedEquities(models.Model):
    stockcode = models.SmallAutoField(primary_key=True)
    securityname = models.CharField(max_length=100)
    symbol = models.CharField(unique=True, max_length=20)
    status = models.CharField(max_length=20, blank=True, null=True)
    sector = models.CharField(max_length=100, blank=True, null=True)
    issuedsharecapital = models.BigIntegerField(blank=True, null=True)
    marketcapitalization = models.DecimalField(
        max_digits=23, decimal_places=2, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'listedequities'


class DailyEquitySummary(models.Model):
    equitytradeid = models.AutoField(primary_key=True)
    date = models.DateField(unique=True)
    stockcode = models.ForeignKey(
        ListedEquities, models.CASCADE, db_column='stockcode')
    openprice = models.DecimalField(
        max_digits=12, decimal_places=2, blank=True, null=True)
    high = models.DecimalField(
        max_digits=12, decimal_places=2, blank=True, null=True)
    low = models.DecimalField(
        max_digits=12, decimal_places=2, blank=True, null=True)
    osbid = models.DecimalField(max_digits=12, decimal_places=2,
                                blank=True, null=True, verbose_name="O/S Bid Price($)")
    osbidvol = models.PositiveIntegerField(
        blank=True, null=True, verbose_name="O/S Bid Volume")
    osoffer = models.DecimalField(
        max_digits=12, decimal_places=2, blank=True, null=True, verbose_name="O/S Offer Price($)")
    osoffervol = models.PositiveIntegerField(
        blank=True, null=True, verbose_name="O/S Offer Volume")
    saleprice = models.DecimalField(
        max_digits=12, decimal_places=2, blank=True, null=True, verbose_name="Last Sale Price($)")
    closeprice = models.DecimalField(
        max_digits=12, decimal_places=2, blank=True, null=True)
    changedollars = models.DecimalField(
        max_digits=7, decimal_places=2, blank=True, null=True)
    volumetraded = models.PositiveIntegerField(blank=True, null=True)
    valuetraded = models.DecimalField(
        max_digits=20, decimal_places=2, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'dailyequitysummary'


class HistoricalDividendInfo(models.Model):
    historicaldividendid = models.AutoField(primary_key=True)
    date = models.DateField(verbose_name='Date')
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
    compositetotalsindexvalue = models.DecimalField(
        max_digits=20, decimal_places=2, blank=True, null=True, verbose_name="Composite Totals Index Value")
    compositetotalsindexchange = models.DecimalField(
        max_digits=10, decimal_places=2, blank=True, null=True, verbose_name="Composite Totals Index Change")
    compositetotalschange = models.DecimalField(
        max_digits=7, decimal_places=2, blank=True, null=True, verbose_name="Composite Totals Change(%)")
    compositetotalsvolumetraded = models.PositiveIntegerField(
        blank=True, null=True, verbose_name="Composite Totals Volume Traded")
    compositetotalsvaluetraded = models.DecimalField(
        max_digits=23, decimal_places=2, blank=True, null=True, verbose_name="Composite Totals Value Traded($)")
    compositetotalsnumtrades = models.PositiveIntegerField(
        blank=True, null=True, verbose_name="Composite Totals Number Traded")
    alltnttotalsindexvalue = models.DecimalField(
        max_digits=20, decimal_places=2, blank=True, null=True, verbose_name="All TnT Totals Index Value")
    alltnttotalsindexchange = models.DecimalField(
        max_digits=10, decimal_places=2, blank=True, null=True, verbose_name="All TnT Totals Index Change")
    alltnttotalschange = models.DecimalField(
        max_digits=7, decimal_places=2, blank=True, null=True, verbose_name="All TnT Totals Change(%)")
    alltnttotalsvolumetraded = models.PositiveIntegerField(
        blank=True, null=True, verbose_name="All TnT Totals Volume Traded")
    alltnttotalsvaluetraded = models.DecimalField(
        max_digits=23, decimal_places=2, blank=True, null=True, verbose_name="All TnT Totals Value Traded($)")
    alltnttotalsnumtrades = models.PositiveIntegerField(
        blank=True, null=True, verbose_name="All TnT Totals Number Traded")
    crosslistedtotalsindexvalue = models.DecimalField(
        max_digits=20, decimal_places=2, blank=True, null=True, verbose_name="Cross-listed Totals Index Value")
    crosslistedtotalsindexchange = models.DecimalField(
        max_digits=10, decimal_places=2, blank=True, null=True, verbose_name="Cross-listed Totals Index Change")
    crosslistedtotalschange = models.DecimalField(
        max_digits=7, decimal_places=2, blank=True, null=True, verbose_name="Cross-lised Totals Change(%)")
    crosslistedtotalsvolumetraded = models.PositiveIntegerField(
        blank=True, null=True, verbose_name="Cross-listed Totals Volume Traded")
    crosslistedtotalsvaluetraded = models.DecimalField(
        max_digits=23, decimal_places=2, blank=True, null=True, verbose_name="Cross-listed Totals Value Traded($)")
    crosslistedtotalsnumtrades = models.PositiveIntegerField(
        blank=True, null=True, verbose_name="Cross-listed Totals Number Traded")
    smetotalsindexvalue = models.DecimalField(
        max_digits=20, decimal_places=2, blank=True, null=True, verbose_name="SME Totals Index Value")
    smetotalsindexchange = models.DecimalField(
        max_digits=10, decimal_places=2, blank=True, null=True, verbose_name="SME Totals Index Change")
    smetotalschange = models.DecimalField(
        max_digits=7, decimal_places=2, blank=True, null=True, verbose_name="SME Totals Change(%)")
    smetotalsvolumetraded = models.PositiveIntegerField(
        blank=True, null=True, verbose_name="SME Totals Volume Traded")
    smetotalsvaluetraded = models.DecimalField(
        max_digits=23, decimal_places=2, blank=True, null=True, verbose_name="SME Totals Value Traded($)")
    smetotalsnumtrades = models.PositiveIntegerField(
        blank=True, null=True, verbose_name="SME Totals Number Traded")
    mutualfundstotalsvolumetraded = models.PositiveIntegerField(
        blank=True, null=True, verbose_name="Mutual Funds Totals Volume Traded")
    mutualfundstotalsvaluetraded = models.DecimalField(
        max_digits=23, decimal_places=2, blank=True, null=True, verbose_name="Mutual Funds Totals Value Traded($)")
    mutualfundstotalsnumtrades = models.PositiveIntegerField(
        blank=True, null=True, verbose_name="Mutual Funds Totals Number Traded")
    secondtiertotalsnumtrades = models.PositiveIntegerField(
        blank=True, null=True, verbose_name="Second Tier Totals Number Traded")

    class Meta:
        managed = False
        db_table = 'historicalmarketsummary'
