from django.db import models
from django.urls import reverse
from .templatetags import stocks_template_tags
from urllib.parse import urlencode
from django.conf import settings
from django.contrib.auth import get_user_model
from django.contrib.auth.models import AbstractUser
from pygments.lexers import get_all_lexers
from pygments.styles import get_all_styles

# CONSTANTS
LEXERS = [item for item in get_all_lexers() if item[1]]
LANGUAGE_CHOICES = sorted([(item[1][0], item[0]) for item in LEXERS])
STYLE_CHOICES = sorted([(item, item) for item in get_all_styles()])

# Model classes


class ListedEquities(models.Model):
    symbol = models.CharField(
        primary_key=True, unique=True, max_length=20, verbose_name="Symbol"
    )
    security_name = models.CharField(max_length=100, verbose_name="Security Name")
    status = models.CharField(max_length=20, blank=True, null=True)
    sector = models.CharField(max_length=100, blank=True, null=True)
    issued_share_capital = models.BigIntegerField(
        blank=True, null=True, verbose_name="Issued Share Capital"
    )
    market_capitalization = models.DecimalField(
        max_digits=23,
        decimal_places=2,
        blank=True,
        null=True,
        verbose_name="Market Capitalization",
    )
    currency = models.CharField(max_length=3, blank=False, null=False)
    financial_year_end = models.CharField(max_length=45, blank=True, null=True)
    website_url = models.CharField(max_length=2083, blank=True, null=True)

    class Meta:
        managed = False
        db_table = "listed_equities"


class ListedEquitiesPerSector(models.Model):
    sector_id = models.SmallAutoField(primary_key=True)
    sector = models.CharField(max_length=100, verbose_name="Sector")
    num_listed = models.SmallIntegerField(
        null=False, verbose_name="Number of Listed Stocks"
    )

    class Meta:
        managed = False
        db_table = "listed_equities_per_sector"


class DailyStockSummary(models.Model):

    daily_share_id = models.AutoField(primary_key=True)
    symbol = models.ForeignKey(ListedEquities, models.CASCADE, db_column="symbol")
    date = models.DateField()
    open_price = models.DecimalField(
        max_digits=12,
        decimal_places=2,
        blank=True,
        null=True,
        verbose_name="Open Price ($)",
    )
    high = models.DecimalField(
        max_digits=12, decimal_places=2, blank=True, null=True, verbose_name="High ($)"
    )
    low = models.DecimalField(
        max_digits=12, decimal_places=2, blank=True, null=True, verbose_name="Low ($)"
    )
    os_bid = models.DecimalField(
        max_digits=12,
        decimal_places=2,
        blank=True,
        null=True,
        verbose_name="O/S Bid Price($)",
    )
    os_bid_vol = models.PositiveIntegerField(
        blank=True, null=True, verbose_name="O/S Bid Volume"
    )
    os_offer = models.DecimalField(
        max_digits=12,
        decimal_places=2,
        blank=True,
        null=True,
        verbose_name="O/S Offer Price($)",
    )
    os_offer_vol = models.PositiveIntegerField(
        blank=True, null=True, verbose_name="O/S Offer Volume"
    )
    last_sale_price = models.DecimalField(
        max_digits=12,
        decimal_places=2,
        blank=True,
        null=True,
        verbose_name="Last Sale Price($)",
    )
    was_traded_today = models.SmallIntegerField(
        blank=True, null=True, verbose_name="Was Traded Today"
    )
    volume_traded = models.PositiveIntegerField(blank=True, null=True)
    close_price = models.DecimalField(
        max_digits=12,
        decimal_places=2,
        blank=True,
        null=True,
        verbose_name="Close Price ($)",
    )
    change_dollars = models.DecimalField(
        max_digits=7,
        decimal_places=2,
        blank=True,
        null=True,
        verbose_name="Daily Change ($)",
    )
    value_traded = models.DecimalField(
        max_digits=20, decimal_places=2, blank=True, null=True
    )

    class Meta:
        managed = False
        db_table = "daily_stock_summary"
        unique_together = (("date", "symbol"),)
        ordering = ["-value_traded"]

    def get_absolute_url(self):
        base_url = reverse("stocks:dailytradingsummary", current_app="stocks")
        query_string = urlencode(
            {
                "date": stocks_template_tags.get_latest_date_dailytradingsummary(),
                "wastradedtoday": 1,
                "sort": "-valuetraded",
            }
        )
        url = "{}?{}".format(base_url, query_string)
        return url


class HistoricalDividendInfo(models.Model):
    dividend_id = models.AutoField(primary_key=True)
    symbol = models.ForeignKey(ListedEquities, models.CASCADE, db_column="symbol")
    record_date = models.DateField(verbose_name="Record Date")
    dividend_amount = models.DecimalField(
        max_digits=20, decimal_places=5, verbose_name="Dividend ($/share)"
    )
    currency = models.CharField(max_length=6, blank=True, null=True)

    class Meta:
        managed = False
        db_table = "historical_dividend_info"
        unique_together = (("record_date", "symbol"),)


class HistoricalDividendYield(models.Model):
    yield_id = models.AutoField(primary_key=True)
    date = models.DateField(verbose_name="Date Yield Calculated")
    symbol = models.ForeignKey(ListedEquities, models.CASCADE, db_column="symbol")
    dividend_yield = models.DecimalField(
        max_digits=20, decimal_places=5, verbose_name="Yield %"
    )

    class Meta:
        managed = False
        db_table = "historical_dividend_yield"
        unique_together = (("date", "symbol"),)


class HistoricalIndicesInfo(models.Model):
    summary_id = models.AutoField(primary_key=True)
    date = models.DateField(verbose_name="Date Recorded", unique=True)
    index_name = models.CharField(
        verbose_name="Market Index Name", null=False, blank=False, max_length=100
    )
    index_value = models.DecimalField(
        max_digits=20,
        decimal_places=2,
        blank=True,
        null=True,
        verbose_name="Index Value",
    )
    index_change = models.DecimalField(
        max_digits=10,
        decimal_places=2,
        blank=True,
        null=True,
        verbose_name="Index Change",
    )
    change_percent = models.DecimalField(
        max_digits=7, decimal_places=2, blank=True, null=True, verbose_name="Change (%)"
    )
    volume_traded = models.PositiveIntegerField(
        blank=True, null=True, verbose_name="Volume Traded (Shares)"
    )
    value_traded = models.DecimalField(
        max_digits=23,
        decimal_places=2,
        blank=True,
        null=True,
        verbose_name="Value Traded ($)",
    )
    num_trades = models.PositiveIntegerField(
        blank=True, null=True, verbose_name="Number of Trades"
    )

    class Meta:
        managed = False
        db_table = "historical_indices_info"


class TechnicalAnalysisSummary(models.Model):
    technical_analysis_id = models.AutoField(primary_key=True)
    symbol = models.ForeignKey(ListedEquities, models.CASCADE, db_column="symbol")
    last_close_price = models.DecimalField(
        max_digits=20,
        decimal_places=2,
        blank=True,
        null=True,
        verbose_name="Last Close Quote",
    )
    sma_20 = models.DecimalField(
        max_digits=20, decimal_places=2, blank=True, null=True, verbose_name="SMA20"
    )
    sma_200 = models.DecimalField(
        max_digits=20, decimal_places=2, blank=True, null=True, verbose_name="SMA200"
    )
    beta = models.DecimalField(
        max_digits=4, decimal_places=2, blank=True, null=True, verbose_name="Beta(TTM)"
    )
    adtv = models.PositiveIntegerField(
        blank=True, null=True, verbose_name="ADTV(Trailing 30d)"
    )
    high_52w = models.DecimalField(
        max_digits=20, decimal_places=2, blank=True, null=True, verbose_name="52W-high"
    )
    low_52w = models.DecimalField(
        max_digits=20, decimal_places=2, blank=True, null=True, verbose_name="52W-low"
    )
    wtd = models.DecimalField(
        max_digits=6, decimal_places=2, blank=True, null=True, verbose_name="WTD"
    )
    mtd = models.DecimalField(
        max_digits=6, decimal_places=2, blank=True, null=True, verbose_name="MTD"
    )
    ytd = models.DecimalField(
        max_digits=6, decimal_places=2, blank=True, null=True, verbose_name="YTD"
    )

    class Meta:
        managed = False
        db_table = "technical_analysis_summary"


class FundamentalAnalysisSummary(models.Model):

    id = models.AutoField(primary_key=True)
    symbol = models.ForeignKey(
        ListedEquities,
        models.CASCADE,
        db_column="symbol",
        related_name="fundamental_analysis_data",
    )
    date = models.DateField(verbose_name="Date")
    report_type = models.CharField(max_length=10)
    RoE = models.DecimalField(max_digits=10, decimal_places=3, blank=True, null=True)
    EPS = models.DecimalField(max_digits=10, decimal_places=3, blank=True, null=True)
    EPS_growth_rate = models.DecimalField(
        max_digits=10,
        decimal_places=2,
        blank=True,
        null=True,
        verbose_name="EPS Growth Rate(%)",
    )
    PEG = models.DecimalField(max_digits=10, decimal_places=3, blank=True, null=True)
    RoIC = models.DecimalField(max_digits=10, decimal_places=3, blank=True, null=True)
    working_capital = models.DecimalField(
        max_digits=40,
        decimal_places=3,
        blank=True,
        null=True,
        verbose_name="Working Capital",
    )
    price_to_earnings_ratio = models.DecimalField(
        max_digits=10, decimal_places=3, blank=True, null=True, verbose_name="P/E"
    )
    price_to_dividends_per_share_ratio = models.DecimalField(
        max_digits=10, decimal_places=3, blank=True, null=True, verbose_name="P/DPS"
    )
    dividend_yield = models.DecimalField(
        max_digits=10,
        decimal_places=3,
        blank=True,
        null=True,
        verbose_name="Dividend Yield(%)",
    )
    dividend_payout_ratio = models.DecimalField(
        max_digits=10,
        decimal_places=3,
        blank=True,
        null=True,
        verbose_name="Dividend Payout Ratio(%)",
    )
    book_value_per_share = models.DecimalField(
        max_digits=10, decimal_places=3, blank=True, null=True, verbose_name="BVPS"
    )
    price_to_book_ratio = models.DecimalField(
        max_digits=10, decimal_places=3, blank=True, null=True, verbose_name="P/B"
    )
    current_ratio = models.DecimalField(
        max_digits=10,
        decimal_places=3,
        blank=True,
        null=True,
        verbose_name="Current Ratio",
    )
    cash_per_share = models.DecimalField(
        max_digits=10,
        decimal_places=3,
        blank=True,
        null=True,
        verbose_name="Cash per Share",
    )

    class Meta:
        managed = False
        db_table = "calculated_fundamental_ratios"


class PortfolioTransactions(models.Model):

    user = models.ForeignKey(settings.AUTH_USER_MODEL, models.CASCADE, default=1)
    symbol = models.ForeignKey(ListedEquities, models.CASCADE, db_column="symbol")
    date = models.DateField(verbose_name="Date")
    bought_or_sold = models.CharField(max_length=10)
    share_price = models.DecimalField(max_digits=12, decimal_places=2)
    num_shares = models.IntegerField()

    class Meta:
        managed = True
        db_table = "portfolio_transactions"
        unique_together = [["user", "date", "symbol", "num_shares", "bought_or_sold"]]


class PortfolioSummary(models.Model):

    user = models.ForeignKey(settings.AUTH_USER_MODEL, models.CASCADE, default=1)
    symbol = models.ForeignKey(ListedEquities, models.CASCADE, db_column="symbol")
    shares_remaining = models.IntegerField()
    average_cost = models.DecimalField(max_digits=12, decimal_places=2, null=True)
    book_cost = models.DecimalField(max_digits=20, decimal_places=2, null=True)
    current_market_price = models.DecimalField(
        max_digits=12, decimal_places=2, null=True
    )
    market_value = models.DecimalField(max_digits=20, decimal_places=2, null=True)
    total_gain_loss = models.DecimalField(max_digits=20, decimal_places=2, null=True)
    gain_loss_percent = models.DecimalField(max_digits=6, decimal_places=2, null=True)

    class Meta:
        managed = True
        db_table = "portfolio_summary"
        unique_together = [["user", "symbol"]]


class PortfolioSectors(models.Model):

    user = models.ForeignKey(settings.AUTH_USER_MODEL, models.CASCADE)
    sector = models.CharField(max_length=100)
    book_cost = models.DecimalField(max_digits=20, decimal_places=2, null=True)
    market_value = models.DecimalField(max_digits=20, decimal_places=2, null=True)
    total_gain_loss = models.DecimalField(max_digits=20, decimal_places=2, null=True)
    gain_loss_percent = models.DecimalField(max_digits=6, decimal_places=2, null=True)

    class Meta:
        managed = True
        db_table = "stocks_portfoliosectors"
        unique_together = [["user", "sector"]]


class SimulatorGames(models.Model):
    game_id = models.AutoField(primary_key=True, unique=True)
    date_created = models.DateField(auto_now_add=True)
    date_ended = models.DateField()
    game_name = models.CharField(max_length=100, unique=True)
    private = models.BooleanField(default=False)
    game_code = models.PositiveIntegerField(null=True)

    class Meta:
        managed = True


class SimulatorPlayers(models.Model):
    simulator_player_id = models.AutoField(primary_key=True, unique=True)
    user = models.ForeignKey(settings.AUTH_USER_MODEL, models.CASCADE)
    simulator_game = models.ForeignKey(
        SimulatorGames,
        models.CASCADE,
        db_column="game_name",
    )
    liquid_cash = models.DecimalField(max_digits=10, decimal_places=2, null=False)

    class Meta:
        managed = True
        unique_together = [["user", "simulator_game"]]


class SimulatorTransactions(models.Model):
    simulator_transaction_id = models.AutoField(primary_key=True, unique=True)
    simulator_player = models.ForeignKey(SimulatorPlayers, models.CASCADE)
    symbol = models.ForeignKey(
        ListedEquities, models.CASCADE, db_column="symbol", default="AGL"
    )
    date = models.DateField(verbose_name="Date")
    bought_or_sold = models.CharField(max_length=10)
    share_price = models.DecimalField(max_digits=12, decimal_places=2)
    num_shares = models.IntegerField()

    class Meta:
        managed = True
        unique_together = [
            [
                "simulator_player_id",
                "date",
                "symbol",
                "num_shares",
                "bought_or_sold",
            ]
        ]


class SimulatorPortfolios(models.Model):
    simulator_portfolio_id = models.AutoField(primary_key=True, unique=True)
    simulator_player_id = models.ForeignKey(
        SimulatorPlayers,
        models.CASCADE,
        db_column="simulator_player_id",
    )
    symbol = models.ForeignKey(
        ListedEquities,
        models.CASCADE,
        db_column="symbol",
    )
    shares_remaining = models.IntegerField()
    average_cost = models.DecimalField(max_digits=12, decimal_places=2, null=True)
    book_cost = models.DecimalField(max_digits=20, decimal_places=2, null=True)
    current_market_price = models.DecimalField(
        max_digits=12, decimal_places=2, null=True
    )
    market_value = models.DecimalField(max_digits=20, decimal_places=2, null=True)
    total_gain_loss = models.DecimalField(max_digits=20, decimal_places=2, null=True)
    gain_loss_percent = models.DecimalField(max_digits=6, decimal_places=2, null=True)

    class Meta:
        managed = True
        unique_together = [["simulator_player_id", "symbol"]]


class User(AbstractUser):
    pass
    # add additional fields in here

    def __str__(self):
        return self.username


class StockNewsData(models.Model):
    news_id = models.AutoField(primary_key=True)
    symbol = models.ForeignKey(ListedEquities, models.CASCADE, db_column="symbol")
    date = models.DateField(verbose_name="Date")
    title = models.CharField(max_length=200)
    link = models.CharField(max_length=500)
    category = models.CharField(max_length=50)

    class Meta:
        managed = False
        db_table = "stock_news_data"
