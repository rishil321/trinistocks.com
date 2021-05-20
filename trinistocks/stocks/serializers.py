from rest_framework import serializers
from .models import (
    LANGUAGE_CHOICES,
    STYLE_CHOICES,
    DailyStockSummary,
    StockNewsData,
    ListedEquities,
    TechnicalAnalysisSummary,
    FundamentalAnalysisSummary,
    HistoricalDividendInfo,
    HistoricalDividendYield,
    HistoricalIndicesInfo,
    PortfolioSummary,
    PortfolioTransactions,
    User,
)
from rest_framework.validators import UniqueTogetherValidator


class DailyStockSummarySerializer(serializers.ModelSerializer):
    class Meta:
        model = DailyStockSummary
        fields = (
            "symbol",
            "date",
            "value_traded",
            "open_price",
            "close_price",
            "high",
            "low",
            "volume_traded",
            "change_dollars",
        )


class StockNewsDataSerializer(serializers.ModelSerializer):
    class Meta:
        model = StockNewsData
        fields = ("symbol", "date", "title", "link", "category")


class ListedStocksSerializer(serializers.ModelSerializer):
    class Meta:
        model = ListedEquities
        fields = (
            "symbol",
            "security_name",
            "status",
            "sector",
            "issued_share_capital",
            "market_capitalization",
            "financial_year_end",
            "currency",
        )


class TechnicalAnalysisSerializer(serializers.ModelSerializer):
    symbol = serializers.CharField(read_only=True, source="symbol.symbol")
    sector = serializers.CharField(read_only=True, source="symbol.sector")

    class Meta:
        model = TechnicalAnalysisSummary
        fields = (
            "symbol",
            "sector",
            "last_close_price",
            "sma_20",
            "sma_200",
            "beta",
            "adtv",
            "high_52w",
            "low_52w",
            "wtd",
            "mtd",
            "ytd",
        )


class FundamentalAnalysisSerializer(serializers.ModelSerializer):
    symbol = serializers.CharField(read_only=True, source="symbol.symbol")
    sector = serializers.CharField(read_only=True, source="symbol.sector")

    class Meta:
        model = FundamentalAnalysisSummary
        fields = (
            "symbol",
            "sector",
            "date",
            "report_type",
            "RoE",
            "EPS",
            "RoIC",
            "current_ratio",
            "price_to_earnings_ratio",
            "dividend_yield",
            "price_to_book_ratio",
            "dividend_payout_ratio",
            "cash_per_share",
        )


class StockPriceSerializer(serializers.ModelSerializer):
    class Meta:
        model = DailyStockSummary
        fields = (
            "symbol",
            "date",
            "open_price",
            "low",
            "high",
            "close_price",
        )


class DividendPaymentsSerializer(serializers.ModelSerializer):
    class Meta:
        model = HistoricalDividendInfo
        fields = (
            "symbol",
            "record_date",
            "dividend_amount",
            "currency",
        )


class DividendYieldSerializer(serializers.ModelSerializer):
    class Meta:
        model = HistoricalDividendYield
        fields = (
            "symbol",
            "date",
            "dividend_yield",
        )


class MarketIndicesSerializer(serializers.ModelSerializer):
    class Meta:
        model = HistoricalIndicesInfo
        fields = (
            "date",
            "index_name",
            "index_value",
            "index_change",
            "change_percent",
            "volume_traded",
            "value_traded",
            "num_trades",
        )


class OutstandingTradesSerializer(serializers.ModelSerializer):
    class Meta:
        model = DailyStockSummary
        fields = (
            "date",
            "os_bid",
            "os_bid_vol",
            "os_offer",
            "os_offer_vol",
            "volume_traded",
        )


class PortfolioSummarySerializer(serializers.ModelSerializer):
    class Meta:
        model = PortfolioSummary
        fields = (
            "symbol",
            "shares_remaining",
            "average_cost",
            "book_cost",
            "current_market_price",
            "market_value",
            "total_gain_loss",
        )


class PortfolioTransactionsSerializer(serializers.ModelSerializer):
    class Meta:
        model = PortfolioTransactions
        fields = (
            "symbol",
            "date",
            "bought_or_sold",
            "share_price",
            "num_shares",
        )


class UserSerializer(serializers.ModelSerializer):
    def create(self, validated_data):
        user = User.objects.create_user(**validated_data)
        return user

    class Meta:
        model = User
        fields = (
            "username",
            "first_name",
            "last_name",
            "email",
            "password",
        )
        validators = [
            UniqueTogetherValidator(
                queryset=User.objects.all(), fields=["username", "email"]
            )
        ]