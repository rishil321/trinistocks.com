import django_filters
from stocks import models


class DailyEquitySummaryFilter(django_filters.FilterSet):
    class Meta:
        model = models.DailyEquitySummary
        fields = ['date']


class StockHistoryFilter(django_filters.FilterSet):
    class Meta:
        model = models.HistoricalStockInfo
        fields = {
            'stockcode': ['exact', ],
            'date': ['gte', 'lte', ],
        }


class DividendHistoryFilter(django_filters.FilterSet):
    class Meta:
        model = models.HistoricalDividendInfo
        fields = {
            'stockcode': ['exact', ],
            'date': ['gte', 'lte', ],
        }


class DividendYieldHistoryFilter(django_filters.FilterSet):
    class Meta:
        model = models.DividendYield
        fields = {
            'stockcode': ['exact', ],
            'date': ['gte', 'lte', ],
        }


class MarketIndexHistoryFilter(django_filters.FilterSet):
    class Meta:
        model = models.HistoricalMarketSummary
        fields = {
            'date': ['gte', 'lte', ],
        }
