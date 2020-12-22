import django_filters
from stocks import models


class DailyTradingSummaryFilter(django_filters.FilterSet):
    class Meta:
        model = models.DailyStockSummary
        fields = {
            'date': ['exact', ],
            'was_traded_today': ['exact', ],
        }


class StockHistoryFilter(django_filters.FilterSet):
    class Meta:
        model = models.DailyStockSummary
        fields = {
            'date': ['gte', 'lte', ],
            'symbol': ['exact', ],
        }


class ListedStocksFilter(django_filters.FilterSet):
    class Meta:
        model = models.ListedEquities
        fields = {
        }


class DividendHistoryFilter(django_filters.FilterSet):
    class Meta:
        model = models.HistoricalDividendInfo
        fields = {
            'symbol': ['exact', ],
            'record_date': ['gte', 'lte', ],
        }


class MarketIndexHistoryFilter(django_filters.FilterSet):
    class Meta:
        model = models.HistoricalIndicesInfo
        fields = {
            'date': ['gte', 'lte', ],
            'index_name': ['exact', ],
            'volume_traded': ['exact', ],
            'value_traded': ['exact', ],
            'num_trades': ['exact', ],
        }


class OSTradesHistoryFilter(django_filters.FilterSet):
    class Meta:
        model = models.DailyStockSummary
        fields = {
            'date': ['gte', 'lte', ],
            'symbol': ['exact', ],
        }


class TechnicalAnalysisSummaryFilter(django_filters.FilterSet):
    class Meta:
        model = models.TechnicalAnalysisSummary
        fields = {}


class PortfolioSummaryFilter(django_filters.FilterSet):
    
    
    class Meta:
        model = models.PortfolioSummary
        fields = {'user_id'}

    @property
    def qs(self):
        parent = super().qs
        # filter default queryset to only allow portfolio summary data
        # for the current user
        current_user = getattr(self.request, 'user', None)
        return parent.filter(user=current_user)
