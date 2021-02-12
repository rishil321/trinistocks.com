from rest_framework import serializers
from .models import DailyStockSummary, LANGUAGE_CHOICES, STYLE_CHOICES


class DailyStockSummarySerializer(serializers.ModelSerializer):

    class Meta:
        model = DailyStockSummary
        fields = ('symbol', 'date', 'value_traded', 'open_price',
                  'close_price', 'high', 'low', 'volume_traded', 'change_dollars')
