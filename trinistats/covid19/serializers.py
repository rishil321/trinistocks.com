# covid19/serializers
from rest_framework import serializers
from .models import Covid19_Paho_Reports, LANGUAGE_CHOICES, STYLE_CHOICES


class Covid19_Paho_Reports_Serializer(serializers.ModelSerializer):

    class Meta:
        model = Covid19_Paho_Reports
        fields = ('date', 'country_or_territory_name', 'confirmed_cases', 'probable_cases',
                  'confirmed_deaths', 'probable_deaths','recovered','percentage_increase_confirmed',
                  'transmission_type')