# covid19/serializers
from rest_framework import serializers
from .models import Covid19_Paho_Reports, LANGUAGE_CHOICES, STYLE_CHOICES, Covid19_Worldometers_Reports


class Covid19_Paho_Reports_Serializer(serializers.ModelSerializer):

    class Meta:
        model = Covid19_Paho_Reports
        fields = ('date', 'country_or_territory_name', 'confirmed_cases', 'probable_cases',
                  'confirmed_deaths', 'probable_deaths','recovered','percentage_increase_confirmed',
                  'transmission_type')
        
class Covid19_Worldometers_Reports_Serializer(serializers.ModelSerializer):

    class Meta:
        model = Covid19_Worldometers_Reports
        fields = ('date', 'country_or_territory_name', 'total_cases', 'new_cases',
                  'total_deaths', 'new_deaths','total_recovered','active_cases',
                  'serious_critical','total_cases_1m_pop','deaths_1m_pop','total_tests',
                  'tests_1m_pop')