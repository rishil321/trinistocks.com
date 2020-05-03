from django.db import models
from pygments.lexers import get_all_lexers
from pygments.styles import get_all_styles

# API sorters
LEXERS = [item for item in get_all_lexers() if item[1]]
LANGUAGE_CHOICES = sorted([(item[1][0], item[0]) for item in LEXERS])
STYLE_CHOICES = sorted([(item, item) for item in get_all_styles()])

# Create your models here.
class Covid19Cases(models.Model):
    idcases = models.AutoField(primary_key=True)
    date = models.DateTimeField(unique=True, verbose_name="Date/Time of M.O.H Update")
    numtested = models.PositiveIntegerField(verbose_name="Number Tested")
    numpositive = models.PositiveIntegerField(verbose_name="Number Tested Positive")
    numdeaths = models.PositiveIntegerField(verbose_name="Number of Deaths")
    numrecovered = models.PositiveIntegerField(verbose_name="Number Recovered")

    class Meta:
        managed = False
        db_table = 'covid19cases'
        
class Covid19DailyData(models.Model):
    idcovid19dailydata = models.AutoField(primary_key=True)
    date = models.DateField(unique=True, verbose_name="Date")
    dailytests = models.PositiveIntegerField(verbose_name="Tests submitted on day")
    dailypositive = models.PositiveIntegerField(verbose_name="Positive results received on day")
    dailydeaths = models.PositiveIntegerField(verbose_name="Deaths on day")
    dailyrecovered = models.PositiveIntegerField(verbose_name="Number recovered on day")

    class Meta:
        managed = False
        db_table = 'covid19dailydata'
        
class Covid19_Paho_Reports(models.Model):
    paho_report_entry_id = models.AutoField(primary_key=True)
    date = models.DateField(unique=True)
    country_or_territory_name = models.CharField(null=False,max_length=100)
    confirmed_cases = models.PositiveIntegerField()
    probable_cases = models.PositiveIntegerField()
    confirmed_deaths = models.PositiveIntegerField()
    probable_deaths = models.PositiveIntegerField()
    recovered = models.PositiveIntegerField()
    percentage_increase_confirmed = models.FloatField()
    transmission_type = models.CharField(null=False,max_length=100)

    class Meta:
        managed = False
        db_table = 'covid19_paho_reports'
        
class Covid19_Worldometers_Reports(models.Model):
    report_entry_id = models.AutoField(primary_key=True)
    date = models.DateField()
    country_or_territory_name = models.CharField(null=False,max_length=100)
    total_cases = models.PositiveIntegerField()
    new_cases = models.PositiveIntegerField()
    total_deaths = models.PositiveIntegerField()
    new_deaths = models.PositiveIntegerField()
    total_recovered = models.PositiveIntegerField()
    active_cases = models.PositiveIntegerField()
    serious_critical = models.PositiveIntegerField()
    total_cases_1m_pop = models.PositiveIntegerField()
    deaths_1m_pop = models.PositiveIntegerField()
    total_tests = models.PositiveIntegerField()
    tests_1m_pop = models.PositiveIntegerField()

    class Meta:
        managed = False
        db_table = 'covid19_worldometers_reports'
        unique_together = (('date', 'country_or_territory_name'),)