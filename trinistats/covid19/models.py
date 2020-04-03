from django.db import models

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
    dailytests = models.PositiveIntegerField(verbose_name="Number of Tests Submitted on Day")
    dailypositive = models.PositiveIntegerField(verbose_name="Number of Positive Results Received on Day")
    dailydeaths = models.PositiveIntegerField(verbose_name="Number of Deaths on Day")
    dailyrecovered = models.PositiveIntegerField(verbose_name="Number Recovered on Day")

    class Meta:
        managed = False
        db_table = 'covid19dailydata'