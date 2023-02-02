# Generated by Django 3.2.14 on 2022-12-15 23:57

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('stocks', '0027_monitoredstocks'),
    ]

    operations = [
        migrations.CreateModel(
            name='SummarizedDividendYield',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('ttm_yield', models.DecimalField(decimal_places=5, max_digits=20, verbose_name='TTM Yield %')),
                ('three_year_yield', models.DecimalField(decimal_places=5, max_digits=20, verbose_name='TTM Yield %')),
                ('five_year_yield', models.DecimalField(decimal_places=5, max_digits=20, verbose_name='TTM Yield %')),
                ('ten_year_yield', models.DecimalField(decimal_places=5, max_digits=20, verbose_name='TTM Yield %')),
                ('symbol', models.ForeignKey(on_delete=django.db.models.deletion.RESTRICT, to='stocks.listedequities', unique=True)),
            ],
            options={
                'managed': True,
            },
        ),
    ]