# Generated by Django 3.1.4 on 2021-07-20 16:19

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('stocks', '0025_auto_20210720_1023'),
    ]

    operations = [
        migrations.AddField(
            model_name='simulatorplayers',
            name='current_portfolio_value',
            field=models.DecimalField(decimal_places=2, max_digits=20, null=True),
        ),
    ]
