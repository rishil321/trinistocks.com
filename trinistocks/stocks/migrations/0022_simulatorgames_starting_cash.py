# Generated by Django 3.1.4 on 2021-06-29 15:35

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('stocks', '0021_auto_20210625_1109'),
    ]

    operations = [
        migrations.AddField(
            model_name='simulatorgames',
            name='starting_cash',
            field=models.PositiveIntegerField(default=10000),
        ),
    ]