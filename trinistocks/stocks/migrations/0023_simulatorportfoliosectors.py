# Generated by Django 3.1.4 on 2021-07-16 19:38

import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('stocks', '0022_simulatorgames_starting_cash'),
    ]

    operations = [
        migrations.CreateModel(
            name='SimulatorPortfolioSectors',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('sector', models.CharField(max_length=100)),
                ('book_cost', models.DecimalField(decimal_places=2, max_digits=20, null=True)),
                ('market_value', models.DecimalField(decimal_places=2, max_digits=20, null=True)),
                ('total_gain_loss', models.DecimalField(decimal_places=2, max_digits=20, null=True)),
                ('gain_loss_percent', models.DecimalField(decimal_places=2, max_digits=6, null=True)),
                ('simulator_player_id', models.ForeignKey(db_column='simulator_player_id', on_delete=django.db.models.deletion.CASCADE, to='stocks.simulatorplayers')),
            ],
            options={
                'db_table': 'stocks_simulatorportfoliosectors',
                'managed': True,
                'unique_together': {('simulator_player_id', 'sector')},
            },
        ),
    ]
