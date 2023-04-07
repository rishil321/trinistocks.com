# Generated by Django 3.0.5 on 2020-10-22 13:53

import django.contrib.auth.models
import django.contrib.auth.validators
import django.db.models.deletion
import django.utils.timezone
from django.conf import settings
from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = [
        ('auth', '0011_update_proxy_permissions'),
    ]

    operations = [
        migrations.CreateModel(
            name='DailyStockSummary',
            fields=[
                ('daily_share_id', models.AutoField(primary_key=True, serialize=False)),
                ('date', models.DateField()),
                ('open_price', models.DecimalField(blank=True, decimal_places=2, max_digits=12, null=True, verbose_name='Open Price ($)')),
                ('high', models.DecimalField(blank=True, decimal_places=2, max_digits=12, null=True, verbose_name='High ($)')),
                ('low', models.DecimalField(blank=True, decimal_places=2, max_digits=12, null=True, verbose_name='Low ($)')),
                ('os_bid', models.DecimalField(blank=True, decimal_places=2, max_digits=12, null=True, verbose_name='O/S Bid Price($)')),
                ('os_bid_vol', models.PositiveIntegerField(blank=True, null=True, verbose_name='O/S Bid Volume')),
                ('os_offer', models.DecimalField(blank=True, decimal_places=2, max_digits=12, null=True, verbose_name='O/S Offer Price($)')),
                ('os_offer_vol', models.PositiveIntegerField(blank=True, null=True, verbose_name='O/S Offer Volume')),
                ('last_sale_price', models.DecimalField(blank=True, decimal_places=2, max_digits=12, null=True, verbose_name='Last Sale Price($)')),
                ('was_traded_today', models.SmallIntegerField(blank=True, null=True, verbose_name='Was Traded Today')),
                ('volume_traded', models.PositiveIntegerField(blank=True, null=True)),
                ('close_price', models.DecimalField(blank=True, decimal_places=2, max_digits=12, null=True, verbose_name='Close Price ($)')),
                ('change_dollars', models.DecimalField(blank=True, decimal_places=2, max_digits=7, null=True, verbose_name='Daily Change ($)')),
                ('value_traded', models.DecimalField(blank=True, decimal_places=2, max_digits=20, null=True)),
            ],
            options={
                'db_table': 'daily_stock_summary',
                'ordering': ['-value_traded'],
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='DividendYield',
            fields=[
                ('dividend_yield_id', models.AutoField(primary_key=True, serialize=False)),
                ('date', models.DateField(verbose_name='Date Yield Calculated')),
                ('yield_percent', models.DecimalField(decimal_places=5, max_digits=20, verbose_name='Yield %')),
            ],
            options={
                'db_table': 'dividend_yield',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='FundamentalAnalysisSummary',
            fields=[
                ('id', models.AutoField(primary_key=True, serialize=False)),
                ('date', models.DateField(verbose_name='Date')),
                ('RoE', models.DecimalField(blank=True, decimal_places=3, max_digits=10, null=True)),
                ('EPS', models.DecimalField(blank=True, decimal_places=3, max_digits=10, null=True)),
                ('EPS_growth_rate', models.DecimalField(blank=True, decimal_places=2, max_digits=10, null=True, verbose_name='EPS Growth Rate(%)')),
                ('PEG', models.DecimalField(blank=True, decimal_places=3, max_digits=10, null=True)),
                ('RoIC', models.DecimalField(blank=True, decimal_places=3, max_digits=10, null=True)),
                ('working_capital', models.DecimalField(blank=True, decimal_places=3, max_digits=10, null=True, verbose_name='Working Capital')),
                ('price_to_earnings_ratio', models.DecimalField(blank=True, decimal_places=3, max_digits=10, null=True, verbose_name='P/E')),
                ('price_to_dividends_per_share_ratio', models.DecimalField(blank=True, decimal_places=3, max_digits=10, null=True, verbose_name='P/DPS')),
                ('dividend_yield', models.DecimalField(blank=True, decimal_places=3, max_digits=10, null=True, verbose_name='Dividend Yield(%)')),
                ('dividend_payout_ratio', models.DecimalField(blank=True, decimal_places=3, max_digits=10, null=True, verbose_name='Dividend Payout Ratio(%)')),
                ('book_value_per_share', models.DecimalField(blank=True, decimal_places=3, max_digits=10, null=True, verbose_name='BVPS')),
                ('price_to_book_ratio', models.DecimalField(blank=True, decimal_places=3, max_digits=10, null=True, verbose_name='P/B')),
            ],
            options={
                'db_table': 'audited_fundamental_calculated_data',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='HistoricalDividendInfo',
            fields=[
                ('dividend_id', models.AutoField(primary_key=True, serialize=False)),
                ('record_date', models.DateField(verbose_name='Record Date')),
                ('dividend_amount', models.DecimalField(decimal_places=5, max_digits=20, verbose_name='Dividend ($/share)')),
                ('currency', models.CharField(blank=True, max_length=6, null=True)),
            ],
            options={
                'db_table': 'historical_dividend_info',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='HistoricalIndicesInfo',
            fields=[
                ('summary_id', models.AutoField(primary_key=True, serialize=False)),
                ('date', models.DateField(unique=True, verbose_name='Date Recorded')),
                ('index_name', models.CharField(max_length=100, verbose_name='Market Index Name')),
                ('index_value', models.DecimalField(blank=True, decimal_places=2, max_digits=20, null=True, verbose_name='Index Value')),
                ('index_change', models.DecimalField(blank=True, decimal_places=2, max_digits=10, null=True, verbose_name='Index Change')),
                ('change_percent', models.DecimalField(blank=True, decimal_places=2, max_digits=7, null=True, verbose_name='Change (%)')),
                ('volume_traded', models.PositiveIntegerField(blank=True, null=True, verbose_name='Volume Traded (Shares)')),
                ('value_traded', models.DecimalField(blank=True, decimal_places=2, max_digits=23, null=True, verbose_name='Value Traded ($)')),
                ('num_trades', models.PositiveIntegerField(blank=True, null=True, verbose_name='Number of Trades')),
            ],
            options={
                'db_table': 'historical_indices_info',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='ListedEquities',
            fields=[
                ('symbol', models.CharField(max_length=20, primary_key=True, serialize=False, unique=True, verbose_name='Symbol')),
                ('security_name', models.CharField(max_length=100, verbose_name='Security Name')),
                ('status', models.CharField(blank=True, max_length=20, null=True)),
                ('sector', models.CharField(blank=True, max_length=100, null=True)),
                ('issued_share_capital', models.BigIntegerField(blank=True, null=True, verbose_name='Issued Share Capital (shares)')),
                ('market_capitalization', models.DecimalField(blank=True, decimal_places=2, max_digits=23, null=True, verbose_name='Market Capitalization ($)')),
                ('currency', models.CharField(max_length=3)),
                ('financial_year_end', models.CharField(blank=True, max_length=45, null=True)),
                ('website_url', models.CharField(blank=True, max_length=2083, null=True)),
            ],
            options={
                'db_table': 'listed_equities',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='ListedEquitiesPerSector',
            fields=[
                ('sector_id', models.SmallAutoField(primary_key=True, serialize=False)),
                ('sector', models.CharField(max_length=100, verbose_name='Sector')),
                ('num_listed', models.SmallIntegerField(verbose_name='Number of Listed Stocks')),
            ],
            options={
                'db_table': 'listed_equities_per_sector',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='TechnicalAnalysisSummary',
            fields=[
                ('technical_analysis_id', models.AutoField(primary_key=True, serialize=False)),
                ('last_close_price', models.DecimalField(blank=True, decimal_places=2, max_digits=20, null=True, verbose_name='Last Close Quote($)')),
                ('sma_20', models.DecimalField(blank=True, decimal_places=2, max_digits=20, null=True, verbose_name='SMA20($)')),
                ('sma_200', models.DecimalField(blank=True, decimal_places=2, max_digits=20, null=True, verbose_name='SMA200($)')),
                ('beta', models.DecimalField(blank=True, decimal_places=2, max_digits=4, null=True, verbose_name='Beta(TTM)')),
                ('adtv', models.PositiveIntegerField(blank=True, null=True, verbose_name='ADTV(shares)(Trailing 30d)')),
                ('high_52w', models.DecimalField(blank=True, decimal_places=2, max_digits=20, null=True, verbose_name='52W-high($)')),
                ('low_52w', models.DecimalField(blank=True, decimal_places=2, max_digits=20, null=True, verbose_name='52W-low($)')),
                ('wtd', models.DecimalField(blank=True, decimal_places=2, max_digits=6, null=True, verbose_name='WTD(%)')),
                ('mtd', models.DecimalField(blank=True, decimal_places=2, max_digits=6, null=True, verbose_name='MTD(%)')),
                ('ytd', models.DecimalField(blank=True, decimal_places=2, max_digits=6, null=True, verbose_name='YTD(%)')),
            ],
            options={
                'db_table': 'technical_analysis_summary',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='User',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('password', models.CharField(max_length=128, verbose_name='password')),
                ('last_login', models.DateTimeField(blank=True, null=True, verbose_name='last login')),
                ('is_superuser', models.BooleanField(default=False, help_text='Designates that this user has all permissions without explicitly assigning them.', verbose_name='superuser status')),
                ('username', models.CharField(error_messages={'unique': 'A user with that username already exists.'}, help_text='Required. 150 characters or fewer. Letters, digits and @/./+/-/_ only.', max_length=150, unique=True, validators=[django.contrib.auth.validators.UnicodeUsernameValidator()], verbose_name='username')),
                ('first_name', models.CharField(blank=True, max_length=30, verbose_name='first name')),
                ('last_name', models.CharField(blank=True, max_length=150, verbose_name='last name')),
                ('email', models.EmailField(blank=True, max_length=254, verbose_name='email address')),
                ('is_staff', models.BooleanField(default=False, help_text='Designates whether the user can log into this admin site.', verbose_name='staff status')),
                ('is_active', models.BooleanField(default=True, help_text='Designates whether this user should be treated as active. Unselect this instead of deleting accounts.', verbose_name='active')),
                ('date_joined', models.DateTimeField(default=django.utils.timezone.now, verbose_name='date joined')),
                ('groups', models.ManyToManyField(blank=True, help_text='The groups this user belongs to. A user will get all permissions granted to each of their groups.', related_name='user_set', related_query_name='user', to='auth.Group', verbose_name='groups')),
                ('user_permissions', models.ManyToManyField(blank=True, help_text='Specific permissions for this user.', related_name='user_set', related_query_name='user', to='auth.Permission', verbose_name='user permissions')),
            ],
            options={
                'verbose_name': 'user',
                'verbose_name_plural': 'users',
                'abstract': False,
            },
            managers=[
                ('objects', django.contrib.auth.models.UserManager()),
            ],
        ),
        migrations.CreateModel(
            name='PortfolioTransactions',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('date', models.DateField(verbose_name='Date')),
                ('bought_or_sold', models.CharField(max_length=10)),
                ('share_price', models.DecimalField(decimal_places=2, max_digits=12)),
                ('num_shares', models.IntegerField()),
                ('user', models.ForeignKey(default=1, on_delete=django.db.models.deletion.CASCADE, to=settings.AUTH_USER_MODEL)),
            ],
            options={
                'db_table': 'portfolio_transactions',
                'managed': True,
            },
        ),
    ]
