#!/usr/bin/env python3
# -*- coding: utf-8 -*-

###########################
# Set up the Django ORM here
###########################

# Turn off bytecode generation
import sys

sys.dont_write_bytecode = True

# Django specific settings
import os
import pathlib

sys.path.extend([str(pathlib.Path(__file__).parent.parent.parent.resolve())])
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "trinistocks.settings")
print(sys.path)
import django

django.setup()

# Import your models for use in your script
from stocks import models

############################################################################
# START OF APPLICATION
############################################################################
# Imports

import argparse
from pid import PidFile
import logging
from datetime import datetime, timedelta
from decimal import Decimal
from django.db.models import QuerySet, Sum, Avg
from django.utils import timezone
from typing import List, Optional
# Local imports
from stocks.background_tasks.utilities import TTDCurrencyConversionRates, fetch_latest_currency_conversion_rates

# Constants
logger: logging.Logger = logging.getLogger('background_tasks')


# Class definitions
class DividendYieldForYear:
    year: int = 1900
    dividend_yield: Decimal = Decimal(0)

    def __init__(self: object, year: int, dividend_yield: Decimal):
        self.year = year
        self.dividend_yield = dividend_yield


# Function definitions
def update_summarized_dividend_yields(TTD_JMD: Decimal, TTD_USD: Decimal, TTD_BBD: Decimal) -> int:
    """
    Update the SummarizedDividendYield model with the latest dividend yield percentages
    according to the latest dividend payments and stock prices
    """
    try:
        twelve_month_ago_date: datetime = timezone.now() - timedelta(weeks=52)
        current_year: int = timezone.now().year
        last_year: int = current_year - 1
        three_years_ago: int = current_year - 3
        five_years_ago: int = current_year - 5
        ten_years_ago: int = current_year - 10
        # fetch all the listed equities that we have data on
        all_listed_equities: QuerySet[models.ListedEquities] = models.ListedEquities.objects.all()
        for listed_equity in all_listed_equities:
            logger.info(f"Now updating yields for {listed_equity.symbol}.")
            # check if a yield summary already exists for this symbol
            symbol_dividend_yield_summary_queryset: QuerySet[
                models.SummarizedDividendYield] = models.SummarizedDividendYield.objects.filter(symbol=listed_equity)
            if symbol_dividend_yield_summary_queryset.exists():
                symbol_dividend_yield_summary: models.SummarizedDividendYield = symbol_dividend_yield_summary_queryset.first()
            else:
                symbol_dividend_yield_summary: models.SummarizedDividendYield = models.SummarizedDividendYield(
                    symbol=listed_equity)
            # fetch all the dividend payments for each of these symbols
            dividend_payments_for_symbol: QuerySet[
                models.HistoricalDividendInfo] = models.HistoricalDividendInfo.objects.filter(symbol=listed_equity)
            # calculate the trailing twelve-month dividend yield
            dividend_payments_within_last_12_months: QuerySet[
                models.HistoricalDividendInfo] = dividend_payments_for_symbol.filter(
                record_date__gte=twelve_month_ago_date)
            if not dividend_payments_within_last_12_months.exists():
                sum_dividend_payments_within_last_12_months: Decimal = Decimal(0)
            else:
                sum_dividend_payments_within_last_12_months: Decimal = next(
                    iter(dividend_payments_within_last_12_months.aggregate(
                        Sum('dividend_amount')).values()))
            # we need to multiply these dividend payments by a factor, depending on the currency
            # most dividends are paid in TTD, so set it to that by default
            currency_multiplication_factor: Decimal = Decimal(1)
            if dividend_payments_within_last_12_months.exists():
                if dividend_payments_within_last_12_months.first().currency == 'USD':
                    currency_multiplication_factor = 1 / TTD_USD
                elif dividend_payments_within_last_12_months.first().currency == 'JMD':
                    currency_multiplication_factor = 1 / TTD_JMD
                elif dividend_payments_within_last_12_months.first().currency == 'BBD':
                    currency_multiplication_factor = 1 / TTD_BBD
            latest_stock_summary: models.DailyStockSummary = models.DailyStockSummary.objects.filter(
                symbol=listed_equity).filter(close_price__gt=0).order_by('-date').first()
            dividend_yield_ttm: Decimal = (
                                                  (
                                                          sum_dividend_payments_within_last_12_months * currency_multiplication_factor) / latest_stock_summary.close_price) * 100
            symbol_dividend_yield_summary.ttm_yield = dividend_yield_ttm
            # now calculate the yields for the previous years
            year_counter: int = current_year - 1
            dividend_yield_for_previous_years: List[DividendYieldForYear] = []
            while year_counter >= ten_years_ago:
                stock_summaries_for_year: QuerySet[models.DailyStockSummary] = models.DailyStockSummary.objects.filter(
                    symbol=listed_equity).filter(close_price__gt=0).filter(date__year=year_counter)
                if not stock_summaries_for_year.exists():
                    # if we don't have any data for the stock for this year, don't add a dividend yield value for this year
                    year_counter -= 1
                    continue
                average_stock_price_for_year: Decimal = next(
                    iter(stock_summaries_for_year.aggregate(
                        Avg('close_price')).values()))
                # initialize the yield to be 0 by default
                dividend_yield_for_year_value: Decimal = Decimal(0)
                dividend_payments_for_year: QuerySet[
                    models.HistoricalDividendInfo] = dividend_payments_for_symbol.filter(record_date__year=year_counter)
                if dividend_payments_for_year.exists():
                    # we have at least one dividend payment, so let's calculate the yield
                    sum_dividend_payments_for_year: Decimal = next(
                        iter(dividend_payments_for_year.aggregate(
                            Sum('dividend_amount')).values()))
                    # we need to multiply these dividend payments by a factor, depending on the currency
                    # most dividends are paid in TTD, so set it to that by default
                    currency_multiplication_factor: Decimal = Decimal(1)
                    if dividend_payments_for_year.first().currency == 'USD':
                        currency_multiplication_factor = 1 / TTD_USD
                    elif dividend_payments_for_year.first().currency == 'JMD':
                        currency_multiplication_factor = 1 / TTD_JMD
                    elif dividend_payments_for_year.first().currency == 'BBD':
                        currency_multiplication_factor = 1 / TTD_BBD
                    dividend_yield_for_year_value: Decimal = (
                                                                     (
                                                                             sum_dividend_payments_for_year * currency_multiplication_factor) / average_stock_price_for_year) * 100
                dividend_yield_for_year: DividendYieldForYear = DividendYieldForYear(year=year_counter,
                                                                                     dividend_yield=dividend_yield_for_year_value)
                dividend_yield_for_previous_years.append(dividend_yield_for_year)
                year_counter -= 1
            # now calculate the averages that we need
            dividend_yield_for_last_three_years: List[DividendYieldForYear] = list(
                filter(lambda dividend_yield: three_years_ago <= dividend_yield.year <= last_year,
                       dividend_yield_for_previous_years))
            total_dividend_yield: Decimal = sum(
                dividend_yield.dividend_yield for dividend_yield in dividend_yield_for_last_three_years)
            three_year_dividend_yield: Decimal = total_dividend_yield / 3
            symbol_dividend_yield_summary.three_year_yield = three_year_dividend_yield
            dividend_yield_for_last_five_years: List[DividendYieldForYear] = list(
                filter(lambda dividend_yield: five_years_ago <= dividend_yield.year <= last_year,
                       dividend_yield_for_previous_years))
            total_dividend_yield: Decimal = sum(
                dividend_yield.dividend_yield for dividend_yield in dividend_yield_for_last_five_years)
            five_year_dividend_yield: Decimal = total_dividend_yield / 5
            symbol_dividend_yield_summary.five_year_yield = five_year_dividend_yield
            dividend_yield_for_last_ten_years: List[DividendYieldForYear] = list(
                filter(lambda dividend_yield: ten_years_ago <= dividend_yield.year <= last_year,
                       dividend_yield_for_previous_years))
            total_dividend_yield: Decimal = sum(
                dividend_yield.dividend_yield for dividend_yield in dividend_yield_for_last_ten_years)
            ten_year_dividend_yield: Decimal = total_dividend_yield / 10
            symbol_dividend_yield_summary.ten_year_yield = ten_year_dividend_yield
            # update the values in the database
            symbol_dividend_yield_summary.save()
            logger.info("Completed.")
    except Exception as exc:
        logger.exception(exc)
        return -1
    else:
        logger.info("Successfully updated summarized dividend yields for all symbols.")
        return 0


def main(args):
    """Main function for updating portfolio data"""
    try:
        with PidFile('update_dividends'):
            conversion_rates: TTDCurrencyConversionRates = fetch_latest_currency_conversion_rates()
            result: int = update_summarized_dividend_yields(TTD_JMD=conversion_rates.TTD_JMD,
                                                            TTD_BBD=conversion_rates.TTD_BBD,
                                                            TTD_USD=conversion_rates.TTD_USD)
    except Exception as exc:
        logger.exception(exc)


if __name__ == "__main__":
    # first check the arguments given to this script
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--daily_update",
        help="Update the portfolio market data with the latest values",
        action="store_true",
    )
    args = parser.parse_args()
    main(args)
