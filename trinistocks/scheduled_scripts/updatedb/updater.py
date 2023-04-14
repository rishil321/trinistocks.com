#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Description of this module/script goes here
:param -f OR --first_parameter: The description of your first input parameter
:returns: Whatever your script returns when called
:raises Exception if any issues are encountered
"""

import argparse
import json
import logging
import multiprocessing
import os
import sys
# region IMPORTS
#
# Put all your imports here, one per line.
# However multiple imports from the same lib are allowed on a line.
# Imports from Python standard libraries
import tempfile
import time
from datetime import date
from logging.config import dictConfig
from typing import List

import numpy as np
import pandas as pd
import requests
import sqlalchemy.exc
# Imports from the cheese factory
from pid import PidFile
from pid.decorator import pidfile
from sqlalchemy import (MetaData, Table)
from sqlalchemy.dialects.mysql import insert

# Imports from the local filesystem
from scheduled_scripts.database_ops import DatabaseConnect
from scheduled_scripts import logging_configs
from scheduled_scripts.crosslisted_symbols import (BBD_DIVIDEND_SYMBOLS, JMD_DIVIDEND_SYMBOLS,
                                                   USD_DIVIDEND_SYMBOLS, USD_STOCK_SYMBOLS)

dictConfig(logging_configs.LOGGING_CONFIG)
logger = logging.getLogger()


# endregion IMPORTS

# region CONSTANTS
# Put your constants here. These should be named in CAPS.


# endregion CONSTANTS
# Put your global variables here.

# Put your class definitions here. These should use the CapWords convention.

# region FUNCTION DEFINITIONS
# Put your function definitions here. These should be lowercase, separated by underscores.


def fetch_latest_currency_conversion_rates():
    logger.debug("Now trying to fetch latest currency conversions.")
    api_response_ttd = requests.get(
        url="https://fcsapi.com/api-v2/forex/base_latest?symbol=TTD&type=forex&access_key=o9zfwlibfXciHoFO4LQU2NfTwt2vEk70DAiOH1yb2ao4tBhNmm"
    )
    if api_response_ttd.status_code == 200:
        # store the conversion rates that we need
        TTD_JMD = float(
            json.loads(api_response_ttd.content.decode("utf-8"))["response"]["JMD"]
        )
        TTD_USD = float(
            json.loads(api_response_ttd.content.decode("utf-8"))["response"]["USD"]
        )
        TTD_BBD = float(
            json.loads(api_response_ttd.content.decode("utf-8"))["response"]["BBD"]
        )
        logger.debug("Currency conversions fetched correctly.")
        return TTD_JMD, TTD_USD, TTD_BBD
    else:
        logger.exception(
            f"Cannot load URL for currency conversions.{api_response_ttd.status_code},{api_response_ttd.reason},{api_response_ttd.url}"
        )


def calculate_fundamental_analysis_ratios(TTD_JMD, TTD_USD, TTD_BBD):
    """
    Calculate the important ratios for fundamental analysis, based off our manually entered data from the financial statements
    """
    raw_data_table_names = ["raw_annual_data", "raw_quarterly_data"]
    calculated_fundamental_ratios_table_name = "calculated_fundamental_ratios"
    daily_stock_summary_table_name = "daily_stock_summary"
    historical_dividend_info_table_name = "historical_dividend_info"
    try:
        for raw_data_table_name in raw_data_table_names:
            with DatabaseConnect() as db_connect:
                logger.info("Successfully connected to database")
                logger.info(f"Now reading raw data from {raw_data_table_name}")
                calculated_fundamental_ratios_table = Table(
                    calculated_fundamental_ratios_table_name,
                    MetaData(),
                    autoload=True,
                    autoload_with=db_connect.dbengine,
                )
                # set date column name
                if "annual" in raw_data_table_name:
                    date_column = "year_end_date"
                else:
                    date_column = "quarter_end_date"
                # read the audited raw table as a pandas df
                raw_data_df = pd.io.sql.read_sql(
                    f"SELECT * FROM {raw_data_table_name} ORDER BY symbol,{date_column} DESC;",
                    db_connect.dbengine,
                )
                # first get the latest share price data
                # get the latest date from the daily stock table
                latest_stock_date = pd.io.sql.read_sql(
                    f"SELECT date FROM {daily_stock_summary_table_name} WHERE os_bid_vol !=0 ORDER BY date DESC LIMIT 1;",
                    db_connect.dbengine,
                )["date"][0].strftime("%Y-%m-%d")
                # get the closing price df
                share_price_df = pd.io.sql.read_sql(
                    f"SELECT symbol,close_price \
                    FROM {daily_stock_summary_table_name} WHERE \
                    {daily_stock_summary_table_name}.date='{latest_stock_date}';",
                    db_connect.dbengine,
                )
                # create a merged df
                calculated_ratios_df = pd.merge(
                    raw_data_df, share_price_df, how="outer", on="symbol"
                )
                # set the report type
                if "annual" in raw_data_table_name:
                    calculated_ratios_df["report_type"] = "annual"
                else:
                    calculated_ratios_df["report_type"] = "quarterly"
                # calculate the return on equity
                calculated_ratios_df["RoE"] = (
                        calculated_ratios_df["net_income"]
                        / calculated_ratios_df["total_shareholders_equity"]
                )
                # now calculate the return on invested capital
                calculated_ratios_df["RoIC"] = (
                        calculated_ratios_df["profit_after_tax"]
                        / calculated_ratios_df["total_shareholders_equity"]
                )
                # now calculate the working capital
                calculated_ratios_df["working_capital"] = (
                        calculated_ratios_df["total_assets"]
                        - calculated_ratios_df["total_liabilities"]
                )
                # and the current ratio
                calculated_ratios_df["current_ratio"] = (
                        calculated_ratios_df["total_assets"]
                        / calculated_ratios_df["total_liabilities"]
                )
                calculated_ratios_df[
                    "share_price_conversion_rates"
                ] = calculated_ratios_df.apply(
                    lambda x: TTD_USD
                    if x.currency == "USD"
                    else (
                        TTD_JMD
                        if x.currency == "JMD"
                        else (TTD_BBD if x.currency == "BBD" else 1.00)
                    ),
                    axis=1,
                )
                calculated_ratios_df["price_to_earnings_ratio"] = (
                                                                          calculated_ratios_df["close_price"]
                                                                          * calculated_ratios_df[
                                                                              "share_price_conversion_rates"]
                                                                  ) / calculated_ratios_df["basic_earnings_per_share"]
                # calculate cash per share
                calculated_ratios_df["cash_per_share"] = (
                                                             calculated_ratios_df["cash_cash_equivalents"]
                                                         ) / (
                                                                 calculated_ratios_df["total_shares_outstanding"]
                                                                 * calculated_ratios_df["share_price_conversion_rates"]
                                                         )
                # calculate dividend yield and dividend payout ratio
                # add the dividend conversion rates for this df as well
                # first note that dividends are paid in various currencies, so we need to convert them all to TTD
                calculated_ratios_df[
                    "dividend_conversion_rates"
                ] = calculated_ratios_df.apply(
                    lambda x: 1 / TTD_USD
                    if x.symbol in USD_DIVIDEND_SYMBOLS
                    else (
                        1 / TTD_JMD
                        if x.symbol in JMD_DIVIDEND_SYMBOLS
                        else (1 / TTD_BBD if x.symbol in BBD_DIVIDEND_SYMBOLS else 1.00)
                    ),
                    axis=1,
                )
                # now calculate a conversion rate for the price for the dividend yields
                calculated_ratios_df[
                    "dividend_stock_price_conversion_rates"
                ] = calculated_ratios_df.apply(
                    lambda x: 1 / TTD_USD if x.symbol in USD_STOCK_SYMBOLS else 1.00,
                    axis=1,
                )
                # note that the price_to_earnings_df contains the share price
                calculated_ratios_df["dividend_yield"] = (
                        100
                        * (
                                calculated_ratios_df["dividends_per_share"]
                                * calculated_ratios_df["dividend_conversion_rates"]
                        )
                        / (
                                calculated_ratios_df["close_price"]
                                * calculated_ratios_df["dividend_stock_price_conversion_rates"]
                        )
                )
                # now dividend payout ratio
                calculated_ratios_df["dividend_payout_ratio"] = (
                        100
                        * calculated_ratios_df["dividends_per_share"]
                        / calculated_ratios_df["basic_earnings_per_share"]
                )
                # now calculate the eps growth rate
                calculated_ratios_df["EPS_growth_rate"] = (
                        calculated_ratios_df["basic_earnings_per_share"].diff() * 100
                )
                # now calculate the price to earnings-to-growth ratio
                calculated_ratios_df["PEG"] = (
                        calculated_ratios_df["price_to_earnings_ratio"]
                        / calculated_ratios_df["EPS_growth_rate"]
                )
                # calculate the book value per share (BVPS)
                calculated_ratios_df["book_value_per_share"] = (
                                                                       calculated_ratios_df["total_assets"]
                                                                       - calculated_ratios_df["total_liabilities"]
                                                               ) / calculated_ratios_df["total_shares_outstanding"]
                # calculate the price to book ratio
                calculated_ratios_df["price_to_book_ratio"] = (
                                                                      calculated_ratios_df["close_price"]
                                                                      * calculated_ratios_df[
                                                                          "share_price_conversion_rates"]
                                                              ) / (
                                                                      (
                                                                              calculated_ratios_df["total_assets"]
                                                                              - calculated_ratios_df[
                                                                                  "total_liabilities"]
                                                                      )
                                                                      / calculated_ratios_df["total_shares_outstanding"]
                                                              )
                # replace inf with None
                calculated_ratios_df = calculated_ratios_df.replace(
                    [np.inf, -np.inf], None
                )
                # remove the columns that we don't need
                calculated_ratios_df = calculated_ratios_df[
                    calculated_ratios_df.columns.intersection(
                        [
                            "symbol",
                            "year_end_date",
                            "quarter_end_date",
                            "report_type",
                            "RoE",
                            "basic_earnings_per_share",
                            "EPS_growth_rate",
                            "PEG",
                            "RoIC",
                            "working_capital",
                            "price_to_earnings_ratio",
                            "dividend_yield",
                            "dividend_payout_ratio",
                            "book_value_per_share",
                            "price_to_book_ratio",
                            "current_ratio",
                            "cash_per_share",
                        ]
                    )
                ]
                # rename the columns
                calculated_ratios_df.rename(
                    columns={
                        "year_end_date": "date",
                        "quarter_end_date": "date",
                        "basic_earnings_per_share": "EPS",
                    },
                    inplace=True,
                )
                # remove the rows where the date is None
                calculated_ratios_df = calculated_ratios_df[
                    ~calculated_ratios_df["date"].isnull()
                ]
                # remove the null values
                calculated_ratios_df = calculated_ratios_df.replace({np.nan: None})
                # now write the df to the database
                logger.info("Now writing fundamental data to database.")
                execute_completed_successfully = False
                execute_failed_times = 0
                while not execute_completed_successfully and execute_failed_times < 5:
                    try:
                        insert_stmt = insert(
                            calculated_fundamental_ratios_table
                        ).values(calculated_ratios_df.to_dict("records"))
                        upsert_stmt = insert_stmt.on_duplicate_key_update(
                            {x.name: x for x in insert_stmt.inserted}
                        )
                        result = db_connect.dbcon.execute(upsert_stmt)
                        execute_completed_successfully = True
                        logger.info(
                            f"Successfully wrote calculated fundamental ratios to db from {raw_data_table_name}"
                        )
                        logger.info(
                            "Number of rows affected in the calculated table was "
                            + str(result.rowcount)
                        )
                    except sqlalchemy.exc.OperationalError as operr:
                        logger.warning(str(operr))
                        time.sleep(1)
                        execute_failed_times += 1
        return 0
    except Exception as exc:
        logger.exception("Could not complete fundamental data update.", exc_info=exc)


def update_dividend_yields(TTD_JMD, TTD_USD, TTD_BBD):
    """Use the historical dividend info table
    to calculate dividend yields per year
    """
    daily_stock_summary_table_name = "daily_stock_summary"
    historical_dividend_info_table_name = "historical_dividend_info"
    historical_dividend_yield_table_name = "historical_dividend_yield"
    summarized_dividend_yield_table_name: str = "summarized_dividend_yield"
    with DatabaseConnect() as db_connect:
        logger.info("Now calculating dividend yields.")
        # first get the dividends per share
        dividends_df: pd.DataFrame = pd.io.sql.read_sql(
            f"SELECT symbol,record_date,dividend_amount FROM {historical_dividend_info_table_name};",
            db_connect.dbengine,
        )
        # get the latest date from the daily stock table
        latest_stock_date: str = pd.io.sql.read_sql(
            f"SELECT date FROM {daily_stock_summary_table_name} WHERE os_bid_vol !=0 ORDER BY date DESC LIMIT 1;",
            db_connect.dbengine,
        )["date"][0].strftime("%Y-%m-%d")
        # then get the share price for each listed stock at this date
        share_price_df: pd.DataFrame = pd.io.sql.read_sql(
            f"SELECT {daily_stock_summary_table_name}.symbol,{daily_stock_summary_table_name}.close_price, listed_equities.currency \
                FROM {daily_stock_summary_table_name}, listed_equities WHERE \
                {daily_stock_summary_table_name}.symbol = listed_equities.symbol AND {daily_stock_summary_table_name}.date='{latest_stock_date}';",
            db_connect.dbengine,
        )
        # now go through each symbol and calculate the yields
        groupby_symbol_year: pd.DataFrameGroupBy = dividends_df.groupby(
            ["symbol", dividends_df["record_date"].map(lambda x: x.year)]
        )["dividend_amount"]
        yearly_dividends_df: pd.DataFrame = groupby_symbol_year.sum().reset_index()
        # add the dividend conversion rates for this df
        symbols_list: List = yearly_dividends_df["symbol"].to_list()
        conversion_rates: List = []
        for symbol in symbols_list:
            if symbol in USD_DIVIDEND_SYMBOLS:
                conversion_rates.append(1 / TTD_USD)
            elif symbol in JMD_DIVIDEND_SYMBOLS:
                conversion_rates.append(1 / TTD_JMD)
            elif symbol in BBD_DIVIDEND_SYMBOLS:
                conversion_rates.append(1 / TTD_BBD)
            else:
                conversion_rates.append(1.00)
        yearly_dividends_df["dividend_conversion_rates"]: pd.Series = pd.Series(
            conversion_rates, index=yearly_dividends_df.index
        )
        # calculate a conversion rate for the stock price
        share_price_df["share_price_conversion_rates"]: pd.Series = share_price_df.apply(
            lambda x: 1 / TTD_USD
            if x.currency == "USD"
            else (
                1 / TTD_JMD
                if x.currency == "JMD"
                else (1 / TTD_BBD if x.currency == "BBD" else 1.00)
            ),
            axis=1,
        )
        # merge this df with the dividends df
        yearly_dividends_df = pd.merge(
            share_price_df, yearly_dividends_df, how="inner", on="symbol"
        )
        # now calculate the dividend yields
        yearly_dividends_df["dividend_yield"]: pd.Series = (
                100
                * (
                        yearly_dividends_df["dividend_amount"]
                        * yearly_dividends_df["dividend_conversion_rates"]
                )
                / (
                        yearly_dividends_df["close_price"]
                        * yearly_dividends_df["share_price_conversion_rates"]
                )
        )
        # drop the columns that we don't need
        yearly_dividends_df.drop(
            columns=[
                "close_price",
                "currency",
                "share_price_conversion_rates",
                "dividend_amount",
                "dividend_conversion_rates",
            ],
            inplace=True,
        )
        # format the dates properly
        yearly_dividends_df.rename(columns={"record_date": "date"}, inplace=True)
        yearly_dividends_df["date"] = yearly_dividends_df["date"].apply(
            lambda x: f"{x}-12-31"
        )
        # now calculate the dividend yields for the summarized table

        # now write the data to the db
        logger.info("Now writing dividend yield data to database.")
        execute_completed_successfully = False
        execute_failed_times = 0
        while not execute_completed_successfully and execute_failed_times < 5:
            try:
                historical_dividend_yield_table = Table(
                    historical_dividend_yield_table_name,
                    MetaData(),
                    autoload=True,
                    autoload_with=db_connect.dbengine,
                )
                insert_stmt = insert(historical_dividend_yield_table).values(
                    yearly_dividends_df.to_dict("records")
                )
                upsert_stmt = insert_stmt.on_duplicate_key_update(
                    {x.name: x for x in insert_stmt.inserted}
                )
                result = db_connect.dbcon.execute(upsert_stmt)
                execute_completed_successfully = True
                logger.info(f"Successfully wrote dividend yield data from table.")
                logger.info(
                    "Number of rows affected in the calculated table was "
                    + str(result.rowcount)
                )
            except sqlalchemy.exc.OperationalError as operr:
                logger.warning(str(operr))
                time.sleep(1)
                execute_failed_times += 1
    return 0


def update_portfolio_summary_book_costs():
    """
    Select all records from the portfolio_transactions table, then
    calculate and upsert the following fields in the portfolio_summary table
    (shares_remaining, average_cost, book_cost)
    """
    logger.info("Now trying to update the book costs in all portfolios.")
    # set up the db connection
    try:
        with DatabaseConnect() as db_connect:
            # set up our dataframe from the portfolio_transactions table
            transactions_df = pd.io.sql.read_sql(
                f"SELECT user_id, symbol, num_shares, share_price, bought_or_sold \
                FROM portfolio_transactions;",
                db_connect.dbengine,
            )
            # get the number of shares remaining in the portfolio for each user
            # first get the total shares bought
            total_shares_bought_df = (
                transactions_df[transactions_df.bought_or_sold == "Bought"]
                .groupby(["user_id", "symbol"])
                .sum()
                .reset_index()
            )
            # total_shares_bought_df.drop(["share_price"], axis=1, inplace=True)
            total_shares_bought_df.rename(
                columns={"num_shares": "shares_bought"}, inplace=True
            )
            total_shares_bought_df['total_transaction_cost'] = total_shares_bought_df['share_price'] * \
                                                               total_shares_bought_df['shares_bought']
            # then get the total shares sold
            total_shares_sold_df = (
                transactions_df[transactions_df.bought_or_sold == "Sold"]
                .groupby(["user_id", "symbol"])
                .sum()
                .reset_index()
            )
            # total_shares_sold_df.drop(["share_price"], axis=1, inplace=True)
            total_shares_sold_df.rename(
                columns={"num_shares": "shares_sold"}, inplace=True
            )
            total_shares_sold_df['total_transaction_cost'] = total_shares_sold_df['share_price'] * \
                                                             total_shares_sold_df['shares_sold']
            # first merge both dataframes
            total_bought_sold_df = total_shares_bought_df.merge(
                total_shares_sold_df, how="outer", on=["user_id", "symbol"]
            )
            # then fill the shares_sold column with 0
            total_bought_sold_df["shares_sold"] = total_bought_sold_df[
                "shares_sold"
            ].replace(np.NaN, 0)
            # total_bought_sold_df["total_transaction_cost"] = total_bought_sold_df[
            #     "total_transaction_cost"
            # ].replace(np.NaN, 0)
            # then find the difference to get our number of shares remaining for each user
            total_bought_sold_df["shares_remaining"] = (
                    total_bought_sold_df["shares_bought"]
                    - total_bought_sold_df["shares_sold"]
            )
            # set up a new df to hold the summary data that we are interested in
            summary_df = total_bought_sold_df[
                ["user_id", "symbol", "shares_remaining"]
            ].copy()
            # get the average cost for each share
            avg_cost_df = transactions_df[
                transactions_df.bought_or_sold == "Bought"
                ].copy()
            # calculate the total book cost for shares purchased
            avg_cost_df["book_cost"] = (
                    avg_cost_df["num_shares"] * avg_cost_df["share_price"]
            )
            avg_cost_df = avg_cost_df.groupby(["user_id", "symbol"]).sum()
            avg_cost_df.drop(["share_price"], axis=1, inplace=True)
            avg_cost_df = avg_cost_df.reset_index()
            # calculate the average cost for each share purchased
            avg_cost_df["average_cost"] = (
                    avg_cost_df["book_cost"] / avg_cost_df["num_shares"]
            )
            # add these two new fields to our dataframe to be written to the db
            summary_df = summary_df.merge(
                avg_cost_df, how="outer", on=["user_id", "symbol"]
            )
            summary_df.drop(["num_shares"], axis=1, inplace=True)
            # now recalculate the book cost to account for sold shares
            summary_df['book_cost'] = (summary_df['shares_remaining'] * summary_df['average_cost'])
            # now write the df to the database
            logger.info("Now writing portfolio book value data to database.")
            execute_completed_successfully = False
            execute_failed_times = 0
            portfolio_summary_table = Table(
                "portfolio_summary",
                MetaData(),
                autoload=True,
                autoload_with=db_connect.dbengine,
            )
            while not execute_completed_successfully and execute_failed_times < 5:
                try:
                    insert_stmt = insert(portfolio_summary_table).values(
                        summary_df.to_dict("records")
                    )
                    upsert_stmt = insert_stmt.on_duplicate_key_update(
                        {x.name: x for x in insert_stmt.inserted}
                    )
                    result = db_connect.dbcon.execute(upsert_stmt)
                    execute_completed_successfully = True
                    logger.info("Successfully wrote portfolio book value data to db.")
                    logger.info(
                        "Number of rows affected in the portfolio_summary table was "
                        + str(result.rowcount)
                    )
                except sqlalchemy.exc.OperationalError as operr:
                    logger.warning(str(operr))
                    time.sleep(1)
                    execute_failed_times += 1
            return 0
    except Exception as exc:
        logger.exception("Could not complete portfolio summary book costs data update.")


def update_simulator_portfolio_summary_book_costs():
    """
    Select all records from the stocks_simulatortransactions table, then
    calculate and upsert the following fields in the stocks_simulatorportfolios table
    (shares_remaining, average_cost, book_cost)
    """
    logger.info("Now trying to update the book values in all simulator portfolios.")
    # set up the db connection
    try:
        with DatabaseConnect() as db_connect:
            # set up our dataframe from the stocks_simulatortransactions table
            transactions_df = pd.io.sql.read_sql(
                f"SELECT simulator_player_id, symbol, num_shares, share_price, bought_or_sold \
                FROM stocks_simulatortransactions;",
                db_connect.dbengine,
            )
            # get the number of shares remaining in the portfolio for each user
            # first get the total shares bought
            total_shares_bought_df = (
                transactions_df[transactions_df.bought_or_sold == "Buy"]
                .groupby(["simulator_player_id", "symbol"])
                .sum()
                .reset_index()
            )
            total_shares_bought_df.drop(["share_price"], axis=1, inplace=True)
            total_shares_bought_df.rename(
                columns={"num_shares": "shares_bought"}, inplace=True
            )
            # then get the total shares sold
            total_shares_sold_df = (
                transactions_df[transactions_df.bought_or_sold == "Sell"]
                .groupby(["simulator_player_id", "symbol"])
                .sum()
                .reset_index()
            )
            total_shares_sold_df.drop(["share_price"], axis=1, inplace=True)
            total_shares_sold_df.rename(
                columns={"num_shares": "shares_sold"}, inplace=True
            )
            # first merge both dataframes
            total_bought_sold_df = total_shares_bought_df.merge(
                total_shares_sold_df, how="outer", on=["simulator_player_id", "symbol"]
            )
            # then fill the shares_sold column with 0
            total_bought_sold_df["shares_sold"] = total_bought_sold_df[
                "shares_sold"
            ].replace(np.NaN, 0)
            # then find the difference to get our number of shares remaining for each user
            total_bought_sold_df["shares_remaining"] = (
                    total_bought_sold_df["shares_bought"]
                    - total_bought_sold_df["shares_sold"]
            )
            # set up a new df to hold the summary data that we are interested in
            summary_df = total_bought_sold_df[
                ["simulator_player_id", "symbol", "shares_remaining"]
            ].copy()
            # get the average cost for each share
            avg_cost_df = transactions_df[
                transactions_df.bought_or_sold == "Buy"
                ].copy()
            # calculate the total book cost for shares purchased
            avg_cost_df["book_cost"] = (
                    avg_cost_df["num_shares"] * avg_cost_df["share_price"]
            )
            avg_cost_df = avg_cost_df.groupby(["simulator_player_id", "symbol"]).sum()
            avg_cost_df.drop(["share_price"], axis=1, inplace=True)
            avg_cost_df = avg_cost_df.reset_index()
            # calculate the average cost for each share purchased
            avg_cost_df["average_cost"] = (
                    avg_cost_df["book_cost"] / avg_cost_df["num_shares"]
            )
            # add these two new fields to our dataframe to be written to the db
            summary_df = summary_df.merge(
                avg_cost_df, how="outer", on=["simulator_player_id", "symbol"]
            )
            summary_df.drop(["num_shares"], axis=1, inplace=True)
            # now write the df to the database
            logger.info("Now writing portfolio book value data to database.")
            execute_completed_successfully = False
            execute_failed_times = 0
            portfolio_summary_table = Table(
                "stocks_simulatorportfolios",
                MetaData(),
                autoload=True,
                autoload_with=db_connect.dbengine,
            )
            while not execute_completed_successfully and execute_failed_times < 5:
                try:
                    insert_stmt = insert(portfolio_summary_table).values(
                        summary_df.to_dict("records")
                    )
                    upsert_stmt = insert_stmt.on_duplicate_key_update(
                        {x.name: x for x in insert_stmt.inserted}
                    )
                    result = db_connect.dbcon.execute(upsert_stmt)
                    execute_completed_successfully = True
                    logger.info("Successfully wrote portfolio book value data to db.")
                    logger.info(
                        "Number of rows affected in the stocks_simulatorportfolios table was "
                        + str(result.rowcount)
                    )
                except sqlalchemy.exc.OperationalError as operr:
                    logger.warning(str(operr))
                    time.sleep(1)
                    execute_failed_times += 1
            return 0
    except Exception as exc:
        logger.exception(
            "Could not complete simulator portfolio summary book costs data update."
        )


def update_simulator_portfolio_summary_market_values():
    """
    Select all records from the stocks_simulatorportfolios table, then
    calculate and upsert the following fields in the stocks_simulatorportfolios table
    (current_market_prices, market_value, total_gain_loss)
    """
    logger.info("Now trying to update the market value in all simulator portfolios.")
    db_connect = None
    try:
        # set up the db connection
        db_connect = DatabaseConnect()
        # set up our dataframe from the portfolio_summary table
        portfolio_summary_df = pd.io.sql.read_sql(
            f"SELECT simulator_player_id, symbol, shares_remaining, book_cost, average_cost \
            FROM stocks_simulatorportfolios;",
            db_connect.dbengine,
        )
        # get the last date that we have scraped data for
        latest_date = pd.io.sql.read_sql(
            f"SELECT date FROM daily_stock_summary WHERE os_bid_vol !=0 ORDER BY date DESC LIMIT 1;",
            db_connect.dbengine,
        )["date"][0]
        # get the closing price for all shares on this date
        closing_price_df = pd.io.sql.read_sql(
            f"SELECT symbol, close_price FROM daily_stock_summary WHERE date='{latest_date}';",
            db_connect.dbengine,
        )
        # now merge the two dataframes to get the closing price
        portfolio_summary_df = portfolio_summary_df.merge(
            closing_price_df, how="inner", on=["symbol"]
        )
        # rename the close price column
        portfolio_summary_df.rename(
            columns={"close_price": "current_market_price"}, inplace=True
        )
        # calculate the market value of the remaining shares in the portfolio
        portfolio_summary_df["market_value"] = (
                portfolio_summary_df["current_market_price"]
                * portfolio_summary_df["shares_remaining"]
        )
        # calculate the total gain or loss
        portfolio_summary_df["total_gain_loss"] = (
                portfolio_summary_df["market_value"] - portfolio_summary_df["book_cost"]
        )
        # calculate the percentage gain or loss
        portfolio_summary_df["gain_loss_percent"] = (
                100
                * portfolio_summary_df["total_gain_loss"]
                / portfolio_summary_df["book_cost"]
        )
        portfolio_summary_df["gain_loss_percent"] = portfolio_summary_df[
            "gain_loss_percent"
        ].replace(np.NaN, 0)
        # now write the df to the database
        logger.info("Now writing portfolio market value data to database.")
        execute_completed_successfully = False
        execute_failed_times = 0
        portfolio_summary_table = Table(
            "stocks_simulatorportfolios",
            MetaData(),
            autoload=True,
            autoload_with=db_connect.dbengine,
        )
        while not execute_completed_successfully and execute_failed_times < 5:
            try:
                portfolio_summary_insert_stmt = insert(portfolio_summary_table).values(
                    portfolio_summary_df.to_dict("records")
                )
                portfolio_summary_upsert_stmt = (
                    portfolio_summary_insert_stmt.on_duplicate_key_update(
                        {x.name: x for x in portfolio_summary_insert_stmt.inserted}
                    )
                )
                result = db_connect.dbcon.execute(portfolio_summary_upsert_stmt)
                execute_completed_successfully = True
                logger.info("Successfully wrote portfolio market value data to db.")
                logger.info(
                    "Number of rows affected in the portfolio_summary table was "
                    + str(result.rowcount)
                )
            except sqlalchemy.exc.OperationalError as operr:
                logger.warning(str(operr))
                time.sleep(1)
                execute_failed_times += 1
        return 0
    except Exception as exc:
        logger.exception("Could not complete portfolio summary data update.")
    finally:
        # Always close the database connection
        if db_connect is not None:
            db_connect.close()
            logger.info("Successfully closed database connection.")


def update_portfolio_summary_market_values():
    """
    Select all records from the portfolio_summary table, then
    calculate and upsert the following fields in the portfolio_summary table
    (current_market_prices, market_value, total_gain_loss)
    """
    logger.info("Now trying to update the market value in all portfolios.")
    db_connect = None
    try:
        # set up the db connection
        db_connect = DatabaseConnect()
        # set up our dataframe from the portfolio_summary table
        portfolio_summary_df = pd.io.sql.read_sql(
            f"SELECT user_id, symbol, shares_remaining, book_cost, average_cost \
            FROM portfolio_summary;",
            db_connect.dbengine,
        )
        # get the last date that we have scraped data for
        latest_date = pd.io.sql.read_sql(
            f"SELECT date FROM daily_stock_summary WHERE os_bid_vol !=0 ORDER BY date DESC LIMIT 1;",
            db_connect.dbengine,
        )["date"][0]
        # get the closing price for all shares on this date
        closing_price_df = pd.io.sql.read_sql(
            f"SELECT symbol, close_price FROM daily_stock_summary WHERE date='{latest_date}';",
            db_connect.dbengine,
        )
        # now merge the two dataframes to get the closing price
        portfolio_summary_df = portfolio_summary_df.merge(
            closing_price_df, how="inner", on=["symbol"]
        )
        # rename the close price column
        portfolio_summary_df.rename(
            columns={"close_price": "current_market_price"}, inplace=True
        )
        # calculate the market value of the remaining shares in the portfolio
        portfolio_summary_df["market_value"] = (
                portfolio_summary_df["current_market_price"]
                * portfolio_summary_df["shares_remaining"]
        )
        # calculate the total gain or loss
        portfolio_summary_df["total_gain_loss"] = (
                portfolio_summary_df["market_value"] - portfolio_summary_df["book_cost"]
        )
        # calculate the percentage gain or loss
        portfolio_summary_df["gain_loss_percent"] = (
                100
                * portfolio_summary_df["total_gain_loss"]
                / portfolio_summary_df["book_cost"]
        )
        portfolio_summary_df["gain_loss_percent"] = portfolio_summary_df[
            "gain_loss_percent"
        ].replace(np.NaN, 0)
        # now write the df to the database
        logger.info("Now writing portfolio market value data to database.")
        execute_completed_successfully = False
        execute_failed_times = 0
        portfolio_summary_table = Table(
            "portfolio_summary",
            MetaData(),
            autoload=True,
            autoload_with=db_connect.dbengine,
        )
        while not execute_completed_successfully and execute_failed_times < 5:
            try:
                portfolio_summary_insert_stmt = insert(portfolio_summary_table).values(
                    portfolio_summary_df.to_dict("records")
                )
                portfolio_summary_upsert_stmt = (
                    portfolio_summary_insert_stmt.on_duplicate_key_update(
                        {x.name: x for x in portfolio_summary_insert_stmt.inserted}
                    )
                )
                result = db_connect.dbcon.execute(portfolio_summary_upsert_stmt)
                execute_completed_successfully = True
                logger.info("Successfully wrote portfolio market value data to db.")
                logger.info(
                    "Number of rows affected in the portfolio_summary table was "
                    + str(result.rowcount)
                )
            except sqlalchemy.exc.OperationalError as operr:
                logger.warning(str(operr))
                time.sleep(1)
                execute_failed_times += 1
        return 0
    except Exception as exc:
        logger.exception("Could not complete portfolio summary data update.")
    finally:
        # Always close the database connection
        if db_connect is not None:
            db_connect.close()
            logger.info("Successfully closed database connection.")


def update_portfolio_sectors_values():
    """
    Select all records from the portfolio_summary table, then
    calculate and upsert the fields in the stocks_portfoliosectors table
    """
    logger.info("Now trying to update the portfolio sector values in all portfolios.")
    db_connect = None
    try:
        # set up the db connection
        db_connect = DatabaseConnect()
        # set up our dataframe from the portfolio_summary table
        portfolio_summary_df = pd.io.sql.read_sql(
            f"SELECT user_id, symbol,shares_remaining, book_cost, market_value, average_cost, current_market_price, total_gain_loss, gain_loss_percent \
            FROM portfolio_summary;",
            db_connect.dbengine,
        )
        # select the sector and symbol from the listed equities dataframe
        listed_equities_df = pd.io.sql.read_sql(
            f"SELECT symbol,sector \
            FROM listed_equities;",
            db_connect.dbengine,
        )
        # now merge the two dataframes to get the sectors for each symbol
        portfolio_summary_df = portfolio_summary_df.merge(
            listed_equities_df, how="inner", on=["symbol"]
        )
        # now group the df by user
        portfolio_summary_df = (
            portfolio_summary_df.groupby(["user_id", "sector"]).sum().reset_index()
        )
        # create a new df with the columns that we need
        portfolio_sector_df = portfolio_summary_df[
            [
                "user_id",
                "sector",
                "book_cost",
                "market_value",
                "total_gain_loss",
                "gain_loss_percent",
            ]
        ].copy()
        # now write the df to the database
        logger.info("Now writing portfolio sector data to database.")
        execute_completed_successfully = False
        execute_failed_times = 0
        portfolio_sector_table = Table(
            "stocks_portfoliosectors",
            MetaData(),
            autoload=True,
            autoload_with=db_connect.dbengine,
        )
        while not execute_completed_successfully and execute_failed_times < 5:
            try:
                insert_stmt = insert(portfolio_sector_table).values(
                    portfolio_sector_df.to_dict("records")
                )
                upsert_stmt = insert_stmt.on_duplicate_key_update(
                    {x.name: x for x in insert_stmt.inserted}
                )
                result = db_connect.dbcon.execute(upsert_stmt)
                execute_completed_successfully = True
                logger.info("Successfully wrote portfolio sector value data to db.")
                logger.info(
                    "Number of rows affected in the portfolio_sector table was "
                    + str(result.rowcount)
                )
            except sqlalchemy.exc.OperationalError as operr:
                logger.warning(str(operr))
                time.sleep(1)
                execute_failed_times += 1
        return 0
    except Exception as exc:
        logger.exception("Could not complete portfolio sector data update.")
    finally:
        # Always close the database connection
        if db_connect is not None:
            db_connect.close()
            logger.info("Successfully closed database connection.")


def update_simulator_games():
    """
    Select all records from the stocks_simulatorgames table, then
    calculate and upsert the interesting fields in the stocks_simulatorgames table
    """
    logger.info("Now trying to update the simulator games.")
    db_connect = None
    try:
        # set up the db connection
        db_connect = DatabaseConnect()
        # set up our dataframes from the db tables
        simulator_games_df = pd.io.sql.read_sql(
            f"SELECT game_id, date_created,date_ended,game_name,game_code,private,is_active, starting_cash, num_players \
            FROM stocks_simulatorgames;",
            db_connect.dbengine,
        )
        simulator_players_df = pd.io.sql.read_sql(
            f"SELECT simulator_player_id, simulator_game_id,liquid_cash, user_id,overall_gain_loss_percent,current_position \
            FROM stocks_simulatorplayers;",
            db_connect.dbengine,
        )
        simulator_portfolio_df = pd.io.sql.read_sql(
            f"SELECT simulator_player_id,total_gain_loss \
            FROM stocks_simulatorportfolios;",
            db_connect.dbengine,
        )
        # first calculate the overall gain/loss for each simulator player
        simulator_portfolio_df = (
            simulator_portfolio_df.groupby(["simulator_player_id"]).sum().reset_index()
        )
        # rename the total_gain_loss column
        simulator_portfolio_df.rename(
            columns={"total_gain_loss": "overall_gain_loss"}, inplace=True
        )
        # merge this df with the simulator players df
        simulator_players_df = simulator_players_df.merge(
            simulator_portfolio_df, how="outer", on=["simulator_player_id"]
        )
        # rename the simulator_game_id field
        simulator_players_df.rename(
            columns={"simulator_game_id": "game_id"}, inplace=True
        )
        #  merge with the simulator games df
        simulator_players_df = simulator_players_df.merge(
            simulator_games_df, how="inner", on=["game_id"]
        )
        # set up a new df column to track current portfolio value ( liquid cash + overall gain)
        # simulator_players_df["current_portfolio_value"] = (
        #     simulator_players_df["overall_gain_loss"]
        #     + simulator_players_df["liquid_cash"]
        # )
        simulator_players_df["current_portfolio_value"] = simulator_players_df[
            "liquid_cash"
        ]
        for index, row in simulator_players_df.iterrows():
            if row["overall_gain_loss"] != np.nan:
                row["current_portfolio_value"] = (
                        row["liquid_cash"] + row["overall_gain_loss"]
                )
        # calculate the overall gain/loss percentage
        simulator_players_df["overall_gain_loss_percent"] = (
                100
                * (
                        simulator_players_df["current_portfolio_value"]
                        - simulator_players_df["starting_cash"]
                )
                / simulator_players_df["starting_cash"]
        )
        # then group by simulator game and sort by portfolio value
        # simulator_players_df["current_position"] = simulator_players_df.sort_values(
        #     ["game_id", "current_portfolio_value"], ascending=False).groupby(["game_id"]).rank(method='min')
        simulator_players_df["current_position"] = simulator_players_df.groupby("game_id")[
            "current_portfolio_value"].rank("first", ascending=True)
        # for each simulator game, check if the game needs be marked as inactive
        simulator_players_df["is_active"] = simulator_players_df.apply(
            lambda x: (1 if (x["date_ended"] > date.today()) else 0),
            axis=1,
        )
        # copy the fields we need to the individual fields to write back to the database
        simulator_games_df = simulator_players_df[
            [
                "game_id",
                "date_created",
                "date_ended",
                "game_name",
                "private",
                "is_active",
                "starting_cash",
                "num_players",
            ]
        ].copy()
        simulator_players_df = simulator_players_df[
            [
                "simulator_player_id",
                "game_id",
                "liquid_cash",
                "user_id",
                "overall_gain_loss",
                "overall_gain_loss_percent",
                "current_position",
                "current_portfolio_value",
            ]
        ].copy()
        simulator_players_df = simulator_players_df.replace(np.NaN, None)
        simulator_players_df.rename(
            columns={
                "game_id": "simulator_game_id",
            },
            inplace=True,
        )
        # now write the df to the database
        logger.info("Now writing portfolio sector data to database.")
        execute_completed_successfully = False
        execute_failed_times = 0
        simulator_games_table = Table(
            "stocks_simulatorgames",
            MetaData(),
            autoload=True,
            autoload_with=db_connect.dbengine,
        )
        simulator_players_table = Table(
            "stocks_simulatorplayers",
            MetaData(),
            autoload=True,
            autoload_with=db_connect.dbengine,
        )
        while not execute_completed_successfully and execute_failed_times < 5:
            try:
                insert_stmt = insert(simulator_games_table).values(
                    simulator_games_df.to_dict("records")
                )
                upsert_stmt = insert_stmt.on_duplicate_key_update(
                    {x.name: x for x in insert_stmt.inserted}
                )
                result = db_connect.dbcon.execute(upsert_stmt)
                execute_completed_successfully = True
                logger.info("Successfully wrote simulator game data to db.")
                logger.info(
                    "Number of rows affected in the simulator game table was "
                    + str(result.rowcount)
                )
                insert_stmt = insert(simulator_players_table).values(
                    simulator_players_df.to_dict("records")
                )
                upsert_stmt = insert_stmt.on_duplicate_key_update(
                    {x.name: x for x in insert_stmt.inserted}
                )
                result = db_connect.dbcon.execute(upsert_stmt)
                execute_completed_successfully = True
                logger.info("Successfully wrote simulator player data to db.")
                logger.info(
                    "Number of rows affected in the simulator player table was "
                    + str(result.rowcount)
                )
            except sqlalchemy.exc.OperationalError as operr:
                logger.warning(str(operr))
                time.sleep(1)
                execute_failed_times += 1
        return 0
    except Exception as exc:
        logger.exception("Could not complete simulator game data update.")
    finally:
        # Always close the database connection
        if db_connect is not None:
            db_connect.close()
            logger.info("Successfully closed database connection.")


def update_simulator_portfolio_sectors_values():
    """
    Select all records from the stocks_simulatorportfolios table, then
    calculate and upsert the fields in the stocks_portfoliosectors table
    """
    logger.info(
        "Now trying to update the portfolio sector values in all simulator portfolios."
    )
    db_connect = None
    try:
        # set up the db connection
        db_connect = DatabaseConnect()
        # set up our dataframe from the portfolio_summary table
        portfolio_summary_df = pd.io.sql.read_sql(
            f"SELECT simulator_player_id, symbol,shares_remaining, book_cost, market_value, average_cost, current_market_price, total_gain_loss, gain_loss_percent \
            FROM stocks_simulatorportfolios;",
            db_connect.dbengine,
        )
        # select the sector and symbol from the listed equities dataframe
        listed_equities_df = pd.io.sql.read_sql(
            f"SELECT symbol,sector \
            FROM listed_equities;",
            db_connect.dbengine,
        )
        # now merge the two dataframes to get the sectors for each symbol
        portfolio_summary_df = portfolio_summary_df.merge(
            listed_equities_df, how="inner", on=["symbol"]
        )
        # now group the df by user
        portfolio_summary_df = (
            portfolio_summary_df.groupby(["simulator_player_id", "sector"])
            .sum()
            .reset_index()
        )
        # create a new df with the columns that we need
        portfolio_sector_df = portfolio_summary_df[
            [
                "simulator_player_id",
                "sector",
                "book_cost",
                "market_value",
                "total_gain_loss",
                "gain_loss_percent",
            ]
        ].copy()
        # now write the df to the database
        logger.info("Now writing simulator portfolio sector data to database.")
        execute_completed_successfully = False
        execute_failed_times = 0
        portfolio_sector_table = Table(
            "stocks_simulatorportfoliosectors",
            MetaData(),
            autoload=True,
            autoload_with=db_connect.dbengine,
        )
        while not execute_completed_successfully and execute_failed_times < 5:
            try:
                insert_stmt = insert(portfolio_sector_table).values(
                    portfolio_sector_df.to_dict("records")
                )
                upsert_stmt = insert_stmt.on_duplicate_key_update(
                    {x.name: x for x in insert_stmt.inserted}
                )
                result = db_connect.dbcon.execute(upsert_stmt)
                execute_completed_successfully = True
                logger.info(
                    "Successfully wrote simulator portfolio sector value data to db."
                )
                logger.info(
                    "Number of rows affected in the portfolio_sector table was "
                    + str(result.rowcount)
                )
            except sqlalchemy.exc.OperationalError as operr:
                logger.warning(str(operr))
                time.sleep(1)
                execute_failed_times += 1
        return 0
    except Exception as exc:
        logger.exception("Could not complete portfolio sector data update.")
    finally:
        # Always close the database connection
        if db_connect is not None:
            db_connect.close()
            logger.info("Successfully closed database connection.")


@pidfile()
def main(args: list[str]):
    """Main function for updating portfolio data"""
    cli_arguments: argparse.Namespace = set_up_arguments(args)
    try:
        with multiprocessing.Pool(os.cpu_count()) as multipool:
            logger.info("Now starting stocks updater module.")
            if cli_arguments.daily_update:
                multipool.apply(update_portfolio_summary_market_values, ())
            else:
                # get the latest conversion rates
                TTD_JMD, TTD_USD, TTD_BBD = multipool.apply(
                    fetch_latest_currency_conversion_rates, ()
                )
                # update the fundamental analysis stock data
                multipool.apply(
                    calculate_fundamental_analysis_ratios,
                    (TTD_JMD, TTD_USD, TTD_BBD),
                )
                multipool.apply(update_dividend_yields, (TTD_JMD, TTD_USD, TTD_BBD))
                # update the portfolio data for all users
                multipool.apply(update_portfolio_summary_book_costs, ())
                multipool.apply(update_portfolio_summary_market_values, ())
                multipool.apply(update_portfolio_sectors_values, ())
                # update the simulator portfolio data for all simulator players
                multipool.apply(update_simulator_portfolio_summary_book_costs, ())
                multipool.apply(
                    update_simulator_portfolio_summary_market_values, ()
                )
                multipool.apply(update_simulator_portfolio_sectors_values, ())
                multipool.apply(update_simulator_games, ())
            multipool.close()
            multipool.join()
            logger.info(os.path.basename(__file__) + " executed successfully.")
        return 0
    except Exception:
        logging.exception("Error in script " + os.path.basename(__file__))


def set_up_arguments(args: list[str]):
    # first check the arguments given to this script
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--daily_update",
        help="Update the portfolio market data with the latest values",
        action="store_true",
    )
    return parser.parse_args(args)


# endregion FUNCTION DEFINITIONS

# If this script is being run from the command-line, then run the main() function
if __name__ == "__main__":
    main(sys.argv[1:])