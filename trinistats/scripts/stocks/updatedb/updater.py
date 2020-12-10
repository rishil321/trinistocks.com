#!/usr/bin/env python3
# -*- coding: utf-8 -*-
 
"""
Description of this module/script goes here
:param -f OR --first_parameter: The description of your first input parameter
:returns: Whatever your script returns when called
:raises Exception if any issues are encountered
"""
 
# Put all your imports here, one per line. 
# However multiple imports from the same lib are allowed on a line.
# Imports from Python standard libraries
import sys
import logging
import os
import pandas as pd
import numpy as np
import tempfile
import multiprocessing
import json
import time
import argparse

# Imports from the cheese factory
from pid import PidFile
import requests
from sqlalchemy import create_engine, Table, select, MetaData, text, and_
from sqlalchemy.dialects.mysql import insert
import sqlalchemy.exc

# Imports from the local filesystem
from ...database_ops import DatabaseConnect
from ... import custom_logging
from ..crosslisted_symbols import USD_DIVIDEND_SYMBOLS,JMD_DIVIDEND_SYMBOLS,BBD_DIVIDEND_SYMBOLS
 
# Put your constants here. These should be named in CAPS.

# Put your global variables here. 
 
# Put your class definitions here. These should use the CapWords convention.
 
# Put your function definitions here. These should be lowercase, separated by underscores.


def fetch_latest_currency_conversion_rates():
    logging.debug("Now trying to fetch latest currency conversions.")
    api_response_ttd = requests.get(
            url="https://fcsapi.com/api-v2/forex/base_latest?symbol=TTD&type=forex&access_key=o9zfwlibfXciHoFO4LQU2NfTwt2vEk70DAiOH1yb2ao4tBhNmm")
    if (api_response_ttd.status_code == 200):
        # store the conversion rates that we need
        TTD_JMD = float(json.loads(api_response_ttd.content.decode('utf-8'))['response']['JMD'])
        TTD_USD = float(json.loads(api_response_ttd.content.decode('utf-8'))['response']['USD'])
        TTD_BBD = float(json.loads(api_response_ttd.content.decode('utf-8'))['response']['BBD'])
        logging.debug("Currency conversions fetched correctly.")
        return TTD_JMD, TTD_USD, TTD_BBD
    else:
        logging.exception(f"Cannot load URL for currency conversions.{api_response_ttd.status_code},{api_response_ttd.reason},{api_response_ttd.url}")


def calculate_fundamental_analysis_ratios(TTD_JMD, TTD_USD, TTD_BBD):
    """
    Calculate the important ratios for fundamental analysis, based off our manually entered data from the financial statements
    """
    audited_raw_table_name = 'audited_fundamental_raw_data'
    audited_calculated_table_name = 'audited_fundamental_calculated_data'
    listed_equities_table_name = 'listed_equities'
    daily_stock_summary_table_name = 'daily_stock_summary'
    historical_dividend_info_table_name = 'historical_dividend_info'
    db_connect = None
    try:
        with DatabaseConnect() as db_connect:
            logging.info("Successfully connected to database")
            logging.info("Now reading raw fundamental data from db")
            # load the audited raw data table
            audited_raw_table = Table(
                audited_raw_table_name, MetaData(), autoload=True, autoload_with=db_connect.dbengine)
            listedequities_table = Table(
                listed_equities_table_name, MetaData(), autoload=True, autoload_with=db_connect.dbengine)
            audited_calculated_table = Table(
                audited_calculated_table_name, MetaData(), autoload=True, autoload_with=db_connect.dbengine)
            # read the audited raw table as a pandas df
            audited_raw_df = pd.io.sql.read_sql(
                f"SELECT * FROM {audited_raw_table_name} ORDER BY symbol,date;", db_connect.dbengine)
            # create new dataframe for calculated ratios
            audited_calculated_df = pd.DataFrame()
            audited_calculated_df['symbol'] = audited_raw_df['symbol'].copy()
            audited_calculated_df['date'] = audited_raw_df['date'].copy()
            # calculate the average equity
            average_equity_df = pd.DataFrame()
            average_equity_df['symbol'] = audited_calculated_df['symbol'].copy()
            average_equity_df['total_stockholders_equity'] = audited_raw_df['total_shareholders_equity'].copy(
            )
            average_equity_df['average_stockholders_equity'] = average_equity_df.groupby('symbol')['total_stockholders_equity'].apply(
                lambda x: (x + x.shift(1))/2)
            # calculate the return on equity
            audited_calculated_df['RoE'] = audited_raw_df['net_income'] / \
                average_equity_df['average_stockholders_equity']
            # now calculate the return on invested capital
            audited_calculated_df['RoIC'] = audited_raw_df['profit_after_tax'] / \
                audited_raw_df['total_shareholders_equity']
            # now calculate the working capital
            audited_calculated_df['working_capital'] = audited_raw_df['total_assets'] / \
                audited_raw_df['total_liabilities']
            # copy basic earnings per share
            audited_calculated_df['EPS'] = audited_raw_df['basic_earnings_per_share']
            # calculate price to earnings ratio
            # first get the latest share price data
            # get the latest date from the daily stock table
            latest_stock_date = pd.io.sql.read_sql(
                f"SELECT date FROM {daily_stock_summary_table_name} ORDER BY date DESC LIMIT 1;", db_connect.dbengine)['date'][0].strftime('%Y-%m-%d')
            # then get the share price for each listed stock at this date
            share_price_df = pd.io.sql.read_sql(
                f"SELECT symbol,close_price FROM {daily_stock_summary_table_name} WHERE date='{latest_stock_date}';", db_connect.dbengine)
            # create a merged df to calculate the p/e
            price_to_earnings_df = pd.merge(
                audited_raw_df, share_price_df, how='inner', on='symbol')
            # calculate a conversion rate for the stock price
            price_to_earnings_df['share_price_conversion_rates'] =  price_to_earnings_df.apply(lambda x: TTD_USD if 
                            x.currency == 'USD' else (TTD_JMD if x.currency == 'JMD' else (TTD_BBD if x.currency == 'BBD' else 1.00)), axis=1)
            audited_calculated_df['price_to_earnings_ratio'] = price_to_earnings_df['close_price'] * price_to_earnings_df['share_price_conversion_rates'] / \
                price_to_earnings_df['basic_earnings_per_share']
            # now calculate the price to dividend per share ratio
            # first get the dividends per share
            dividends_df = pd.io.sql.read_sql(
                f"SELECT symbol,record_date,dividend_amount FROM {historical_dividend_info_table_name};", db_connect.dbengine)
            # merge this df with the share_price_df
            dividends_df = pd.merge(
                share_price_df, dividends_df, how='inner', on='symbol')
            # we need to set up a series with the conversion factors for different currencies
            symbols_list = dividends_df['symbol'].to_list()
            conversion_rates = []
            for symbol in symbols_list:
                if symbol in USD_DIVIDEND_SYMBOLS:
                    conversion_rates.append(1/TTD_USD)
                elif symbol in JMD_DIVIDEND_SYMBOLS:
                    conversion_rates.append(1/TTD_JMD)
                elif symbol in BBD_DIVIDEND_SYMBOLS:
                    conversion_rates.append(1/TTD_BBD)
                else:
                    conversion_rates.append(1.00)
            # now add this new series to our df
            dividends_df['dividend_conversion_rates'] = pd.Series(conversion_rates,index=dividends_df.index)
            # calculate the price to dividend per share ratio
            audited_calculated_df['price_to_dividends_per_share_ratio'] = dividends_df['close_price'] / \
                (dividends_df['dividend_amount']*dividends_df['dividend_conversion_rates'])
            # now calculate the eps growth rate
            audited_calculated_df['EPS_growth_rate'] = audited_raw_df['basic_earnings_per_share'].diff(
            )*100
            # now calculate the price to earnings-to-growth ratio
            audited_calculated_df['PEG'] = audited_calculated_df['price_to_earnings_ratio'] / \
                audited_calculated_df['EPS_growth_rate']
            audited_calculated_df = audited_calculated_df.where(
                pd.notnull(audited_calculated_df), None)
            # calculate dividend yield and dividend payout ratio
            # note that the price_to_earnings_df contains the share price
            audited_calculated_df['dividend_yield'] = 100 * \
                price_to_earnings_df['dividends_per_share'] / (price_to_earnings_df['close_price'] * price_to_earnings_df['share_price_conversion_rates'])
            audited_calculated_df['dividend_payout_ratio'] = 100* price_to_earnings_df['total_dividends_paid'] / \
                price_to_earnings_df['net_income']
            # calculate the book value per share (BVPS)
            audited_calculated_df['book_value_per_share'] = (audited_raw_df['total_assets'] - audited_raw_df['total_liabilities']) / \
                                                            audited_raw_df['total_shares_outstanding']
            # calculate the price to book ratio
            audited_calculated_df['price_to_book_ratio'] = (price_to_earnings_df['close_price'] * price_to_earnings_df['share_price_conversion_rates']) / \
                ((price_to_earnings_df['total_assets'] - price_to_earnings_df['total_liabilities']) / \
                                                            price_to_earnings_df['total_shares_outstanding'])
            # replace inf with None
            audited_calculated_df = audited_calculated_df.replace([np.inf, -np.inf], None)
            # now write the df to the database
            logging.info("Now writing fundamental data to database.")
            execute_completed_successfully = False
            execute_failed_times = 0
            while not execute_completed_successfully and execute_failed_times < 5:
                try:
                    insert_stmt = insert(
                        audited_calculated_table).values(audited_calculated_df.to_dict('records'))
                    upsert_stmt = insert_stmt.on_duplicate_key_update(
                        {x.name: x for x in insert_stmt.inserted})
                    result = db_connect.dbcon.execute(
                        upsert_stmt)
                    execute_completed_successfully = True
                    logging.info(
                        "Successfully scraped and wrote fundamental data to db.")
                    logging.info(
                        "Number of rows affected in the audited fundamental calculated table was "+str(result.rowcount))
                except sqlalchemy.exc.OperationalError as operr:
                    logging.warning(str(operr))
                    time.sleep(1)
                    execute_failed_times += 1
        return 0
    except Exception:
        logging.exception(
            "Could not complete fundamental data update.")
        custom_logging.flush_smtp_logger()


def update_portfolio_summary_book_costs():
    """
    Select all records from the portfolio_transactions table, then 
    calculate and upsert the following fields in the portfolio_summary table
    (shares_remaining, average_cost, book_cost)
    """
    logging.info("Now trying to update the book value in all portfolios.")
    db_connect = None
    try:
        # set up the db connection
        db_connect = DatabaseConnect()
        # set up our dataframe from the portfolio_transactions table
        transactions_df = pd.io.sql.read_sql(f"SELECT user_id, symbol_id, num_shares, share_price, bought_or_sold \
            FROM portfolio_transactions;", db_connect.dbengine)
        # get the number of shares remaining in the portfolio for each user
        # first get the total shares bought
        total_shares_bought_df = transactions_df[transactions_df.bought_or_sold == "Bought"].groupby(['user_id','symbol_id']).sum().reset_index()
        total_shares_bought_df.drop(['share_price'], axis=1, inplace=True)
        total_shares_bought_df.rename(columns={'num_shares':'shares_bought'}, inplace=True)
        # then get the total shares sold
        total_shares_sold_df = transactions_df[transactions_df.bought_or_sold == "Sold"].groupby(['user_id','symbol_id']).sum().reset_index()
        total_shares_sold_df.drop(['share_price'], axis=1, inplace=True)
        total_shares_sold_df.rename(columns={'num_shares':'shares_sold'}, inplace=True)
        # first merge both dataframes
        total_bought_sold_df = total_shares_bought_df.merge(total_shares_sold_df, how='outer',on=['user_id','symbol_id'])
        # then fill the shares_sold column with 0
        total_bought_sold_df['shares_sold'] = total_bought_sold_df['shares_sold'].replace(np.NaN, 0)
        # then find the difference to get our number of shares remaining for each user
        total_bought_sold_df['shares_remaining'] = total_bought_sold_df['shares_bought'] - total_bought_sold_df['shares_sold']
        # set up a new df to hold the summary data that we are interested in
        summary_df = total_bought_sold_df[['user_id','symbol_id','shares_remaining']].copy()
        # get the average cost for each share
        avg_cost_df = transactions_df[transactions_df.bought_or_sold == "Bought"].copy()
        # calculate the total book cost for shares purchased 
        avg_cost_df['book_cost'] = avg_cost_df['num_shares'] * avg_cost_df['share_price']
        avg_cost_df = avg_cost_df.groupby(['user_id','symbol_id']).sum()
        avg_cost_df.drop(['share_price'], axis=1, inplace=True)
        avg_cost_df = avg_cost_df.reset_index()
        # calculate the average cost for each share purchased
        avg_cost_df['average_cost'] = avg_cost_df['book_cost'] / avg_cost_df['num_shares']
        # add these two new fields to our dataframe to be written to the db
        summary_df = summary_df.merge(avg_cost_df, how='outer', on=['user_id','symbol_id'])
        summary_df.drop(['num_shares'], axis=1, inplace=True)
        # now write the df to the database
        logging.info("Now writing portfolio book value data to database.")
        execute_completed_successfully = False
        execute_failed_times = 0
        portfolio_summary_table = Table(
            'portfolio_summary', MetaData(), autoload=True, autoload_with=db_connect.dbengine)
        while not execute_completed_successfully and execute_failed_times < 5:
            try:
                insert_stmt = insert(
                    portfolio_summary_table).values(summary_df.to_dict('records'))
                upsert_stmt = insert_stmt.on_duplicate_key_update(
                    {x.name: x for x in insert_stmt.inserted})
                result = db_connect.dbcon.execute(
                    upsert_stmt)
                execute_completed_successfully = True
                logging.info(
                    "Successfully wrote portfolio book value data to db.")
                logging.info(
                    "Number of rows affected in the portfolio_summary table was "+str(result.rowcount))
            except sqlalchemy.exc.OperationalError as operr:
                logging.warning(str(operr))
                time.sleep(1)
                execute_failed_times += 1
        return 0
    except Exception as exc:
        logging.exception(
            "Could not complete portfolio summary data update.")
        custom_logging.flush_smtp_logger()
    finally:
        # Always close the database connection
        if db_connect is not None:
            db_connect.close()
            logging.info("Successfully closed database connection.")


def update_portfolio_summary_market_values():
    """
    Select all records from the portfolio_summary table, then 
    calculate and upsert the following fields in the portfolio_summary table
    (current_market_prices, market_value, total_gain_loss)
    """
    logging.info("Now trying to update the market value in all portfolios.")
    db_connect = None
    try:
        # set up the db connection
        db_connect = DatabaseConnect()
        # set up our dataframe from the portfolio_summary table
        portfolio_summary_df = pd.io.sql.read_sql(f"SELECT user_id, symbol_id, shares_remaining, book_cost, average_cost \
            FROM portfolio_summary;", db_connect.dbengine)
        # get the last date that we have scraped data for 
        latest_date = pd.io.sql.read_sql(f"SELECT date FROM daily_stock_summary ORDER BY date DESC LIMIT 1;", db_connect.dbengine)['date'][0]
        # get the closing price for all shares on this date
        closing_price_df = pd.io.sql.read_sql(f"SELECT symbol, close_price FROM daily_stock_summary WHERE date='{latest_date}';", db_connect.dbengine)
        # rename the symbol column
        closing_price_df.rename(columns={'symbol':'symbol_id'}, inplace=True)
        # now merge the two dataframes to get the closing price 
        portfolio_summary_df = portfolio_summary_df.merge(closing_price_df, how='inner', on=['symbol_id'])
        # rename the close price column
        portfolio_summary_df.rename(columns={'close_price':'current_market_price'}, inplace=True)
        # calculate the market value of the remaining shares in the portfolio
        portfolio_summary_df['market_value'] = portfolio_summary_df['current_market_price'] * portfolio_summary_df['shares_remaining']
        # calculate the total gain or loss 
        portfolio_summary_df['total_gain_loss'] = portfolio_summary_df['market_value'] - portfolio_summary_df['book_cost']
        # now write the df to the database
        logging.info("Now writing portfolio book value data to database.")
        execute_completed_successfully = False
        execute_failed_times = 0
        portfolio_summary_table = Table(
            'portfolio_summary', MetaData(), autoload=True, autoload_with=db_connect.dbengine)
        while not execute_completed_successfully and execute_failed_times < 5:
            try:
                portfolio_summary_insert_stmt = insert(portfolio_summary_table).values(
                    portfolio_summary_df.to_dict('records'))
                portfolio_summary_upsert_stmt = portfolio_summary_insert_stmt.on_duplicate_key_update(
                    {x.name: x for x in portfolio_summary_insert_stmt.inserted})
                result = db_connect.dbcon.execute(
                    portfolio_summary_upsert_stmt)
                execute_completed_successfully = True
                logging.info(
                    "Successfully wrote portfolio market value data to db.")
                logging.info(
                    "Number of rows affected in the portfolio_summary table was "+str(result.rowcount))
            except sqlalchemy.exc.OperationalError as operr:
                logging.warning(str(operr))
                time.sleep(1)
                execute_failed_times += 1
        return 0
    except Exception as exc:
        logging.exception(
            "Could not complete portfolio summary data update.")
        custom_logging.flush_smtp_logger()
    finally:
        # Always close the database connection
        if db_connect is not None:
            db_connect.close()
            logging.info("Successfully closed database connection.")


def main(args):
    """Main function for updating portfolio data"""
    try:
        # set up logging
        q_listener, q = custom_logging.setup_logging(
            logdirparent=str(os.path.dirname(os.path.realpath(__file__))),
            logfilestandardname=os.path.basename(__file__),
            stdoutlogginglevel=logging.DEBUG,
            smtploggingenabled=True,
            smtplogginglevel=logging.ERROR,
            smtpmailhost='localhost',
            smtpfromaddr='server1@trinistats.com',
            smtptoaddr=['latchmepersad@gmail.com'],
            smtpsubj='Automated report from Python script: '+os.path.basename(__file__))
        with PidFile(piddir=tempfile.gettempdir()):
            with multiprocessing.Pool(os.cpu_count(), custom_logging.logging_worker_init, [q]) as multipool:
                logging.info("Now starting trinistats stocks updater module.")
                if args.daily_update:
                    multipool.apply(
                        update_portfolio_summary_market_values, ())
                else:
                    # get the latest conversion rates
                    TTD_JMD, TTD_USD, TTD_BBD = multipool.apply(fetch_latest_currency_conversion_rates,())
                    # update the fundamental analysis stock data
                    multipool.apply_async(
                            calculate_fundamental_analysis_ratios, (TTD_JMD, TTD_USD, TTD_BBD))
                    # update the portfolio data for all users
                    multipool.apply(update_portfolio_summary_book_costs(), ())
                multipool.close()
                multipool.join()
                logging.info(os.path.basename(__file__) +
                             " executed successfully.")
                q_listener.stop()
                return 0
    except Exception:
        logging.exception("Error in script "+os.path.basename(__file__))
        sys.exit(1)
 
# If this script is being run from the command-line, then run the main() function
if __name__ == "__main__":
    # first check the arguements given to this script
    parser = argparse.ArgumentParser()
    parser.add_argument("-m",
                        "--daily_update", help="Update the portfolio market data with the latest values", action="store_true")
    args = parser.parse_args()
    main(args)