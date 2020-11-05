#!/usr/bin/env python3
# -*- coding: utf-8 -*-
 
"""
This script was used as a one-off to import some old stock prices that I pulled from a website
into the TTSE database 
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
import pathlib
import json
from datetime import datetime
import time

# Imports from the cheese factory
from sqlalchemy import create_engine, Table, select, MetaData, text, and_
import pandas as pd
import numpy as np

# Imports from the local filesystem
from scrape_ttse.databaseconnect import DatabaseConnect
from sqlalchemy.dialects.mysql import insert
import sqlalchemy.exc
 
# Put your constants here. These should be named in CAPS.

# Put your global variables here. 
 
# Put your class definitions here. These should use the CapWords convention.
 
# Put your function definitions here. These should be lowercase, separated by underscores.
 
def main():
    logging.basicConfig(level=logging.DEBUG)
    try:
        # this script expects a directory named raw_json in the same directory as itself
        current_script_path = pathlib.Path(__file__).parent.absolute()
        raw_json_path = current_script_path.joinpath('raw_json')
        # get all json files in this folder
        raw_json_files = []
        for filename in os.listdir(raw_json_path):
            if filename.endswith(".json"):
                raw_json_files.append(raw_json_path.joinpath(filename))
        # now retrieve the data from each file
        past_stock_close_prices = []
        for json_file in raw_json_files:
            with open(json_file, 'r') as opened_file:
                try:
                    json_data = json.loads(opened_file.read())
                    for json_obj in json_data['data']:
                        date = datetime.fromtimestamp(json_obj['date']/1000)
                        close_price = json_obj['close']
                        if date.year < 2010:
                            stock_data = {'symbol':json_file.name.split('_')[1],'date':date.strftime('%Y-%m-%d'),'open_price':close_price,'high':close_price,'low':close_price,'os_bid':None,'os_bid_vol':None,'os_offer':None,'os_offer_vol':None,'last_sale_price':close_price,'was_traded_today':None,'volume_traded':None,'close_price':close_price,'value_traded':None}
                            past_stock_close_prices.append(stock_data)
                            pass
                except json.decoder.JSONDecodeError:
                    logging.exception(f"Error while reading file: {json_file}")
        # create a df
        stock_price_df = pd.DataFrame(past_stock_close_prices)
        stock_price_df['change_dollars'] = stock_price_df.groupby('symbol')['close_price'].diff()
        stock_price_df = stock_price_df.replace({np.nan: None})
        # now insert data into db
        db_connect = DatabaseConnect()
        daily_stock_table = Table(
            'daily_stock_summary', MetaData(), autoload=True, autoload_with = db_connect.dbengine)
        logging.info("Now writing fundamental data to database.")
        execute_completed_successfully = False
        execute_failed_times = 0
        while not execute_completed_successfully and execute_failed_times < 5:
            try:
                insert_stmt = insert(
                    daily_stock_table).values(stock_price_df.to_dict('records'))
                upsert_stmt = insert_stmt.on_duplicate_key_update(
                    {x.name: x for x in insert_stmt.inserted})
                result = db_connect.dbcon.execute(
                    upsert_stmt)
                execute_completed_successfully = True
            except sqlalchemy.exc.OperationalError as operr:
                logging.warning(str(operr))
                time.sleep(1)
                execute_failed_times += 1
            logging.info(
                "Successfully scraped and wrote fundamental data to db.")
            logging.info(
                "Number of rows affected in the audited fundamental calculated table was "+str(result.rowcount))
    except Exception as exc:
        logging.error(f"Error in script {os.path.basename(__file__)}. Here's what we know: {exc}")
    else:
        logging.info(os.path.basename(__file__)+" executed successfully.")
        sys.exit(0)
 
# If this script is being run from the command-line, then run the main() function
if __name__ == "__main__":
	main()