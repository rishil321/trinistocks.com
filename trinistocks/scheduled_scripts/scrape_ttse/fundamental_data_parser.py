#!/usr/bin/env python3
# -*- coding: utf-8 -*-
 
"""
Description of this module/script goes here
:param -f OR --first_parameter: The description of your first input parameter
:returns: Whatever your script returns when called
:raises Exception if any issues are encountered
"""
 
import glob
import logging
import os
import re
import shutil
import sys
import tempfile
import time
from datetime import datetime, timedelta
# Put all your imports here, one per line. 
# However multiple imports from the same lib are allowed on a line.
# Imports from Python standard libraries
from multiprocessing import Value
from pathlib import Path

import camelot
import numpy
# Imports from the cheese factory
import ocrmypdf
import pandas as pd
import sqlalchemy.exc
from pid import PidFile
from sqlalchemy import MetaData, Table, and_, create_engine, select, text
from sqlalchemy.dialects.mysql import insert

# Imports from the local filesystem
from scripts import custom_logging
from scripts.database_ops import DatabaseConnect

# Put your constants here. These should be named in CAPS.

# Put your global variables here. 
 
# Put your class definitions here. These should use the CapWords convention.
 
# Put your function definitions here. These should be lowercase, separated by underscores.
def convert_quarterly_reports_text():
    """
    Use the ocrmypdf lib to convert the pics of quarterly pdfs to text-based
    pdfs
    """
    logging.getLogger('pdfminer').setLevel(logging.ERROR)
    # check for all the pdfs in the 'incoming_quarterly_reports' directory
    current_dir = Path(os.path.realpath(__file__)).parent
    raw_quarterly_reports = glob.glob(os.path.join(current_dir,'incoming_quarterly_reports')+'/*.pdf')
    # then process all the pdfs and ensure that they are text-based 
    # and place these new text-based pdfs in the 'processed_quarterly_reports' directory
    for report in raw_quarterly_reports:
        logging.info(f"Now trying to recognise characters from {report} and produce text-based pdf.")
        ocrmypdf.ocr(report,report.replace("incoming_","processed_"),deskew=True,
            optimize=0,oversample=5,clean=True,remove_background=True)
        logging.info("Processing successful!")
    return 0


def convert_annual_reports_text():
    """
    Use the ocrmypdf lib to convert the pics of annual pdfs to text-based
    pdfs
    """
    logging.getLogger('pdfminer').setLevel(logging.ERROR)
    logging.getLogger('ocrmypdf').setLevel(logging.INFO)
    # check for all the pdfs in the 'incoming_quarterly_reports' directory
    current_dir = Path(os.path.realpath(__file__)).parent
    raw_annual_reports = glob.glob(os.path.join(current_dir,'incoming_annual_reports')+'/*.pdf')
    # then process all the pdfs and ensure that they are text-based 
    # and place these new text-based pdfs in the 'processed_quarterly_reports' directory
    for report in raw_annual_reports:
        try:
            logging.info(f"Now trying to recognise characters from {report} and produce text-based pdf.")
            ocrmypdf.ocr(report,report.replace("incoming_","processed_"),deskew=True,
                optimize=0,oversample=5,clean=True,remove_background=True)
            logging.info("Processing successful!")
        except ocrmypdf.exceptions.PriorOcrFoundError:
            logging.warning("This pdf is already text-based. Copying it to the output directory")
            shutil.copyfile(report, report.replace("incoming_","processed_"))
    return 0


def parse_to_int(raw_string):
    """Remove non numeric characters from the 
    input string and try to parse it to an int
    :param raw_string: The raw string to try to parse to an int
    :type raw_string: str
    :return: An integer
    :rtype: int
    """
    # check if there is an opening bracket in the string
    # if there is, we need to return a negative number
    if '(' in raw_string:
        parsed_int = int(re.sub("[^0-9]","",raw_string))*-1
    else:
        # else it is a positive int
        parsed_int = int(re.sub("[^0-9]","",raw_string))
    # raise an error if the result is None
    if not parsed_int:
        raise ValueError(f'Possible empty string specified? {raw_string}')
    return parsed_int

def clean_and_lower_string(input_string):
    """Removes unwanted characters from strings and lowercases them

    :param input_string: Input string to be cleaned
    :type input_string: str
    :return: The cleaned and lowercased string
    :rtype: str
    """
    if isinstance(input_string,str):
        output_string = re.sub(r"\n|\\n|\s+","",input_string, flags=re.UNICODE)
        output_string = output_string.lower()
        return output_string
    else:
        logging.debug('Non-string passed to input. Returning empty string.')
        return ''

def series_to_clean_string(input_series):
    """Parses a Pandas series into a string, or accepts a regular string
    and strips certain unwanted characters
    :param raw_table_row: A row from a pandas dataframe, or a regular string
    :type raw_table_row: Series or str
    """
    if not isinstance(input_series,str):
        input_series = input_series.to_string(max_rows=None)
    row_string = clean_and_lower_string(input_series)
    return row_string

def parse_date(raw_date_string):
    """Attempts to parse a proper date from whatever string 
    is read in by the Camelot library

    :param raw_date_string: The raw date string read in
    :type raw_date_string: str
    """
    processed_string = raw_date_string.replace(" ","") \
        .replace("Sept","Sep").replace("\n","").replace('20138','2018')
    parsed_date = datetime.strptime(processed_string,"%d%b%Y")
    return parsed_date

def process_quarterly_reports():
    """
    Read in the pdfs in the 'processed quarterly reports' directory
    and write them to the db
    """
    logging.getLogger('pdfminer').setLevel(logging.ERROR)
    logging.getLogger('camelot').setLevel(logging.INFO)
    current_dir = Path(os.path.realpath(__file__)).parent
    processed_reports = glob.glob(os.path.join(current_dir,'processed_quarterly_reports')+'/*.pdf')
    # set up a new dataframe to store our parsed data
    parsed_data_df = pd.DataFrame(columns=['symbol','date','total_assets','total_liabilities',
        'revenue','operating_profit','profit_for_period','eps'])
    # now go through these new text-based pdfs and parse the data
    for report in processed_reports:
        # pull the data from all pages of the pdf
        logging.info(f"Now trying to parse data from {report}")
        all_tables = camelot.read_pdf(report,flavor='stream', edge_tol=10, pages='all')
        # since each company lists their reports in a slightly different format, we have to process these
        # on a case-by-case basis
        if "Agostini" in report:
            symbol = 'AGL'
            logging.info(f"Now trying to parse report data from {symbol}.")
            try:
                # get a list of values of the balance sheet (1st table in the pdf)
                raw_balance_sheet_df = all_tables[0].df.copy(deep=True)
                # set up the df to store the parsed data
                parsed_balance_sheet_df = pd.DataFrame(columns=['symbol','date','total_assets','total_liabilities'])
                # check if we have newline characters in this table
                if "\n" in raw_balance_sheet_df[3][5]:
                    # and split the columns by newline if there are
                    raw_balance_sheet_df[[3,4,5]] = raw_balance_sheet_df[3].str.split('\n',expand=True)
                # first get the indices of the rows that contain our important data
                important_data_indices = {'total_asset_index':None,'non_current_liabilities_index':None,
                    'current_liabilities_index':None,'date_index':None}
                for index,row in raw_balance_sheet_df.iterrows():
                    if 'unauditedaudited' in re.sub(r'\d','',series_to_clean_string(row)) and not important_data_indices['date_index']:
                        important_data_indices['date_index'] = index+1
                    elif 'totalassets' in series_to_clean_string(row) and not important_data_indices['total_asset_index']:
                        important_data_indices['total_asset_index'] = index
                    elif 'non-currentliabilities' in row.to_string().replace(' ','').lower() and not important_data_indices['non_current_liabilities_index']:
                        important_data_indices['non_current_liabilities_index'] = index
                    elif 'currentliabilities' in row.to_string().replace(' ','').lower() and not important_data_indices['current_liabilities_index']:
                        important_data_indices['current_liabilities_index'] = index
                # ensure that all indices were found
                for key in important_data_indices:
                    if not important_data_indices[key]:
                        raise KeyError()
                # now check which columns in the date row have data
                empty_columns = []
                for index,column in enumerate(raw_balance_sheet_df.values[5]):
                    if column == '':
                        empty_columns.append(index)
                # now remove the empty columns from our df
                raw_balance_sheet_df.drop(empty_columns,axis=1, inplace=True)
                raw_balance_sheet_df = raw_balance_sheet_df.T.reset_index(drop=True).T
                if len(raw_balance_sheet_df.columns) == 6:
                    raw_balance_sheet_df.drop(2,axis=1, inplace=True)
                    raw_balance_sheet_df = raw_balance_sheet_df.T.reset_index(drop=True).T
                elif len(raw_balance_sheet_df.columns) == 7:
                    raw_balance_sheet_df.drop([5,6],axis=1, inplace=True)
                    raw_balance_sheet_df = raw_balance_sheet_df.T.reset_index(drop=True).T
                # then parse it and append it to our dataframe
                for column in raw_balance_sheet_df.columns:
                    report_data = dict()
                    report_data['symbol'] = symbol
                    report_data['date'] = parse_date(raw_balance_sheet_df[column][important_data_indices['date_index']])
                    report_data['total_assets'] = parse_to_int(raw_string=raw_balance_sheet_df[column][important_data_indices['total_asset_index']])*1000
                    report_data['total_liabilities'] = 1000*(parse_to_int(raw_string=raw_balance_sheet_df[column][important_data_indices['non_current_liabilities_index']]) + parse_to_int(raw_string=raw_balance_sheet_df[column][important_data_indices['current_liabilities_index']]))
                    parsed_balance_sheet_df = parsed_balance_sheet_df.append(report_data,ignore_index=True)
                if parsed_balance_sheet_df.empty:
                    raise RuntimeError(f'Could not parse balance sheet data from {report}')
                logging.info("Finished parsing data from balance sheet.")
                # now get data from the statement of income
                # set up an empty dataframe for the statement of income
                raw_income_statement_df = pd.DataFrame()
                # set up a temp dataframe to store our parsed data
                parsed_income_statement_df = pd.DataFrame(columns=['date','revenue','operating_profit',
                    'profit_for_period','eps'])
                # check if the statement of income has been put into the same page
                same_page_statement_of_income = False
                first_page_df = all_tables[0].df
                for column in first_page_df.columns:
                    if first_page_df[column].str.upper().str.replace(" ","").str.contains('STATEMENTOFINCOME').any():
                        same_page_statement_of_income = True
                        # if the statement of income is in that same page, split the dataframe
                        # get the index of the row that contains the header for the statement of income
                        split_index = first_page_df.loc[first_page_df[column].str.upper().str.replace(" ","").str.contains('STATEMENTOFINCOME')].index[0]
                        # split the df
                        raw_income_statement_df = first_page_df.iloc[split_index:,:].copy(deep=True).reset_index()
                        # drop the old index column
                        raw_income_statement_df = raw_income_statement_df.drop(raw_income_statement_df.columns[[0]], axis=1)
                if not same_page_statement_of_income:
                    # if the statement of income is not on the same page
                    raw_income_statement_df = all_tables[1].df
                if not raw_income_statement_df.empty:
                    # get the last 3 columns since the dates match the first table
                    # and exclude the first 4 rows as they are header rows
                    trimmed_income_statement_df = raw_income_statement_df.iloc[4:,-3:]
                    # rename the columns
                    trimmed_income_statement_df.columns = ['most_recent_three_months', 'last_year_corresponding_three_months', 'last_year_ended']
                    # get the indices of the rows that contain our important data
                    important_data_indices = {'date_index':None,'revenue_index':None,
                        'operating_profit_index':None,'profit_for_period_index':None,
                        'eps_index':None}
                    for index,row in raw_income_statement_df.iterrows():
                        if 'unauditedaudited' in re.sub(r'\d','',series_to_clean_string(row)) and not important_data_indices['date_index']:
                            important_data_indices['date_index'] = index+2
                        elif 'revenue' in series_to_clean_string(row) and not important_data_indices['revenue_index']:
                            important_data_indices['revenue_index'] = index
                        elif 'operatingprofit' in series_to_clean_string(row) and not important_data_indices['operating_profit_index']:
                            important_data_indices['operating_profit_index'] = index
                        elif 'profitfortheperiod' in series_to_clean_string(row) and not important_data_indices['profit_for_period_index']:
                            important_data_indices['profit_for_period_index'] = index
                        elif 'basic' in series_to_clean_string(row) and not important_data_indices['eps_index']:
                            if raw_income_statement_df.iloc[index,-1]:
                                important_data_indices['eps_index'] = index
                            elif raw_income_statement_df.iloc[index-1,-1]:
                                important_data_indices['eps_index'] = index-1
                            else:
                                raise RuntimeError('Could not locate EPS')
                    # ensure that all indices were found
                    for key in important_data_indices:
                        if not important_data_indices[key]:
                            raise KeyError(f'Important index missing in this report! {key}')
                    # now actually store the data
                    for column in trimmed_income_statement_df.columns:
                        report_data = dict()
                        # check if the date strings are jumbled up
                        try:
                            report_data['date'] = parse_date(trimmed_income_statement_df[column][important_data_indices['date_index']])
                        except ValueError:
                            logging.warning('Issue with parsing date. Trying to split dates into separate columns...')
                            # they are jumbled, so split them up 
                            trimmed_income_statement_df.loc[important_data_indices['date_index']] = \
                                re.findall(r'.*?\d{4}', trimmed_income_statement_df[column][important_data_indices['date_index']])
                            # now store only the one for this column
                            report_data['date'] = parse_date(trimmed_income_statement_df[column][important_data_indices['date_index']])
                        if 'three_months' in column:
                            report_data['period'] = 'Three Months'
                        else:
                            report_data['period'] = 'One Year'
                        report_data['revenue'] = parse_to_int(raw_string=trimmed_income_statement_df[column][important_data_indices['revenue_index']])*1000
                        report_data['operating_profit'] = parse_to_int(raw_string=trimmed_income_statement_df[column][important_data_indices['operating_profit_index']])*1000
                        report_data['profit_for_period'] = parse_to_int(raw_string=trimmed_income_statement_df[column][important_data_indices['profit_for_period_index']])*1000
                        report_data['eps'] = float(trimmed_income_statement_df[column][important_data_indices['eps_index']].replace(' ','').replace('$',''))
                        parsed_income_statement_df = parsed_income_statement_df.append(report_data,ignore_index=True)
                else:
                    raise RuntimeError(f'We could not parse the statement of income from this report: {report}')
                # check our assets column to look for significant variations (misreads in OCR)
                min_assets = parsed_balance_sheet_df['total_assets'].min()
                for assets in parsed_balance_sheet_df['total_assets']:
                    if assets>5*min_assets:
                        logging.warning(f"We found an anomolous asset in {report}. The original value was {assets}. We are dividing it by 10.")
                        parsed_balance_sheet_df['total_assets'] = parsed_balance_sheet_df['total_assets'].replace([assets],assets/10)
                # now merge the dataframes to get all of our data
                all_parsed_data_df = pd.merge(parsed_balance_sheet_df,parsed_income_statement_df, on='date',how='outer')
                # and append to our larger collection dataframe
                parsed_data_df = parsed_data_df.append(all_parsed_data_df,ignore_index=True)
                logging.info(f"Successfully added {symbol} data from {report}")
            except KeyError as exc:
                logging.error(f"We could not locate a dictionary key ({exc}). Possible that this is a weird report that we can't parse? {report}")
    logging.info("Finished processing all reports")
    # now write the data into the db
    execute_completed_successfully = False
    execute_failed_times = 0
    db_connect = None
    result = None
    logging.info("Now trying to insert data into database.")
    while not execute_completed_successfully and execute_failed_times < 5:
        try:
            db_connect = DatabaseConnect()
            raw_fundamental_data_scraped = Table(
                'raw_fundamental_data_scraped', MetaData(), autoload=True, autoload_with=db_connect.dbengine)
            raw_fundamental_data_scraped_insert_stmt = insert(
                raw_fundamental_data_scraped).values(parsed_data_df.to_dict('records'))
            raw_fundamental_data_scraped_upsert_stmt = raw_fundamental_data_scraped_insert_stmt.on_duplicate_key_update(
                {x.name: x for x in raw_fundamental_data_scraped_insert_stmt.inserted})
            result = db_connect.dbcon.execute(
                raw_fundamental_data_scraped_upsert_stmt)
            execute_completed_successfully = True
        except sqlalchemy.exc.OperationalError as operr:
            logging.warning(str(operr))
            time.sleep(1)
            execute_failed_times += 1
        if result:
            logging.info(
                "Successfully scraped and wrote to db technical summary data.")
            logging.info(
                "Number of rows affected in the technical analysis summary table was "+str(result.rowcount))
    return 0


def process_annual_reports():
    """
    Read in the pdfs in the 'processed quarterly reports' directory
    and write them to the db
    """
    pd.set_option("display.max_colwidth", 10000)
    logging.getLogger('pdfminer').setLevel(logging.ERROR)
    logging.getLogger('camelot').setLevel(logging.INFO)
    current_dir = Path(os.path.realpath(__file__)).parent
    processed_reports = glob.glob(os.path.join(current_dir,'processed_annual_reports')+'/*.pdf')
    # set up a new dataframe to store our parsed data
    parsed_data_df = pd.DataFrame(columns=['symbol','date','total_assets','total_liabilities',
        'revenue','operating_profit','profit_for_period','eps'])
    # now go through these new text-based pdfs and parse the data
    for report in processed_reports:
        # pull the data from all pages of the pdf
        logging.info(f"Now trying to parse data from {report}")
        all_tables = camelot.read_pdf(report,flavor='stream', edge_tol=0, pages='all')
        # since each company lists their reports in a slightly different format, we have to process these
        # on a case-by-case basis
        if "Agostini" in report:
            # set up our symbol
            symbol = 'AGL'
            logging.info(f"Now trying to parse report data from {symbol}.")
            try:
                # set up empty dataframes for the tables that we need
                raw_financial_position_df = pd.DataFrame()
                raw_income_df = pd.DataFrame()
                raw_comprehensive_income_df = pd.DataFrame()
                raw_equity_changes_df = pd.DataFrame()
                raw_cash_flows_df = pd.DataFrame()
                # set up indices to slice the read dataframes to the various statements
                statement_indices = {
                    'raw_financial_position_df_start':None,'raw_financial_position_df_end':None,
                    'raw_income_df_start':None,'raw_income_df_end':None,
                    'raw_comprehensive_income_df_start':None,'raw_comprehensive_income_df_end':None,
                    'raw_equity_changes_df_start':None,'raw_equity_changes_df_end':None,
                    'raw_cash_flows_df_start':None,'raw_cash_flows_df_end':None
                    }
                # find the columns and row indices that contain our important statements
                for table in all_tables:
                    table_df = table.df
                    for column in table_df.columns:
                        # go through each column and row in each table read, and slice the tables to create our raw dataframes
                        for index,element in enumerate(table_df[column]):
                            # ignore all empty strings
                            if clean_and_lower_string(element):
                                # first find the indices
                                if not statement_indices['raw_financial_position_df_start'] and \
                                clean_and_lower_string(element) == 'summaryconsolidatedstatementoffinancialposition':
                                    logging.info("Statement of financial position found.")
                                    # get the first index and the last index for this table
                                    statement_indices['raw_financial_position_df_start'] = index
                                if not statement_indices['raw_financial_position_df_end'] and \
                                clean_and_lower_string(element) == 'summaryconsolidatedstatementofincome':
                                    statement_indices['raw_financial_position_df_end'] = index-1
                                    statement_indices['raw_income_df_start'] = index
                                if not statement_indices['raw_income_df_end'] and \
                                clean_and_lower_string(element) == "summaryconsolidatedstatementofcomprehensiveincome":
                                    statement_indices['raw_income_df_end'] = index -1
                                    statement_indices['raw_comprehensive_income_df_start'] = index
                                if not statement_indices['raw_comprehensive_income_df_end'] and \
                                (clean_and_lower_string(element) == "reportoftheindependentauditoronthesummaryconsolidatedfinancialstatementstotheshareholdersofagostini'slimited" \
                                or clean_and_lower_string(element) == "summaryconsolidatedstatementofchangesinequity" or \
                                clean_and_lower_string(element) == "opinionsummaryconsolidatedfinancialstatements"):
                                    statement_indices['raw_comprehensive_income_df_end'] = index -1
                                if not statement_indices['raw_equity_changes_df_start'] and \
                                clean_and_lower_string(element) == "summaryconsolidatedstatementofchangesinequity":
                                    statement_indices['raw_equity_changes_df_start'] = index
                                # if we haven't found the end of the equity changes statement and
                                # we find the statement of cash flows, or we've reached the end of the current column and
                                # we already found the start of the equity changes statement in this column
                                # then end it at the end of this column (assume that the equity statement ends here)
                                if not statement_indices['raw_equity_changes_df_end'] and \
                                (clean_and_lower_string(element) == "summaryconsolidatedstatementofcashflows" or \
                                ((index == len(table_df[column])-1) and statement_indices['raw_equity_changes_df_start'])):
                                    statement_indices['raw_equity_changes_df_end'] = index -1
                                if not statement_indices['raw_cash_flows_df_start'] and \
                                clean_and_lower_string(element) == "summaryconsolidatedstatementofcashflows":
                                    statement_indices['raw_cash_flows_df_start'] = index
                                if not statement_indices['raw_cash_flows_df_end'] and \
                                clean_and_lower_string(element) == "notes":
                                    statement_indices['raw_cash_flows_df_end'] = index-1
                                # then if we have found our required indices, set up our dataframe one time
                                # note that column 0 has all data aligned, but other columns do not, so we need to modify the column numbers
                                if raw_financial_position_df.empty and \
                                statement_indices['raw_financial_position_df_start'] and statement_indices['raw_financial_position_df_end']:
                                    if column == 0:
                                        raw_financial_position_df = table_df.iloc[statement_indices['raw_financial_position_df_start']: \
                                        statement_indices['raw_financial_position_df_end']].copy(deep=True)[column].to_frame()
                                    elif column == 2:
                                        raw_financial_position_df = table_df.iloc[statement_indices['raw_financial_position_df_start']: \
                                        statement_indices['raw_financial_position_df_end']].copy(deep=True)[column+1].to_frame()
                                    else:
                                        raise RuntimeError('Statement of financial position was found in a weird position.')
                                if raw_income_df.empty and \
                                statement_indices['raw_income_df_start'] and statement_indices['raw_income_df_end']:
                                    if column == 0:
                                        raw_income_df = table_df.iloc[statement_indices['raw_income_df_start']: \
                                        statement_indices['raw_income_df_end']].copy(deep=True)[column].to_frame()
                                    elif column == 2:
                                        raw_income_df = table_df.iloc[statement_indices['raw_income_df_start']: \
                                        statement_indices['raw_income_df_end']].copy(deep=True)[column+1].to_frame()
                                    else:
                                        raise RuntimeError('Statement of income was found in a weird position.')
                                if raw_comprehensive_income_df.empty and \
                                statement_indices['raw_comprehensive_income_df_start'] and statement_indices['raw_comprehensive_income_df_end']:
                                    if column == 0:
                                        raw_comprehensive_income_df = table_df.iloc[statement_indices['raw_comprehensive_income_df_start']: \
                                        statement_indices['raw_comprehensive_income_df_end']].copy(deep=True)[column].to_frame()
                                    elif column == 2:
                                        raw_comprehensive_income_df = table_df.iloc[statement_indices['raw_comprehensive_income_df_start']: \
                                        statement_indices['raw_comprehensive_income_df_end']].copy(deep=True)[column+1].to_frame()
                                    else:
                                        raise RuntimeError('Statement of comprehensive income was found in a weird position.')
                                if raw_equity_changes_df.empty and \
                                statement_indices['raw_equity_changes_df_start'] and statement_indices['raw_equity_changes_df_end']:
                                    if column == 0:
                                        raw_equity_changes_df = table_df.iloc[statement_indices['raw_equity_changes_df_start']: \
                                        statement_indices['raw_equity_changes_df_end']].copy(deep=True)[column].to_frame()
                                    elif column == 2:
                                        raw_equity_changes_df = table_df.iloc[statement_indices['raw_equity_changes_df_start']: \
                                        statement_indices['raw_equity_changes_df_end']].copy(deep=True)[column+1].to_frame()
                                    else:
                                        raise RuntimeError('Statement of equity changes was found in a weird position.')
                                if raw_cash_flows_df.empty and \
                                statement_indices['raw_cash_flows_df_start'] and statement_indices['raw_cash_flows_df_end']:
                                    if column == 0:
                                        raw_cash_flows_df = table_df.iloc[statement_indices['raw_cash_flows_df_start']: \
                                        statement_indices['raw_cash_flows_df_end']].copy(deep=True)[column].to_frame()
                                    elif column == 2:
                                        raw_cash_flows_df = table_df.iloc[statement_indices['raw_cash_flows_df_start']: \
                                        statement_indices['raw_cash_flows_df_end']].copy(deep=True)[column+1].to_frame()
                                    else:
                                        raise RuntimeError('Statement of cash flows was found in a weird position.')
                # raise an error if we were not able to locate any of the dataframes
                if raw_financial_position_df.empty:
                    raise RuntimeError('We were not able to locate the Statement of Financial Position.')
                # set up the df to store the parsed data
                parsed_financial_position_df = pd.DataFrame(columns=['symbol','date','total_assets','total_liabilities'])
                # split the sub-columns 
                # raw_financial_position_df = raw_financial_position_df[0].str.split('\n',expand=True)
                # now get the indices of the rows that contain our important data
                important_data_indices = {'total_asset_index':None,'non_current_liabilities_index':None,
                    'current_liabilities_index':None,'date_index':None}
                for index,row in raw_financial_position_df.iterrows():
                    if 'yearendedyearended' in re.sub(r'\d','',series_to_clean_string(row)) and not important_data_indices['date_index']:
                        important_data_indices['date_index'] = index+2
                    elif 'otalassets' in series_to_clean_string(row) and not important_data_indices['total_asset_index']:
                        important_data_indices['total_asset_index'] = index
                    elif 'non-currentliabilities' in row.to_string().replace(' ','').lower() and not important_data_indices['non_current_liabilities_index']:
                        important_data_indices['non_current_liabilities_index'] = index
                    elif 'currentliabilities' in row.to_string().replace(' ','').lower() and not important_data_indices['current_liabilities_index']:
                        important_data_indices['current_liabilities_index'] = index
                # ensure that all indices were found
                for key in important_data_indices:
                    if not important_data_indices[key]:
                        raise KeyError()
                # now parse our data
                # first get our dates
                parsed_dates = []
                for raw_date in raw_financial_position_df[0][important_data_indices['date_index']].split('\n'):
                    try:
                        if not raw_date or raw_date.isspace():
                            raise ValueError('String is empty. Was supposed to be a date.')
                        parsed_dates.append(parse_date(raw_date))
                    except ValueError:
                        logging.error(f'Could not parse date: {raw_date}')
                parsed_total_assets = []
                for raw_total_assets in raw_financial_position_df[0][important_data_indices['total_asset_index']].split('\n'):
                    try:
                        if not raw_total_assets or raw_total_assets.isspace():
                            raise ValueError('String is empty. Was supposed to be total assets.')
                        numeric_noncurrent_liabilities = parse_to_int(raw_string=raw_total_assets)
                        parsed_total_assets.append(numeric_noncurrent_liabilities*1000)
                    except ValueError:
                        logging.error(f'Could not parse total assets value: {raw_total_assets}')
                parsed_current_liabilities = []
                for raw_noncurrent_liabilities in raw_financial_position_df[0][important_data_indices['current_liabilities_index']].split('\n'):
                    try:
                        if not raw_noncurrent_liabilities or raw_noncurrent_liabilities.isspace():
                            raise ValueError('String is empty. Was supposed to be current liabilities.')
                        numeric_noncurrent_liabilities = parse_to_int(raw_string=raw_noncurrent_liabilities)
                        parsed_current_liabilities.append(numeric_noncurrent_liabilities*1000)
                    except ValueError:
                        logging.error(f'Could not parse current liabilities value: {raw_noncurrent_liabilities}')
                parsed_noncurrent_liabilities = []
                for raw_noncurrent_liabilities in raw_financial_position_df[0][important_data_indices['non_current_liabilities_index']].split('\n'):
                    try:
                        if not raw_noncurrent_liabilities or raw_noncurrent_liabilities.isspace():
                            raise ValueError('String is empty. Was supposed to be noncurrent liabilities.')
                        numeric_noncurrent_liabilities = parse_to_int(raw_string=raw_noncurrent_liabilities)
                        parsed_noncurrent_liabilities.append(numeric_noncurrent_liabilities*1000)
                    except ValueError:
                        logging.error(f'Could not parse noncurrent liabilities value: {raw_noncurrent_liabilities}')
                # compute the total liabilities
                if not len(parsed_current_liabilities) == len(parsed_noncurrent_liabilities):
                    raise RuntimeError('The length of the current liabilities doesnt equal the length of noncurrent liabilities')
                parsed_total_liabilities = []
                for index,value in enumerate(parsed_current_liabilities):
                    parsed_total_liabilities.append(parsed_current_liabilities[index]
                        +parsed_noncurrent_liabilities[index])
                if not len(parsed_total_liabilities) == len(parsed_total_assets) == len(parsed_dates):
                    raise RuntimeError('Length of total assets doesnt equal length of total liabilities or length of dates')
                # now set up our list of dictionaries
                report_data = []
                for index,value in enumerate(parsed_dates):
                    report_data.append({'symbol':symbol,'date':parsed_dates[index],
                        'total_assets':parsed_total_assets[index],'total_liabilities':parsed_total_liabilities[index]})
                # and add this data to our dataframe
                parsed_financial_position_df = parsed_financial_position_df.append(report_data,ignore_index=True)
                if parsed_financial_position_df.empty:
                    raise RuntimeError(f'Could not parse statement of financial position data from {report}')
                logging.info("Finished parsing data from financial position statement.")
                # now get data from the statement of income
                raw_income_statement_df = pd.DataFrame()
                # find the column that contains our statement of income
                for table in all_tables:
                    table_df = table.df
                    for column in table_df.columns:
                        if table_df[column].str.lower().str.replace(" ","").str.contains('statementofincome').any():
                            logging.info("Statement of Income found.")
                            raw_income_statement_df = table_df.copy(deep=True)[column].to_frame()
                # now try to pull the data
                important_data_indices = {'date_index':None,'revenue_index':None,
                    'operating_profit_index':None,'profit_for_period_index':None,
                    'eps_index':None}
                for index,row in raw_income_statement_df.iterrows():
                    if 'yearendedyearended' in re.sub(r'\d','',series_to_clean_string(row)) and not important_data_indices['date_index']:
                        important_data_indices['date_index'] = index+2
                    elif 'revenue' in series_to_clean_string(row) and not important_data_indices['revenue_index']:
                        important_data_indices['revenue_index'] = index
                    elif 'operatingprofit' in series_to_clean_string(row) and not important_data_indices['operating_profit_index']:
                        important_data_indices['operating_profit_index'] = index
                    elif 'profitfortheperiod' in series_to_clean_string(row) and not important_data_indices['profit_for_period_index']:
                        important_data_indices['profit_for_period_index'] = index
                    elif 'earningspershare' in series_to_clean_string(row) and not important_data_indices['eps_index']:
                        if raw_income_statement_df.iloc[index+1,-1]:
                            important_data_indices['eps_index'] = index+1
                        elif raw_income_statement_df.iloc[index+2,-1]:
                            important_data_indices['eps_index'] = index+2
                        else:
                            raise RuntimeError('Could not locate EPS')
                # ensure that all indices were found
                for key in important_data_indices:
                    if not important_data_indices[key]:
                        raise KeyError(f'Important index missing in this report! {key}')
                # now actually store the data
                for column in trimmed_income_statement_df.columns:
                    report_data = dict()
                    # check if the date strings are jumbled up
                    try:
                        report_data['date'] = parse_date(trimmed_income_statement_df[column][important_data_indices['date_index']])
                    except ValueError:
                        logging.warning('Issue with parsing date. Trying to split dates into separate columns...')
                        # they are jumbled, so split them up 
                        trimmed_income_statement_df.loc[important_data_indices['date_index']] = \
                            re.findall(r'.*?\d{4}', trimmed_income_statement_df[column][important_data_indices['date_index']])
                        # now store only the one for this column
                        report_data['date'] = parse_date(trimmed_income_statement_df[column][important_data_indices['date_index']])
                    if 'three_months' in column:
                        report_data['period'] = 'Three Months'
                    else:
                        report_data['period'] = 'One Year'
                    report_data['revenue'] = parse_to_int(raw_string=trimmed_income_statement_df[column][important_data_indices['revenue_index']])*1000
                    report_data['operating_profit'] = parse_to_int(raw_string=trimmed_income_statement_df[column][important_data_indices['operating_profit_index']])*1000
                    report_data['profit_for_period'] = parse_to_int(raw_string=trimmed_income_statement_df[column][important_data_indices['profit_for_period_index']])*1000
                    report_data['eps'] = float(trimmed_income_statement_df[column][important_data_indices['eps_index']].replace(' ','').replace('$',''))
                    parsed_income_statement_df = parsed_income_statement_df.append(report_data,ignore_index=True)
                else:
                    raise RuntimeError(f'We could not parse the statement of income from this report: {report}')
                # check our assets column to look for significant variations (misreads in OCR)
                min_assets = parsed_balance_sheet_df['total_assets'].min()
                for assets in parsed_balance_sheet_df['total_assets']:
                    if assets>5*min_assets:
                        logging.warning(f"We found an anomolous asset in {report}. The original value was {assets}. We are dividing it by 10.")
                        parsed_balance_sheet_df['total_assets'] = parsed_balance_sheet_df['total_assets'].replace([assets],assets/10)
                # now merge the dataframes to get all of our data
                all_parsed_data_df = pd.merge(parsed_balance_sheet_df,parsed_income_statement_df, on='date',how='outer')
                # and append to our larger collection dataframe
                parsed_data_df = parsed_data_df.append(all_parsed_data_df,ignore_index=True)
                logging.info(f"Successfully added {symbol} data from {report}")
            except KeyError as exc:
                logging.error(f"We could not locate a dictionary key ({exc}). Possible that this is a weird report that we can't parse? {report}")
    logging.info("Finished processing all reports")
    # now write the data into the db
    execute_completed_successfully = False
    execute_failed_times = 0
    db_connect = None
    result = None
    logging.info("Now trying to insert data into database.")
    while not execute_completed_successfully and execute_failed_times < 5:
        try:
            db_connect = DatabaseConnect()
            raw_fundamental_data_scraped = Table(
                'raw_fundamental_data_scraped', MetaData(), autoload=True, autoload_with=db_connect.dbengine)
            raw_fundamental_data_scraped_insert_stmt = insert(
                raw_fundamental_data_scraped).values(parsed_data_df.to_dict('records'))
            raw_fundamental_data_scraped_upsert_stmt = raw_fundamental_data_scraped_insert_stmt.on_duplicate_key_update(
                {x.name: x for x in raw_fundamental_data_scraped_insert_stmt.inserted})
            result = db_connect.dbcon.execute(
                raw_fundamental_data_scraped_upsert_stmt)
            execute_completed_successfully = True
        except sqlalchemy.exc.OperationalError as operr:
            logging.warning(str(operr))
            time.sleep(1)
            execute_failed_times += 1
        if result:
            logging.info(
                "Successfully scraped and wrote to db technical summary data.")
            logging.info(
                "Number of rows affected in the technical analysis summary table was "+str(result.rowcount))
    return 0

def main():
    """
    The main steps in coordinating the downloading and processing of fundamental data from the TTSE
    """
    try:
        # Set up logging for this module
        q_listener, q = custom_logging.setup_logging(
            logdirparent=str(os.path.dirname(os.path.realpath(__file__))),
            modulename=os.path.basename(__file__),
            stdoutlogginglevel=logging.DEBUG,
            smtploggingenabled=True,
            smtplogginglevel=logging.ERROR,
            smtpmailhost='localhost',
            smtpfromaddr='server1@trinistats.com',
            smtptoaddr=['latchmepersad@gmail.com'],
            smtpsubj='Automated report from Python script: '+os.path.basename(__file__))
        # Set up a pidfile to ensure that only one instance of this script runs at a time
        with PidFile(piddir=tempfile.gettempdir()):
            pass
            logging.info(os.path.basename(__file__)+" executed successfully.")
            sys.exit(0)
    except Exception:
        logging.exception("Error in script "+os.path.basename(__file__))
        sys.exit(1)
 
# If this script is being run from the command-line, then run the main() function
if __name__ == "__main__":
	main()