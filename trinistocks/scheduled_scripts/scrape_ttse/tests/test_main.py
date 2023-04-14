#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
The testing module for the scraper.py file
:param -f OR --first_parameter: The description of your first input parameter
:returns: Whatever your script returns when called
:raises Exception if any issues are encountered
"""

import argparse
import logging
import os
# Put all your imports here, one per line.
# However multiple imports from the same lib are allowed on a line.
# Imports from Python standard libraries
import sys
from datetime import datetime

from dateutil.relativedelta import relativedelta

# Imports from the local filesystem
from scheduled_scripts.scrape_ttse.main import main,set_up_arguments


# Imports from the cheese factory


# Put your constants here. These should be named in CAPS.

# Put your global variables here.

# Put your class definitions here. These should use the CapWords convention.

# Put your function definitions here. These should be lowercase, separated by underscores.
def test_intradaily_updates():
    assert main(['-d','2','--daily_summary_data']) == 0


def test_update_daily_trades():
    assert scraper.update_daily_trades() == 0


def test_full_updates():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-f",
        "--full_history",
        help="Record all data from 2010 to now",
        action="store_true",
    )
    parser.add_argument(
        "-d",
        "--daily_update",
        help="Only update data for the daily summary for today",
        action="store_true",
    )
    args = parser.parse_args(["-f"])
    assert main(args) == 0


def test_update_technical_analysis_data():
    assert scraper.update_technical_analysis_data() == 0


def test_scrape_dividend_data():
    assert scraper.scrape_dividend_data() == 0


def test_scrape_newsroom_data():
    start_date = (datetime.now() + relativedelta(days=-7)).strftime("%Y-%m-%d")
    end_date = (datetime.now()).strftime("%Y-%m-%d")
    assert scraper.scrape_newsroom_data(start_date, end_date) == 0
