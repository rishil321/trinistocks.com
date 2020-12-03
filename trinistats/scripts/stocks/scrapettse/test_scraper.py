#!/usr/bin/env python3
# -*- coding: utf-8 -*-
 
"""
The testing module for the scraper.py file
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
import argparse

# Imports from the cheese factory

# Imports from the local filesystem
from . import scraper

# Put your constants here. These should be named in CAPS.

# Put your global variables here. 
 
# Put your class definitions here. These should use the CapWords convention.
 
# Put your function definitions here. These should be lowercase, separated by underscores.
def test_daily_trades():
    parser = argparse.ArgumentParser()
    parser.add_argument("-f",
                        "--full_history", help="Record all data from 2010 to now", action="store_true")
    parser.add_argument("-d",
                        "--daily_update", help="Only update data for the daily summary for today", action="store_true")
    args = parser.parse_args(['-d'])
    assert scraper.main(args) == 0

def test_normal_updates():
    parser = argparse.ArgumentParser()
    parser.add_argument("-f",
                        "--full_history", help="Record all data from 2010 to now", action="store_true")
    parser.add_argument("-d",
                        "--daily_update", help="Only update data for the daily summary for today", action="store_true")
    args = parser.parse_args([])
    assert scraper.main(args) == 0

def test_full_updates():
    parser = argparse.ArgumentParser()
    parser.add_argument("-f",
                        "--full_history", help="Record all data from 2010 to now", action="store_true")
    parser.add_argument("-d",
                        "--daily_update", help="Only update data for the daily summary for today", action="store_true")
    args = parser.parse_args(['-f'])
    assert scraper.main(args) == 0

def test_update_technical_analysis_data():
    assert scraper.update_technical_analysis_data() == 0


def test_process_quarterly_reports():
    assert scraper.process_quarterly_reports() == 0

 
def main():
    """Docstring description for each function"""
    try:
        # All main code here
        pass
    except Exception:
        logging.exception("Error in script "+os.path.basename(__file__))
        sys.exit(1)
    else:
        logging.info(os.path.basename(__file__)+" executed successfully.")
        sys.exit(0)
 
# If this script is being run from the command-line, then run the main() function
if __name__ == "__main__":
    main()