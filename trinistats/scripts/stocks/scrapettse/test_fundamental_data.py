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
from . import fundamental_data

# Put your constants here. These should be named in CAPS.

# Put your global variables here. 
 
# Put your class definitions here. These should use the CapWords convention.
 
# Put your function definitions here. These should be lowercase, separated by underscores.
def test_convert_quarterly_reports_text():
    assert fundamental_data.convert_quarterly_reports_text() == 0

def test_convert_annual_reports_text():
    assert fundamental_data.convert_annual_reports_text() == 0

def test_process_quarterly_reports():
    assert fundamental_data.process_quarterly_reports() == 0

def test_process_annual_reports():
    assert fundamental_data.process_annual_reports() == 0
 
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