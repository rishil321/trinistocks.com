#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
The testing module for the updater.py file
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

# Imports from the local filesystem
from . import fetch_financial_reports


# Imports from the cheese factory
# Put your constants here. These should be named in CAPS.

# Put your global variables here.

# Put your class definitions here. These should use the CapWords convention.

# Put your function definitions here. These should be lowercase, separated by underscores.


def test_alert_me_new_annual_reports():
    new_annual_reports: set = fetch_financial_reports.alert_me_new_annual_reports()
    assert isinstance(new_annual_reports, set)


def test_alert_me_new_audited_statements():
    new_audited_statements = fetch_financial_reports.alert_me_new_audited_statements()
    print(new_audited_statements)
    assert isinstance(new_audited_statements, set)


def test_alert_me_new_quarterly_reports():
    new_quarterly_statements = fetch_financial_reports.alert_me_new_quarterly_statements()
    print(new_quarterly_statements)
    assert isinstance(new_quarterly_statements, set)


def test_main():
    parser = argparse.ArgumentParser()
    args = parser.parse_args([])
    assert fetch_financial_reports.main(args) == 0


def main():
    """Docstring description for each function"""
    try:
        # All main code here
        pass
    except Exception:
        logging.exception("Error in script " + os.path.basename(__file__))
        sys.exit(1)
    else:
        logging.info(os.path.basename(__file__) + " executed successfully.")
        sys.exit(0)


# If this script is being run from the command-line, then run the main() function
if __name__ == "__main__":
    main()
