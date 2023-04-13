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
from . import updater

# Imports from the cheese factory


# Put your constants here. These should be named in CAPS.

# Put your global variables here.

# Put your class definitions here. These should use the CapWords convention.

# Put your function definitions here. These should be lowercase, separated by underscores.


def test_calculate_fundamental_analysis_ratios():
    assert (
            updater.calculate_fundamental_analysis_ratios(
            TTD_JMD=21.33, TTD_USD=0.15, TTD_BBD=0.29
        )
            == 0
    )


def test_update_dividend_yields():
    assert (
            updater.update_dividend_yields(TTD_JMD=21.33, TTD_USD=0.15, TTD_BBD=0.29) == 0
    )


def test_update_portfolio_summary_book_costs():
    assert updater.update_portfolio_summary_book_costs() == 0


def test_update_simulator_portfolio_summary_book_costs():
    assert updater.update_simulator_portfolio_summary_book_costs() == 0


def test_update_portfolio_summary_market_values():
    assert updater.update_portfolio_summary_market_values() == 0


def test_update_simulator_portfolio_market_values():
    assert updater.update_simulator_portfolio_summary_market_values() == 0


def test_update_portfolio_sectors():
    assert updater.update_portfolio_sectors_values() == 0


def test_update_simulator_portfolio_sectors_values():
    assert updater.update_simulator_portfolio_sectors_values() == 0


def test_update_simulator_games():
    assert updater.update_simulator_games() == 0


def test_main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--daily_update",
        help="Update the portfolio market data with the latest values",
        action="store_true",
    )
    args = parser.parse_args([])
    assert updater.main(args) == 0


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