#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
This module holds the symbols of stocks that are cross-listed on the TTSE
"""

# Most stocks on the TTSE have prices and dividends listed in TTD. These are the exceptions.
# The following stocks have both prices and dividends listed in USD on the exchange
USD_STOCK_SYMBOLS = ['MPCCEL']
# These have prices in TTD, but dividends in USD
USD_DIVIDEND_SYMBOLS = ['SFC','FCI','MPCCEL']
# These have prices listed in TTD, but dividends in JMD
JMD_DIVIDEND_SYMBOLS = ['GKC','JMMBGL','NCBFG']
# These have prices in TTD, but dividends in BBD
BBD_DIVIDEND_SYMBOLS = ['CPFV']