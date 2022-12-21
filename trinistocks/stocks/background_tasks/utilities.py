#!/usr/bin/env python3
# -*- coding: utf-8 -*-

###########################
# Set up the Django ORM here
###########################

# Turn off bytecode generation
import sys

sys.dont_write_bytecode = True

# Django specific settings
import os
import pathlib

sys.path.extend([str(pathlib.Path(__file__).parent.parent.parent.resolve())])
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "trinistocks.settings")
print(sys.path)
import django

django.setup()

# Import your models for use in your script
from stocks import models

####################################
# MODULE/APP START
####################################

# Imports
import logging
import requests
import json
from decimal import Decimal
from typing import Optional

# Constants
logger: logging.Logger = logging.getLogger('background_tasks')


# Class definitions
class TTDCurrencyConversionRates:
    TTD_JMD: Decimal = 0.0
    TTD_USD: Decimal = 0.0
    TTD_BBD: Decimal = 0.0


# Function definitions
def fetch_latest_currency_conversion_rates() -> Optional[TTDCurrencyConversionRates]:
    logger.debug("Now trying to fetch latest currency conversions.")
    api_response_ttd = requests.get(
        url="https://fcsapi.com/api-v2/forex/base_latest?symbol=TTD&type=forex&access_key=o9zfwlibfXciHoFO4LQU2NfTwt2vEk70DAiOH1yb2ao4tBhNmm"
    )
    if api_response_ttd.status_code == 200:
        # store the conversion rates that we need
        TTD_JMD = Decimal(
            json.loads(api_response_ttd.content.decode("utf-8"))["response"]["JMD"]
        )
        TTD_USD = Decimal(
            json.loads(api_response_ttd.content.decode("utf-8"))["response"]["USD"]
        )
        TTD_BBD = Decimal(
            json.loads(api_response_ttd.content.decode("utf-8"))["response"]["BBD"]
        )
        logger.debug("Currency conversions fetched correctly.")
        conversion_rates: TTDCurrencyConversionRates = TTDCurrencyConversionRates()
        conversion_rates.TTD_JMD = TTD_JMD
        conversion_rates.TTD_USD = TTD_USD
        conversion_rates.TTD_BBD = TTD_BBD
        return conversion_rates
    else:
        logger.exception(
            f"Cannot load URL for currency conversions.{api_response_ttd.status_code},{api_response_ttd.reason},{api_response_ttd.url}"
        )
        return None
