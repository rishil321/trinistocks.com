from stocks.background_tasks import utilities
import unittest
def test_fetch_latest_currency_conversion_rates():
    unittest.TestCase.assertIsInstance(obj=utilities.fetch_latest_currency_conversion_rates(),cls=utilities.TTDCurrencyConversionRates)