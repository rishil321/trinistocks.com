from stocks.background_tasks import update_dividends
from decimal import Decimal

def test_update_dividends():
    assert update_dividends.update_summarized_dividend_yields(TTD_JMD=Decimal(22.50),TTD_BBD=Decimal(0.297),TTD_USD=Decimal(0.147)) == 0
