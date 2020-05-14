import dailyscrape


def test_scrape_listed_equity_data():
    result = dailyscrape.scrape_listed_equity_data()
    assert type(result) is list and len(result) > 0


def test_write_listed_equity_data_to_db():
    all_listed_equity_data = []
    assert dailyscrape.write_listed_equity_data_to_db(
        all_listed_equity_data) == 0


def test_scrape_dividend_data():
    result = dailyscrape.scrape_dividend_data()
    assert type(result) is list and len(result) > 0


def test_write_dividend_data_to_db():
    all_dividend_data = [{'symbol': 'test', 'equityid': 170,
                          'recorddate': '2020-02-22', 'dividendamount': 0.50, 'currency': 'USD'}]
    assert dailyscrape.write_dividend_data_to_db(all_dividend_data) == 0


def test_scrape_historical_data():
    result = dailyscrape.scrape_historical_data()
    assert type(result) is list and len(result) > 0


def test_write_historical_data_to_db():
    all_historical_stock_data = [{'date': '2010-01-01', 'equityid': '223',
                                  'closingquote': '100.00', 'changedollars': '1.00',
                                  'volumetraded': '100', 'currency': 'TTD', 'symbol': 'TEST'}]
    assert dailyscrape.writehistoricaldatatodb(all_historical_stock_data) == 0


def test_update_dividend_yield():
    assert dailyscrape.update_dividend_yield() == 0


def test_update_equity_summary_data():
    assert dailyscrape.update_equity_summary_data() == 0
