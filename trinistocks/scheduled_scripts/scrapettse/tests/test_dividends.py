from scheduled_scripts.scrapettse.dividends import DividendScraper


def test_scrape_dividend_data():
    dividend_scraper: DividendScraper = DividendScraper()
    result = dividend_scraper.scrape_dividend_data()
    assert result == 0
