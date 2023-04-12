from scripts.stocks.scraping_engine import ScrapingEngine


def test_scraping_engine():
    scraping_engine: ScrapingEngine = ScrapingEngine()
    html = scraping_engine.get_url_and_return_html(url="https://www.stockex.co.tt/manage-stock/FCI/")
    assert html != ''
