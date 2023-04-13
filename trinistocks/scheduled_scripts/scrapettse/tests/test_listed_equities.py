from scheduled_scripts.scrapettse.listed_equities import ListedEquitiesScraper


def test_scrape_listed_equity_data():
    listed_equities_scraper: ListedEquitiesScraper = ListedEquitiesScraper()
    assert listed_equities_scraper.scrape_listed_equity_data() == 0

def test_update_num_equities_in_sectors():
    listed_equities_scraper: ListedEquitiesScraper = ListedEquitiesScraper()
    assert listed_equities_scraper.update_num_equities_in_sectors() == 0