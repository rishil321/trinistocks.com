from datetime import datetime

from dateutil.relativedelta import relativedelta

from scheduled_scripts.scrapettse.newsroom_data import NewsroomDataScraper


def test_scrape_newsroom_data():
    newsroom_data_scraper = NewsroomDataScraper()
    start_date = (datetime.now() + relativedelta(days=-1)).strftime("%Y-%m-%d")
    end_date = datetime.now().strftime("%Y-%m-%d")
    result = newsroom_data_scraper.scrape_newsroom_data(start_date, end_date)
    assert result == 0
