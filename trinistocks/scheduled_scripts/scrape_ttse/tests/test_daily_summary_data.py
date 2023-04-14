from datetime import datetime

from dateutil.relativedelta import relativedelta

from scheduled_scripts.scrape_ttse.daily_summary_data import DailySummaryDataScraper


def test_update_daily_trade_data_for_today():
    daily_summary_data_scraper = DailySummaryDataScraper()
    result = daily_summary_data_scraper.update_daily_trade_data_for_today()
    assert result == 0


def test_update_update_equity_summary_data():
    daily_summary_data_scraper = DailySummaryDataScraper()
    start_date = (datetime.now() + relativedelta(days=-7)).strftime("%Y-%m-%d")
    dates_to_fetch_sublists = daily_summary_data_scraper.build_lists_of_missing_dates_for_each_subprocess(start_date=start_date)
    assert dates_to_fetch_sublists is not None
    assert dates_to_fetch_sublists.__class__ == list


def test_scrape_equity_summary_data_in_subprocess():
    daily_summary_data_scraper = DailySummaryDataScraper()
    dates_to_fetch_sublist = ['2023-04-08', '2023-04-09', '2023-04-10', '2023-04-11', '2023-04-12']
    result = daily_summary_data_scraper.scrape_equity_summary_data_in_subprocess(
        dates_to_fetch=dates_to_fetch_sublist)
    assert result == 0
