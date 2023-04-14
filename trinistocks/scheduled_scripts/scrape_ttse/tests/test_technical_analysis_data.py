from scheduled_scripts.scrape_ttse.technical_analysis_data import TechnicalAnalysisDataScraper


def test_update_technical_analysis_data():
    technical_analysis_scraper: TechnicalAnalysisDataScraper = TechnicalAnalysisDataScraper()
    result = technical_analysis_scraper.update_technical_analysis_data()
    assert result == 0
