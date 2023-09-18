from scheduled_scripts.scrape_wise import orchestrator


def test_scrape_and_parse_all_missing_reports():
    result: bool = orchestrator.scrape_and_parse_all_missing_reports()
    assert result is True
