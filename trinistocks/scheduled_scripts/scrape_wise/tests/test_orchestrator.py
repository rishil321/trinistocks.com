from scheduled_scripts.scrape_wise import orchestrator


def test_scrape_and_parse_all_missing_reports():
    result: bool = orchestrator.scrape_and_parse_all_missing_reports()
    assert result is True


def test_scrape_and_parse_specific_report():
    result: bool = orchestrator.scrape_and_parse_specific_report("09/10/2023")
    assert result is True
