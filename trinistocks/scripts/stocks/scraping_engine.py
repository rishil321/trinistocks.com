import logging
from typing_extensions import Self

from selenium import webdriver
from selenium.webdriver.chrome.options import Options

LOGGER = logging.getLogger(__name__)


class ScrapingEngine:
    def __init__(self: Self):
        self.driver = webdriver.Chrome(executable_path="/usr/bin/chromedriver", options=_set_chrome_options())

    def __del__(self):
        self.driver.close()

    def get_url_and_return_html(self: Self, url: str) -> str:
        self.driver.get(url)
        html: str = self.driver.page_source
        LOGGER.debug(f"Successfully loaded webpage: {url}")
        return html


def _set_chrome_options() -> Options:
    """Sets chrome options for Selenium.

    Chrome options for headless browser is enabled.

    """
    option = Options()
    option.add_argument("--disable-gpu")
    option.add_argument("--disable-extensions")
    option.add_argument("--disable-infobars")
    option.add_argument('--disable-blink-features=AutomationControlled')  ## to avoid getting detected
    option.add_argument("--disable-notifications")
    option.add_argument('--headless')
    option.add_argument('--no-sandbox')
    option.add_argument('--disable-dev-shm-usage')
    return option
