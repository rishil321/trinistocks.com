import logging
import random
from itertools import cycle
from logging.config import dictConfig
import requests
from fake_useragent import UserAgent
from lxml.html import fromstring
from selenium.webdriver.chrome.options import Options
from typing_extensions import Self
from seleniumwire import webdriver

from scheduled_scripts import logging_configs

dictConfig(logging_configs.LOGGING_CONFIG)
LOGGER = logging.getLogger()


class ScrapingEngine:
    def __init__(self: Self):
        self.proxies = self._get_proxies()
        if not self.proxies:
            raise RuntimeError("Could not find any valid proxies to use.")
        self.driver = webdriver.Chrome(executable_path="/usr/bin/chromedriver", options=self._set_chrome_options())
        # self.driver = webdriver.Chrome(executable_path="C:\Program Files(x86)\Google\Chrome\Application", options=_set_chrome_options())
        # self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        self.driver.implicitly_wait(10)
        self.driver.set_page_load_timeout(10)

    def __del__(self: Self):
        if hasattr(self, 'driver'):
            self.driver.close()

    def get_url_and_return_html(self: Self, url: str) -> str:
        html_scraped_successfully: bool = False
        html: str = ''
        while not html_scraped_successfully:
            try:
                LOGGER.info(f"Now trying to load webpage: {url}")
                self.driver.get(url)
                html: str = self.driver.page_source
                html_scraped_successfully = self.validate_html_scraped_successfully(html, html_scraped_successfully)
            except:
                LOGGER.info("Failed to load webpage. Retrying.")
                self.driver = webdriver.Chrome(executable_path="/usr/bin/chromedriver",
                                               options=self._set_chrome_options())
        return html

    def validate_html_scraped_successfully(self, html, html_scraped_successfully):
        # LOGGER.debug(f"Now validating html {html}")
        partial_html_strings_to_avoid = ('<title>Sucuri WebSite Firewall - Access Denied</title>',)
        complete_html_strings_to_avoid = ('', '<html><head></head><body></body></html>')
        if html not in complete_html_strings_to_avoid and not any(
                string in html for string in partial_html_strings_to_avoid):
            html_scraped_successfully = True
            LOGGER.debug("Success!")
        else:
            raise RuntimeError("Failed.")
        return html_scraped_successfully

    def _get_proxies(self: Self):
        url = 'https://free-proxy-list.net/'
        response = requests.get(url)
        parser = fromstring(response.text)
        proxies = []
        for i in parser.xpath('//tbody/tr')[:300]:
            if i.xpath('.//td[7][contains(text(),"yes")]'):
                proxy = ":".join([i.xpath('.//td[1]/text()')[0], i.xpath('.//td[2]/text()')[0]])
                if proxy not in proxies:
                    proxies.append(proxy)
        random.shuffle(proxies)
        return proxies

    def _set_chrome_options(self: Self) -> Options:
        """Sets chrome options for Selenium.

        Chrome options for headless browser is enabled.

        """
        options = Options()
        options.add_argument("--disable-gpu")
        options.add_argument("--nogpu")
        options.add_argument("--incognito")
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-infobars")
        options.add_argument("--disable-blink-features")
        options.add_argument('--disable-blink-features=AutomationControlled')  ## to avoid getting detected
        options.add_argument("--disable-notifications")
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument("--window-size=1920,1080")
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        options.add_argument("--enable-javascript")
        options.add_argument('--lang=en_US')
        # options.add_argument("--proxy-server='direct://'")
        # options.add_argument("--proxy-bypass-list=*")
        options.add_argument("--start-maximized")
        # options.add_argument("--ignore-certificate-errors")
        ua = UserAgent()
        user_agent = ua.random
        options.add_argument(f'user-agent={user_agent}')
        proxy_pool = cycle(self.proxies)
        proxy = next(proxy_pool)
        options.add_argument(f'--proxy-server={proxy}')
        return options
