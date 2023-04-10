import logging
import time

from fake_useragent import UserAgent
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium_stealth import stealth
from typing_extensions import Self

LOGGER = logging.getLogger(__name__)


class ScrapingEngine:
    def __init__(self: Self):
        self.driver = webdriver.Chrome(executable_path="/usr/bin/chromedriver", options=_set_chrome_options())
        # self.driver = webdriver.Chrome(executable_path="C:\Program Files(x86)\Google\Chrome\Application", options=_set_chrome_options())
        self.driver.implicitly_wait(10)
        self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        ua = UserAgent()
        userAgent = ua.random
        self.driver.execute_cdp_cmd('Network.setUserAgentOverride', {"userAgent": userAgent})

    def __del__(self: Self):
        self.driver.close()

    def get_url_and_return_html(self: Self, url: str) -> str:
        stealth(
            self.driver,
            languages=["en-US", "en"],
            vendor="Google Inc.",
            platform="Win32",
            webgl_vendor="Intel Inc.",
            renderer="Intel Iris OpenGL Engine",
            fix_hairline=True,
        )
        self.driver.get(url)
        time.sleep(5)
        html: str = self.driver.page_source
        LOGGER.debug(f"Successfully loaded webpage: {url}")
        html = self.driver.execute_script("return document.getElementsByTagName('html')[0].innerHTML")
        for entry in self.driver.get_log('browser'):
            print(entry)
        return html


def _set_chrome_options() -> Options:
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
    options.add_argument("--proxy-server='direct://'")
    options.add_argument("--proxy-bypass-list=*")
    options.add_argument("--start-maximized")
    ua = UserAgent()
    userAgent = ua.random
    options.add_argument(f'user-agent={userAgent}')
    return options
