import concurrent
import logging
from datetime import datetime
from logging.config import dictConfig
from typing import List

from bs4 import BeautifulSoup
from sqlalchemy import MetaData, Table, insert
from typing_extensions import Self

from scripts.stocks.scraping_engine import ScrapingEngine
from .. import logging_configs
from ... import custom_logging
from ...database_ops import DatabaseConnect, _read_symbols_and_ids_from_db

dictConfig(logging_configs.LOGGING_CONFIG)
logger = logging.getLogger(__name__)


class NewsroomDataScraper:
    def __init__(self: Self):
        self.scraping_engine = ScrapingEngine()
        pass

    def __del__(self: Self):
        pass

    def scrape_newsroom_data(self, start_date, end_date) -> int:
        """Use the requests and pandas libs to fetch the current listed equities at
        https://www.stockex.co.tt/listed-securities/?IdInstrumentType=1&IdSegment=&IdSector=
        and scrape the useful output into a list of dictionaries to write to the db
        """
        logger.debug(f"Now trying to scrape newsroom date from {start_date} to {end_date}")
        try:
            all_listed_symbols = _read_symbols_and_ids_from_db()
            all_news_data = self._setup_newsroom_data_scrapers_in_subprocesses(all_listed_symbols, end_date, start_date)
            if not all_news_data:
                # if we could not parse any news data for today
                logger.warning("No news data could be parsed for today. Possibly no news released today?")
                return 0
            # else we have some data
            return self._write_newsroom_data_to_db(all_news_data)
        except Exception as exc:
            logger.exception("Ran into an issue while trying to fetch news data.")
            custom_logging.flush_smtp_logger()

    def _write_newsroom_data_to_db(self, all_news_data):
        with DatabaseConnect() as db_connection:
            # now write the list of dicts to the database
            stock_news_table = Table(
                "stock_news_data",
                MetaData(),
                autoload=True,
                autoload_with=db_connection.dbengine,
            )
            logger.debug("Inserting scraped news data into stock_news table")
            insert_stmt = insert(stock_news_table).values(all_news_data)
            upsert_stmt = insert_stmt.on_duplicate_key_update({x.name: x for x in insert_stmt.inserted})
            result = db_connection.dbcon.execute(upsert_stmt)
            logger.debug("Database update successful. Number of rows affected was " + str(result.rowcount))
            return 0

    def _setup_newsroom_data_scrapers_in_subprocesses(self, all_listed_symbols, end_date, start_date):
        # set up a variable to store all data to be written to the db table
        all_news_data = []
        # set up some threads to speed up the process
        num_threads = 2
        # split the complete list of symbols into sublists for the threads
        per_thread_symbols = [all_listed_symbols[i::num_threads] for i in range(num_threads)]
        # submit the work to the thread workers
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=num_threads, thread_name_prefix="fetch_news_data"
        ) as executor:
            future_to_news_fetch = {
                executor.submit(self._scrape_newsroom_data_in_subprocess, symbols, start_date, end_date): symbols
                for symbols in per_thread_symbols
            }
            logger.debug(f"Newsroom pages now being fetched by worker threads.")
            for future in concurrent.futures.as_completed(future_to_news_fetch):
                per_thread_symbols = future_to_news_fetch[future]
                try:
                    news_data = future.result()
                except Exception:
                    logger.exception(f"Ran into an issue with this set of symbols: {per_thread_symbols}")
                else:
                    logger.debug("Successfully got data for symbols. Adding to master list.")
                    all_news_data += news_data
        return all_news_data

    def _scrape_newsroom_data_in_subprocess(self: Self, symbols_to_fetch_data_for, start_date, end_date) -> List:
        """In a single thread, take a subset of symbols and fetch the news data for each symbol"""
        all_newsroom_data = []
        for symbol in symbols_to_fetch_data_for:
            logger.debug(f"Now attempting to fetch news data for {symbol}")
            try:
                # loop through each page of news until we reach pages that have no news
                page_num = 1
                while True:
                    news_articles = self._scrape_news_articles_for_page(end_date, page_num, start_date, symbol)
                    if not news_articles:
                        # if we have an empty list of news articles, stop incrementing the list, since we have reached the end
                        logger.debug(f"Finished fetching news articles for {symbol['symbol']}")
                        break
                    # else we have some news articles to parse
                    self._parse_news_articles_from_page(news_articles, all_newsroom_data, symbol)
                    # increment the page num and restart the loop
                    page_num += 1
            except Exception:
                logger.warning(f"We ran into a problem while checking news for {symbol['symbol']}")
        return all_newsroom_data

    def _parse_news_articles_from_page(self, news_articles, news_data, symbol):
        # else process the list
        for article in news_articles:
            try:
                link = article.contents[1].attrs["href"]
                # load the link to get the main article page
                logger.debug("Now clicking news link. Navigating to " + link)
                news_page = self.scraping_engine.get_url_and_return_html(url=link)
                # find the elements we want on the page
                per_stock_page_soup = BeautifulSoup(news_page, "lxml")
                # try to get the category
                category_type_soup = per_stock_page_soup.select("div.elementor-text-editor.elementor-clearfix")
                possible_categories = [
                    "Annual Report",
                    "Articles",
                    "Audited Financial Statements",
                    "Quarterly Financial Statements",
                ]
                category = None
                for possible_category in category_type_soup:
                    if possible_category.string is not None and (
                        possible_category.string.strip() in possible_categories
                    ):
                        category = possible_category.string.strip()
                if not category:
                    logger.warning("Could not parse category for this URL.")
                # try to get the date
                date = None
                date_soup = per_stock_page_soup.select("h2.elementor-heading-title.elementor-size-default")
                for possible_date in date_soup:
                    if possible_date.string is not None:
                        try:
                            date = datetime.strptime(possible_date.string.strip(), "%d/%m/%Y")
                        except ValueError as exc:
                            pass
                if not date:
                    raise RuntimeError(f"We were not able to find the date for {article}")
                # try to get title
                title = None
                title_soup = per_stock_page_soup.select("h1.elementor-heading-title.elementor-size-xl")
                for possible_title in title_soup:
                    if possible_title.string is not None:
                        if len(possible_title.string.strip().split("–")) == 2:
                            title = possible_title.string.strip().split("–")[1].strip()
                        else:
                            title = possible_title.string.strip()
                if not title:
                    raise RuntimeError(f"We were not able to find the title for {article}")
                # try to get full pdf link
                link = None
                link_soup = per_stock_page_soup.select("div.elementor-text-editor.elementor-clearfix")
                for possible_link in link_soup:
                    if (
                        len(possible_link.contents) > 1
                        and "href" in possible_link.contents[1].attrs
                        and possible_link.contents[1].attrs["href"].strip()
                    ):
                        link = possible_link.contents[1].attrs["href"].strip()
                if not link:
                    raise RuntimeError("We were not able to find the link for {article}")
                # now append the data to our list
                news_data.append(
                    {
                        "symbol": symbol["symbol"],
                        "category": category,
                        "date": date,
                        "title": title,
                        "link": link,
                    }
                )
            except Exception as exc:
                logger.warning(f"Could not parse article from {article}", exc_info=exc)

    def _scrape_news_articles_for_page(self, end_date, page_num, start_date, symbol):
        # Construct the full URL using the symbol
        news_url = f"https://www.stockex.co.tt/news/?symbol={symbol['symbol_id']}&category=&date={start_date}&date_to={end_date}&search&page={page_num}#search_c"
        logger.debug("Navigating to " + news_url)
        news_page = self.scraping_engine.get_url_and_return_html(url=news_url)
        # get the dataframes from the page
        per_stock_page_soup = BeautifulSoup(news_page, "lxml")
        news_articles = per_stock_page_soup.findAll("div", class_=["news_item"])
        return news_articles
