import logging
import re
from logging.config import dictConfig

import pandas as pd
from bs4 import BeautifulSoup, Tag
from sqlalchemy import MetaData, Table
from sqlalchemy.dialects.mysql import insert
from typing_extensions import Self

from scheduled_scripts.scraping_engine import ScrapingEngine
from scheduled_scripts.crosslisted_symbols import USD_STOCK_SYMBOLS
from scheduled_scripts import custom_logging, logging_configs
from scheduled_scripts.database_ops import DatabaseConnect

dictConfig(logging_configs.LOGGING_CONFIG)
logger = logging.getLogger()


class ListedEquitiesScraper:
    def __init__(self: Self):
        self.scraping_engine = ScrapingEngine()
        pass

    def __del__(self: Self):
        pass

    def scrape_listed_equity_data(self: Self) -> int:
        """Use the requests and pandas libs to fetch the current listed equities at
        https://www.stockex.co.tt/listed-securities/?IdInstrumentType=1&IdSegment=&IdSector=
        and scrape the useful output into a list of dictionaries to write to the db
        """
        try:
            logger.debug("Now scraping listing data from all listed equities.")
            listed_stocks_summary_url = "https://www.stockex.co.tt/listed-securities/"
            listed_stocks_summary_page = self.scraping_engine.get_url_and_return_html(listed_stocks_summary_url)
            # get a list of tables from the URL
            dataframe_list = pd.read_html(listed_stocks_summary_page)
            # for each dataframe in the list, get the symbols
            listed_stock_symbols = self._build_list_of_all_symbols_listed(dataframe_list)
            all_listed_equity_data = self._scrape_full_data_for_each_symbol_listed(listed_stock_symbols)
            # set up a dataframe with all our data
            all_listed_equity_data_df = pd.DataFrame(all_listed_equity_data)
            # now find the symbol ids? used for the news page for each symbol
            symbol_ids, symbols = self._scrape_symbol_ids()
            # now set up a dataframe
            symbol_id_df = pd.DataFrame(list(zip(symbols, symbol_ids)), columns=["symbol", "symbol_id"])
            # merge the two dataframes
            all_listed_equity_data_df = pd.merge(all_listed_equity_data_df, symbol_id_df, on="symbol", how="left")
            # Now write the data to the database
            self._write_listed_equities_to_db(all_listed_equity_data_df)
            return 0
        except Exception as exc:
            logger.exception(f"Problem encountered while updating listed equities. Here's what we know: {str(exc)}")
            return -1
        finally:
            custom_logging.flush_smtp_logger()

    def _write_listed_equities_to_db(self, all_listed_equity_data_df):
        with DatabaseConnect() as db_obj:
            listed_equities_table = Table(
                "listed_equities",
                MetaData(),
                autoload=True,
                autoload_with=db_obj.dbengine,
            )
            logger.debug("Inserting scraped data into listed_equities table")
            listed_equities_insert_stmt = insert(listed_equities_table).values(
                all_listed_equity_data_df.to_dict("records")
            )
            listed_equities_upsert_stmt = listed_equities_insert_stmt.on_duplicate_key_update(
                {x.name: x for x in listed_equities_insert_stmt.inserted}
            )
            result = db_obj.dbcon.execute(listed_equities_upsert_stmt)
            logger.debug("Database update successful. Number of rows affected was " + str(result.rowcount))

    def _scrape_symbol_ids(self):
        logger.info("Now trying to fetch symbol ids for news")
        news_url = "https://www.stockex.co.tt/news/"
        logger.info(f"Navigating to {news_url}")
        equity_page = self.scraping_engine.get_url_and_return_html(url=news_url)
        # get all the options for the dropdown select, since these contain the ids
        news_page_soup = BeautifulSoup(equity_page, "lxml")
        all_symbol_mappings = news_page_soup.find(id="symbol")
        # now parse the soup and get the symbols and their ids
        symbols = []
        symbol_ids = []
        for mapping in all_symbol_mappings:
            if isinstance(mapping, Tag):
                symbol = mapping.contents[0].split()[0]
                symbol_id = mapping.attrs["value"]
                if symbol and symbol_id:
                    symbols.append(symbol)
                    symbol_ids.append(symbol_id)
        return symbol_ids, symbols

    def _scrape_full_data_for_each_symbol_listed(self, listed_stock_symbols):
        # Go to the main summary page for each symbol
        # This list of dicts will contain all data to be written to the db
        all_listed_equity_data = []
        for symbol in listed_stock_symbols:
            try:
                per_stock_url = f"https://www.stockex.co.tt/manage-stock/{symbol}/"
                logger.debug("Navigating to " + per_stock_url)
                equity_page = self.scraping_engine.get_url_and_return_html(url=per_stock_url)
                # set up a dict to store the data for this equity
                equity_data = dict(symbol=symbol)
                # use beautifulsoup to get the securityname, sector, status, financial year end, website
                per_stock_page_soup = BeautifulSoup(equity_page, "lxml")
                equity_data["security_name"] = (
                    per_stock_page_soup.find(text="Security:").find_parent("h2").find_next("h2").text.title()
                )
                # apply some custom formatting to our names
                if equity_data["security_name"] == "Agostini S Limited":
                    equity_data["security_name"] = "Agostini's Limited"
                elif equity_data["security_name"] == "Ansa Mcal Limited":
                    equity_data["security_name"] = "ANSA McAL Limited"
                elif equity_data["security_name"] == "Ansa Merchant Bank Limited":
                    equity_data["security_name"] = "ANSA Merchant Bank Limited"
                elif equity_data["security_name"] == "Cinemaone Limited":
                    equity_data["security_name"] = "CinemaOne Limited"
                elif equity_data["security_name"] == "Clico Investment Fund":
                    equity_data["security_name"] = "CLICO Investment Fund"
                elif equity_data["security_name"] == "Firstcaribbean International Bank Limited":
                    equity_data["security_name"] = "CIBC FirstCaribbean International Bank Limited"
                elif equity_data["security_name"] == "Gracekennedy Limited":
                    equity_data["security_name"] = "GraceKennedy Limited"
                elif equity_data["security_name"] == "Jmmb Group Limited":
                    equity_data["security_name"] = "JMMB Group Limited"
                elif equity_data["security_name"] == "Mpc Caribbean Clean Energy Limited":
                    equity_data["security_name"] = "MPC Caribbean Clean Energy Limited"
                elif equity_data["security_name"] == "Ncb Financial Group Limited":
                    equity_data["security_name"] = "NCB Financial Group Limited"
                elif equity_data["security_name"] == "Trinidad And Tobago Ngl Limited":
                    equity_data["security_name"] = "Trinidad And Tobago NGL Limited"
                equity_sector = per_stock_page_soup.find(text="Sector:").find_parent("h2").find_next("h2").text.title()
                if equity_sector != "Status:":
                    equity_data["sector"] = equity_sector
                else:
                    equity_data["sector"] = None
                if equity_data["sector"] == "Manufacturing Ii":
                    equity_data["sector"] = "Manufacturing II"
                equity_data["status"] = (
                    per_stock_page_soup.find(text="Status:").find_parent("h2").find_next("h2").text.title()
                )
                equity_data["financial_year_end"] = (
                    per_stock_page_soup.find(text="Financial Year End:").find_parent("h2").find_next("h2").text
                )
                website_url = per_stock_page_soup.find(text="Website:").find_parent("h2").find_next("h2").text
                if website_url != "Issuers":
                    equity_data["website_url"] = website_url
                else:
                    equity_data["website_url"] = None
                # store the currency that the stock is listed in
                if equity_data["symbol"] in USD_STOCK_SYMBOLS:
                    equity_data["currency"] = "USD"
                else:
                    equity_data["currency"] = "TTD"
                # get a list of tables from the URL
                dataframe_list = pd.read_html(equity_page)
                # use pandas to get the issued share capital and market cap
                equity_data["issued_share_capital"] = int(float(dataframe_list[0]["Opening Price"][8]))
                equity_data["market_capitalization"] = float(
                    re.sub("[ |$|,]", "", dataframe_list[0]["Closing Price"][8])
                )
                # Now we have all the important information for this equity
                # So we can add the dictionary object to our global list
                # But first we check that this symbol has not been added already
                symbol_already_added = next(
                    (item for item in all_listed_equity_data if item["symbol"] == symbol),
                    False,
                )
                if not symbol_already_added:
                    all_listed_equity_data.append(equity_data)
                # else don't add a duplicate equity
                logger.info("Successfully added basic listing data for: " + equity_data["security_name"])
            except Exception as exc:
                logger.warning(f"Could not load page for equity:{symbol}. Here's what we know: {str(exc)}")
        return all_listed_equity_data

    def _build_list_of_all_symbols_listed(self, dataframe_list):
        listed_stock_symbols = []
        for dataframe in dataframe_list:
            if "Symbol" in dataframe:
                for symbol in dataframe["Symbol"]:
                    if symbol:
                        bad_symbols = ["{{", ".", "(S"]
                        if all(string not in symbol for string in bad_symbols):
                            listed_stock_symbols.append(symbol)
        return listed_stock_symbols

    def update_num_equities_in_sectors(self: Self) -> int:
        try:
            logger.info("Now computing number of equities in each sector.")
            with DatabaseConnect() as db_connection:
                unique_listed_equities_df = self._calculate_num_equities_per_sector(db_connection)
                # update the table in the db
                self._write_num_listed_equities_per_sector_to_db(db_connection, unique_listed_equities_df)
            logger.info(f"Successfully updated equity data in db.")
            return 0
        except Exception as exc:
            logger.exception("Problem encountered while calculating number of equities in each sector." + str(exc))
        finally:
            custom_logging.flush_smtp_logger()

    def _calculate_num_equities_per_sector(self, db_connection):
        # set up the tables from the db
        # read the listedequities table into a dataframe
        listed_equities_df = pd.io.sql.read_sql("SELECT sector FROM listed_equities;", db_connection.dbengine)
        # create a copy of the dataframe and drop the duplicates to get all sectors
        unique_listed_equities_df = listed_equities_df.copy().drop_duplicates()
        # get the number of times the sector occurs in the df
        listed_equities_sector_counts_df = listed_equities_df["sector"].value_counts(dropna=False)
        # map the counts to the unique df
        unique_listed_equities_df["num_listed"] = unique_listed_equities_df["sector"].map(
            listed_equities_sector_counts_df
        )
        # get the rows that are not na
        unique_listed_equities_df = unique_listed_equities_df[unique_listed_equities_df["num_listed"].notna()]
        return unique_listed_equities_df

    def _write_num_listed_equities_per_sector_to_db(self, db_connection, unique_listed_equities_df):
        listed_equities_per_sector_table = Table(
            "listed_equities_per_sector",
            MetaData(),
            autoload=True,
            autoload_with=db_connection.dbengine,
        )
        listed_equities_per_sector_insert_stmt = insert(listed_equities_per_sector_table).values(
            unique_listed_equities_df.to_dict("records")
        )
        listed_equities_per_sector_upsert_stmt = listed_equities_per_sector_insert_stmt.on_duplicate_key_update(
            {x.name: x for x in listed_equities_per_sector_insert_stmt.inserted}
        )
        result = db_connection.dbcon.execute(listed_equities_per_sector_upsert_stmt)
        logger.info("Database update successful. Number of rows affected was " + str(result.rowcount))
