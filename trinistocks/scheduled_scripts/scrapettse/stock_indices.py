import logging
from logging.config import dictConfig

import pandas as pd
from sqlalchemy import MetaData, Table
from sqlalchemy.dialects.mysql import insert
from typing_extensions import Self

from scheduled_scripts.scraping_engine import ScrapingEngine
from scheduled_scripts import custom_logging, logging_configs
from scheduled_scripts.database_ops import DatabaseConnect

dictConfig(logging_configs.LOGGING_CONFIG)
logger = logging.getLogger(__name__)


class StockIndicesScraper:
    def __init__(self: Self):
        self.scraping_engine = ScrapingEngine()
        pass

    def __del__(self: Self):
        pass

    def scrape_historical_indices_data(self: Self):
        """Use the requests and pandas libs to fetch data for all indices at
        https://www.stockex.co.tt/indices/
        and scrape the useful output into a list of dictionaries to write to the db
        """
        try:
            logger.debug("Now scraping historical data for all indices.")
            all_indices_data = self._scrape_historical_data_for_ttse_indices()
            self._write_historical_indices_data_to_db(all_indices_data)
            return 0
        except Exception as exc:
            logger.exception(f"Problem encountered while updating listed equities. Here's what we know: {str(exc)}")
        finally:
            custom_logging.flush_smtp_logger()

    def _scrape_historical_data_for_ttse_indices(self):
        # This list of dicts will contain all data to be written to the db
        all_indices_data = []
        # create a list of all index ids and names to be scraped
        all_ttse_indices = [
            dict(name="All T&T Index", id=4),
            dict(name="Composite Index", id=5),
            dict(name="Cross-Listed Index", id=6),
            dict(name="SME Index", id=15),
        ]
        for ttse_index in all_ttse_indices:
            index_url = f"https://www.stockex.co.tt/indices/?indexId={ttse_index['id']}"
            logger.debug("Navigating to " + index_url)
            index_page = self.scraping_engine.get_url_and_return_html(url=index_url)
            # get a list of tables from the URL
            dataframe_list = pd.read_html(index_page)
            # get the table that holds the historical index values
            historical_index_values_df = dataframe_list[1]
            # rename the columns
            historical_index_values_df = historical_index_values_df.rename(
                columns={
                    "Trade Date": "date",
                    "Value": "index_value",
                    "Change ($)": "index_change",
                    "Change (%)": "change_percent",
                    "Volume Traded": "volume_traded",
                }
            )
            # convert the date column
            historical_index_values_df["date"] = pd.to_datetime(historical_index_values_df["date"], format="%d %b %Y")
            # add a series for the index name
            historical_index_values_df["index_name"] = pd.Series(
                data=ttse_index["name"], index=historical_index_values_df.index
            )
            # convert the dataframe to a list of dicts and add to the large list
            all_indices_data += historical_index_values_df.to_dict("records")
        return all_indices_data

    def _write_historical_indices_data_to_db(self, all_indices_data):
        # Now write the data to the database
        with DatabaseConnect() as db_connection:
            historical_indices_table = Table(
                "historical_indices_info",
                MetaData(),
                autoload=True,
                autoload_with=db_connection.dbengine,
            )
            logger.debug("Inserting scraped data into historical_indices table")
            insert_stmt = insert(historical_indices_table).values(all_indices_data)
            upsert_stmt = insert_stmt.on_duplicate_key_update({x.name: x for x in insert_stmt.inserted})
            result = db_connection.dbcon.execute(upsert_stmt)
            logger.debug("Database update successful. Number of rows affected was " + str(result.rowcount))