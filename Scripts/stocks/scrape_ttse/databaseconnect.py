import logging
import scrape_ttse.ttsescraperconfig
import scrape_ttse.customlogging
from sqlalchemy import create_engine, Table, select, MetaData, text, and_

class DatabaseConnect:
    """
    Manages connections the the backend MySQL database
    """

    dbcon = None
    dbengine = None

    def __init__(self,):
        logging.debug("Creating a new DatabaseConnect object.")
        # Get the required login info from our config file
        dbuser = scrape_ttse.ttsescraperconfig.dbusername
        dbpass = scrape_ttse.ttsescraperconfig.dbpassword
        dbaddress = scrape_ttse.ttsescraperconfig.dbaddress
        dbschema = scrape_ttse.ttsescraperconfig.schema
        self.dbengine = create_engine("mysql://"+dbuser+":"+dbpass+"@"+dbaddress+"/" +
                                      dbschema, echo=False)
        self.dbcon = self.dbengine.connect()
        if self.dbcon:
            logging.debug("Connected to database successfully")
        else:
            raise ConnectionError(
                "Could not connect to database at "+dbaddress)

    def close(self,):
        """
        Close the database connection
        """
        if self.dbengine:
            self.dbengine.dispose()