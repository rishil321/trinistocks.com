#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""This module is used to manage connections to the MySQL backend database

:raises ConnectionError: If unable to connect to the specified database.
:return: Returns 0 if database connection is set up correctly.
:rtype: int
"""

import logging
from . import configs
from sqlalchemy import create_engine, Table, MetaData

class DatabaseConnect:

    def __init__(self,dbuser=configs.dbusername,dbpass=configs.dbpassword,
        dbaddress=configs.dbaddress,dbschema=configs.schema):
        """Set up the database connection object with some default parameters

        :param dbuser: [description], defaults to configs.dbusername
        :type dbuser: [type], optional
        :param dbpass: [description], defaults to configs.dbpassword
        :type dbpass: [type], optional
        :param dbaddress: [description], defaults to configs.dbaddress
        :type dbaddress: [type], optional
        :param dbschema: [description], defaults to configs.schema
        :type dbschema: [type], optional
        :raises ConnectionError: [description]
        :return: [description]
        :rtype: [type]
        """
        logging.debug("Creating a new DatabaseConnect object.")
        self.dbengine = create_engine("mysql://"+dbuser+":"+dbpass+"@"+dbaddress+"/" +
                                      dbschema, echo=False)
        self.dbcon = self.dbengine.connect()
        if self.dbcon:
            logging.info("Connected to database successfully")
            return 0
        else:
            raise ConnectionError(
                "Could not connect to database at "+dbaddress)

    @property
    def get_dbengine(self):
        return self._dbengine

    @property
    def get_dbcon(self):
        return self._dbcon

    def close(self,):
        """
        Close the database connection
        """
        if self.dbengine:
            self.dbengine.dispose()
            logging.debug("Database connection closed successfully.")
            return 0
        else:
            logging.debug("Database connection not established. Ignoring request to close.")
            return 0