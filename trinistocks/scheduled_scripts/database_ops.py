#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""This module is used to manage connections to the MySQL backend database

:raises ConnectionError: If unable to connect to the specified database.
:return: Returns 0 if database connection is set up correctly.
:rtype: int
"""

import logging
from typing import List

import pymysql
from sqlalchemy import create_engine, Table, MetaData, select
from sqlalchemy.engine import CursorResult
from typing_extensions import Self

from . import configs

pymysql.install_as_MySQLdb()


class DatabaseConnect:
    def __init__(
            self, dbuser=configs.dbusername, dbpass=configs.dbpassword, dbaddress=configs.dbaddress,
            dbschema=configs.schema
    ):
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
        self.logger = logging.getLogger(__name__)
        self.logger.debug("Creating a new DatabaseConnect object.")
        self.dbengine = create_engine(
            "mysql://" + dbuser + ":" + dbpass + "@" + dbaddress + "/" + dbschema, echo=False
        )
        self.dbcon = self.dbengine.connect()
        if self.dbcon:
            self.logger.info("Connected to database successfully")
        else:
            raise ConnectionError("Could not connect to database at " + dbaddress)

    @property
    def get_dbengine(self: Self):
        return self.dbengine

    @property
    def get_dbcon(self: Self):
        return self.dbcon

    def close(
            self,
    ):
        """
        Close the database connection
        """
        if self.dbengine:
            self.dbengine.dispose()
            self.logger.debug("Database connection closed successfully.")
            return 0
        else:
            self.logger.debug("Database connection not established. Ignoring request to close.")
            return 0

    def __enter__(self):
        """Opens a database connection

        :return: [description]
        :rtype: [type]
        """
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.close()


# function definitions
def _read_listed_symbols_from_db() -> List[str]:
    # First read all symbols from the listed_equities table
    all_listed_symbols = []
    with DatabaseConnect() as db_connection:
        listed_equities_table = Table(
            "listed_equities",
            MetaData(),
            autoload=True,
            autoload_with=db_connection.dbengine,
        )
        selectstmt = select([listed_equities_table.c.symbol])
        result: CursorResult = db_connection.dbcon.execute(selectstmt)
        for row in result:
            all_listed_symbols.append(row[0])
    return all_listed_symbols


def _read_symbols_and_ids_from_db():
    all_listed_symbols = []
    with DatabaseConnect() as db_connection:
        listed_equities_table = Table(
            "listed_equities",
            MetaData(),
            autoload=True,
            autoload_with=db_connection.dbengine,
        )
        selectstmt = select([listed_equities_table.c.symbol, listed_equities_table.c.symbol_id])
        result = db_connection.dbcon.execute(selectstmt)
        for row in result:
            all_listed_symbols.append({"symbol": row[0], "symbol_id": row[1]})
    return all_listed_symbols
