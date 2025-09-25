#!/usr/local/bin/python3
"""
This is a simple DB helper function - to make our DB operations easy in our main script. With this we don't have to worry about
cursor,transaction and connection management.

I didn't add Docstring to this due to lack of time.
"""
import sqlite3
import os
from typing import Optional, List, Union
from utils.get_logger import get_logger


class DBHelper:
    def __init__(self, db_path: str):
        self.logger = get_logger(self.__class__.__name__)
        self.db_path = os.path.abspath(db_path)
        print(self.db_path)
        self.connection: Optional[sqlite3.Connection] = None

    # __enter__ & __exit__ are used for context managing. Automatically closing the db connection once all ops are done.
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def connect(self) -> sqlite3.Connection:
        # This method simply returns a connection to our database. If connection already exists, then returns the same.
        if self.connection:
            return self.connection
        try:
            db = os.path.basename(self.db_path)
            self.logger.info(f"Connecting to {db} database")
            self.connection = sqlite3.connect(self.db_path, check_same_thread=False)
            self.logger.info("Successfully connected to db")
            return self.connection
        except Exception as e:
            self.logger.error(f"Error in connecting to the database: {e}")
            self.close()
            raise

    def close(self):
        # This method when called closes the active connection to our database.
        if self.connection:
            self.connection.close()
            self.logger.info("Successfully closed the connection to db")
            self.connection = None

    def execute(self, query: str, params: Optional[Union[tuple, dict]] = None):
        # This method executes INSERT,UPDATE,DELETE queries.
        try:
            conn = self.connect()
            # Utilizing the connection as context_manger for proper handling of transactions. commit and rollback are taken care
            with conn:
                if params:
                    conn.executemany(query, params)
                else:
                    conn.executemany(query)
            self.logger.info("Successfully executed the query")
        except Exception as e:
            self.logger.error(f"Error executing query: {e}. Query: {query}")
            raise

    def fetch(self, query: str, params: Optional[Union[tuple, dict]] = None, fetch_all: bool = True) -> Union[List[tuple], Optional[tuple]]:
        # fetches the row data - just for testing purpose
        try:
            conn = self.connect()
            cur = conn.cursor()
            if params:
                cur.execute(query, params)
            else:
                cur.execute(query)
            rows = cur.fetchall() if fetch_all else cur.fetchone()
            cur.close()
            self.logger.info("Successfully fetched data from db")
            return rows
        except Exception as e:
            self.logger.error(f"Error fetching data: {e}")
            raise

    def ensure_schema(self, schema: str):
        # Initial table creation and verification. Just in case - to avoid table operation before the table was created.
        try:
            conn = self.connect()
            with conn:
                conn.execute(schema)
            self.logger.info("Schema ensured successfully.")
        except Exception as e:
            self.logger.error(f"Error ensuring schema: {e}")
            raise
