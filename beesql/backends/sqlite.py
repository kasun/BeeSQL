#!/usr/bin/env python

''' Contains sqlite Database Connection. '''

# Author: Kasun Herath <kasunh01@gmail.com>
# Source: https://github.com/kasun/beesql

import sqlite3

from base import BeeSQLBaseConnection
from beesql import BeeSQLError
from beesql import BeeSQLDatabaseError

class SQLITEConnection(BeeSQLBaseConnection):
    ''' SQLlite Database Connection. '''
    def __init__(self, username, password, host='localhost', port=3306, db=None, unix_socket=None):
        ''' Initialize Sqlite connection. '''
        BeeSQLBaseConnection.__init__(self)
        if not db: 
            raise BeeSQLError('Engine sqlite requires db')
        try:
            self.db_connection = sqlite3.connect(db)
            self.db_connection.row_factory = sqlite3.Row
            self.cursor = self.db_connection.cursor()
        except sqlite3.OperationalError, oe:
            raise BeeSQLDatabaseError(str(oe))

    def query(self, sql, escapes=None):
        ''' Run provided query.
        Arguments:
            sql: Query to run.
            escapes: Optional, A tuple of escape values to escape provided sql. '''
        try:
            return self.run_query(sql, escapes)
        except sqlite3.OperationalError, oe:
            raise BeeSQLDatabaseError(str(oe))

    def close(self):
        ''' Close connection to Databaes. '''
        self.db_connection.close()
