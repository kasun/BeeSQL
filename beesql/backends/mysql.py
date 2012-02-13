#!/usr/bin/env python

''' Contains Mysql Database Connection. '''

# Author: Kasun Herath <kasunh01@gmail.com>
# Source: https://github.com/kasun/BeeSQL

import pymysql

from base import BeeSQLBaseConnection
from beesql import BeeSQLError
from beesql import BeeSQLDatabaseError

class MYSQLConnection(BeeSQLBaseConnection):
    ''' MySQL Database Connection. '''
    def __init__(self, username, password, host='localhost', port=3306, db=None, unix_socket=None):
        ''' Initialize MysqlConnection. Initialize BaseConnection, register database connection and cursor.
            If db is specified issue a use query. '''
        BeeSQLBaseConnection.__init__(self)
        if (not username or password is None):
            raise BeeSQLError('Engine mysql requires username and password')
        try:
            if not unix_socket:
                self.db_connection = pymysql.connect(user=username, passwd=password, host=host, port=port)
            else:
                self.db_connection = pymysql.connect(user=username, passwd=password, unix_socket=unix_socket)
            self.cursor = self.db_connection.cursor(pymysql.cursors.DictCursor)
            if db and db != '':
                self.cursor.execute('use %s' % (db))
        except pymysql.err.DatabaseError, de:
            raise BeeSQLDatabaseError(str(de))

    def query(self, sql, escapes=None):
        ''' Run provided query.
        Arguments:
            sql: Query to run.
            escapes: Optional, A tuple of escape values to escape provided sql. '''
        try:
            return self.run_query(sql, escapes)
        except pymysql.err.DatabaseError, de:
            raise BeeSQLDatabaseError(str(de))

    def insert(self, table, **values):
        ''' Insert values into table.
        Arguments:
            table: Table to be inserted into.
            values: Dictionary of column names and values to be inserted. 

        Examples:
            SQL - INSERT INTO beesql_version (version, release_manager) VALUES ('0.1', 'Kasun Herath') 
            BeeSQL insert - connection.insert('beesql_version', version='0.1', release_manager='Kasun Herath') '''

        try:
            sql = "INSERT INTO %s (%s) VALUES (%s)" % (table, ', '.join([columnname for columnname in values.keys()]), ', '.join(['%s' for columnname in values.values()]))
            escapes = tuple(values.values())
            self.run_query(sql, escapes)
        except pymysql.err.DatabaseError, de:
            raise BeeSQLDatabaseError(str(de))

    def use(self, db):
        ''' Issue a mysql use command against the provided database. '''
        try:
            sql = "USE %s" % (db)
            self.run_query(sql)
        except pymysql.err.DatabaseError, de:
            raise BeeSQLDatabaseError(str(de))

    def create(self, db, if_not_exists=False):
        ''' Create provided database.
        Arguments:
            db: Database to be created.
            if_not_exists: Try creating the database only if it does not exist, used to prevent errors if database does exist. '''
        try:
            if if_not_exists:
                sql = "CREATE DATABASE IF NOT EXISTS %s" % (db)
            else:
                sql = "CREATE DATABASE %s" % (db)
            self.run_query(sql)
        except pymysql.err.DatabaseError, de:
            raise BeeSQLDatabaseError(str(de))

    def drop(self, db, if_exists=False):
        ''' Drop provided database.
        Arguments:
            db: Database to be dropped.
            if_exists: Try dropping the database only if it exists, used to prevent errors if database does not exist. '''
        try:
            if if_exists:
                sql = "DROP DATABASE IF EXISTS %s" % (db)
            else:
                sql = "DROP DATABASE %s" % (db)
            self.run_query(sql)
        except pymysql.err.DatabaseError, de:
            raise BeeSQLDatabaseError(str(de))

    def close(self):
        ''' Close connection to Databaes. '''
        self.db_connection.close()
