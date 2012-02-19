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

        Example:
            BeeSQL insert - connection.insert('beesql_version', version='0.1', release_manager='Kasun Herath')
            SQL - INSERT INTO beesql_version (version, release_manager) VALUES ('0.1', 'Kasun Herath') '''

        try:
            sql = "INSERT INTO %s (%s) VALUES (%s)" % (table, ', '.join([columnname for columnname in values.keys()]), ', '.join(['%s' for columnname in values.values()]))
            escapes = tuple(values.values())
            self.run_query(sql, escapes)
        except pymysql.err.DatabaseError, de:
            raise BeeSQLDatabaseError(str(de))

    def delete(self, table, where=None, limit=None, **where_conditions):
        ''' Delete values from table.
        Arguments:
            table: Table to delete values from.
            where: Optional, where condition as a string.
            limit: Optional, places a limit on the number of rows to be deleted.
            where_conditions: Optional, condition pairs to contruct where conditional clause
                              if where is not provided.
        Examples:
            connection.delete('beesql_version', where="version < 2.0")
            sql - DELETE FROM beesql_version WHERE version < 2.0

            connection.delete('beesql_version', limit=2, version=2.0, release_name='bumblebee')
            sql - DELETE FROM beesql_version WHERE version=2.0 and release_name='bumblebee' LIMIT 2 '''
        escapes = None
        sql = 'DELETE FROM %s' % (table)
        if where:
            sql = sql + ' WHERE %s' % (where)
        # If where condition is not supplied as a string derive it using where_conditions.
        elif where_conditions:
            sql = sql + ' WHERE ' + ' AND '.join(['%s=%s' % (k, '%s') for k in where_conditions.keys()])
            escapes = tuple(where_conditions.values())
        if limit:
            sql = sql + ' LIMIT %s' % (limit)
        try:
            self.run_query(sql, escapes)
        except pymysql.err.DatabaseError, de:
            raise BeeSQLDatabaseError(str(de))

    def truncate(self, table):
        ''' Empty provided table. '''
        sql = 'TRUNCATE TABLE %s' % (table)
        try:
            self.run_query(sql)
        except pymysql.err.DatabaseError, de:
            raise BeeSQLDatabaseError(str(de))
    def drop_table(self, *tables, **kargs):
        ''' Drop tables provided.
        Arguments:
            if_exists: Try dropping tables only if exists, used to prevent errors if a table does not exist.
            tables: Tuple of tables to be deleted. '''
        if 'if_exists' in kargs and kargs['if_exists']:
            sql = "DROP TABLE IF EXISTS "
        else:
            sql = "DROP TABLE "
        sql = sql + ", ".join(list(tables))
        try:
            self.run_query(sql)
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
