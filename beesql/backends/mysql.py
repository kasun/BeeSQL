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
            self.last_sql = sql
            self.last_escapes = escapes
            return self.run_query(sql, escapes)
        except pymysql.err.DatabaseError, de:
            raise BeeSQLDatabaseError(str(de))

    def get(self, table, where=None, **where_conditions):
        ''' Retrieve a single row.
        Arguments:
            table: Table to retrieve from.
            where: Optional where conditional clause as a string.
            where_conditions: Optional, condition pairs to contruct where conditional clause.
                                if where is not provided. '''
        sql = 'SELECT * FROM %s' % table
        escapes= None
        if where:
            sql = sql + ' WHERE %s' % where
        elif where_conditions:
            sql = sql + ' WHERE ' + ' AND '.join([k + '=%s' for k in where_conditions.keys()])
            escapes = tuple(where_conditions.values())
        sql = sql + ' LIMIT 1'
        result = self.query(sql, escapes)
        if result:
            return result[0]
        return None

    def select(self, table, columns=None, distinct=False, where=None, group_by=None, group_by_asc=True, having=None, 
                order_by=None, order_by_asc=True, limit=False, **where_conditions):
        ''' Select columns from table.
        Arguments:
            table: Table to select from.
            columns: Tuple of columns or single column name to select. If not provided all columns are selected.
            where: Optional where conditional clause as a string.
            group_by: Optional column name to group results.
            group_by_asc: Default to True to Group columns in ascending order. 
            having: Having clause as a string.
            order_by: Optional, used to sort results using column(s).
            order_by_asc: Default to True to order results in ascending order.
            limit: Limit results to provided number of rows.
            where_conditions: Optional, condition pairs to contruct where conditional clause
                                if where is not provided. 

        Examples:
            connection.select('beesql_version', ('version', 'release_manager'))
            sql - SELECT version, release_manager FROM beesql_version

            connection.select('beesql_version', where="version > 2.0 AND release_manager='John Doe'")
            sql - SELECT * FROM beesql_version WHERE version > 2.0 AND release_manager='John Doe'

            connection.select('beesql_version', release_year=2012, release_manager='John Doe')
            sql - SELECT * FROM beesql_version WHERE release_year=2012 AND release_manager='John Doe' '''
        sql = 'SELECT '
        escapes= None
        if distinct:
            sql = sql + 'DISTINCT '
        if columns:
            # Columns is either a tuple of columns or single column name
            if type(columns) is tuple:
                sql = sql + ', '.join([column for column in columns]) 
            else:
                sql = sql + columns
        else:
            sql = sql + '*'
        sql = sql + ' FROM %s' % (table)
        if where:
            sql = sql + ' WHERE %s' % (where)
        elif where_conditions:
            sql = sql + ' WHERE ' + ' AND '.join([k + '=%s' for k in where_conditions.keys()])
            escapes = tuple(where_conditions.values())
        if group_by:
            sql = sql + ' GROUP BY '
            if type(group_by) is tuple:
                sql = sql + ','.join(group_by_column for group_by_column in group_by)
            else:
                sql = sql +  '%s' % (group_by)
            if not group_by_asc:
                sql = sql + ' DESC'
        if having:
            sql = sql + ' HAVING %s' % (having)
        if order_by:
            sql = sql + ' ORDER BY '
            if type(order_by) is tuple:
                sql = sql + ','.join(order_by_column for order_by_column in order_by)
            else:
                sql = sql +  '%s' % (order_by)
            if not order_by_asc:
                sql = sql + ' DESC'
        if limit:
            sql = sql + ' LIMIT %s' % (limit)
        try:
            return self.query(sql, (escapes))
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
            self.query(sql, escapes)
        except pymysql.err.DatabaseError, de:
            raise BeeSQLDatabaseError(str(de))

    def update(self, table, updated_values, where=None, limit=None, **where_conditions):
        ''' Update table with provided updated values.
        Arguments:
            table: Table to be updated.
            updated_values: A dictionary representing values to be updated.
            where: Optional, where condition as a string.
            limit: Optional, used to limit the number of rows to be updated.
            where_conditions: Optional, condition pairs to contruct where conditional clause
                              if where is not provided. 

        Examples:
            updates = {'release_manager':'John Doe'}

            connection.update('beesql_version', updates, where="release_manager='John Smith' AND version > 2.0")
            sql - UPDATE beesql_version SET release_manager='John Doe' WHERE release_manager='John Smith AND version > 2.0'

            connection.update('beesql_version', updates, release_manager='John Smith', release_year=2012, limit=1)
            sql - UPDATE beesql_version SET release_manager='John Doe' WHERE release_manager='John Smith' AND release_year=2012 LIMIT 1
            '''
        if not type(updated_values) is dict:
            raise TypeError('updated_values should be of type dict')
        sql = 'UPDATE %s SET ' % (table) + ' , '.join([k + '=%s' for k in updated_values.keys()])
        escapes_list = updated_values.values()
        if where:
            sql = sql + ' WHERE %s' % (where)
        elif where_conditions:
            sql = sql + ' WHERE ' + ' AND '.join([k + '=%s' for k in where_conditions.keys()])
            escapes_list.extend(where_conditions.values())
        if limit:
            sql = sql + ' LIMIT %s' % (limit)
        try:
            self.query(sql, tuple(escapes_list))
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
            self.query(sql, escapes)
        except pymysql.err.DatabaseError, de:
            raise BeeSQLDatabaseError(str(de))

    def truncate(self, table):
        ''' Empty provided table. '''
        sql = 'TRUNCATE TABLE %s' % (table)
        try:
            self.query(sql)
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
            self.query(sql)
        except pymysql.err.DatabaseError, de:
            raise BeeSQLDatabaseError(str(de))
            

    def use(self, db):
        ''' Issue a mysql use command against the provided database. '''
        try:
            sql = "USE %s" % (db)
            self.query(sql)
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
            self.query(sql)
        except pymysql.err.DatabaseError, de:
            raise BeeSQLDatabaseError(str(de))

    def tables(self):
        ''' Return tables of database. '''
        sql = 'SHOW TABLES'
        return [tableinfo.values()[0] for tableinfo in self.query(sql)]

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
            self.query(sql)
        except pymysql.err.DatabaseError, de:
            raise BeeSQLDatabaseError(str(de))

    @property
    def lastrowid(self):
        ''' Return row ID of last insert. '''
        return self.cursor.lastrowid

    @property
    def lastsql(self):
        ''' Return last run sql statement. '''
        return self.last_sql

    @property
    def lastescapes(self):
        ''' Return lastly used escape values as a tuple. '''
        return self.last_escapes

    def close(self):
        ''' Close connection to Databaes. '''
        self.db_connection.close()
