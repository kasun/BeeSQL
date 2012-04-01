#!/usr/bin/env python

''' Contains sqlite Database Connection. '''

# Author: Kasun Herath <kasunh01@gmail.com>
# Source: https://github.com/kasun/BeeSQL

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
            self.db_connection.row_factory = self.__dict_factory
            self.cursor = self.db_connection.cursor()
        except sqlite3.OperationalError, oe:
            raise BeeSQLDatabaseError(str(oe))

    def __dict_factory(self, cursor, row):
        ''' Custom Row factory to return results as a dictionary. '''
        d = {}
        for idx, col in enumerate(cursor.description):
            d[col[0]] = row[idx]
        return d

    def query(self, sql, escapes=None):
        ''' Run provided query.
        Arguments:
            sql: Query to run.
            escapes: Optional, A tuple of escape values to escape provided sql. '''
        try:
            return self.run_query(sql, escapes)
        except sqlite3.OperationalError, oe:
            raise BeeSQLDatabaseError(str(oe))

    def select(self, table, columns=None, distinct=False, where=None, group_by=None, having=None,
                order_by=None, order_by_asc=True, limit=False, **where_conditions):
        ''' Select columns from table.
        Arguments:
            table: Table to select from.
            columns: Tuple of columns or single column name to select. If not provided all columns are selected.
            where: Optional where conditional clause as a string.
            group_by: Optional column name to group results.
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
            sql = sql + ' WHERE ' + ' AND '.join([k + '=?' for k in where_conditions.keys()])
            escapes = tuple(where_conditions.values())
        if group_by:
            sql = sql + ' GROUP BY '
            if type(group_by) is tuple:
                sql = sql + ','.join(group_by_column for group_by_column in group_by)
            else:
                sql = sql +  '%s' % (group_by)
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
        return self.query(sql, (escapes))

    def insert(self, table, **values):
        ''' Insert values into table.
        Arguments:
            table: Table to be inserted into.
            values: Dictionary of column names and values to be inserted. 

        Example:
            BeeSQL insert - connection.insert('beesql_version', version='0.1', release_manager='Kasun Herath')
            SQL - INSERT INTO beesql_version (version, release_manager) VALUES ('0.1', 'Kasun Herath') '''

        sql = "INSERT INTO %s (%s) VALUES (%s)" % (table, ', '.join([columnname for columnname in values.keys()]), ', '.join(['?' for columnname in values.values()]))
        escapes = tuple(values.values())
        self.query(sql, escapes)

    def update(self, table, updated_values, where=None, **where_conditions):
        ''' Update table with provided updated values.
        Arguments:
            table: Table to be updated.
            updated_values: A dictionary representing values to be updated.
            where: Optional, where condition as a string.
            where_conditions: Optional, condition pairs to contruct where conditional clause
                              if where is not provided. 

        Examples:
            updates = {'release_manager':'John Doe'}

            connection.update('beesql_version', updates, where="release_manager='John Smith' AND version > 2.0")
            sql - UPDATE beesql_version SET release_manager='John Doe' WHERE release_manager='John Smith AND version > 2.0'

            connection.update('beesql_version', updates, release_manager='John Smith', release_year=2012)
            sql - UPDATE beesql_version SET release_manager='John Doe' WHERE release_manager='John Smith' AND release_year=2012
            '''
        if not type(updated_values) is dict:
            raise TypeError('updated_values should be of type dict')
        sql = 'UPDATE %s SET ' % (table) + ' , '.join([k + '=?' for k in updated_values.keys()])
        escapes_list = updated_values.values()
        if where:
            sql = sql + ' WHERE %s' % (where)
        elif where_conditions:
            sql = sql + ' WHERE ' + ' AND '.join([k + '=?' for k in where_conditions.keys()])
            escapes_list.extend(where_conditions.values())
        self.query(sql, tuple(escapes_list))

    def delete(self, table, where=None, **where_conditions):
        ''' Delete values from table.
        Arguments:
            table: Table to delete values from.
            where: Optional, where condition as a string.
            where_conditions: Optional, condition pairs to contruct where conditional clause,
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
            sql = sql + ' WHERE ' + ' AND '.join(['%s=%s' % (k, '?') for k in where_conditions.keys()])
            escapes = tuple(where_conditions.values())
        self.query(sql, escapes)

    def tables(self):
        ''' Return list of tables in current database. '''
        sql = "select name from sqlite_master where type = 'table'"
        table_dicts = self.query(sql)
        return [table_dict['name'] for table_dict in table_dicts]

    def drop_table(self, table, **kargs):
        ''' Drop tables provided.
        Arguments:
            if_exists: Try dropping tables only if exists, used to prevent errors if a table does not exist.
            table: Table to be deleted. '''
        if 'if_exists' in kargs and kargs['if_exists']:
            sql = "DROP TABLE IF EXISTS "
        else:
            sql = "DROP TABLE "
        sql = sql + table
        self.query(sql)

    def close(self):
        ''' Close connection to Databaes. '''
        self.db_connection.close()
