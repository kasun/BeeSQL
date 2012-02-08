#!/usr/bin/env python

''' Contains Base Database Connection. '''

# Author: Kasun Herath <kasunh01@gmail.com>
# Source: https://github.com/kasun/BeeSQL

class BeeSQLBaseConnection(object):
    ''' Base Abstract Database Connection. '''
    def __init__(self):
        self.transaction = False
   
    def run_query(self, sql, escapes=None):
        ''' Run provided query using implemented class's DB Cursor. Use Escape values if provided. ''' 
        if not escapes:
            self.cursor.execute(sql)
        else:
            self.cursor.execute(sql, escapes)
        if not self.transaction:
            self.db_connection.commit()
        return self.cursor.fetchall()

    def commit(self):
        ''' Commit a transaction. '''
        self.db_connection.commit()

    def rollback(self):
        ''' Rollback a transaction. '''
        self.db_connection.rollback()

    def transaction_on(self):
        ''' Set transaction mode on. '''
        self.transaction = True

    def transaction_off(self):
        ''' Set transaction mode off. '''
        self.transaction = False
