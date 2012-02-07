#!/usr/bin/env python

''' Contains Base Database Connection. '''

# Author: Kasun Herath <kasunh01@gmail.com>
# Source: https://github.com/kasun/beesql

class BeeSQLBaseConnection(object):
    ''' Base Abstract Database Connection. '''
   
    def query(self, sql, escapes=None):
        ''' Run provided query using implemented classes DB Cursor. Use Escape values if provided. ''' 
        if not escapes:
            self.cursor.execute(sql)
        else:
            self.cursor.execute(sql, escapes)
        return self.cursor.fetchall()
