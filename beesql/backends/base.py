#!/usr/bin/env python

''' Contains Base Database Connection. '''

# Author: Kasun Herath <kasunh01@gmail.com>
# Source: https://github.com/kasun/beesql

class BeeSQLBaseConnection(object):
    ''' Base Abstract Database Connection. '''
   
    def query(self, sql):
        ''' Run provided query using implemented classes DB Cursor. ''' 
        self.cursor.execute(sql)
        return self.cursor.fetchall()
