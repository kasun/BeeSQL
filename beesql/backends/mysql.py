#!/usr/bin/env python

''' Contains Mysql Database engine. '''

# Author: Kasun Herath <kasunh01@gmail.com>
# Source: https://github.com/kasun/beesql

import pymysql

from base import BeeSQLBaseConnection
from beesql import BeeSQLError

class MysqlConnection(BeeSQLBaseConnection):
    ''' MySQL Database Connection. '''
    def __init__(self, username, password, host='localhost', port=3306, db=None, unix_socket=None):
        if (not username or password is None):
            raise BeeSQLError('Engine mysql requires username and password')
        if not unix_socket:
            self.db_connection = pymysql.connect(user=username, passwd=password, host=host, port=port)
        else:
            self.db_connection = pymysql.connect(user=username, passwd=password, unix_socket=unix_socket)
        self.cursor = self.db_connection.cursor()
        if db and db != '':
            self.cursor.execute('use %s' % (db))
