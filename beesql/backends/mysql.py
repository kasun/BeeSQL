#!/usr/bin/env python

''' Contains Mysql Database engine. '''

from base import BeeSQLBaseConnection
from beesql import BeeSQLError

class MysqlConnection(BeeSQLBaseConnection):
    ''' MySQL Database Connection. '''
    def __init__(self, username, password, host='localhost', db=None, unix_socket=None):
        if (not username or not password):
            raise BeeSQLError('Engine mysql requires username and password')


