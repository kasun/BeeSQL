#!/usr/bin/env python

''' BeeSQL core. '''

# Author: Kasun Herath <kasunh01@gmail.com>
# Source: https://github.com/kasun/beesql

def connection(engine='mysql', username=None, password=None, host='localhost', db=None, unix_socket=None):
    ''' 
    Create and return a connection to a Database.
    Arguments:
        engine: Database to use; Default to mysql. 
        username: Username used to connect to Database; Not used with sqlite.
        password: Password used to connect to Database; Not used with sqlite.
        host: Host of Database; Default to localhost, Not used with sqlite
        db: Database name; Optional, if engine is sqlite a filename is expected.
        unix_socket: Used to connect to the Database through a unix socket; Optional, not used with sqlite. '''

    pass

