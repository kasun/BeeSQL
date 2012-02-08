#!/usr/bin/env python

''' BeeSQL core. '''

# Author: Kasun Herath <kasunh01@gmail.com>
# Source: https://github.com/kasun/BeeSQL

import beesql

def connection(engine='mysql', username=None, password=None, host='localhost', port=3306, db=None, unix_socket=None):
    ''' 
    Create and return a connection to a Database using specified engine.
    Arguments:
        engine: Database to use; Default to mysql. 
        username: Username used to connect to Database; Not used with sqlite.
        password: Password used to connect to Database; Not used with sqlite.
        host: Host of Database; Default to localhost, Not used with sqlite.
        port: port to connect to Database; Default to 3306, Not used with sqlite.
        db: Database name; Optional, if engine is sqlite a filename is expected.
        unix_socket: Used to connect to the Database through a unix socket; Optional, not used with sqlite. '''

    try:
        mod = __import__('beesql.backends.%s' % (engine), fromlist=['beesql.backends'])
        connection = getattr(mod, '%sConnection' % (engine.upper()))(username, password, host, port, db, unix_socket)
        return connection
    except ImportError:
        raise beesql.BeeSQLError('Invalid engine: %s' % (engine))
    except AttributeError:
        raise beesql.BeeSQLError('Invalid engine: %s' % (engine))

