#!/usr/bin/env python

''' BeeSQL Errors. '''

# Author: Kasun Herath <kasunh01@gmail.com>
# Source: https://github.com/kasun/BeeSQL

class BeeSQLError(Exception):
    ''' Base BeeSQL Error. '''

class BeeSQLDatabaseError(Exception):
    ''' Database operation related BeeSQL Error. '''


