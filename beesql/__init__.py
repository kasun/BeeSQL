#!/usr/bin/env python

'''
BeeSQL: Python SQL wrapper. 
        
Goals
    1. Hide Boring Repetitive Steps in Python Database API.
    2. Allow for mapping between python datastructures and sql. '''

# Author: Kasun Herath <kasunh01@gmail.com>
# Source: https://github.com/kasun/beesql

from core import connection
from exceptions import BeeSQLError
