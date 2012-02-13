#!/usr/bin/env python

''' Test Cases for mysql connection. '''

# Author: Kasun Herath <kasunh01@gmail.com>
# Source: https://github.com/kasun/BeeSQL

import unittest
import mock

import beesql

class TestMysqlConnection(unittest.TestCase):

    def setUp(self):
        self.db = beesql.connection(username='root', password='thinkcube')

    def test_insert(self):
        ''' Insert method should generate valid sql '''
        self.db.run_query = mock.Mock()
        self.db.insert('beesql_version', version='0.1', name='Kasun Herath')

        self.assertEqual(self.db.run_query.call_args[0][0].lower(), "INSERT INTO beesql_version (version, name) VALUES (%s, %s)".lower())
        self.assertEqual(self.db.run_query.call_args[0][1], ('0.1', 'Kasun Herath'))

    def test_use(self):
        ''' use method should generate valid sql. '''
        self.db.run_query = mock.Mock()
        self.db.use('beesql_version')
        self.assertEqual(self.db.run_query.call_args[0][0].lower(), "USE beesql_version".lower())

    def test_create(self):
        ''' Create method should generate valid sql for both if_not_exists=False and if_not_exists=True. '''
        self.db.run_query = mock.Mock()
        self.db.create('beesql_version')
        self.assertEqual(self.db.run_query.call_args[0][0].lower(), "CREATE DATABASE beesql_version".lower())

        self.db.create('beesql_version', if_not_exists=True)
        self.assertEqual(self.db.run_query.call_args[0][0].lower(), "CREATE DATABASE IF NOT EXISTS beesql_version".lower())

        self.db.create('beesql_version', if_not_exists=False)
        self.assertEqual(self.db.run_query.call_args[0][0].lower(), "CREATE DATABASE beesql_version".lower())

    def test_drop(self):
        ''' Drop method should generate valid sql for both if_exists=False and if_exists=True. '''
        self.db.run_query = mock.Mock()
        self.db.drop('beesql_version')
        self.assertEqual(self.db.run_query.call_args[0][0].lower(), "DROP DATABASE beesql_version".lower())
        self.db.drop('beesql_version', if_exists=True)
        self.assertEqual(self.db.run_query.call_args[0][0].lower(), "DROP DATABASE IF EXISTS beesql_version".lower())
        self.db.drop('beesql_version', if_exists=False)
        self.assertEqual(self.db.run_query.call_args[0][0].lower(), "DROP DATABASE beesql_version".lower())

    def tearDown(self):
        self.db.close()

if __name__ == '__main__':
    unittest.main()
