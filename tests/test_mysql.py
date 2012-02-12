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

    def tearDown(self):
        self.db.close()

if __name__ == '__main__':
    unittest.main()
