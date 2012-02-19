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

    def test_update(self):
        ''' Mysql update should generate valid sql for both, string where condition clause 
            and conditional clause as a set of values. '''
        self.db.run_query = mock.Mock()
        updates = {'release_manager':'John Doe'}

        self.db.update('beesql_version', updates, where="release_manager='John Smith' AND version > 2.0")
        self.assertEqual(self.db.run_query.call_args[0][0].lower(), 
             "UPDATE beesql_version SET release_manager=%s WHERE release_manager='John Smith' AND version > 2.0".lower())
        self.assertEqual(self.db.run_query.call_args[0][1], ('John Doe',))

        self.db.update('beesql_version', updates, release_manager='John Smith', release_year=2012, limit=1)
        self.assertEqual(self.db.run_query.call_args[0][0].lower(), 
             "UPDATE beesql_version SET release_manager=%s WHERE release_manager=%s AND release_year=%s LIMIT 1".lower())
        self.assertEqual(self.db.run_query.call_args[0][1], ('John Doe', 'John Smith', 2012))

    def test_delete(self):
        ''' Mysql delete should generate valid sql for when where condition is provided
            as a string as well as condition pairs. '''
        self.db.run_query = mock.Mock()

        self.db.delete('beesql_version', where="version < 2.0")
        self.assertEqual(self.db.run_query.call_args[0][0].lower(), "DELETE FROM beesql_version WHERE version < 2.0".lower())

        self.db.delete('beesql_version', limit=2, version=2.0, release_name='bumblebee')
        self.assertEqual(self.db.run_query.call_args[0][0].lower(), "DELETE FROM beesql_version WHERE version=%s and release_name=%s LIMIT 2".lower())
        self.assertEqual(self.db.run_query.call_args[0][1], (2.0, 'bumblebee'))

    def test_truncatetable(self):
        ''' Truncate should generate valid sql. '''
        self.db.run_query = mock.Mock()
        self.db.truncate('beesql_version')
        self.assertEqual(self.db.run_query.call_args[0][0].lower(), "TRUNCATE TABLE beesql_version".lower())

    def test_droptable(self):
        ''' Drop table method should generate valid sql for single and multiple tables. '''
        self.db.run_query = mock.Mock()
        self.db.drop_table('beesql_version')
        self.assertEqual(self.db.run_query.call_args[0][0].lower(), "DROP TABLE beesql_version".lower())

        self.db.drop_table('beesql_version', if_exists=True)
        self.assertEqual(self.db.run_query.call_args[0][0].lower(), "DROP TABLE IF EXISTS beesql_version".lower())

        self.db.drop_table('beesql_version', 'beesql_downloads')
        self.assertEqual(self.db.run_query.call_args[0][0].lower(), "DROP TABLE beesql_version, beesql_downloads".lower())

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
