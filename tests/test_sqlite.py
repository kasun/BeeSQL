#!/usr/bin/env python

''' Test Cases for mysql connection. '''

# Author: Kasun Herath <kasunh01@gmail.com>
# Source: https://github.com/kasun/BeeSQL

import unittest
import mock

import beesql


class TestSQLiteConnection(unittest.TestCase):
    def setUp(self):
        self.db = beesql.connection(engine='sqlite', db=':memory:')

    def test_select(self):
        ''' Mysql select method should generate valid sql. '''
        self.db.run_query = mock.Mock()

        self.db.select('beesql_version')
        self.assertEqual(self.db.run_query.call_args[0][0].lower(), "SELECT * FROM beesql_version".lower())

        self.db.select('beesql_version', 'release_manager', distinct=True)
        self.assertEqual(self.db.run_query.call_args[0][0].lower(), "SELECT DISTINCT release_manager FROM beesql_version".lower())

        self.db.select('beesql_version', ('version', 'release_manager'))
        self.assertEqual(self.db.run_query.call_args[0][0].lower(), "SELECT version, release_manager FROM beesql_version".lower())

        self.db.select('beesql_version', release_year=2012, release_manager='John Doe')
        self.assertEqual(self.db.run_query.call_args[0][0].lower(), "SELECT * FROM beesql_version WHERE release_manager=? AND release_year=?".lower())
        self.assertEqual(self.db.run_query.call_args[0][1], ('John Doe', 2012))

        self.db.select('beesql_version', 'release_name', where="release_year=2012 AND release_manager='John Doe'")
        self.assertEqual(self.db.run_query.call_args[0][0].lower(), "SELECT release_name FROM beesql_version WHERE release_year=2012 AND release_manager='John Doe'".lower())

        self.db.select('beesql_version', ('version', 'release_name'), release_manager='John Doe')
        self.assertEqual(self.db.run_query.call_args[0][0].lower(), "SELECT version, release_name FROM beesql_version WHERE release_manager=?".lower())
        self.assertEqual(self.db.run_query.call_args[0][1], ('John Doe',))

        self.db.select('beesql_version', 'SUM(billed_hours)', where='release_year > 2010', group_by='release_manager', having='SUM(billed_hours) > 100')
        self.assertEqual(self.db.run_query.call_args[0][0].lower(), "SELECT SUM(billed_hours) FROM beesql_version WHERE release_year > 2010 GROUP BY release_manager HAVING SUM(billed_hours) > 100".lower())

    def test_insert(self):
        ''' Insert method should generate valid sql '''
        self.db.run_query = mock.Mock()
        self.db.insert('beesql_version', version='0.1', name='Kasun Herath')

        self.assertEqual(self.db.run_query.call_args[0][0].lower(), "INSERT INTO beesql_version (version, name) VALUES (?, ?)".lower())
        self.assertEqual(self.db.run_query.call_args[0][1], ('0.1', 'Kasun Herath'))

    def test_delete(self):
        ''' Mysql delete should generate valid sql for when where condition is provided
            as a string as well as condition pairs. '''
        self.db.run_query = mock.Mock()

        self.db.delete('beesql_version', where="version < 2.0")
        self.assertEqual(self.db.run_query.call_args[0][0].lower(), "DELETE FROM beesql_version WHERE version < 2.0".lower())

        self.db.delete('beesql_version', version=2.0, release_name='bumblebee')
        self.assertEqual(self.db.run_query.call_args[0][0].lower(), "DELETE FROM beesql_version WHERE version=? and release_name=?".lower())
        self.assertEqual(self.db.run_query.call_args[0][1], (2.0, 'bumblebee'))

if __name__ == '__main__':
    unittest.main()
