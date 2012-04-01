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

    def test_get(self):
        ''' Get should retrieve only one row if where condition match or else None. '''
        self.db.query("""CREATE TABLE beesql_version(
        id INTEGER PRIMARY_KEY,
        version VARCHAR(10),
        release_manager VARCHAR(100))""")
        self.db.insert('beesql_version', id=1, version='0.1.1', release_manager='John Doe')

        result = self.db.get('beesql_version', version='0.1.1')
        self.assertEqual(result['release_manager'], 'John Doe')

        result = self.db.get('beesql_version', version='0.1.2')
        self.assertIsNone(result)

    def test_select(self):
        ''' Select method should generate valid sql. '''
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

    def test_update(self):
        ''' Update should generate valid sql for instances when no where condition is provided,
            when it is provided as a string or as a set of values. '''
        self.db.run_query = mock.Mock()
        updates = {'release_manager':'John Doe'}

        self.db.update('beesql_version', updates)
        self.assertEqual(self.db.run_query.call_args[0][0].lower(), 
             "UPDATE beesql_version SET release_manager=?".lower())
        self.assertEqual(self.db.run_query.call_args[0][1], ('John Doe',))

        self.db.update('beesql_version', updates, where="release_manager='John Smith' AND version > 2.0")
        self.assertEqual(self.db.run_query.call_args[0][0].lower(), 
             "UPDATE beesql_version SET release_manager=? WHERE release_manager='John Smith' AND version > 2.0".lower())
        self.assertEqual(self.db.run_query.call_args[0][1], ('John Doe',))

        self.db.update('beesql_version', updates, release_manager='John Smith', release_year=2012)
        self.assertEqual(self.db.run_query.call_args[0][0].lower(), 
             "UPDATE beesql_version SET release_manager=? WHERE release_manager=? AND release_year=?".lower())
        self.assertEqual(self.db.run_query.call_args[0][1], ('John Doe', 'John Smith', 2012))

    def test_delete(self):
        ''' Delete should generate valid sql for when where condition is provided
            as a string as well as condition pairs. '''
        self.db.run_query = mock.Mock()

        self.db.delete('beesql_version', where="version < 2.0")
        self.assertEqual(self.db.run_query.call_args[0][0].lower(), "DELETE FROM beesql_version WHERE version < 2.0".lower())

        self.db.delete('beesql_version', version=2.0, release_name='bumblebee')
        self.assertEqual(self.db.run_query.call_args[0][0].lower(), "DELETE FROM beesql_version WHERE version=? and release_name=?".lower())
        self.assertEqual(self.db.run_query.call_args[0][1], (2.0, 'bumblebee'))

    def test_transaction(self):
        ''' When in a transaction auto commit should be false. '''
        self.db.query("""CREATE TABLE beesql_version(
        id INTEGER PRIMARY_KEY,
        version VARCHAR(10),
        release_manager VARCHAR(100))""")
        self.db.insert('beesql_version', id=1, version='0.1.1', release_manager='John Doe')
        self.db.transaction_on()
        updates = {'release_manager': 'John Smith'}
        self.db.update('beesql_version', updates, release_manager='John Doe')
        self.db.rollback()
        row = self.db.select('beesql_version', where="id=1")[0]
        self.assertNotEqual(row['release_manager'], updates['release_manager'])

    def test_tables(self):
        ''' tables should return tables of current database as a list. '''
        self.db.query("""CREATE TABLE beesql_version(
        id INTEGER PRIMARY_KEY,
        version VARCHAR(10),
        release_manager VARCHAR(100))""")
        tables = self.db.tables()
        self.assertEqual(tables, ['beesql_version'])

    def test_droptable(self):
        ''' Drop table method should generate valid sql. '''
        self.db.run_query = mock.Mock()
        self.db.drop_table('beesql_version')
        self.assertEqual(self.db.run_query.call_args[0][0].lower(), "DROP TABLE beesql_version".lower())

        self.db.drop_table('beesql_version', if_exists=True)
        self.assertEqual(self.db.run_query.call_args[0][0].lower(), "DROP TABLE IF EXISTS beesql_version".lower())

    def test_lastrowid(self):
        ''' Attribute lastrowid should be equal to last row id of insert statement. '''
        self.db.query("""CREATE TABLE beesql_version(
        id INTEGER PRIMARY_KEY,
        version VARCHAR(10),
        release_manager VARCHAR(100))""")
        self.db.insert('beesql_version', id=1, version='0.1.1', release_manager='John Doe')
        self.assertEqual(self.db.lastrowid, 1)

    def test_lastsqlandescapes(self):
        ''' Attributes lastsql and lastescapes should be equal to lastly run sql and escapes used. '''
        self.db.query("""CREATE TABLE beesql_version(
        id INTEGER PRIMARY_KEY,
        version VARCHAR(10),
        release_manager VARCHAR(100))""")
        self.db.insert('beesql_version', id=1, version='0.1.1', release_manager='John Doe')
        self.assertEqual(self.db.lastsql.lower(), 'INSERT INTO beesql_version (release_manager, version, id) VALUES (?, ?, ?)'.lower())
        self.assertEqual(self.db.lastescapes, ('John Doe', '0.1.1', 1))

    def tearDown(self):
        self.db.close()

if __name__ == '__main__':
    unittest.main()
