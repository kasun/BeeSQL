.. _install:

Quick Start
===========

.. note::
    Current supported engines are ``mysql`` and ``sqlite``.

Common operations
-----------------

**Importing beesql**::

    import beesql


**Creating a database connection**::
    
    db = beesql.connection(engine='engine', username='user', password='pass', db='database')

The engine should be one of supported engines, it defaults to ``mysql``. For MySQL username and password should be provided and database can be provided with ``db``. For SQLite only ``db`` should be provided and it should be the filepath to the database file or special value ``:memory:`` to create the database in memory.

    - ``db = beesql.connection(username='root', password='rootpass', db='beesql')``
    - ``db = beesql.connection(engine='sqlite', db='beesql.db')``

The first statement Would return a mysql connection connected to database ``beesql`` using username ``root`` and password ``rootpass`` while the second statement would return a sqlite connection using ``beesql.db`` as the database file. For a list of all available options have a refer the API.

**Executing a SQL statement**::

    versions = db.query('SELECT * from beesql_version')

The above statement would return a tuple of dictionaries representing rows. Columns should be accessed using column names::

    for version in versions:
        print(version['release_manager'])

.. note:: 

    :func:`query` commits the SQL statement automatically.

:func:`query` also accepts an optional tuple of escape values used to escape SQL statements.
MySQL example::

    db.query('UPDATE beesql_version SET release_name = %s WHERE ID = %s', ('bumblebee', 2))

SQLite example::

    db.query('UPDATE beesql_version SET release_name = ? WHERE ID = ?', ('bumblebee', 2))

**Executing a transaction**::

    db.transaction_on()
    error = False
    for sql in sql_statements:
        try:
            db.query(sql)
        except beesql.exceptions.BeeSQLDatabaseError
            error = True
            db.rollback()
            break
    if not error:
        db.commit()
    db.transaction_off()

**Listing tables of currently used database**::

    tables = db.tables()

**Select statement**::

    versions = db.select('beesql_version', 'version', 'release_date', release_manager='John Doe')

The above would return all rows with columns ``version`` and ``release_date`` where release_manager is ``John Doe``

For all options :func:`select` supports refer the API.

**Retrieve a single row**::

    row = db.get('beesql_version', release_manager='John Doe')

The above would retrieve a single row from ``beesql_version`` where ``release_manager`` is ``John Doe``. Alternatively the where condition can be supplied as a string::
    
    row = db.get(''beesql_version', where="release_manager='John Doe'")

**Insert statement**::

    db.insert('beesql_version', version='0.1', release_manager='John Doe')

The above would insert a row into ``beesql_version`` with ``0.1`` as ``version``` and ``John Doe`` as ``release_manager``.

**Update statement**::

    updated_values = {'release_manager': 'John Smith'}
    db.update('beesql_version', updated_values, release_manager='John Doe', release_year=2012)

The SQL equivalent of above statement is ``UPDATE beesql_version SET release_manager='John Smith' WHERE release_manager='John Doe' AND release_year=2012``. Alternatively the where condition can be supplied as a string. For all option which :func:`update` support refer the API. 

**Delete statement**::

    db.delete('beesql_version', version=2.0, release_name='bumblebee')

The SQL equivalent of above statement is ``DELETE FROM beesql_version WHERE version=2.0 AND release_name='bumblebee'``. Alternatively the where condition can be provided as a string. For all option which :func:`delete` support refer the API.

**Closing a connection**::

    db.close() 

**Attributes of connections**::

    db.lastrowid - Insert ID of last statment if last statement was an insert.
    db.lastsql - Generated SQL for last run statement.
    db.lastescapes - Escape values used for last run statement.

MySQL operations
----------------

MySQL connections additionally support following functions.

**Truncate**::

    db.truncate('tablename')

**Drop tables**::

    db.drop_table('table1', 'table2', if_exists=True)

MySQL :func:`drop_table` accepts a variable number of tables to be dropped.

**Use a database**::

    db.use('database_name')

**Create a database**::

    db.create('database_name',  if_not_exists=True)

**Drop database**::
    
    db.drop('database_name', if_exists=True)

SQLite Operations
-----------------

SQLite connections additionally support following functions.

**Drop table**::

    db.drop_table('table_name', if_exists=True)

