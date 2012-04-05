.. _api:

beesql
======

.. module:: beesql

BeeSQL operations are executed through a database connection. Create a database connection using :func:`connection`.

.. autofunction:: connection

MySQL
=====

.. module:: beesql.backends.mysql

MySQL connection is used to operate on a MySQL database.

.. autoclass:: MYSQLConnection
   :inherited-members:

SQLite
======

.. module:: beesql.backends.sqlite

SQLite connection is used to operate on a SQLite database.

.. autoclass:: SQLITEConnection
   :inherited-members:
