.. BeeSQL documentation master file, created by
   sphinx-quickstart on Tue Apr  3 10:43:12 2012.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

BeeSQL: Pythonic SQL Library!
=============================

Introduction
------------

BeeSQL is a Pythonic SQL Wrapper that helps;

- Minimize boring repetitive steps in Python DB-API.
- Write less SQL.
- Map SQL to Python datastructures.

Programming with relational databases is a real pain because, there are boring repetitive steps even for a single SQL statement, there is no easily mapping mechanism between language datastructures and SQL, writing SQL statements is no fun and changing a database engine breaks your code.

BeeSQL was born out of efforts to write wrapper functions that cut down steps needed to execute SQL statements, allow mapping between Python datastructures and SQL, help minimize writting SQL statements as much as possible and have a unified api that work across all common relational databases.

Although there are wonderful ORM's in the world of Python they are an over-kill for smaller projects. BeeSQL is intended for such situations.

Currently BeeSQL supports following databases.

- MySQL
- SQLite

BeeSQL uses following modules under the hood.

- For MySQL connections:  PyMySQL.
- For SQLite connections: Standard Library module sqlite3.

UserGuide
---------

.. toctree::
    :maxdepth: 2

    user/reqs
    user/install
    user/quickstart

API
---

.. toctree::
    :maxdepth: 2

    api
