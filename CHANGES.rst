OAR lib changes
===============

Version 0.4.1
-------------

Released on September 30th 2015

- [all] Switched the license to BSD
- [all] Dropped python 3.3 support
- [all] Little code refactoring

- [database] Supported session read-only mode via oar_reader user
- [database] ``engine.execute`` is not used directly anymore, prefer ``session.execute``
- [database] Used the same database connection with alembic and sqlalchemy
- [database] Added ephemeral session context manager that will rollback the transaction at the end
- [database] Added ``db.show()`` method to quickly show the database content
- [database] Used truncate instead of delete on postgresql in ``db.delete_all()``
- [database] ``db.close()`` method closes the session and disposes the engine
- [database] Fixed ``db.close`` method by removing deadlock
- [database] Raised ``KeyError`` exception from ``Database`` if wanted ``Model`` or ``Table`` are missing
- [database] Configured automatically default table args with ``DeclarativeMeta`` class
- [database] Created ``DeferredReflectionModel`` metaclass per ``Database`` instance to avoid conflict during database reflection
- [database] Collected declared tables and models when they are defined
- [database] Avoided circular import of db object
- [database] Generated a tablename if none specified

- [models] The ``__repr__`` method in ``Model`` classes include the identity of the object (primary key)
- [models] Set column ``Resource.scheduler_priority`` to ``BigInteger``
- [models] Added ``BaseModel.to_json method()`` method to convert a sqlalchemy entity to json
- [models] Added ``BaseModel.to_dict()`` method to convert a sqlalchemy entity to a dictionary
- [models] Fixed ``BaseModel.create()`` method

- [utils] Added ability to run command without timeout
- [utils] Used decimal module to check if input is a number
- [utils] Reset ``ResultProxyIter`` iterator automatically
- [utils] Fixed ``ResultProxyIter`` rowcount
- [utils] Reset ``cached_property`` attribut when deleted
- [utils] Added ``merge_dicts()`` that merge given dictionaries into a new dictionary

- [basequery] Updated ``Query.render()`` method to return a special string that have useful ``__repr__``
- [basequery] Added ``BaseQuery.get_jobs_for_user()`` query

- [test] Tested on postgresql and sqlite-file/memory with tox and travis.ci
- [test] Added test helper scripts to populate database
- [test] Fixed tests on python 3.4
- [test] More tests

- [compat] Removed unused ``SimpleNamespace`` class from compat module


- [all] Moved submission core parts from oar.cli.oarsub to oar-lib
- [all] Switched the license to BSD
- [all] Moved interval, hierarchy, resource and tools from oar.kao to oar-lib
- [all] Dropped python 3.3 support
- [all] Little code refactoring

- [database] Supported session read-only mode via oar_reader user
- [database] ``engine.execute`` is not used directly anymore, prefer ``session.execute``
- [database] Used the same database connection with alembic and sqlalchemy
- [database] Added ephemeral session context manager that will rollback the transaction at the end
- [database] Added ``db.show()`` method to quickly show the database content
- [database] Used truncate instead of delete on postgresql in ``db.delete_all()``
- [database] ``db.close()`` method closes the session and disposes the engine
- [database] Fixed ``db.close`` method by removing deadlock
- [database] Raised ``KeyError`` exception from ``Database`` if wanted ``Model`` or ``Table`` are missing
- [database] Configured automatically default table args with ``DeclarativeMeta`` class
- [database] Created ``DeferredReflectionModel`` metaclass per ``Database`` instance to avoid conflict during database reflection
- [database] Collected declared tables and models when they are defined
- [database] Avoided circular import of db object
- [database] Generated a tablename if none specified

- [models] The ``__repr__`` method in ``Model`` classes include the identity of the object (primary key)
- [models] Set column ``Resource.scheduler_priority`` to ``BigInteger``
- [models] Added ``BaseModel.to_json method()`` method to convert a sqlalchemy entity to json
- [models] Added ``BaseModel.to_dict()`` method to convert a sqlalchemy entity to a dictionary
- [models] Fixed ``BaseModel.create()`` method

- [utils] Added ability to run command without timeout
- [utils] Used decimal module to check if input is a number
- [utils] Reset ``ResultProxyIter`` iterator automatically
- [utils] Fixed ``ResultProxyIter`` rowcount
- [utils] Reset ``cached_property`` attribut when deleted
- [utils] Added ``merge_dicts()`` that merge given dictionaries into a new dictionary

- [basequery] Updated ``Query.render()`` method to return a special string that have useful ``__repr__``
- [basequery] Added ``BaseQuery.get_jobs_for_user()`` query

- [test] Tested on postgresql and sqlite-file/memory with tox and travis.ci
- [test] Added test helper scripts to populate database
- [test] Fixed tests on python 3.4
- [test] More tests

- [compat] Removed unused ``SimpleNamespace`` class from compat module


Version 0.3.0
-------------

Released on July 07th 2015

- [database] Fixed ``Database.delete_all`` method to remove all database content
- [database] Listed all datetime columns in ``models.TIME_COLUMNS``
- [database] Made table and model import easier with getitem syntax (Eg. db['table_name'])
- [database] Added an alembic operator as ``Database.op`` attribut
- [database] Kept columns order during dictionary conversion
- [database] Put MySQLdb SSCursor tweak on standby

- [models] Added ``ResultProxyIter`` class that make SQLAlchemy ResultProxies iterable by dicts
- [models] Fixed columns orders for admission_rules table
- [models] ``models.all_tables`` returns a dictionary with table names as keys now
- [models] Forwarded log to STDOUT if ``get_logger(..)`` receive ``stdout=True``

- [configuration] New default configuration values for *DB_PORT=5432* and *DB_TYPE=Pg*
- [configuration] Handled ``OARCONFFILE`` environment variable to load OAR configuration

- [basequery] Added ``Query.render`` that generate an SQL expression string from statement
- [basequery] Added ``get_gantt_visu_scheduled_jobs_resources`` method that returns all nodes allocated to a (waiting) reservation
- [basequery] Added ``get_assigned_jobs_resources`` and groups results by job_id
- [basequery] Optimized get_user_jobs query by using JOIN instead of IN operator
- [basequery] Loaded only some columns with Load ORM object


- [utils] Added ``utils.Command`` class to run subprocess commands with a timeout option
- [utils] Moved ``JSONEncoder`` class from oar-rest-api to oar-lib
- [utils] Removed unsed ``IterStream`` class
- [utils] Added ``utils.row2dict`` function helpers to convert a RowProxy to a dict

- [compat] Used simplejson if available

Version 0.2.0
-------------

Released on June 23rd 2015

- [config] Added clear parameter to clear the config before loading a new file
- [config] only one default configuration file
- [config] Made load file configuration atomic
- [config] Warned user when configuration loading failed
- [compat] Used iterator version of zip and range method
- [compat] Removed unused string_types

- [database] pg_bulk_insert that use COPY clause to perform batch inserts
- [database] Added db.queries object that included all oar-lib sql queries
- [database] Workaround to support table inheritance and __table_args__
- [database] Moved the BaseQuery class to the basequery module

- [models] Added missings relations between tables as dicts
- [models] Used BigInteger type on Accounting fields
- [models] Added all_tables method to get all tables

- [basequery] make models module easier to read
- [basequery] Added get_job_resources query
- [basequery] Added filter_jobs_for_user method to build jobs query
- [basequery] Added get_resources
- [basequery] Added get_jobs_for_user query

- [utils] Added IterStream class that give a stream like interface for any iterator

- Added alembic and sqlalchemy-utils requirements

Version 0.1.1
-------------

Released on April 30th 2015

- Minor bugfixes

Version 0.1.0
-------------

Released on April 21st 2015

First public release of oar-lib
