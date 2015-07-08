OAR CLI changelog
=================

Version 0.3.5
-------------

Released on July 08th 2015

- Emptied admission_rules table before migration
- Used postgresql binary COPY by default
- Split big queries with BETWEEN clause to improved performance and avoided mysql crashes

Version 0.3.4
-------------

Released on July 07th 2015

- Removed OFFSET sql clause from big queries to avoided mysql crashes

Version 0.3.3
-------------

Released on June 24th 2015

- Added ``--conf`` option to used custom oar configuration file
- Used postgresql COPY with csv format by default
- Fixed modify nullable operation during schema upgrade


Version 0.3.2
-------------

Released on June 23rd 2015

- Minor bug fixes

Version 0.3.1
-------------

Released on June 23rd 2015

- Fixed project description in Pypi

Version 0.3.0
-------------

Released on June 23rd 2015

- Renamed the project to OAR CLI !
- Added ``oar-database-migrate`` script.
- Added ``--schema-only`` and ``--data-only`` features to ``oar-database-migrate`` script
- Supported postgresql bulk insert using COPY clause to improved performance.
- Handled database connection errors.
- Managed the schema upgrade with alembic
- Fixed max_job_to_sync query if we want to copy all jobs


Version 0.2.0
-------------

Released on June 05th 2015

Database Archive
~~~~~~~~~~~~~~~~

    - Made deterministic order_by to sync queries in order to avoid IntegrityError during copy (Fixed #1)
    - Handled IntegrityError during bulk INSERT (Fixed #1)
    - Used Postgresql DELETE with the USING clause to improve performance (Fixed #2)
    - Made the delete orphan queries faster with LEFT JOIN in Mysql (Fixed #2)
    - Removed count query when performing a bulk delete query (Fixed #2)
    - Configured debug logging to displayed SQL queries
    - Added option to disable pagination during sync operation
    - Displayed default values in the CLI

Version 0.1.2
-------------

Released on May 05th 2015

- [database-archive] : Added ability to ignore all resources during purge

Version 0.1.1
-------------

Released on May 04th 2015

- Fixed pypi package

Version 0.1.0
-------------

First public preview release.
