Python OAR Utils
================

Custom scripts and various utility functions for OAR.


Installation
------------

Requirements:
  - python >= 2.7
  - python-mysqldb/python-psycopg2 (for oar database archive)

You can install, upgrade, uninstall oar-utils with these commands::

  $ pip install [--user] oar-utils
  $ pip install [--user] --upgrade oar-utils
  $ pip uninstall oar-utils

Or from git (last development version)::

  $ pip install git+https://github.com/oar-team/python-oar-utils.git

Or if you already pulled the sources::

  $ pip install path/to/sources

Or if you don't have pip::

  $ easy_install oar-utils


OAR Database archive
--------------------

This script allow you to copy your OAR database to another database for
archiving purposes.


Usage
~~~~~
::

    Usage: oar-database-archive [OPTIONS] COMMAND1 [ARGS]... [COMMAND2
                                [ARGS]...]...

      Archive OAR database.

    Options:
      --version        Show the version and exit.
      -y, --force-yes  Never prompts for user intervention
      --debug          Enable Debug.
      -h, --help       Show this message and exit.

    Commands:
      inspect  Analyze all databases.
      purge    Purge old resources and old jobs from your current database.
      sync     Send all resources and finished jobs to archive database.



Examples
~~~~~~~~

::

  oar-database-archive sync --archive-db-url postgresql://oar:oar@server:5432/oar_archive

This command will dump your current OAR database (configured in oar.conf) in
the given database. The second run will only copy the missing rows.

**From postgresql to mysql**::

  oar-database-archive sync --current-db-url postgresql://user:password@server1:5432/oar \
                            --archive-db-url mysql://user:password@server2:3306/oar_archive

**From mysql to sqlite**::

  oar-database-archive sync --current-db-url mysql://user:password@server1:3306/oar \
                            --archive-db-url sqlite:////tmp/archive.db
