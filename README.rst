Python OAR Utils
================

Custom scripts and various utility functions for OAR.

OAR Database archive
--------------------

This script allow you to copy your OAR database to another database for
archiving purposes.

Usage
~~~~~
::

    Usage: oar-database-archive [OPTIONS]

      Archive OAR database.

    Options:
      --version              Show the version and exit.
      --sql                  Dump the SQL (offline mode)
      --db-url TEXT          the url for your OAR database.
      --db-archive-url TEXT  the url for your archive database.
      -h, --help             Show this message and exit.


Examples
~~~~~~~~

::

  oar-database-archive --db-archive-url postgresql://user:password@server:5432/oar_archive

This command will dump your current OAR database (configured in oar.conf) in
the given database. The second run will only copy the missing rows.

**From postgresql to mysql**::

  oar-database-archive --db-url postgresql://user:password@server1:5432/oar \
                       --db-archive-url mysql://user:password@server2:3306/oar_archive

**From mysql to sqlite**::

  oar-database-archive --db-url mysql://user:password@server1:3306/oar \
                       --db-archive-url sqlite:////tmp/archive.db
