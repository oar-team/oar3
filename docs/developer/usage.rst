Usage
=====

Read Configuration
------------------

::

    >>> from oar import config
    >>>
    >>> config.load_file("/path/to/config")
    True
    >>> config["ENERGY_SAVING_INTERNAL"]
    'no'
    >>> config.get_namespace('OARSUB_')
    {'default_resources': '/resource_id=1', 'force_job_key': 'no', 'nodes_resources': 'network_address'}


Query the database
------------------

::

    >>> from oar import db, Resource
    >>> db
    <Database engine=None>

::

    >>> db.query(Resource).first()
    <oar.models.Resource object at 0x2becb10>
    >>> Resource.query.first()
    <oar.models.Resource object at 0x2becb10>

::

    >>> db
    <Database engine=Engine(postgresql://oar:***@server:5432/oar)>

::

    >>> for r in Resource.query.filter(Resource.core > 3).limit(2):
    ...     print(r.id, r.network_address)
    ...
    (12L, u'node3')
    (9L, u'node3')

::

    >>> req = db.query(Resource.id, Resource.network_address)
    >>> req.filter(Resource.core > 3).limit(2).all()
    [(12L, u'node3'), (9L, u'node3')]


Logger
------

Logging from the root logger::

    >> from oar import logger as log
    >> log.info("Init confiuration")
    ## [INFO] [2014-12-05 14:13:42,239] [oar]: Init confiuration

Logging from your own sublogger::

    >> from oar import get_logger
    >> log = get_logger("oar.kao")
    >> log.error("fatal error")
    ## [ERROR] [2014-12-05 14:14:32,116] [oar.kao]: fatal error

