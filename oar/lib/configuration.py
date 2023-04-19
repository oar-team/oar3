# -*- coding: utf-8 -*-
import pprint
import sys
from io import open

from .exceptions import InvalidConfiguration
from .utils import reraise, try_convert_decimal


class Configuration(dict):
    """Works exactly like a dict but provides ways to fill it from files.

    :param defaults: an optional dictionary of default values
    """

    DEFAULT_CONFIG_FILE = "/etc/oar/oar.conf"

    DEFAULT_CONFIG = {
        "DB_PORT": "5432",
        "DB_TYPE": "Pg",
        "SQLALCHEMY_ECHO": False,
        "SQLALCHEMY_POOL_SIZE": None,
        "SQLALCHEMY_POOL_TIMEOUT": None,
        "SQLALCHEMY_POOL_RECYCLE": None,
        "SQLALCHEMY_MAX_OVERFLOW": None,
        "LOG_LEVEL": 3,
        "LOG_FILE": ":stderr:",
        "LOG_FORMAT": "[%(levelname)8s] [%(asctime)s] [%(name)s]: %(message)s",
        "OAR_SSH_CONNECTION_TIMEOUT": 120,
        "SERVER_HOSTNAME": "localhost",
        "SERVER_PORT": "6666",
        "APPENDICE_SERVER_PORT": "6670",
        "BIPBIP_COMMANDER_SERVER": "localhost",
        "BIPBIP_COMMANDER_PORT": "6671",
        "LEON_SOFT_WALLTIME": 20,
        "LEON_WALLTIME": 300,
        "TIMEOUT_SSH": 120,
        "SERVER_PROLOGUE_EPILOGUE_TIMEOUT": 60,
        "SERVER_PROLOGUE_EXEC_FILE": None,
        "SERVER_EPILOGUE_EXEC_FILE": None,
        "BIPBIP_OAREXEC_HASHTABLE_SEND_TIMEOUT": 30,
        "DEAD_SWITCH_TIME": 0,
        "OAREXEC_DIRECTORY": "/var/lib/oar",
        "OAREXEC_PID_FILE_NAME": "pid_of_oarexec_for_jobId_",
        "OARSUB_FILE_NAME_PREFIX": "oarsub_connections_",
        "PROLOGUE_EPILOGUE_TIMEOUT": 60,
        "PROLOGUE_EXEC_FILE": None,
        "EPILOGUE_EXEC_FILE": None,
        "SUSPEND_RESUME_SCRIPT_TIMEOUT": 60,
        "SSH_RENDEZ_VOUS": "oarexec is initialized and ready to do the job",
        "JOB_RESOURCE_MANAGER_FILE": "/etc/oar/job_resource_manager_cgroups.pl",
        "MONITOR_FILE_SENSOR": "/etc/oar/oarmonitor_sensor.pl",
        "SUSPEND_RESUME_FILE_MANAGER": "/etc/oar/suspend_resume_manager.pl",
        "OAR_SSH_AUTHORIZED_KEYS_FILE": ".ssh/authorized_keys",
        "NODE_FILE_DB_FIELD": "network_address",
        "NODE_FILE_DB_FIELD_DISTINCT_VALUES": "resource_id",
        "NOTIFY_TCP_SOCKET_ENABLED": 1,
        "SUSPECTED_HEALING_TIMEOUT": 10,
        "SUSPECTED_HEALING_EXEC_FILE": None,
        "DEBUG_REMOTE_COMMANDS": "YES",
        "COSYSTEM_HOSTNAME": "127.0.0.1",
        "DEPLOY_HOSTNAME": "127.0.0.1",
        "OPENSSH_CMD": "/usr/bin/ssh -p 6667",
        "OAREXEC_DEBUG_MODE": "1",
        "HULOT_SERVER": "localhost",
        "HULOT_PORT": 6672,
        # kao
        "METASCHEDULER_MODE": "internal",
        # Tell the metascheduler that it runs into an oar2 installation.
        "METASCHEDULER_OAR3_WITH_OAR2": "no",
        "HIERARCHY_LABELS": "resource_id,network_address",
        "SCHEDULER_RESOURCE_ORDER": "resource_id ASC",
        "SCHEDULER_JOB_SECURITY_TIME": "60",  # TODO should be int
        "SCHEDULER_AVAILABLE_SUSPENDED_RESOURCE_TYPE": "default",
        "FAIRSHARING_ENABLED": "no",
        "SCHEDULER_FAIRSHARING_MAX_JOB_PER_USER": "30",
        "RESERVATION_WAITING_RESOURCES_TIMEOUT": "300",
        "SCHEDULER_TIMEOUT": "10",
        "ENERGY_SAVING_INTERNAL": "no",
        "SCHEDULER_NODE_MANAGER_WAKEUP_TIME": 1,
        "EXTRA_METASCHED": "default",
        "EXTRA_METASCHED_CONFIG": "",
        "ENERGY_SAVING_MODE": "",
        "WALLTIME_ALLOWED_USERS_TO_DELAY_JOBS": "",
        "WALLTIME_MAX_INCREASE": "{'default': 7200}",
        "WALLTIME_ALLOWED_USERS_TO_FORCE": "{'_': '*', 'besteffort': ''}",
        "WALLTIME_CHANGE_ENABLED": "NO",
        "WALLTIME_MIN_FOR_CHANGE": 0.0,
        "WALLTIME_CHANGE_APPLY_TIME": 0.0,
        "WALLTIME_INCREMENT": 0.0,
        "SCHEDULER_FAIRSHARING_WINDOW_SIZE": 3600 * 30 * 24,
        "SCHEDULER_FAIRSHARING_PROJECT_TARGETS": "{default => 21.0}",
        "SCHEDULER_FAIRSHARING_USER_TARGETS": "{default => 22.0}",
        "SCHEDULER_FAIRSHARING_COEF_PROJECT": "0",
        "SCHEDULER_FAIRSHARING_COEF_USER": "1",
        "SCHEDULER_FAIRSHARING_COEF_USER_ASK": "1",
        "QUOTAS": "no",
        "QUOTAS_CONF_FILE": "/etc/oar/quotas_conf.json",
        "QUOTAS_PERIOD": 1296000,  # 15 days in seconds
        "QUOTAS_ALL_NB_RESOURCES_MODE": "default_not_dead",  # ALL w/ correspond to all default source
        "QUOTAS_WINDOW_TIME_LIMIT": 4
        * 1296000,  # 2 months, window time limit for a scheduling round where to place a job
        "PROXY": "no",  # or treafik this only one supported proxy
        "PROXY_TRAEFIK_ENTRYPOINT": "http://localhost:5000",
        "PROXY_TRAEFIK_RULES_FILE": "/etc/oar/rules_oar_traefik_proxy.toml",
        "OAR_PROXY_BASE_URL": "/proxy",
        "JOB_PRIORITY": "FIFO",
        "CUSTOM_JOB_SORTING": "",
        "CUSTOM_JOB_SORTING_CONFIG_FILE": "",
        "STAGEIN_DIR": "/tmp",
        "DEFAULT_JOB_WALLTIME": 3600,
        "OARSUB_DEFAULT_RESOURCES": "/resource_id=1",
        "OARSUB_NODES_RESOURCES": "resource_id",
        "QUEUE": "default",
        "PROJECT": "default",
        "SIGNAL": 12,
        # Hulot stuff
        "HULOT_SERVER": "localhost",
        "HULOT_PORT": 6672,
        "ENERGY_SAVING_WINDOW_FORKER_SIZE": 20,
        "ENERGY_SAVING_WINDOW_TIME": 60,
        "ENERGY_SAVING_WINDOW_TIMEOUT": 120,
        "ENERGY_SAVING_NODE_MANAGER_WAKEUP_TIMEOUT": 900,
        "ENERGY_MAX_CYCLES_UNTIL_REFRESH": 5000,
        "ENERGY_SAVING_NODES_KEEPALIVE": "type='default':0",
    }

    def __init__(self, defaults=None):
        defaults = dict(self.DEFAULT_CONFIG)
        defaults.update(defaults or {})
        dict.__init__(self, defaults)

    def load_default_config(self, silent=True):
        self.load_file(self.DEFAULT_CONFIG_FILE, silent=silent)

    def load_file(
        self, filename, comment_char="#", strip_quotes=True, silent=False, clear=False
    ):
        """Updates the values in the config from a config file.
        :param filename: the filename of the config.  This can either be an
            absolute filename or a filename relative to the root path.
        :param comment_char: The string character used to comment line.
        :param strip_quotes: Strip the quotes.
        :param silent: If True, fail silently.
        :param clear: If True, delete clear all old values before loading
            the new file.
        """
        try:
            conf = {}
            equal_char = "="
            with open(filename, encoding="utf-8") as config_file:
                for line in config_file:
                    if comment_char in line:
                        line, _ = line.split(comment_char, 1)
                    if equal_char in line:
                        key, value = line.split(equal_char, 1)
                        key = key.strip()
                        value = value.strip()
                        value = value.strip("\"'")
                        value = try_convert_decimal(value)
                        conf[key] = value
            if clear:
                self.clear()
            for k, v in conf.items():
                self[k] = v
        except IOError as e:
            e.strerror = "Unable to load configuration file (%s)" % e.strerror
            if silent:
                # from . import logger

                # logger.warning(e.strerror)
                return False
            else:
                exc_type, exc_value, tb = sys.exc_info()
                reraise(exc_type, exc_value, tb.tb_next)

        return True

    def get_sqlalchemy_uri(self, read_only=False):
        if read_only:
            login = "base_login_ro"
            passwd = "base_passwd_ro"
        else:
            login = "base_login"
            passwd = "base_passwd"
        try:
            db_conf = self.get_namespace("DB_")
            db_conf["type"] = db_conf["type"].lower()
            if db_conf["type"] == "sqlite":
                return "{type}:///{base_file}".format(**db_conf)
            elif db_conf["type"] in ("pg", "psql", "pgsql"):
                db_conf["type"] = "postgresql"
            if db_conf.get(passwd, "") != "":
                auth = "{%s}:{%s}" % (login, passwd)
            else:
                auth = "{%s}" % login
            url = "{type}://%s@{hostname}:{port}/{base_name}" % (auth)
            return url.format(**db_conf)
        except KeyError as e:
            keys = tuple(("DB_%s" % i.upper() for i in e.args))
            raise InvalidConfiguration("Cannot find %s" % keys)

    def setdefault_config(self, default_config):
        # import pdb; pdb.set_trace()
        for k, v in default_config.items():
            self.setdefault(k, v)

    def get_namespace(self, namespace, lowercase=True, trim_namespace=True):
        """Returns a dictionary containing a subset of configuration options
        that match the specified namespace/prefix. Example usage::

            config['OARSUB_DEFAULT_RESOURCES'] = 2
            config['OARSUB_FORCE_JOB_KEY'] = '/resource_id=1'
            config['OARSUB_NODES_RESOURCES'] = 'network_address'
            oarsub_config = config.get_namespace('OARSUB_')

        The resulting dictionary `oarsub_config` would look like::

            {
                'default_resources': 2,
                'force_job_key': '/resource_id=1',
                'nodes_resources': 'network_address'
            }

        This is often useful when configuration options map directly to keyword
        arguments in functions or class constructors.

        :param namespace: a configuration namespace
        :param lowercase: a flag indicating if the keys of the resulting
        dictionary should be lowercase
        :param trim_namespace: a flag indicating if the keys of the resulting
        dictionary should not include the namespace
        """
        rv = {}
        for k, v in self.items():
            if not k.startswith(namespace):
                continue
            if trim_namespace:
                key = k[len(namespace) :]
            else:
                key = k
            if lowercase:
                key = key.lower()
            rv[key] = v
        return rv

    def __str__(self):
        return pprint.pprint(self)
