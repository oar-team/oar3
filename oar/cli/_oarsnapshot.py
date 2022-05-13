import os
import sys
import tempfile
import time

import click

import oar.kao.kamelot as kamelot
from oar.lib import Resource, config, db  # EventLogHostname,; ResourceLog,
from oar.lib.job_handling import insert_job  # , gantt_flush_tables

click.disable_unicode_literals_warning = True

DEFAULT_CONFIG = {
    "DETACH_JOB_FROM_SERVER": 1,
    "ENERGY_SAVING_INTERNAL": "no",
    "LOG_CATEGORIES": "all",
    "LOG_FILE": "",
    "LOG_FORMAT": "[%(levelname)s] [%(asctime)s] [%(name)s]: %(message)s",
    "LOG_LEVEL": 3,
    "OARSUB_DEFAULT_RESOURCES": "/resource_id=1",
    "OARSUB_FORCE_JOB_KEY": "no",
    "OARSUB_NODES_RESOURCES": "network_address",
    "OAR_RUNTIME_DIRECTORY": "/var/lib/oar",
    "SCHEDULER_AVAILABLE_SUSPENDED_RESOURCE_TYPE": "default",
    "SCHEDULER_GANTT_HOLE_MINIMUM_TIME": 300,
    "SCHEDULER_JOB_SECURITY_TIME": 60,
    "SCHEDULER_NB_PROCESSES": 1,
    "SCHEDULER_PRIORITY_HIERARCHY_ORDER": "network_address/resource_id",
    "SCHEDULER_RESOURCE_ORDER": "scheduler_priority ASC, state_num ASC, available_upto DESC, suspended_jobs ASC, network_address ASC, resource_id ASC",
    # SCHEDULER_RESOURCE_ORDER": "resource_id ASC",
    "SCHEDULER_TIMEOUT": 30,
    "SERVER_HOSTNAME": "server",
    "SERVER_PORT": 6666,
    "SQLALCHEMY_ECHO": False,
    "SQLALCHEMY_MAX_OVERFLOW": None,
    "SQLALCHEMY_POOL_RECYCLE": None,
    "SQLALCHEMY_POOL_SIZE": None,
    "SQLALCHEMY_POOL_TIMEOUT": None,
    "TAKTUK_CMD": "/usr/bin/taktuk -t 30 -s",
    "QUOTAS": "no",
    "QUOTAS_PERIOD": 1296000,  # 15 days in seconds
    "QUOTAS_WINDOW_TIME_LIMIT": 4 * 1296000,  # 2 months
    "HIERARCHY_LABELS": "resource_id,network_address",
    # OAR2 scheduler
    "SCHEDULER_FAIRSHARING_MAX_JOB_PER_USER": 1000,
    "JOB_RESOURCE_MANAGER_PROPERTY_DB_FIELD": "cpuset",
}

CONFIGURATION_FILE = "/tmp/oar3.conf"


def create_resources(resources_csv):
    headers = None
    headers_name = {}
    kw = {"nullable": True}
    resources_cols = Resource.__table__.columns.keys()

    with open(resources_csv) as csv:
        for line in csv:
            splitted_line = line.strip().split(",")
            # First get the headers to locate the needed information
            if headers is None:
                headers = {}
                for header in range(len(splitted_line)):
                    headers[
                        splitted_line[header]
                    ] = header  # header[header_name] = header_idx
                    headers_name[header] = splitted_line[header]
                    if splitted_line[header] not in resources_cols:
                        db.op.add_column(
                            "resources",
                            db.Column(splitted_line[header], db.String(255), **kw),
                        )
        db.commit()


def setup_database(resources_csv):
    db_type = os.environ.get("DB_TYPE", "psql")
    os.environ.setdefault("DB_TYPE", db_type)
    tempdir = tempfile.mkdtemp()

    if db_type == "sqlite":
        config["DB_BASE_FILE"] = os.path.join(tempdir, "db.sqlite")
        config["DB_TYPE"] = "sqlite"
    elif db_type == "memory":
        config["DB_TYPE"] = "sqlite"
        config["DB_BASE_FILE"] = ":memory:"
    else:
        config["DB_TYPE"] = "Pg"
        config["DB_PORT"] = "5432"
        config["DB_BASE_NAME"] = os.environ.get("POSTGRES_DB", "oar")
        config["DB_BASE_PASSWD"] = os.environ.get("POSTGRES_PASSWORD", "oar")
        config["DB_BASE_LOGIN"] = os.environ.get("POSTGRES_USER", "oar")
        config["DB_BASE_PASSWD_RO"] = os.environ.get("POSTGRES_PASSWORD", "oar_ro")
        config["DB_BASE_LOGIN_RO"] = os.environ.get("POSTGRES_USER_RO", "oar_ro")
        config["DB_HOSTNAME"] = os.environ.get("POSTGRES_HOST", "localhost")

    db.metadata.drop_all(bind=db.engine)
    db.create_all(bind=db.engine)

    create_resources(resources_csv)
    db.reflect()


def setup_config(extra_config={}, db_type="memory"):
    # config.update(DEFAULT_CONFIG.copy())
    # Override with function parameters
    config.update(extra_config)

    tempdir = tempfile.mkdtemp()
    print(f"Temporay directory for oar.log: {tempdir}")
    config["LOG_FILE"] = os.path.join(tempdir, "oar.log")

    config["DB_TYPE"] = "Pg"
    config["DB_PORT"] = "5432"
    config["DB_BASE_NAME"] = os.environ.get("POSTGRES_DB", "oar")
    config["DB_BASE_PASSWD"] = os.environ.get("POSTGRES_PASSWORD", "oar")
    config["DB_BASE_LOGIN"] = os.environ.get("POSTGRES_USER", "oar")
    config["DB_BASE_PASSWD_RO"] = os.environ.get("POSTGRES_PASSWORD", "oar_ro")
    config["DB_BASE_LOGIN_RO"] = os.environ.get("POSTGRES_USER_RO", "oar_ro")
    config["DB_HOSTNAME"] = os.environ.get("POSTGRES_HOST", "server")


def import_table_from_scv(tablename, csv):
    with open(csv, "r") as f:
        conn = db.engine.raw_connection()
        cursor = conn.cursor()
        print(cursor)
        cmd = f"COPY {tablename} FROM STDIN WITH (FORMAT CSV, HEADER TRUE)"
        cursor.copy_expert(cmd, f)
        conn.commit()


def launch_kamelot_intern(queue="default"):
    now = time.time()
    sys.argv = ["test_kamelot", "default", now]
    kamelot.main()
    return time.time() - now


def create_jobs_from_csv(csv_file=None):
    """
    Insert jobs in the database based on a csv file passed in parameters.
    The csv must be formatted as follow:
    ```
    nb_res,walltime
    12,3600
    ```
    """
    # This function might be slow, bet the easy alternative is
    # to add a dependency to pandas. I am not sure it is worth it...
    headers = None
    nb_jobs = 0
    with open(csv_file) as csv:
        for line in csv:
            splitted_line = line.strip().split(",")
            # First get the headers to locate the needed information
            if headers is None:
                headers = {}
                for header in range(len(splitted_line)):
                    headers[splitted_line[header]] = header

            else:
                # Get job information
                job_nb_resources = splitted_line[headers["nb_res"]]
                job_walltime = splitted_line[headers["walltime"]]
                job_user = splitted_line[headers["job_user"]]
                # Create the allocation
                job_resources = f"resource_id={job_nb_resources}"
                # Insert the job in the database
                insert_job(
                    res=[(int(job_walltime), [(job_resources, "")])],
                    properties="",
                    user=job_user,
                )
                nb_jobs += 1

        db.commit()
    return nb_jobs


@click.command()
@click.option(
    "-f", "--folder", type=click.STRING, help="Folder containing the snapshot data."
)
@click.option(
    "-r",
    "--result-file",
    type=click.STRING,
    help="Result file that should contain the data.",
)
@click.option("-V", "--version", is_flag=True, help="Print OAR version.")
def cli(folder, version, result_file):
    """"""
    setup_config()
    setup_database(f"{folder}/resources.csv")

    # Import resources
    import_table_from_scv("resources", f"{folder}/resources.csv")

    # Import job information
    import_table_from_scv(
        "moldable_job_descriptions", f"{folder}/moldable_descriptions.csv"
    )
    import_table_from_scv("job_resource_groups", f"{folder}/job_resource_groups.csv")
    import_table_from_scv(
        "job_resource_descriptions", f"{folder}/job_resource_descriptions.csv"
    )

    import_table_from_scv("jobs", f"{folder}/running_jobs.csv")
    import_table_from_scv("jobs", f"{folder}/queued_jobs.csv")

    launch_kamelot_intern()


if __name__ == "__main__":
    print("prout")
    cli()
