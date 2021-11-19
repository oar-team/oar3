import fileinput
import os
import shutil
import sys
import tempfile
import time

import click
import ptpython.repl

import oar
import oar.kao.kamelot as kamelot
import oar.lib
import oar.lib.tools as tools
from oar.lib import (  # EventLogHostname,; ResourceLog,
    Accounting,
    AssignedResource,
    Challenge,
    EventLog,
    FragJob,
    GanttJobsPrediction,
    GanttJobsPredictionsVisu,
    GanttJobsResource,
    GanttJobsResourcesVisu,
    Job,
    JobDependencie,
    JobResourceDescription,
    JobResourceGroup,
    JobStateLog,
    JobType,
    MoldableJobDescription,
    Resource,
    config,
    db,
)
from oar.lib.job_handling import insert_job  # , gantt_flush_tables
from oar.lib.tools import PIPE, Popen, TimeoutExpired, local_to_sql

from .utils import CommandReturns

click.disable_unicode_literals_warning = True

DEFAULT_CONFIG = {
    "DB_BASE_FILE": ":memory:",
    "DB_TYPE": "sqlite",
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
    "SCHEDULER_FAIRSHARING_MAX_JOB_PER_USER": 30,
    "SCHEDULER_GANTT_HOLE_MINIMUM_TIME": 300,
    "SCHEDULER_JOB_SECURITY_TIME": 60,
    "SCHEDULER_NB_PROCESSES": 1,
    "SCHEDULER_PRIORITY_HIERARCHY_ORDER": "network_address/resource_id",
    # "SCHEDULER_RESOURCE_ORDER':
    #    'scheduler_priority ASC, state_num ASC, available_upto DESC, '
    #    'suspended_jobs ASC, network_address ASC, resource_id ASC',
    "SCHEDULER_RESOURCE_ORDER": "resource_id ASC",
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
    "HIERARCHY_LABELS": "resource_id,network_address,cpu,core",
}

db = db
FILE_RESULT = None
oar2_path = None
try:
    hash_oar2 = os.readlink(shutil.which("Almighty")).split("/")[3]
    oar2_path = f"/nix/store/{hash_oar2}/oar"
except Exception as err:
    print(f"Does not find oar2_path when considering Nix context: {err}")

if oar2_path:
    oar2_path_sched = f"{oar2_path}/schedulers"
    oar2_timesharing = f"{oar2_path_sched}/oar_sched_gantt_with_timesharing"


def print_res(msg, mode="all"):
    if mode == "all":
        print(msg)
    if FILE_RESULT:
        FILE_RESULT.write(msg)


def setup_config(db_type="memory"):
    config.update(DEFAULT_CONFIG.copy())
    tempdir = tempfile.mkdtemp()
    print(f"Temporay directory for oar.log: {tempdir}")
    config["LOG_FILE"] = os.path.join(tempdir, "oar.log")

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

    def dump_configuration(filename):
        folder = os.path.dirname(filename)
        if not os.path.exists(folder):
            os.makedirs(folder)
        with open(filename, "w", encoding="utf-8") as fd:
            for key, value in config.items():
                if not key.startswith("SQLALCHEMY_"):
                    fd.write("%s=%s\n" % (key, str(value)))

    dump_configuration("/etc/oar/oar.conf")

    # if db_type == "Pg":
    #     drop_db()
    #     create_db()
    # else:
    # dump_configuration("/tmp/oar.conf")
    db.metadata.drop_all(bind=db.engine)
    db.create_all(bind=db.engine)

    kw = {"nullable": True}
    db.op.add_column("resources", db.Column("core", db.Integer, **kw))
    db.op.add_column("resources", db.Column("cpu", db.Integer, **kw))
    db.op.add_column("resources", db.Column("host", db.String(255), **kw))
    db.op.add_column("resources", db.Column("mem", db.Integer, **kw))
    db.reflect()


def create_resources(nb_nodes=8, nb_cpus=2, nb_cores=16):
    resources = []
    for i in range(nb_nodes):
        for k in range(nb_cpus):
            for m in range(nb_cores):
                resources.append({"network_address": f"node{i}", "cpu": f"{k}"})
    db.session.execute(oar.lib.Resource.__table__.insert(), resources)
    db.commit()


# Add queue
def create_jobs(nb_jobs=10, job_resources="resource_id=2", mode="same", mode_args={}):
    print_res("# create_job")
    print_res(f"# mode: {mode}")
    print_res(f"# nb_jobs: {nb_jobs}")
    if mode == "same":
        # if resources:
        #    print_res(f"# resources: {resources} ")
        for i in range(nb_jobs):
            insert_job(res=[(60, [(job_resources, "")])], properties="")
    else:
        click.echo(f"Job's mode creation does not exist {mode}")
        raise click.Abort()
    db.commit()


def delete_resources():
    db.query(Resource).delete(synchronize_session=False)
    db.commit()


def delete_gantt_tables():
    db.query(GanttJobsPrediction).delete(synchronize_session=False)
    db.query(GanttJobsResource).delete(synchronize_session=False)
    db.query(GanttJobsPredictionsVisu).delete(synchronize_session=False)
    db.query(GanttJobsResourcesVisu).delete(synchronize_session=False)
    db.commit()


def delete_all():
    db.delete_all()
    db.commit()


def delete_jobs():
    for t in [
        Job,
        JobDependencie,
        JobResourceDescription,
        JobResourceGroup,
        JobStateLog,
        JobType,
        MoldableJobDescription,
        Challenge,
        FragJob,
        EventLog,
        AssignedResource,
        Accounting,
    ]:
        db.query(t).delete(synchronize_session=False)


def len_gantt_jobs_prediction():
    return len(db.query(GanttJobsPrediction).all())


def init_db(mode="reuse"):
    if mode == "reuse":
        pass


def drop_db():
    tools.call("oar-database-manage drop", shell=True)

    for user in [config["DB_BASE_LOGIN"], config["DB_BASE_LOGIN_RO"]]:
        tools.call(f"sudo -u postgres psql postgres -c 'drop user {user}'", shell=True)


def create_db():
    tools.call("rm /var/lib/oar/db-created && systemctl start oardb-init", shell=True)


def change_in_place_conf(var="LOG_LEVEL", value='"3"', confile="/etc/oar/oar.conf"):
    print(f"try do change value of {var} to {value}")
    n = 0
    with fileinput.FileInput(confile, inplace=True, backup=".bak") as f:
        for line in f:
            if var in line:
                print(f"{var}={value}", end="\n")
                n = n + 1
            else:
                print(line, end="")
    print(f"nb changes: {n}")


def launch_kamelot_intern(nb_jobs=10, queue="default"):
    now = time.time()
    sys.argv = ["test_kamelot", "default", now]
    kamelot.main()
    return time.time() - now


def minimal_bench_kamelot_intern(nb_jobs=10):
    setup_config()
    create_resources()
    create_jobs(nb_jobs)

    launch_kamelot_intern(nb_jobs=10, queue="default")

    print(f"#nb_scheduled jobs: {len_gantt_jobs_prediction()}")


def launch_scheduler(scheduler="kamelot", queue="default", nb_jobs=10, timeout=300):

    now = time.time()

    initial_time_sec = now
    initial_time_sql = local_to_sql(initial_time_sec)

    cmd = [scheduler, queue, str(initial_time_sec), initial_time_sql]
    proc = Popen(
        cmd,
        stdin=PIPE,
        stdout=PIPE,
        stderr=PIPE,
    )

    try:
        out, err = proc.communicate()
    except TimeoutExpired:
        print("Scheduler TimeoutExpired")
        proc.kill()
        outs, errs = proc.communicate(timeout=300)

    output = out.decode()
    error = err.decode()

    if output:
        print(output)
    if error:
        print(error)

    return time.time() - now


def minimal_bench_scheduler(
    scheduler="kamelot", queue="default", nb_jobs=10, timeout=300
):

    create_resources()
    create_jobs(nb_jobs)

    launch_scheduler(scheduler, queue, nb_jobs, timeout)

    print(f"nb_scheduled jobs: {len_gantt_jobs_prediction()}")


def simple_bench_scheduler(
    scheduler="kamelot",
    nb_node_list=[1, 10, 100],
    nb_job_list=[100],
    job_nb_resources=2,
    file_res=None,
):

    nb_cpus = 2
    nb_cores = 16

    if job_nb_resources:
        assert job_nb_resources < nb_cpus * nb_cores
        job_resources = f"resource_id={job_nb_resources}"
    else:
        click.echo("not yet implemented")
        raise click.Abort()

    for nb_nodes in nb_node_list:
        delete_resources()
        # setup_config(db_type="Pg")
        create_resources(nb_nodes, nb_cpus, nb_cores)
        nb_resources = nb_nodes * nb_cpus * nb_cores
        for nb_jobs in nb_job_list:
            delete_jobs()
            delete_gantt_tables()
            create_jobs(nb_jobs, job_resources)
            t = launch_scheduler(scheduler, "default", nb_jobs)
            result = f"{nb_jobs} {job_nb_resources} {nb_resources} {t}"
            print(result)
            if file_res:
                file_res.write(result)
    delete_resources()
    delete_jobs()
    delete_gantt_tables()


def oarbench(bench_file, version):
    cmd_ret = CommandReturns(cli)
    if version:
        cmd_ret.print_("OAR version : " + oar.VERSION)
        return cmd_ret

    # user = os.environ["USER"]
    # if "OARDO_USER" in os.environ:
    #     user = os.environ["OARDO_USER"]

    # if not (user == "oar" or user == "root"):
    #     comment = "You must be oar or root"
    #     cmd_ret.error(comment, 1, 8)
    #     return cmd_ret

    ptpython.repl.embed(locals(), globals())

    return cmd_ret


@click.command()
@click.option(
    "-f ", "--bench-file", type=click.STRING, help="Benchmark configuration file."
)
@click.option("-V", "--version", is_flag=True, help="Print OAR version.")
def cli(bench_file, version):
    """Send a message tag to OAR's Almighty"""
    cmd_ret = oarbench(bench_file, version)
    cmd_ret.exit()