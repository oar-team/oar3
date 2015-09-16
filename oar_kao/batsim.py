#!/usr/bin/env python
# coding: utf-8
from __future__ import unicode_literals, print_function

import argparse
import struct
import socket
import sys
import os
import json
if sys.version_info[0] == 2:
    from sets import Set

from oar.lib import (db, config, get_logger, Job, Resource, Queue)
from oar.lib.compat import iteritems

from oar.kao.job import (insert_job, set_job_state)

from oar.kao.simsim import ResourceSetSimu, JobSimu
from oar.kao.interval import itvs2ids
from oar.kao.kamelot import schedule_cycle
from oar.kao.platform import Platform

from oar.kao.meta_sched import meta_schedule
import oar.kao.utils

offset_idx = 0
plt = None

BATSIM_DEFAULT_CONFIG = {
    'DB_BASE_FILE': ':memory:',
    'DB_TYPE': 'sqlite',
    'LOG_CATEGORIES': 'all',
    'LOG_FILE': '',
    'LOG_FORMAT': '[%(levelname)s] [%(asctime)s] [%(name)s]: %(message)s',
    'LOG_LEVEL': 3,
    'HIERARCHY_LABEL': 'resource_id,network_address',
    'SCHEDULER_RESOURCE_ORDER': 'resource_id ASC',
    'SCHEDULER_JOB_SECURITY_TIME': '60',
    'SCHEDULER_AVAILABLE_SUSPENDED_RESOURCE_TYPE': 'default',
    'FAIRSHARING_ENABLED': 'no',
    'SCHEDULER_FAIRSHARING_MAX_JOB_PER_USER': '3',
    'QUOTAS': 'no',
    'SCHEDULER_RESOURCE_ORDER': 'resource_id ASC',
    'SCHEDULER_TIMEOUT': 30,
    'SERVER_HOSTNAME': 'server',
    'SERVER_PORT': 6666,
    'ENERGY_SAVING_INTERNAL': 'no',
    'SQLALCHEMY_CONVERT_UNICODE': True,
    'SQLALCHEMY_ECHO': False,
    'SQLALCHEMY_MAX_OVERFLOW': None,
    'SQLALCHEMY_POOL_RECYCLE': None,
    'SQLALCHEMY_POOL_SIZE': None,
    'SQLALCHEMY_POOL_TIMEOUT': None,
    'TAKTUK_CMD': '/usr/bin/taktuk -t 30 -s',
}

# config.clear()
# config.update(BATSIM_DEFAULT_CONFIG)

#config['LOG_FILE'] = '/tmp/yop'

logger = get_logger("oar.batsim")

jobs = {}
jobs_completed = []
jobs_waiting = []

sched_delay = 5.0

nb_completed_jobs = 0
nb_jobs = 0
nb_res = 0


def create_uds(uds_name):
    # Make sure the socket does not already exist
    try:
        os.unlink(uds_name)
    except OSError:
        if os.path.exists(uds_name):
            raise

    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)

    # Bind the socket to the port
    print('starting up on %s' % uds_name, file=sys.stderr)
    sock.bind(uds_name)

    # Listen for incoming connections
    sock.listen(1)

    return sock


def read_bat_msg(connection):
    lg_str = connection.recv(4)

    if not lg_str:
        print("connection is closed by batsim core")
        exit(1)

    # print 'from client (lg_str): %r' % lg_str
    lg = struct.unpack("i", lg_str)[0]
    # print 'size msg to recv %d' % lg
    if sys.version_info[0] == 2:
        msg = connection.recv(lg)
    else:
        chunks = []
        bytes_recd = 0
        while bytes_recd < lg:
            chunk = connection.recv(lg - bytes_recd)
            if chunk == b'':
                raise RuntimeError("socket connection broken")
            chunks.append(chunk)
            bytes_recd = bytes_recd + len(chunk)
        msg = (b''.join(chunks)).decode("utf-8")

    print('from batsim : %s' % msg)
    sub_msgs = msg.split('|')
    data = sub_msgs[0].split(":")
    # version = int(data[0])
    now = float(data[1])

    jobs_submitted = []
    new_jobs_completed = []
    for i in range(1, len(sub_msgs)):
        data = sub_msgs[i].split(':')
        if data[1] == 'S':
            jobs_submitted.append(int(data[2]))
        elif data[1] == 'C':
            time = float(data[0])
            jid = int(data[2])
            jobs[jid].state = "Terminated"
            jobs[jid].run_time = time - jobs[jid].run_time
            new_jobs_completed.append(jid)
        else:
            raise Exception("Unknow submessage type" + data[1])

    return (now, jobs_submitted, new_jobs_completed)


def send_bat_msg(connection, now, jids_to_launch, jobs):
    msg = "0:" + str(now) + "|"
    if jids_to_launch:
        msg += str(now) + ":J:"
        for jid in jids_to_launch:
            msg += str(jid) + "="
            for r in itvs2ids(jobs[jid].res_set):
                msg += str(r - offset_idx) + ","
            # replace last comma by semicolon separtor between jobs
            msg = msg[:-1] + ";"
        msg = msg[:-1]  # remove last semicolon

    else:  # Do nothing
        msg += str(now) + ":N"

    print(msg)
    lg = struct.pack("i", int(len(msg)))
    connection.sendall(lg)
    connection.sendall(msg.encode("utf-8"))


def load_json_workload_profile(filename):
    wkp_file = open(filename)
    wkp = json.load(wkp_file)
    return wkp["jobs"], wkp["nb_res"]


def monkeypatch_oar_kao_utils():
    oar.kao.utils.init_judas_notify_user = lambda: None
    oar.kao.utils.create_almighty_socket = lambda: None
    oar.kao.utils.notify_almighty = lambda x: len(x)
    oar.kao.utils.notify_tcp_socket = lambda addr, port, msg: len(msg)
    oar.kao.utils.notify_user = lambda job, state, msg: len(state + msg)
    oar.kao.utils.get_date = lambda: plt.get_time()


def db_initialization(nb_res):

    print("Set default queue")
    db.add(Queue(name='default', priority=3, scheduler_policy='kamelot', state='Active'))

    print("add resources")
    # add some resources
    for i in range(nb_res):
        db.add(Resource(network_address="localhost"))


def db_add_job():
    pass


class BatEnv(object):

    def __init__(self, now):
        self.now = now


class BatSched(object):

    def __init__(self, res_set, jobs, mode_platform="simu", db_jid2s_jid={}, sched_delay=5,
                 uds_name='/tmp/bat_socket'):

        self.mode_platform = mode_platform
        self.sched_delay = sched_delay
        self.db_jid2s_jid = db_jid2s_jid
        self.env = BatEnv(0)
        self.platform = Platform(
            mode_platform, env=self.env, resource_set=res_set, jobs=jobs, db_jid2s_jid=db_jid2s_jid)
        global plt
        plt = self.platform
        self.jobs = jobs
        self.nb_jobs = len(jobs)
        self.sock = create_uds(uds_name)
        logger.info('waiting for a connection')
        self.connection, self.client_address = self.sock.accept()

        self.platform.running_jids = []
        if sys.version_info[0] == 2:
            self.waiting_jids = Set()
        else:
            self.waiting_jids = set()
        self.platform.waiting_jids = self.waiting_jids
        self.platform.completed_jids = []

    def sched_loop(self):
        nb_completed_jobs = 0
        while nb_completed_jobs < self.nb_jobs:

            now_str, jobs_submitted, new_jobs_completed = read_bat_msg(
                self.connection)

            # now_str = "10"
            # jobs_submitted = [1]
            # new_jobs_completed = []

            if jobs_submitted:
                for jid in jobs_submitted:
                    self.waiting_jids.add(jid)
                    if self.mode_platform == 'batsim-db':
                        print('set_job_state("Waiting"):', self.jobs[jid].db_jid)
                        set_job_state(self.jobs[jid].db_jid, 'Waiting')

            nb_completed_jobs += len(new_jobs_completed)

            print("new job completed: %s" % new_jobs_completed)

            for jid in new_jobs_completed:
                jobs_completed.append(jid)
                if jid in self.platform.running_jids:
                    self.platform.running_jids.remove(jid)
                if self.mode_platform == 'batsim-db':
                    set_job_state(self.jobs[jid].db_jid, 'Terminated')

            now = int(now_str)
            self.env.now = now  # TODO can be remove ???

            print("jobs running: %s" % self.platform.running_jids)
            print("jobs waiting: %s" % self.waiting_jids)
            print("jobs completed: %s" % jobs_completed)

            jids_to_launch = []

            if self.mode_platform == 'simu':
                print("call schedule_cycle.... %s" % now)
                schedule_cycle(self.platform, now, "default")

                # retrieve jobs to launch
                jids_to_launch = []
                for jid, job in iteritems(self.platform.assigned_jobs):
                    print(">>>>>>> job.start_time %s" % job.start_time)
                    if job.start_time == now:
                        self.waiting_jids.remove(jid)
                        jids_to_launch.append(jid)
                        job.state = "Running"
                        print("tolaunch: %s" % jid)
                        self.platform.running_jids.append(jid)

            else:
                print("call meta_schedule('internal')")
                meta_schedule('internal', plt)
                # Launching phase
                # Retrieve job to Launch

                result = db.query(Job).filter(Job.state == 'toLaunch')\
                                      .order_by(Job.id).all()

                for job_db in result:
                    set_job_state(job_db.id, 'Running')
                    jid = self.db_jid2s_jid[job_db.id]
                    self.waiting_jids.remove(jid)
                    jids_to_launch.append(jid)
                    self.jobs[jid].state = "Running"
                    print("_tolaunch: %s" % jid)
                    self.platform.running_jids.append(jid)

            now += self.sched_delay
            self.env.now = now
            send_bat_msg(self.connection, now, jids_to_launch, self.jobs)

    def run(self):
        self.sched_loop()

##############


def main(wkp_filename, database_mode):

    #
    # Load workload
    #

    json_jobs, nb_res = load_json_workload_profile(wkp_filename)

    print("nb_res:", nb_res)

    if database_mode == 'no-db':
        #
        # generate ResourceSet
        #

        hy_resource_id = [[(i, i)] for i in range(nb_res)]
        res_set = ResourceSetSimu(
            rid_i2o=range(nb_res),
            rid_o2i=range(nb_res),
            roid_itvs=[(0, nb_res - 1)],
            hierarchy={'resource_id': hy_resource_id},
            available_upto={2147483600: [(0, nb_res - 1)]}
        )

        #
        # prepare jobs
        #

        for j in json_jobs:
            print("Genererate jobs")
            jid = int(j["id"])
            jobs[jid] = JobSimu(id=jid,
                                state="Waiting",
                                queue="test",
                                start_time=0,
                                walltime=0,
                                types={},
                                res_set=[],
                                moldable_id=0,
                                mld_res_rqts=[(jid, j["walltime"],
                                               [([("resource_id", j["res"])],
                                                 [(0, nb_res - 0)])])],
                                run_time=0,
                                deps=[],
                                key_cache={},
                                ts=False, ph=0,
                                assign=False, find=False)

        BatSched(res_set, jobs, 'simu', {}).run()

    elif database_mode == 'memory':

        global offset_idx
        offset_idx = 1
        monkeypatch_oar_kao_utils()
        db_initialization(nb_res)

        #
        # prepare jobs
        #
        db_jid2s_jid = {}
        print("Prepare jobs")
        for i, j in enumerate(json_jobs):
            jid = int(j["id"])
            jobs[jid] = JobSimu(id=jid,
                                state="Waiting",
                                queue="test",
                                start_time=0,
                                walltime=0,
                                moldable_id=0,
                                mld_res_rqts=[(jid, j["walltime"],
                                               [([("resource_id", j["res"])],
                                                 [(0, nb_res - 0)])])],
                                run_time=0,
                                db_jid=i + 1,
                                assign=False,
                                find=False)

            insert_job(res=[(j["walltime"], [('resource_id=' + str(j["res"]), "")])],
                       state='Hold', properties='', user='')
            db_jid2s_jid[i + 1] = jid

        db.flush()

        BatSched([], jobs, 'batsim-db', db_jid2s_jid).run()


if __name__ == '__main__':  # pragma: no cover
    parser = argparse.ArgumentParser(description='Adaptor to run oar-kao with BatSim.')
    parser.add_argument('wkp_filename', metavar='F',
                        help='a file which contains the workload profile.')
    parser.add_argument('--database-mode', default='no-db',
                        help="select database mode (no-db, memory, oarconf)")
    args = parser.parse_args()
    print(args)
    if args.database_mode == 'memory':
        config.clear()
        config.update(BATSIM_DEFAULT_CONFIG)

    main(args.wkp_filename, args.database_mode)
