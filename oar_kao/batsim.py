from __future__ import print_function
import struct
import socket
import sys
import os
import json
from sets import Set

from oar.lib import config, get_logger
from oar.kao.simsim import ResourceSetSimu, JobSimu
from oar.kao.interval import itvs2ids
from oar.kao.kamelot import schedule_cycle
from oar.kao.platform import Platform


DEFAULT_CONFIG = {
    'LOG_CATEGORIES': 'all',
    'LOG_FILE': '',
    'LOG_FORMAT': '[%(levelname)s] [%(asctime)s] [%(name)s]: %(message)s',
    'LOG_LEVEL': 3,
    'HIERARCHY_LABEL': 'resource_id,network_address',
    'SCHEDULER_RESOURCE_ORDER': 'resource_id ASC',
    'SCHEDULER_JOB_SECURITY_TIME': '60',
    'SCHEDULER_AVAILABLE_SUSPENDED_RESOURCE_TYPE': 'default',
    'FAIRSHARING_ENABLED': 'no',
    'SCHEDULER_FAIRSHARING_MAX_JOB_PER_USER': '3'
}

config.clear()
config.update(DEFAULT_CONFIG)
config['LOG_FILE'] = '/tmp/yop'

log = get_logger("oar.batsim")

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
    msg = connection.recv(lg)
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
                msg += str(r) + ","
            # replace last comma by semicolon separtor between jobs
            msg = msg[:-1] + ";"
        msg = msg[:-1]  # remove last semicolon

    else:  # Do nothing
        msg += str(now) + ":N"

    print(msg)
    lg = struct.pack("i", int(len(msg)))
    connection.sendall(lg)
    connection.sendall(msg)


def load_json_workload_profile(filename):
    wkp_file = open(filename)
    wkp = json.load(wkp_file)
    return wkp["jobs"], wkp["nb_res"]


class BatEnv(object):

    def __init__(self, now):
        self.now = now


class BatSched(object):

    def __init__(self, res_set, jobs, sched_delay=5,
                 uds_name='/tmp/bat_socket', mode_platform="simu"):

        self.sched_delay = sched_delay

        self.env = BatEnv(0)
        self.platform = Platform(
            mode_platform, env=self.env, resource_set=res_set, jobs=jobs)

        self.jobs = jobs
        self.nb_jobs = len(jobs)
        self.sock = create_uds(uds_name)
        log.info('waiting for a connection')
        self.connection, self.client_address = self.sock.accept()

        self.platform.running_jids = []
        self.waiting_jids = Set()
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

            nb_completed_jobs += len(new_jobs_completed)

            print("new job completed: %s" % new_jobs_completed)

            for jid in new_jobs_completed:
                jobs_completed.append(jid)
                self.platform.running_jids.remove(jid)

            now = int(now_str)
            self.env.now = now  # TODO can be remove ???

            print("jobs running: %s" % self.platform.running_jids)
            print("jobs waiting: %s" % self.waiting_jids)
            print("jobs completed: %s" % jobs_completed)

            print("call schedule_cycle.... %s" % now)
            schedule_cycle(self.platform, now, "test")

            # retrieve jobs to launch
            jids_to_launch = []
            for jid, job in self.platform.assigned_jobs.iteritems():
                print(">>>>>>> job.start_time %s" % job.start_time)
                if job.start_time == now:
                    self.waiting_jids.remove(jid)
                    jids_to_launch.append(jid)
                    job.state = "Running"
                    print("tolaunch: %s" % jid)
                    self.platform.running_jids.append(jid)

            now += self.sched_delay
            self.env.now = now

            send_bat_msg(self.connection, now, jids_to_launch, self.jobs)

    def run(self):
        self. sched_loop()

##############


#
# Load workload
#

json_jobs, nb_res = load_json_workload_profile(sys.argv[1])

print("nb_res: %s %s" % (nb_res, type(nb_res)))

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
# generate jobs
#

for j in json_jobs:
    print("retrieve jobjob")
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
                        ts=False, ph=0)

BatSched(res_set, jobs).run()
