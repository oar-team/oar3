# coding: utf-8
"""Proxy to exploit BatSim compatible scheduler in OAR"""

import json
import zmq
import click

from oar.lib import (config, get_logger)
from oar.lib.tools import get_date
from oar.lib.interval import batsim_str2itvs
from oar.kao.batsim import DataStorage
from oar.kao.job import (get_jobs_ids_in_multiple_states, JobPseudo)
from oar.kao.scheduling import set_slots_with_prev_scheduled_jobs

# Set undefined config value to default one
DEFAULT_CONFIG = {
    'BATSCHED_ENDPOINT': 'tcp://localhost:6679',
    'REDIS_HOSTNAME': 'localhost',
    'REDIS_PORT': '6379',
    'SCHEDULER_JOB_SECURITY_TIME': '60',
    'WLOAD_BATSIM': 'oar3',
    'DS_PREFIX': 'oar3'
}

config.setdefault_config(DEFAULT_CONFIG)

logger = get_logger("oar.kao.batsim_sched_proxy", forward_stderr=True)

class BatsimSchedProxy(object):

    def __init__(self, plt, scheduled_jobs, all_slot_sets, job_security_time, queue, now):
        self.plt = plt
        self.scheduled_jobs = scheduled_jobs 
        self.all_slot_sets = all_slot_sets
        self.job_security_time = job_security_time
        self.queue = queue
        self.now = now
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REQ)
        self.socket.connect(config['BATSCHED_ENDPOINT'])
        self.data_store = DataStorage(config['DS_PREFIX'])
        self.wload = config['WLOAD_BATSIM']

        self.data_store.redis.set('active_job_ids', json.dumps([]))
        self.resource_set = plt.resource_set() # Could by obtained directly from metasched

        
    def ask_schedule(self):
        logger.debug('Start ask_schedule')
        next_active_jids = []
        finished_jids = []
        # Retrieve cached list of active id jobs from Redis
        cached_active_jids = json.loads(self.data_store.redis.get('active_job_ids'))
        
        # Retrieve waiting and running jobs from
        waiting_jobs, waiting_jids, _ = self.plt.get_waiting_jobs(self.queue.name)
        active_jids = get_jobs_ids_in_multiple_states(['Running', 'toLaunch', 'Launching',
                                                       'Finishing', 'Suspended', 'Resuming'])

        # Determine new submitted and finished jobs
        for cached_jid in cached_active_jids:
            if cached_jid in active_jids:
                next_active_jids.append(cached_jid)
            elif cached_jid in waiting_jids:
                next_active_jids.append(cached_jid)
                waiting_jids.remove(cached_jid)
            else:
                finished_jids.append(cached_jid)

        cached_active_jids += waiting_jids

        # Save active_job_ids in redis
        self.data_store.redis.set("active_job_ids", json.dumps(cached_active_jids))

        batmsg_body = ""
        now = float(self.now)
        now_event = now

        if len(finished_jids) > 0:
            for jid in finished_jids:
                batmsg_body += str(now_event) + ':C:' + self.wload + '!' + str(jid) + '|'
                now_event = 0.0001

        if len(waiting_jids) > 0:
            self.plt.get_data_jobs(waiting_jobs, waiting_jids,
                                   self.resource_set, config['SCHEDULER_JOB_SECURITY_TIME'])

            for waiting_jid in waiting_jids:
                # BE CAREFUL: Moldable job is not supported and only the first
                #requested resource number is considered
                # mld_res_rqts=[(1, 60, [([("resource_id", 2)], [])])]
                # walltime: 60
                # res: 2
                mld_res_rqts = waiting_jobs[waiting_jid].mld_res_rqts
                subtime = waiting_jobs[waiting_jid].submission_time,
                walltime = mld_res_rqts[0][1]  # walltime
                res = mld_res_rqts[0][2][0][0][0][1]  # take the first requested resource number (2 from above example)

                self.data_store.set_job(waiting_jid, subtime, walltime, res)

                batmsg_body += str(now_event) + ':S:' + self.wload + '!' + str(waiting_jid) + '|'
                now_event = 0.0001

        batmsg_header = '2:' + str(now) + '|'

        if batmsg_body == '':
            batmsg_body = str(now) + ':N|'

        batmsg_req = batmsg_header + batmsg_body[:-1]

        # send req
        logger.debug("Message sent to Batsim compatible scheduler:\n" + batmsg_req)
        self.socket.send_string(batmsg_req)

        # recv rep
        logger.debug("Waiting response from scheduler")
        batmsg_rep = self.socket.recv()
        logger.debug("Message from scheduler:\n" + batmsg_rep)

        sub_msgs = batmsg_rep.split('|')
        data = sub_msgs[0].split(":")
        version = data[0]

        nodes_2_halt = []
        nodes_2_wakeup = []
        
        #sched_time = float(data[1])
        logger.debug("From scheduler: version: " + version)

        for i in range(1, len(sub_msgs)):
            data = sub_msgs[i].split(':')
            if data[1] == 'J':
                jobs = []
                for job_alloc in data[2].split(';'):

                    jid_alloc = job_alloc.split('=')
                    jid = int(jid_alloc[0].split('!')[1])
                    res_set = batsim_str2itvs(jid_alloc[1])
                    json_dict = json.loads(self.data_store.get(jid))
                    walltime = json_dict["walltime"]

                    jobs.append(JobPseudo(id=jid, moldable_id=jid, start_time=self.now,
                                          walltime=walltime, res_set=res_set))
                    
                if jobs:
                    set_slots_with_prev_scheduled_jobs(self.all_slot_sets, jobs,
                                                       self.job_security_time)
                    self.plt.save_assigns(jobs, self.resource_set)

            elif data[1] == 'N':
                pass

            elif data[1] == 'P':
                subdata = data[2].split('=')
                
            else:
                raise Exception("Un submessage type " + data[1])            


@click.command()
@click.option('-s', '--send', default='A',
              help="send Batsim protocol commands to scheduler. \
              Two commands are supported A (defafor start  or Z for stop.")
def cli(send):
    """Command to send start/stop sequence to Batsim compatible scheduler"""

    print("Command to send to Batsim compatible scheduler: ", send)

    # open zmq socket (REQ/REP)
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.connect(config['BATSCHED_ENDPOINT'])

    # send command
    now = str(get_date())
    msg = '2:' + now + '|' + now + ':' + send
    logger.info("Batsim_sched_proxy CLI send: " + msg)
    socket.send_string(msg)

    msg = socket.recv()
    logger.info("Batsim_sched_proxy CLI recv: " + msg)
