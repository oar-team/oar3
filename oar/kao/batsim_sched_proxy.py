# coding: utf-8
"""Proxy to exploit BatSim compatible scheduler in OAR"""

import re
import zmq
import time

from oar.kao.batsim import DataStorage
from oar.kao.job import (get_jobs_ids_in_multiple_states, JobPseudo)
from oar.lib import (config, get_logger)

# Set undefined config value to default one
DEFAULT_CONFIG = {
    'BATSCHED_ENDPOINT': '6679',
    'REDIS_HOSTNAME': 'localhost',
    'REDIS_PORT': '6379',
    'SCHEDULER_JOB_SECURITY_TIME': '60',
    'WLOAD_BATSIM' = 'oar3',
    'DS_PREFIX': 'oar3'
}

config.setdefault_config(DEFAULT_CONFIG)

logger = get_logger("oar.modules.batsched", forward_stderr=True)
logger.info('Start Batsched')

class BatsimSchedProxy(object):
    def __init__(self, plt, queue_name='default'):
        self.plt = plt
        self.queue_name = queue_name
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REQ)
        self.socket.connect(config['BATSCHED_ENDPOINT'])
        self.data_store = DataStorage(config['DS_PREFIX'])
        self.wload = config['WLOAD_BATSIM']
        
    def ask_schedule(self):
        next_active_jids = []
        finished_jids = []
        # Retrieve cached list of active id jobs from Redis
        cached_active_jids = self.data_store.redis.get('active_job_ids') #mode do
            
        # Retrieve waiting and running jobs from
        waiting_jobs, waiting_jids, _ = self.plt.get_waiting_jobs(self.queue_name)
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
        self.redis.set("active_job_ids", cached_active_jids) 

        batmsg_body = ""
        now = float(self.plt.get_time())
        now_event = now

        if len(finished_jids) > 0:
            for jid in finished_jids:
                batmsg_body += now_event ':C:' + str(jid) + '|'
                now_event = 0.0001
        
        if len(waiting_jids) > 0:
            self.plt.get_data_jobs(waiting_jobs, waiting_jids,
                                   self.plt.resource_set, config['SCHEDULER_JOB_SECURITY_TIME'])

            for waiting_jid in waiting_jids:
                # BE CAREFUL: Moldable job is not supported and only the first requested resource number is
                # considered
                # mld_res_rqts=[(1, 60, [([("resource_id", 2)], [])])]
                # walltime: 60
                # res: 2
                mld_res_rqts = waiting_jobs[waiting_jid].mld_res_rqts
                subtime = waiting_jobs[waiting_jid].submission_time,
                walltime = mld_res_rqts[0][1] # walltime 
                res = mld_res_rqts[0][2][0][0][0][1] # take the first requested resource number (2 from above example)

                self.data_store.set_job(waiting_jid, subtime, walltime, res)

                batmsg_body += now_event ':S:' + str(waiting_jid) + '|'
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
        version = int(data[0])
                      
        #sched_time = float(data[1])
        logger.debug("From scheduler: version: " + version)
        
        for i in range(1, len(sub_msgs)):
            data = sub_msgs[i].split(':')
            if data[1] == 'J':
                jobs = {}
                for job_alloc in data[2].split(';'):
                    jid_alloc = job_alloc.split('=')
                    jid = jid_alloc[0]
                    res_set = [(i,i) for i in  jid_alloc[1].split(',')]
                    jobs[jid] = JobPseudo(id=jid, moldable_id=jid, start_time=now,res_set=res_set)
                if jobs:
                    save_assigns(jobs, self.plt.resource_set):
                
            elif data[1] == 'N':
                pass
            else:
                raise Exception("Un submessage type " + data[1])            
