import collections
from random import seed, randint

from sets import Set
import simpy
from simpy.events import AnyOf

from oar.lib import config
from oar.kao.kamelot import schedule_cycle
from oar.kao.platform import Platform

config['LOG_FILE'] = '/dev/null'

class SimSched:
    def __init__(self, res_set, jobs, submission_time_jids, mode_platforme = "simu"):

        self.env = simpy.Environment()

        self.platform = Platform("simu", env=self.env, resource_set=res_set, jobs=jobs )

        self.jobs = jobs
        self.sub_time_jids = submission_time_jids
        self.sub_time_idx = 0
        self.sub_time_len = len(submission_time_jids)

        self.sched_proc = self.env.process(self.sched())

        self.evt_running_jobs = Set()
        self.running_jids = []
        self.platform.running_jids = []
        self.waiting_jids = Set()
        self.platform.waiting_jids = self.waiting_jids
        self.platform.finished_jids = []

    def run(self):
        self.env.run()
    
    def sched(self):
        
        next_job_arrival = self.job_arrival()

        while True:

            print 'Wait for job arrivals or job endings', self.env.now

            events = list(self.evt_running_jobs)
            if next_job_arrival != None:
                events.append(next_job_arrival)
            any_of_events = AnyOf(self.env, events)
            ev = yield any_of_events
            
            #print ev

            for k,v in ev.iteritems():
              #print "event:..... ", k
              if k == next_job_arrival:
                  print "job arrives !", v
                  for jid in v:
                      self.waiting_jids.add(jid)
                  next_job_arrival = self.job_arrival()
                  
              else:
                  print "job endings !", k, v
                  #if k in self.evt_running_jobs:
                  #print "remove ev: ", k
                  self.evt_running_jobs.remove(k)
                  self.jobs[v].state = "Terminated"
                  self.platform.finished_jids.append(v)
                  self.platform.running_jids.remove(v)

            now = self.env.now

            if (next_job_arrival == None) and not self.waiting_jids and not self.evt_running_jobs:
                print "All job submitted, no more waiting or running jobs ...", now
                self.env.exit()
            
            print "call schedule_cycle.... ", now
            
            schedule_cycle(self.platform,now, "test")
            
            #launch jobs if needed
            for jid, job in self.platform.assigned_jobs.iteritems():
                if job.start_time == now:
                    self.waiting_jids.remove(jid)
                    job.state = "Running"
                    print "launch:", jid
                    evt_running_job = self.env.timeout(job.run_time,jid)
                    self.evt_running_jobs.add(evt_running_job)

                    self.platform.running_jids.append(jid)

    def job_arrival(self):
        if self.sub_time_idx < self.sub_time_len:
            t, jids = self.sub_time_jids[self.sub_time_idx]
            self.sub_time_idx += 1
            return self.env.timeout(t, jids)
        else:
            return None
 
class ResourceSetSimu():
    def __init__(self, **kwargs):
        for key, value in kwargs.iteritems():
            setattr(self, key, value)

class JobSimu():
    def __init__(self, **kwargs):
        for key, value in kwargs.iteritems():
            setattr(self, key, value)

