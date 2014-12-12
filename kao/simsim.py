import collections
import kamelot
from platform import Platform
from random import seed, randint
from sets import Set
import simpy
from simpy.events import AnyOf

class SimSched:
    def __init__(self, env, plt, jobs, submission_time_jids):
        #self.env = env

        self.platform = plt
        self.jobs = jobs
        self.sub_time_jids = submission_time_jids
        self.sub_time_idx = 0
        self.sub_time_len = len(submission_time_jids)

        self.sched_proc = env.process(self.sched(env))

        self.evt_running_jobs = Set()
        self.evt_2_job = {}
        self.waiting_jobs = Set()

    def sched(self, env):
        
        next_job_arrival = self.job_arrival(env)

        while True:

            print 'Wait for job arrivals or job endings', env.now

            events = list(self.evt_running_jobs)
            events.append(next_job_arrival)
            any_of_events = AnyOf(env, events)
            ev = yield any_of_events
            
            print ev

            for k,v in ev.iteritems():
              print "event:..... ", k
              if k == next_job_arrival:
                  print "job arrives !", v
                  for jid in v:
                      self.waiting_jobs.add(v)
                  next_job_arrival = self.job_arrival(env)
                  
              else:
                  print "job endings !", k, v
                  #if k in self.evt_running_jobs:
                  print "remove ev: ", k
                  self.evt_running_jobs.remove(k)
            
            print "sched is running...."
            #TODO call sched
        
            
            if self.waiting_jobs:
                j_to_launch = random.choice(self.waiting_jobs)
                self.waiting_jobs.remove(j_to_launch)
                #launch job
                print "launch", j_to_launch
                evt_running_job = env.timeout(randint(5,10),j_to_launch)
                self.evt_running_jobs.add(evt_running_job)
                self.evt_2_job[evt_running_job] = j_to_launch

    def job_arrival(self, env):
        if self.sub_time_idx < self.sub_time_len:
            t, jids = self.sub_time_jids[self.sub_time_idx]
            self.sub_time_idx += 1
            return env.timeout(t, jids)
        else:
            return None

        #self.new_job_id += 1
        #new_job_ids = [self.new_job_id]
        #print new_job_ids
        #return env.timeout(randint(5,15),  new_job_ids)
 
class ResourceSetSimu():
    def __init__(self, **kwargs):
        for key, value in kwargs.iteritems():
            setattr(self, key, value)
        
class JobSimu():
    def __init__(self, **kwargs):
        for key, value in kwargs.iteritems():
            setattr(self, key, value)

env = simpy.Environment()
nb_res = 10

#
# generate ResourceSet
#
res_set = ResourceSetSimu(
    rid_i2o = range(nb_res),
    rid_o2i = range(nb_res),
    roid_itvs = [(0,nb_res-1)]
)

#
# generate jobs
#

nb_jobs = 10
jobs = {}
submission_time_jids = []

for i in range(1,nb_jobs + 1):
    jobs[i] = JobSimu( id = i,
                       state = "Wainting",
                       start_time = 0,
                       types = {},
                       res_set = [],
                       moldable_id = 0,
                       mld_res_rqts = [],
                       )
    
    submission_time_jids.append( (10, [i]) )

plt = Platform("simu", env=env, resource_set = res_set )
simsched = SimSched(env, plt, jobs, submission_time_jids)
env.run(until=40)
