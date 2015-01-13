import collections
from random import seed, randint

from sets import Set
import simpy
from simpy.events import AnyOf

from oar.kao.helpers import plot_slots_and_job
from oar.kao.kamelot import schedule_cycle
from oar.kao.platform import Platform


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
        self.running_jids = []
        plt.running_jids = []
        self.waiting_jids = Set()
        plt.waiting_jids = self.waiting_jids
        plt.finished_jids = []

    def sched(self, env):
        
        next_job_arrival = self.job_arrival(env)

        while True:

            print 'Wait for job arrivals or job endings', env.now

            events = list(self.evt_running_jobs)
            if next_job_arrival != None:
                events.append(next_job_arrival)
            any_of_events = AnyOf(env, events)
            ev = yield any_of_events
            
            #print ev

            for k,v in ev.iteritems():
              #print "event:..... ", k
              if k == next_job_arrival:
                  print "job arrives !", v
                  for jid in v:
                      self.waiting_jids.add(jid)
                  next_job_arrival = self.job_arrival(env)
                  
              else:
                  print "job endings !", k, v
                  #if k in self.evt_running_jobs:
                  #print "remove ev: ", k
                  self.evt_running_jobs.remove(k)
                  jobs[v].state = "Terminated"
                  plt.finished_jids.append(v)
                  plt.running_jids.remove(v)

            now = env.now

            if (next_job_arrival == None) and not self.waiting_jids and not self.evt_running_jobs:
                print "All job submitted, no more waiting or running jobs ...", now
                env.exit()
            
            print "call schedule_cycle.... ", now
            
            schedule_cycle(plt,now, "test")
            
            #launch jobs if needed
            for jid, job in plt.assigned_jobs.iteritems():
                if job.start_time == now:
                    self.waiting_jids.remove(jid)
                    job.state = "Running"
                    print "launch:", jid
                    evt_running_job = env.timeout(job.run_time,jid)
                    self.evt_running_jobs.add(evt_running_job)

                    plt.running_jids.append(jid)

    def job_arrival(self, env):
        if self.sub_time_idx < self.sub_time_len:
            t, jids = self.sub_time_jids[self.sub_time_idx]
            self.sub_time_idx += 1
            return env.timeout(t, jids)
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

env = simpy.Environment()
nb_res = 32

#
# generate ResourceSet
#
hy_resource_id = [[(i,i)] for i in range(1,nb_res+1)]
res_set = ResourceSetSimu(
    rid_i2o = range(nb_res+1),
    rid_o2i = range(nb_res+1),
    roid_itvs = [(1,nb_res)],
    hierarchy = {'resource_id': hy_resource_id},
    available_upto = {2147483600:[(1,nb_res)]}
)

#
# generate jobs
#

nb_jobs = 4
jobs = {}
submission_time_jids = []

for i in range(1,nb_jobs + 1):
    jobs[i] = JobSimu( id = i,
                       state = "Waiting",
                       queue = "test",
                       start_time = 0,
                       walltime = 0,
                       types = {},
                       res_set = [],
                       moldable_id = 0,
                       mld_res_rqts =  [(i, 60, [([("resource_id", 15)], [(0,nb_res-1)])])],
                       run_time = 20*i,
                       key_cache = "",
                       ts=False, ph=0
                       )
    
    submission_time_jids.append( (10, [i]) )

#submission_time_jids= [(10, [1,2,3,4])]
#submission_time_jids= [(10, [1,2]), (10, [3])]

plt = Platform("simu", env=env, resource_set=res_set, jobs=jobs )
simsched = SimSched(env, plt, jobs, submission_time_jids)
env.run()

print "Number finished jobs:", len(plt.finished_jids)
print "Finished job ids:", plt.finished_jids

print jobs

#for jid,job in jobs.iteritems():
#    jres_set = job.res_set
#    r_ids = [ res_set.rid_o2i[roid] for roid in itvs2ids(jres_set) ]
#    job.res_set = unordered_ids2itvs(r_ids)
#    print jid, job.state, job.start_time, job.walltime, job.res_set

last_finished_job = jobs[plt.finished_jids[-1]]
print last_finished_job
plot_slots_and_job({}, jobs, nb_res, last_finished_job.start_time +  last_finished_job.walltime)
