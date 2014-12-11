from random import seed, randint
from sets import Set
import simpy
from simpy.events import AnyOf

class SimSched:
    def __init__(self, env):
        self.env = env
        self.sched_proc = env.process(self.sched(env))
        self.new_job_id = 0
        self.evt_running_jobs = Set()
        self.evt_2_job = {}
        self.waiting_jobs = []

    def sched(self, env):
        
        next_job_arrival = self.job_arrival(env)

        i = 1
        new_jobs = []
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
                  self.waiting_jobs.extend(v)
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
        #TODO generate from SWF trace
        self.new_job_id += 1
        new_job_ids = [self.new_job_id]
        print new_job_ids
        return env.timeout(randint(5,15),  new_job_ids)
            
env = simpy.Environment()
simsched = SimSched(env)
env.run(until=40)
