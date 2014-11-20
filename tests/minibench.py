import sys
sys.path.append('../kao/')
from scheduling import * 
#http://www.huyng.com/posts/python-performance-analysis/
import time

class Timer(object):
    def __init__(self, verbose=False):
        self.verbose = verbose

    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        self.end = time.time()
        self.secs = self.end - self.start
        self.msecs = self.secs * 1000  # millisecs
        if self.verbose:
            print 'elapsed time: %f ms' % self.msecs

def create_job(i, nb_res, res):
    pass

def create_jobs(n, nb_res, res, mode="default", **kwargs):
    jobs = {}
    for i in range(1, n+1):
        jobs[i] = Job(i,"Waiting", 0, 0, "yop", "", "",{}, [], 0, 
                      [ 
                          (1, 60, 
                           #   [  ( [("node", (i % nb_res) + 1) ], res ) ]
#10
                           [  ( [("node", 10) ], res ) ]
                       )
                      ]         
                  ) 
    return( [x for x in range(1, i+1)], jobs)


def init_data_structure(nb_res):
    res = [(1, 201)]
    ss = SlotSet(Slot(1, 0, 0, res, 0, 2**31))
    all_ss = {0:ss}

    h0_res_itvs = [ [(i, i)] for i in range(1, nb_res+1) ]
    print h0_res_itvs
    hy = { 'node': h0_res_itvs }

    return (res, hy, all_ss)

def eva_sched_foo(all_ss, jobs, hy, j_ids ):
    schedule_id_jobs_ct(all_ss, jobs, hy, j_ids, 10)


nb_res = 200
for i in range(20, nb_res):
    print i
    (res, hy, all_ss) = init_data_structure(i)
    (j_ids, jobs) = create_jobs(i, nb_res, [(1,nb_res+1)])
 
    with Timer() as t:
        eva_sched_foo(all_ss, jobs, hy, j_ids )
    print "=> yop: %s s" % t.secs
        
