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

def create_simple_job(i, res_rqt, ctnts_res, key_cache=""):

    return Job(i,"Waiting", 0, 0, "yop", "", "",{}, [], 0, [(1, 60, [  ( [("node", res_rqt)], list(ctnts_res) )] )], key_cache )


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
    res = [(1, nb_res + 1)]
    ss = SlotSet(Slot(1, 0, 0, list(res), 0, 2**31))
    all_ss = {0:ss}

    h0_res_itvs = [ [(i, i)] for i in range(1, nb_res+1) ]
    #print h0_res_itvs
    hy = { 'node': h0_res_itvs }

    return (res, hy, all_ss)

def simple_same_jobs_nb_res(nb_job, nb_rqt_res, ctnts_res, key_cache = ""):
    jobs = {}

    for i in range(1, nb_job + 1):
        jobs[i] = create_simple_job(i, nb_rqt_res, ctnts_res, key_cache)

    return (range(1, nb_job+1), jobs)


def eva_sched_foo(all_ss, jobs, hy, j_ids ):
    schedule_id_jobs_ct(all_ss, jobs, hy, j_ids, 10)


def simple_bench_1(key_cache=""):
    nb_res = 10

    x = []
    y = []
    for k in range(1, 12):
        i = 2**k
        print "nb_jobs", i
        (res, hy, all_ss) = init_data_structure(nb_res)
        (j_ids, jobs) = simple_same_jobs_nb_res(i, 10, res, key_cache)

        with Timer() as t:
            eva_sched_foo(all_ss, jobs, hy, j_ids )
        print "=> nb_job:", i, " time: %s s" % t.secs

        x.append(i)
        y.append(t.secs)

    print x
    print y


def simple_bench_0(key_cache=""):
    nb_res = 10
    i = 1024
    print "nb_jobs", i
    (res, hy, all_ss) = init_data_structure(nb_res)
    (j_ids, jobs) = simple_same_jobs_nb_res(i, 10, res, key_cache)

    eva_sched_foo(all_ss, jobs, hy, j_ids )


simple_bench_1()

#   res = [(1, 201)]
#nb_res = 200
#  [  ( [("node", 10) ], res ) ]
#j_id: 4
#(id: 1 p: 0 n: 2 ) b: 0 e: 59 itvs: [(21, 201)]
#(id: 2 p: 1 n: 3 ) b: 60 e: 59 itvs: [(11, 201)]
#(id: 3 p: 2 n: 0 ) b: 60 e: 2147483648 itvs: [(1, 201)]
