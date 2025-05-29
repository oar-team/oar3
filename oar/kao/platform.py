# coding: utf-8
import time

from procset import ProcSet

from oar.kao.karma import (
    get_sum_accounting_by_project,
    get_sum_accounting_by_user,
    get_sum_accounting_window,
)
from oar.lib.job_handling import (
    get_data_jobs,
    get_scheduled_jobs,
    get_waiting_jobs,
    save_assigns,
)
from oar.lib.resource import ResourceSet


class Platform(object):
    """
    The base :class:`Platform` provides an unified way to access
    information (jobs and resources) of the current platform.

    This class can be overridden (e.g. :class:`BatsimPlatform`) to switch in simulation mode.
    """

    def __init__(self):
        pass

    def resource_set(self, session=None, config=None):
        return ResourceSet(session, config)

    def get_time(self):
        return int(time.time())

    def get_waiting_jobs(self, queue, reservation="None", session=None):
        return get_waiting_jobs(session, queue, reservation)

    def get_data_jobs(self, *args):
        return get_data_jobs(*args)

    def get_scheduled_jobs(self, *args):
        return get_scheduled_jobs(*args)

    def save_assigns(self, *args):
        return save_assigns(*args)

    def get_sum_accounting_window(self, *args):
        return get_sum_accounting_window(*args)

    def get_sum_accounting_by_project(self, *args):
        return get_sum_accounting_by_project(*args)

    def get_sum_accounting_by_user(self, *args):
        return get_sum_accounting_by_user(*args)

    #
    # SimSim and BatSim mode simu
    #

    def resource_set_simu(self):
        return self.res_set

    def get_time_simu(self):
        return self.env.now

    def get_waiting_jobs_simu(self, queue):
        print(" get_waiting_jobs_simu:", self.waiting_jids)
        waiting_jobs = {}
        waiting_jids_lst = []
        nb_waiting_jobs = 0
        for jid in self.waiting_jids:
            job = self.jobs[jid]
            waiting_jobs[jid] = job
            waiting_jids_lst.append(jid)
            nb_waiting_jobs += 1

        waiting_jids_lst = sorted(waiting_jids_lst)

        print(waiting_jobs, waiting_jids_lst, nb_waiting_jobs)

        return (waiting_jobs, waiting_jids_lst, nb_waiting_jobs)

    def get_scheduled_jobs_simu(self, resource_set, job_security_time, now):
        running_jobs = [self.jobs[jid] for jid in self.running_jids]
        # for job in running_jobs:
        # print "running_jobs", job.id, job.start_time, job.walltime,
        # job.res_set
        return running_jobs

    def get_data_jobs_simu(self, *args):
        print("get_data_jobs_simu")
        pass

    def save_assigns_simu(self, jobs, resource_set):
        print("save_assigns_simu")

        for jid, job in jobs.items():
            jres_set = job.res_set
            print("job.res_set before", jid, job.res_set)
            r_ids = [resource_set.rid_o2i[roid] for roid in list(jres_set)]
            job.res_set = ProcSet(*r_ids)
        self.assigned_jobs = jobs

    def save_assigns_simu_and_default(self, jobs, resource_set):
        print("save_assigns_simu_and_default........................")
        # assigned_jobs = {}
        for jid, job in jobs.items():
            sid = self.db_jid2s_jid[jid]
            jobsimu = self.jobs[sid]
            jres_set = job.res_set
            r_ids = [resource_set.rid_o2i[roid] for roid in list(jres_set)]
            jobsimu.res_set = ProcSet(*r_ids)
            print(
                "save assign jid, sid, res_set: ", jid, " ", sid, " ", jobsimu.res_set
            )
            jobsimu.start_time = job.start_time
            jobsimu.walltime = job.walltime
            # assigned_jobs[sid] = jobsimu

        # self.assigned_jobs = assigned_jobs

        return save_assigns(jobs, resource_set)


class SimuPlatform(Platform):
    def __init__(self, env, resource_set, jobs):
        self.env = env
        self.res_set = resource_set
        self.assigned_jobs = {}

        self.jobs = jobs
        self.running_jids = None
        self.waiting_jids = None
        self.completed_jids = None

    # Karma
    def get_sum_accounting_window(self, *args):
        print("get_sum_accounting_window NOT IMPLEMENTED")

    def get_sum_accounting_by_project(self, *args):
        print("get_sum_accounting_by_project NOT IMPLEMENTED")

    def get_sum_accounting_by_user(self, *args):
        print("get_sum_accounting_by_user NOT IMPLEMENTED")

    def get_waiting_jobs(self, queue):
        print(" get_waiting_jobs_simu:", self.waiting_jids)
        waiting_jobs = {}
        waiting_jids_lst = []
        nb_waiting_jobs = 0
        for jid in self.waiting_jids:
            job = self.jobs[jid]
            waiting_jobs[jid] = job
            waiting_jids_lst.append(jid)
            nb_waiting_jobs += 1

        waiting_jids_lst = sorted(waiting_jids_lst)

        print(waiting_jobs, waiting_jids_lst, nb_waiting_jobs)

        return (waiting_jobs, waiting_jids_lst, nb_waiting_jobs)

    def get_data_jobs(self, *args):
        print("get_data_jobs_simu")
        pass

    def get_scheduled_jobs(self, resource_set, job_security_time, now):
        running_jobs = [self.jobs[jid] for jid in self.running_jids]
        # for job in running_jobs:
        # print "running_jobs", job.id, job.start_time, job.walltime,
        # job.res_set
        return running_jobs

    def save_assigns(self, jobs, resource_set):
        print("save_assigns_simu")

        for jid, job in jobs.items():
            jres_set = job.res_set
            print("job.res_set before", jid, job.res_set)
            r_ids = [resource_set.rid_o2i[roid] for roid in list(jres_set)]
            job.res_set = ProcSet(*r_ids)
        self.assigned_jobs = jobs

    def resource_set(self):
        return self.res_set

    def get_time(self):
        return self.env.now


class BatsimPlatform(Platform):
    def __init__(self, env, jobs, db_jid2s_jid):
        self.env = env
        self.assigned_jobs = {}
        self.jobs = jobs
        self.db_jid2s_jid = db_jid2s_jid
        self.running_jids = None
        self.waiting_jids = None
        self.completed_jids = None

    def get_time(self):
        return self.env.now

    def save_assigns(self, jobs, resource_set):
        print("save assigns for BatsimPlatform........................")
        # assigned_jobs = {}
        for jid, job in jobs.items():
            sid = self.db_jid2s_jid[jid]
            jobsimu = self.jobs[sid]
            jres_set = job.res_set
            r_ids = [resource_set.rid_o2i[roid] for roid in list(jres_set)]
            jobsimu.res_set = ProcSet(*r_ids)
            print(
                "save assign jid, sid, res_set: ", jid, " ", sid, " ", jobsimu.res_set
            )
            jobsimu.start_time = job.start_time
            jobsimu.walltime = job.walltime
            # assigned_jobs[sid] = jobsimu

        # self.assigned_jobs = assigned_jobs

        return save_assigns(jobs, resource_set)
