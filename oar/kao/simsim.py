# coding: utf-8
import re

import simpy
from simpy.events import AnyOf

from oar.kao.kamelot import schedule_cycle
from oar.kao.platform import Platform
from oar.lib.globals import init_oar

config, _, log, session_factory = init_oar()

config["LOG_FILE"] = ":stderr:"


class SimSched(object):
    def __init__(self, res_set, jobs, submission_time_jids, mode_platform="simu"):
        self.env = simpy.Environment()

        self.platform = Platform(
            mode_platform, env=self.env, resource_set=res_set, jobs=jobs
        )

        self.jobs = jobs
        self.sub_time_jids = submission_time_jids
        self.sub_time_idx = 0
        self.sub_time_len = len(submission_time_jids)

        self.sched_proc = self.env.process(self.sched())

        self.evt_running_jobs = set()
        self.running_jids = []  # TO REMOVE ???
        self.platform.running_jids = []
        self.waiting_jids = set()
        self.platform.waiting_jids = self.waiting_jids
        self.platform.completed_jids = []

    def run(self):
        self.env.run()

    def sched(self):
        next_job_arrival = self.job_arrival()

        while True:
            print("Wait for job arrivals or job endings", self.env.now)

            events = list(self.evt_running_jobs)
            if next_job_arrival is not None:
                print("append next_job_arrival evt")
                events.append(next_job_arrival)
            any_of_events = AnyOf(self.env, events)
            ev = yield any_of_events

            for k, v in (ev.todict()).items():
                if k == next_job_arrival:
                    print("job arrives !", v)
                    for jid in v:
                        self.waiting_jids.add(jid)
                    next_job_arrival = self.job_arrival()

                else:
                    print("job endings !", k, v)
                    # if k in self.evt_running_jobs:
                    # print("remove ev: ", k)
                    self.evt_running_jobs.remove(k)
                    self.jobs[v].state = "Terminated"
                    self.platform.completed_jids.append(v)
                    self.platform.running_jids.remove(v)

            now = self.env.now

            if (
                (next_job_arrival is None)
                and not self.waiting_jids
                and not self.evt_running_jobs
            ):
                print("All job submitted, no more waiting or running jobs ...", now)
                return
            print("call schedule_cycle.... ", now)

            schedule_cycle(self.platform, now, ["test"])

            # launch jobs if needed
            for jid, job in self.platform.assigned_jobs.items():
                if job.start_time == now:
                    self.waiting_jids.remove(jid)
                    job.state = "Running"
                    print("launch:", jid)
                    evt_running_job = self.env.timeout(job.run_time, jid)
                    self.evt_running_jobs.add(evt_running_job)

                    self.platform.running_jids.append(jid)

    def job_arrival(self):
        if self.sub_time_idx < self.sub_time_len:
            t, jids = self.sub_time_jids[self.sub_time_idx]
            self.sub_time_idx += 1
            print("next jobs ", jids, "submitted in:", t, " sec")
            return self.env.timeout(t, jids)
        else:
            return None


class ResourceSetSimu(object):
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)


class JobSimu(object):
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)


"""
SWF Format (extracted from http://www.cs.huji.ac.il/labs/parallel/workload/swf.html)

1    Job Number -- a counter field, starting from 1.

2    Submit Time -- in seconds. The earliest time the log refers to is zero, and is usually
         the submittal time of the first job. The lines in the log are sorted by ascending submittal times.
         It makes sense for jobs to also be numbered in this order.

3    Wait Time -- in seconds. The difference between the job's submit time and the time at which it actually began to run.
         Naturally, this is only relevant to real logs, not to models.

4    Run Time -- in seconds. The wall clock time the job was running (end time minus start time).
         We decided to use ``wait time'' and ``run time'' instead of the equivalent ``start time'' and
         ``end time'' because they are directly attributable to the scheduler and application, and are more
         suitable for models where only the run time is relevant.
         Note that when values are rounded to an integral number of seconds (as often happens in logs)
         a run time of 0 is possible and means the job ran for less than 0.5 seconds. On the other hand
         it is permissable to use floating point values for time fields.

5    Number of Allocated Processors -- an integer. In most cases this is also the number of processors
         the job uses; if the job does not use all of them, we typically don't know about it.

6    Average CPU Time Used -- both user and system, in seconds. This is the average over all processors
         of the CPU time used, and may therefore be smaller than the wall clock runtime.
         If a log contains the total CPU time used by all the processors, it is divided by the number of allocated processors to derive the average.

7    Used Memory -- in kilobytes. This is again the average per processor.

8    Requested Number of Processors.

9    Requested Time. This can be either runtime (measured in wallclock seconds), or average CPU time
         per processor (also in seconds) -- the exact meaning is determined by a header comment.
         In many logs this field is used for the user runtime estimate (or upper bound) used in backfilling.
         If a log contains a request for total CPU time, it is divided by the number of requested processors.

10   Requested Memory (again kilobytes per processor).

11   Status 1 if the job was completed, 0 if it failed, and 5 if cancelled. If information about chekcpointing
         or swapping is included, other values are also possible. See usage note below. This field is meaningless for models, so would be -1.

12   User ID -- a natural number, between one and the number of different users.

13   Group ID -- a natural number, between one and the number of different groups.
         Some systems control resource usage by groups rather than by individual users.

14   Executable (Application) Number -- a natural number, between one and the number of different applications
         appearing in the workload. in some logs, this might represent a script file used to run jobs rather than
         the executable directly; this should be noted in a header comment.

15   Queue Number -- a natural number, between one and the number of different queues in the system.
         The nature of the system's queues should be explained in a header comment. This field is where batch and interactive
         jobs should be differentiated: we suggest the convention of denoting interactive jobs by 0.

16   Partition Number -- a natural number, between one and the number of different partitions in the systems.
         The nature of the system's partitions should be explained in a header comment.
         For example, it is possible to use partition numbers to identify which machine in a cluster was used.

17   Preceding Job Number -- this is the number of a previous job in the workload, such that the current job
         can only start after the termination of this preceding job. Together with the next field, this allows
         the workload to include feedback as described below.

18    Think Time from Preceding Job -- this is the number of seconds that should elapse between the termination of the preceding job
      and the submittal of this one.
"""
JID = 0
SUB_TIME = 1
WAIT_TIME = 2
RUN_TIME = 3
NB_ALLOC_PROCS = 4
AVG_CPU = 5
USE_MEM = 6
REQ_PROCS = 7
REQ_TIME = 8
REQ_MEM = 9
STATUS = 10
USER_ID = 11
GROUP_ID = 12
APP_NUM = 13
QUEUE_NUM = 14
PARTITION_NUM = 15
PREC_JOB = 16
THINK_TIME = 16


class SWFWorkload(object):
    def __init__(self, filename):
        self.jobs_fields = {}
        self.sub_times = []
        self.sub_absolute_times_w_jids = {}
        for line in open(filename):
            li = line.strip()
            if not li.startswith(";"):
                # print(line.rstrip())
                fields = [int(f) for f in re.split(r"\W+", line) if f != ""]
                jid = fields[JID]
                sub_time = fields[SUB_TIME]
                self.jobs_fields[fields[JID]] = fields
                self.sub_times.append(sub_time)
                if sub_time in self.sub_absolute_times_w_jids:
                    self.sub_absolute_times_w_jids[sub_time].append(jid)
                else:
                    self.sub_absolute_times_w_jids[sub_time] = [jid]
                # TODO test time monotonicy according to job_id

        self.sub_times = sorted(set(self.sub_times))

    def gene_jobsim_sub_time(self, jid_begin, jid_end, nb_res):
        simu_jobs = {}
        sub_time_jids = []
        t_begin = self.jobs_fields[jid_begin][SUB_TIME]
        t_end = self.jobs_fields[jid_end][SUB_TIME]
        i = 0
        t_prev = t_begin
        while True:
            t = self.sub_times[i]
            if t > t_end:
                break
            elif t >= t_begin:
                jids = self.sub_absolute_times_w_jids[t]
                sub_time_jids.append((t - t_prev, jids))
                t_prev = t
                for jid in jids:
                    fields = self.jobs_fields[jid]
                    req_walltime = fields[REQ_TIME]
                    req_procs = fields[REQ_PROCS]
                    simu_jobs[jid] = JobSimu(
                        id=jid,
                        state="Waiting",
                        queue=str(fields[QUEUE_NUM]),
                        start_time=0,
                        walltime=0,
                        types={},
                        res_set=[],
                        moldable_id=0,
                        mld_res_rqts=[
                            (
                                i,
                                req_walltime,
                                [([("resource_id", req_procs)], [(0, nb_res - 1)])],
                            )
                        ],
                        run_time=fields[RUN_TIME],
                        deps=[],
                        key_cache={},
                        ts=False,
                        ph=0,
                        assign=False,
                        find=False,
                        no_quotas=False,
                    )

            i += 1

        return (simu_jobs, sub_time_jids)
