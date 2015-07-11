# coding: utf-8
from __future__ import unicode_literals, print_function
import json
from collections import defaultdict
from copy import deepcopy
from oar.lib import config
from oar.kao.interval import itvs_size, intersec
import oar.kao.resource as rs
# import pdb

quotas_job_types = ['*']
quotas_rules = {}


class Quotas(object):
    """

    Implements quotas on:
       - the amount of busy resources at a time
       - the number of running jobs at a time
       - the resource time in use at a time (nb_resources X hours)
    This can be seen like a surface used by users, projects, types, ...

    depending on:

    - job queue name ("-q" oarsub option)
    - job project name ("--project" oarsub option)
    - job types ("-t" oarsub options)
    - job user

    Syntax is like:

    quotas[queue, project, job_type, user] = [int, int, float];
                                               |    |     |
              maximum used resources ----------+    |     |
              maximum number of running jobs -------+     |
              maximum resources times (hours) ------------+



       '*' means "all" when used in place of queue, project,
           type and user, quota will encompass all queues or projects or
           users or type
       '/' means "any" when used in place of queue, project and user
           (cannot be used with type), quota will be "per" queue or project or
           user
        -1 means "no quota" as the value of the integer or float field

 The lowest corresponding quota for each job is used (it depends on the
 consumptions of the other jobs). If specific values are defined then it is
 taken instead of '*' and '/'.

 The default quota configuration is (infinity of resources and jobs):

       $Gantt_quotas->{'*'}->{'*'}->{'*'}->{'*'} = [-1, -1, -1] ;

 Examples:

   - No more than 100 resources used by 'john' at a time:

       $Gantt_quotas->{'*'}->{'*'}->{'*'}->{'john'} = [100, -1, -1] ;

   - No more than 100 resources used by 'john' and no more than 4 jobs at a
     time:

       $Gantt_quotas->{'*'}->{'*'}->{'*'}->{'john'} = [100, 4, -1] ;

   - No more than 150 resources used by jobs of besteffort type at a time:

       $Gantt_quotas->{'*'}->{'*'}->{'besteffort'}->{'*'} = [150, -1, -1] ;

   - No more than 150 resources used and no more than 35 jobs of besteffort
     type at a time:

       $Gantt_quotas->{'*'}->{'*'}->{'besteffort'}->{'*'} = [150, 35, -1] ;

   - No more than 200 resources used by jobs in the project "proj1" at a
     time:

       $Gantt_quotas->{'*'}->{'proj1'}->{'*'}->{'*'} = [200, -1, -1] ;

   - No more than 20 resources used by 'john' in the project 'proj12' at a
     time:

       $Gantt_quotas->{'*'}->{'proj12'}->{'*'}->{'john'} = [20, -1, -1] ;

   - No more than 80 resources used by jobs in the project "proj1" per user
     at a time:

       $Gantt_quotas->{'*'}->{'proj1'}->{'*'}->{'/'} = [80, -1, -1] ;

   - No more than 50 resources used per user per project at a time:

       $Gantt_quotas->{'*'}->{'/'}->{'*'}->{'/'} = [50, -1, -1] ;

   - No more than 200 resource hours used per user at a time:

       $Gantt_quotas->{'*'}->{'*'}->{'*'}->{'/'} = [-1, -1, 200] ;

     For example, a job can take 1 resource for 200 hours or 200 resources for
     1 hour.

 Note: If the value is only one integer then it means that there is no limit
       on the number of running jobs and rsource hours. So the 2 following
       statements have the same meaning:

           $Gantt_quotas->{'*'}->{'*'}->{'*'}->{'john'} = 100 ;
           $Gantt_quotas->{'*'}->{'*'}->{'*'}->{'john'} = [100, -1, -1] ;


    Note1: Quotas are applied globally, only the jobs of the type container are not taken in 
account (but the inner jobs are used to compute the quotas).

    Note2: Besteffort jobs are not taken in account except in the besteffort queue.


    """

    def __init__(self):
        self.counters = defaultdict(lambda: [0, 0, 0])

        def deepcopy_from(self, quotas):
            self.counters = deepcopy(quotas.counters)

    def update(self, job, prev_nb_res=0, prev_duration=0):

        queue = job.queue
        project = job.project
        user = job.user

        if hasattr(job, 'res_set'):
            if not hasattr(self, 'nb_res'):
                job.nb_res = itvs_size(intersec(job.res_set, rs.default_resource_itvs))
                nb_resources = job.nb_res
        else:
            nb_resources = prev_nb_res

        if hasattr(job, 'walltime'):
            duration = job.walltime
        else:
            duration = prev_duration

        for t in quotas_job_types:
            if (t == '*') or (t in job.types):
                # Update the number of used resources
                self.counters['*', '*', t, '*'][0] += nb_resources
                self.counters['*', '*', t, user][0] += nb_resources
                self.counters['*', project, t, '*'][0] += nb_resources
                self.counters[queue, '*', t, '*'][0] += nb_resources
                self.counters[queue, project, t, user][0] += nb_resources
                self.counters[queue, project, t, '*'][0] += nb_resources
                self.counters[queue, '*', t, user][0] += nb_resources
                self.counters['*', project, t, user][0] += nb_resources
                # Update the number of running jobs
                self.counters['*', '*', t, '*'][1] += 1
                self.counters['*', '*', t, user][1] += 1
                self.counters['*', project, t, '*'][1] += 1
                self.counters[queue, '*', t, '*'][1] += 1
                self.counters[queue, project, t, user][1] += 1
                self.counters[queue, project, t, '*'][1] += 1
                self.counters[queue, '*', t, user][1] += 1
                self.counters['*', project, t, user][1] += 1
                # Update the resource * second
                self.counters['*', '*', t, '*'][2] += nb_resources * duration
                self.counters['*', '*', t, user][2] += nb_resources * duration
                self.counters['*', project, t, '*'][2] += nb_resources * duration
                self.counters[queue, '*', t, '*'][2] += nb_resources * duration
                self.counters[queue, project, t, user][2] += nb_resources * duration
                self.counters[queue, project, t, '*'][2] += nb_resources * duration
                self.counters[queue, '*', t, user][2] += nb_resources * duration
                self.counters['*', project, t, user][2] += nb_resources * duration

    def combine(self, quotas, duration):
        for key, value in quotas.counters.iteritems():
            self.counters[key][0] = max(self.counters[key][0], value[0])
            self.counters[key][1] = max(self.counters[key][1], value[1])
            self.counters[key][2] += value[1] * duration

    def check(self, job, job_nb_resources, duration):
        global quotas_rules

        for rl_fields, rl_quotas in quotas_rules.iteritems():
            # pdb.set_trace()
            rl_queue, rl_project, rl_job_type, rl_user = rl_fields
            rl_nb_resources, rl_nb_jobs, rl_resources_time = rl_quotas
            for fields, counters in self.counters.iteritems():
                queue, project, job_type, user = fields
                nb_resources, nb_jobs, resources_time = counters
                # match queue
                if ((rl_queue == '*') and (queue == '*')) or\
                   ((rl_queue == queue) and (job.queue == job.queue)) or\
                   (rl_queue == '/'):
                    # match project
                    if ((rl_project == '*') and (project == '*')) or\
                       ((rl_project == project) and (job.project == project)) or\
                       (rl_project == '/'):
                        # match job_typ
                        if ((rl_job_type == '*') and (job_type == '*')) or\
                           ((rl_job_type == job_type) and (job_type in job.types)):
                            # match user
                            if ((rl_user == '*') and (user == '*')) or\
                               ((rl_user == user) and (job.user == user)) or\
                               (rl_user == '/'):
                                # test quotas values plus job's ones
                                # 1) test nb_resources
                                if (rl_nb_resources > -1) and (rl_nb_resources < nb_resources):
                                        return (False, 'nb resources quotas failed',
                                                rl_fields, rl_nb_resources)
                                # 2) test nb_jobs
                                if (rl_nb_jobs > -1) and (rl_nb_jobs < nb_jobs):
                                        return (False, 'nb jobs quotas failed',
                                                rl_fields, rl_nb_jobs)
                                # 3) test resources_time (work)
                                if (rl_resources_time > -1) and (rl_resources_time < resources_time):
                                        return (False, 'resources hours quotas failed',
                                                rl_fields, rl_resources_time)
        return (True, 'quotas ok', '', 0)


def check_slots_quotas(slots, sid_left, sid_right, job, job_nb_resources, duration):
    # loop over slot_set
    slots_quotas = Quotas()
    slots_quotas.update(job, job_nb_resources)

    sid = sid_left
    while True:
        slot = slots[sid]

        slots_quotas.combine(slot.quotas, slot.e - slot.b)

        if (sid == sid_right):
            break
        else:
            sid = slot.next

    return slots_quotas.check(job, job_nb_resources, duration)


def load_quotas_rules():
    global quotas_rules
    """
    {
        "quotas": {
               "*,*,*,*": [120,-1,-1],
                "*,*,*,john": [150,-1,-1]
        }
    }

    """
    quotas_rules_filename = config['QUOTAS_FILE']
    with open(quotas_rules_filename) as json_file:
        json_quotas = json.load(json_file)
        for k, v in json_quotas['quotas'].iteritems():
            quotas_rules[tuple(k.split(','))] = [v[0], v[1], 3600*v[2]]
