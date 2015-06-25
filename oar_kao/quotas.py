from collections import defaultdict
from copy import deepcopy
from oar.lib import config
from oar.job import nb_default_resources


quotas_job_types = ['*']

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


    Note1: Quotas are applied globally, only the jobs of the type container are not taken in account (but the inner jobs are used to compute the quotas).

    Note2: Besteffort jobs are not taken in account except in the besteffort queue.


    """

    
    def __init__(self):
        self.quotas = defaultdict(lambda: [0,0,0])

        
    def deepcopy_from(self, quotas):
        self.quotas = deepcopy(quotas)

        
    def update(self, job):

        queue = job.queue
        project = job.project
        user = job.user
        if hasattr(self, 'nb_res'):
            job.
        
        if resources_time < 0:
            resources_time = 0
        
        for t in quotas_job_types:
            # Update the number of used resources
            self.quotas['*','*',t,'*'][0] += nb_resources
            self.quotas['*','*',t,user][0] += nb_resources
            self.quotas['*',project,t,'*'][0] += nb_resources
            self.quotas[queue,'*',t,'*'][0] += nb_resources
            self.quotas[queue,project,t,user][0] += nb_resources
            self.quotas[queue,project,t,'*'][0] += nb_resources
            self.quotas[queue,'*',t,user][0] += nb_resources
            self.quotas['*',project,t,user][0] += nb_resources
            # Update the number of running jobs
            self.quotas['*','*',t,'*'][1] += 1
            self.quotas['*','*',t,user][1] += 1
            self.quotas['*',project,t,'*'][1] += 1
            self.quotas[queue,'*',t,'*'][1] += 1
            self.quotas[queue,project,t,user][1] += 1
            self.quotas[queue,project,t,'*'][1] += 1
            self.quotas[queue,'*',t,user][1] += 1
            self.quotas['*',project,t,user][1] += 1
            # Update the resource X hours occupation (=~cputime)
            self.quotas['*','*',t,'*'][2] += resources_time
            self.quotas['*','*',t,user][2] += resources_time
            self.quotas['*',project,t,'*'][2] += resources_time
            self.quotas[queue,'*',t,'*'][2] += resources_time
            self.quotas[queue,project,t,user][2] += resources_time
            self.quotas[queue,project,t,'*'][2] += resources_time
            self.quotas[queue,'*',t,user][2] += resources_time
            self.quotas['*',project,t,user][2] += resources_time
    


    def check(self):
    pass
