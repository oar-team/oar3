from oar.lib import config, logging, db, Accounting
import re
#from sqlalchemy.sql import func

# convert perl hash 2 dict
def perl_hash_2_dict(str):
    d = {}
    #remove space and curly bracket
    str = re.sub("{|}|\s", "", str)
    for pair in re.split(",", str):
        kv = re.split("=>", pair)
        d[kv[0]] = kv[1]
    return d

def get_sum_accounting_window(queue, window_start, window_stop):

    req = db.query( Accounting.consumption_type, func.sum(Accounting.consumption) )\
            .filter(Accounting.queue_name == queue)\
            .filter(Accounting.window_start >= window_start)\
            .filter(Accounting.window_stop < window_stop)\
            .group_by(Accounting.consumption_type).all()

    karma_sum_time_asked = 1
    karma_sum_time_used = 1

    for (consumption_type, total_consumption) in req: 
        if consumption_type == "ASKED":
             karma_sum_time_asked = total_consumption
        elif consumption_type == "USED":
             karma_sum_time_used = total_consumption

    return (karma_sum_time_asked, karma_sum_time_used)
    
#    and karma_projects_asked, karma_projects_used = Iolib.get_sum_accounting_for_param dbh queue "accounting_project" window_start window_stop

def get_sum_accounting_by_project(queue, window_start, window_stop):

    req = db.query( Accountin.accounting_project, Accounting.consumption_type, 
                    func.sum(Accounting.consumption) )\
            .filter(Accounting.queue_name == queue)\
            .filter(Accounting.window_start >= window_start)\
            .filter(Accounting.window_stop < window_stop)\
            .group_by(Accounting.accounting_project, Accounting.consumption_type)\
            .all()
    
    karma_used = {}
    karma_asked = {}
    
    for (project, consumption_type, total_consumption) in req: 
        if consumption_type == "ASKED":
             karma_asked[project] = total_consumption
        elif consumption_type == "USED":
             karma_used[project] = total_consumption 

    return (karma_asked, karma_used)

def get_sum_accounting_by_user(queue, window_start, window_stop):

    req = db.query( Accountin.accounting_user, Accounting.consumption_type, 
                    func.sum(Accounting.consumption) )\
            .filter(Accounting.queue_name == queue)\
            .filter(Accounting.window_start >= window_start)\
            .filter(Accounting.window_stop < window_stop)\
            .group_by(Accounting.accounting_user, Accounting.consumption_type)\
            .all()
    
    karma_used = {}
    karma_asked = {}
    
    for (user, consumption_type, total_consumption) in req: 
        if consumption_type == "ASKED":
             karma_asked[user] = total_consumption
        elif consumption_type == "USED":
             karma_used[user] = total_consumption 

    return (karma_asked, karma_used)


#
# Karma and Fairsharing stuff 
#                             
def karma_jobs_sorting(queue, now, jids, jobs, plt): 

    fairsharing_nb_job_limit = config["SCHEDULER_FAIRSHARING_MAX_JOB_PER_USER"]
    karma_window_size =  3600 * 30 * 24 # TODO in conf ???

    #Set undefined config value to default one
    default_config = { "SCHEDULER_FAIRSHARING_PROJECT_TARGETS": "{default => 21.0}",
                       "SCHEDULER_FAIRSHARING_USER_TARGETS": "{default => 22.0}",
                       "SCHEDULER_FAIRSHARING_COEF_PROJECT": "0",
                       "SCHEDULER_FAIRSHARING_COEF_USER": "1",
                       "SCHEDULER_FAIRSHARING_COEF_USER_ASK": "1"
                   }
    for k,v in default_config.iteritems():
        if not k in config:
            config[k] = k

    # defaults values for fairsharing 
    k_proj_targets = "{default => 21.0}"
    k_user_targets = "{default => 22.0}"
    k_coeff_proj_consumption = "0"
    k_coeff_user_consumption = "1"
    k_karma_coeff_user_asked_consumption = "1"
    #get fairsharing config if any
    karma_proj_targets =  perl_hash_2_dict(conf["SCHEDULER_FAIRSHARING_PROJECT_TARGETS"])
    karma_user_targets = perl_hash_2_dict(conf["SCHEDULER_FAIRSHARING_USER_TARGETS"])
    karma_coeff_proj_consumption = config["SCHEDULER_FAIRSHARING_COEF_PROJECT"]
    karma_coeff_user_consumption = config["SCHEDULER_FAIRSHARING_COEF_USER"]
    karma_coeff_user_asked_consumption = config["SCHEDULER_FAIRSHARING_COEF_USER_ASK"]

    #
    # Sort jobs accordingly to karma value (fairsharing)  *)
    #                                                     *)

    window_start = now - karma_window_size
    window_stop = now

    karma_sum_time_asked, karma_sum_time_used = plt.get_sum_accounting_window(queue, window_start, window_stop)
    karma_projects_asked, karma_projects_used = plt.get_sum_accounting_by_project(queue, window_start, window_stop)
    karma_users_asked, karma_users_used = plt.get_sum_accounting_by_user(queue, window_start, window_stop)
    #
    # compute karma for each job
    #

    for job in jobs.itervalues():
        if job.project in karma_projects_used:
            karma_proj_used_j =  karma_projects_used[job.project]
        else:
            karma_proj_used_j = 0.0

        if job.user in karma_users_used:
            karma_user_used_j =  karma_users_used[job.user]
        else:
            karma_user_used_j = 0.0
        
        if job.user in karma_users_asked:
            karma_user_asked_j =  karma_users_asked[job.user]
        else:
            karma_user_asked_j = 0.0

        if job.project in karma_proj_targets:
            karma_proj_target = karma_proj_targets[job.project]
        else:
            karma_proj_target = 0.0

        if job.user in karma_user_targets:
            karma_user_target = karma_user_targets[job.user] / 100.0
        else:
            karma_user_target = 0.0
 
        job.karma = ( karma_coeff_proj_consumption * ((karma_proj_used_j / karma_sum_time_used) - (karma_proj_target / 100.0)) +
                      karma_coeff_user_consumption * ((karma_user_used_j / karma_sum_time_used) - (karma_user_target / 100.0)) +
                      karma_coeff_user_asked_consumption * ((karma_user_asked_j / karma_sum_time_asked) - (karma_user_target / 100.0))
                  )

    # sort jids according to jobs' karma value
    sorted(jids, key=lambda jid: jobs[jid].karma)
