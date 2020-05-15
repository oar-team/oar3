import simplejson as json
from oar.lib import (config, get_logger)
# Log category

logger = get_logger('oar.kao.priorty')

def apply_priority(queues, now, jids, jobs, plt):
    """
    Job's Priority = Sum of (criterion_weight * criterion_factor) where criteria are: 
    age, queue, work, size, karma, qos, nice
       
    Criterion Factors:

    age_factor = max(1, age_coef * age) 
    queue_factor = f(queue)
    work_factor = 1 / min(1, work) [small],  1 - 1 / min(1, work) [big] 
    size_factor = 1 - (size / cluster_size) [small],  1 - (size / cluster_size) [big] 
    karma_factor = 1 / (1 + karma)
    qos_factor = 0.0 - 1.0  
    nice_factor = max(nice, 1.0) 

    Criterion Weights:

    Each criterion is positive integer 


    Note: The priorty approach is largely inspired from Slurm's one
    """
    
    age_weight = 0
    age_coef = 1.65e-06 #7 days in seconds
    queue_weight = 0
    work_weight = 0
    work_mode = 0 # priorize small job
    size_weight = 0
    siez_mode = 0 # priorize small job
    karma_weight = 0
    qos_weight = 0
    nice_weight = 0

    priority_conf_filename = config['PRIORITY_CONF_FILE']
    with open(quotas_rules_filename) as json_file:
        json_priority = json.load(json_file)


def multifactor_jobs_sorting(queues, now, jids, jobs, plt):
    pass
