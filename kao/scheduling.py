from hierarchy import *
from job import *
from slot import *

def set_slots_with_prev_scheduled_jobs(slots_sets, jobs, ordered_id_jobs, security_time ):
    for j_id in ordered_id_jobs:
        job = jobs[j_id]
        
        if job.types.has_key("container"):
            t_e = job.start_time + job.walltime - security_time
            slots_sets[j_id] = SlotSet(Slot(1, 0, 0, job.res_set, job.start_time, t_e))

        ss_id =0
        if job.types.has_key("inner"):
            ss_id = job.types["inner"]
            
        split_slots_prev_scheduled_one_job(slots_sets[ss_id], [job])
    
def find_resource_hierarchies_job(itvs_slots, hy_res_rqts, hy):
    '''find resources in interval for all resource subrequests of a moldable instance 
    of a job'''
    result = []
    for hy_res_rqt in hy_res_rqts:
        (hy_level, hy_nb, constraints) = hy_res_rq
        itvs_cts_slot = inter_intervals(contraints, itvs_slot)
        num_hy = hy[hy_level]
        result.extend( find_resource_hierarchies_scattered(itvs_cts_slot, hy_level, hy_nb) )

    return result

def find_first_suitable_contiguous_slots(slots, job, res_rqt, hy):
    '''find first_suitable_contiguous_slot '''
    (mld_id, walltime, hy_res_rqts) = res_rqt
    itvs = []
    sid_left = 0
    slot_e = slots[sid_right].e
    while(itvs == []):
        #find next contiguous slots_time
        sid_left += 1 
        slot_s = slots[sid_left].s
        while( (slot_e-slot_s+1) < walltime) ) :
            sid_right += 1
            slot_e = slots[sid_right].e
        #
        itvs_avail = inter_itvs_slots(slots, sid_left, sid_right) 
        itvs = find_resource_hierarchies_job(itvs_avail, hy_res_rqts, hy)

    return (itvs, sid_left, sid_right)

        
def assign_resources_job_split_slots:
    '''not implemented see assign_resources_mld_job_split_slots'''

def assign_resources_mld_job_split_slots(slots, job, hy):
    '''Assign resources to a job and update the list of slots accordingly by 
    splitting concerned slot - moldable version'''
    prev_t_finish = 2**32-1 # large enough
    prev_res_set = []
    prev_res_req = []
    prev_id_slots = []

    for res_rqt in job.mld_res_rqts:
        (mld_id, walltime, hy_res_rqts) = res_rqt
        (res_set, sid_left, sid_right) = find_first_suitable_contiguous_slots(slots, job, res_rqt, hy)
        t_finish = slots[sid_left].b + walltime
        if (t_finish < prev_t_finish):
            prev_t_finish = t_finish
            prev_res_set = res_set
            prev_res_req = res_rqt
            prev_sid_left = sid_left
            prev_sid_right = sid_right

    (mld_id, walltime, hy_res_rqts) = prev_res_rqt
    job.res_set = prev_res_set
    job.w = walltime
    job.mld_id = mld_id

    split_slots(prev_sid_left, prev_sid_right, job)

def schedule_id_jobs_ct_dep(slots_set, jobs, hy_levels, jobs_dependencies, req_jobs_status, id_jobs security_time):
    '''Schedule loop with support for jobs container - can be recursive 
    (recursivity has not be tested) plus dependencies support actual schedule
    function used '''

    for j_id in id_jobs:
        pass
