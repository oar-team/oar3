# coding: utf-8
from __future__ import unicode_literals, print_function
import oar.kao.scheduling
from oar.kao.interval import intersec, itvs_size

# assign_resources_mld_job_split_slots, find_resource_hierarchies_job


def find_legacy(itvs_avail, hy_res_rqts, hy):
    """Simple wrap function to default function for test purpose"""
    return oar.kao.scheduling.find_resource_hierarchies_job(itvs_avail, hy_res_rqts, hy)


def assign_legacy(slots_set, job, hy, min_start_time):
    """Simple wrap function to default function for test purpose"""
    return oar.kao.scheduling.assign_resources_mld_job_split_slots(slots_set, job, hy, min_start_time)


def find_simple_contiguous(itvs_avail, hy_res_rqts, hy):
    # NOT FOR PRODUCTION USE
    # Notes support only one resource group and ordered resource_id hierarchy level

    result = []
    hy_level_nbs, constraints = hy_res_rqts[0]  # one resource group
    l_name, n = hy_level_nbs[0]  # one hierarchy level
    # hy_level = hy[l_name]

    itvs_cts_slots = intersec(constraints, itvs_avail)
    if l_name == "resource_id":
        for itvs in itvs_cts_slots:
            if itvs_size(itvs) > n:
                result = [(itvs[0], itvs[0]+n-1)]
                break

    return result


def assign_one_time_find(slots_set, job, hy, min_start_time):
    '''Assign resources to a job and update by spliting the concerned slots - moldable version'''
    # NOT FOR PRODUCTION USE

    flag_find = True
    prev_t_finish = 2 ** 32 - 1  # large enough
    prev_res_set = []
    prev_res_rqt = []

    slots = slots_set.slots
    prev_start_time = slots[1].b

    for res_rqt in job.mld_res_rqts:
        mld_id, walltime, hy_res_rqts = res_rqt
        res_set, sid_left, sid_right = oar.kao.scheduling.find_first_suitable_contiguous_slots(
            slots_set, job, res_rqt, hy, min_start_time)
        if res_set == []:  # no suitable time*resources found
            job.res_set = []
            job.start_time = -1
            job.moldable_id = -1
            return
        # print("after find fisrt suitable")
        t_finish = slots[sid_left].b + walltime
        if (t_finish < prev_t_finish):
            prev_start_time = slots[sid_left].b
            prev_t_finish = t_finish
            prev_res_set = res_set
            prev_res_rqt = res_rqt
            prev_sid_left = sid_left
            prev_sid_right = sid_right
        if flag_find:
            flag_find = False
            job.find = False

    job.find = True  # If job is not reload for next schedule round

    (mld_id, walltime, hy_res_rqts) = prev_res_rqt
    job.moldable_id = mld_id
    job.res_set = prev_res_set
    job.start_time = prev_start_time
    job.walltime = walltime

    # Take avantage of job.starttime = slots[prev_sid_left].b
    # print(prev_sid_left, prev_sid_right, job.moldable_id , job.res_set,)
    # job.start_time , job.walltime, job.mld_id

    slots_set.split_slots(prev_sid_left, prev_sid_right, job)
