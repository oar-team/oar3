# coding: utf-8
from __future__ import unicode_literals, print_function
from copy import deepcopy
import oar.kao.scheduling
from oar.lib.interval import (intersec, itvs_size, extract_n_scattered_block_itv,
                              aggregate_itvs)

try:
    from oar.coorm.server_scheduling import find_coorm, assign_coorm  # noqa
except ImportError:
    pass

# assign_resources_mld_job_split_slots, find_resource_hierarchies_job


def find_default(itvs_avail, hy_res_rqts, hy, *find_args, **find_kwargs):
    """Simple wrap function to default function for test purpose"""
    return oar.kao.scheduling.find_resource_hierarchies_job(itvs_avail, hy_res_rqts, hy)


def assign_default(slots_set, job, hy, min_start_time, *assign_args, **assign_kwargs):
    """Simple wrap function to default function for test purpose"""
    return oar.kao.scheduling.assign_resources_mld_job_split_slots(slots_set, job, hy, min_start_time)


def find_contiguous_1h(itvs_avail, hy_res_rqts, hy):
    # NOT FOR PRODUCTION USE
    # Notes support only one resource group and ordered resource_id hierarchy level

    result = []
    hy_level_nbs, constraints = hy_res_rqts[0]  # one resource group
    l_name, n = hy_level_nbs[0]  # one hierarchy level
    # hy_level = hy[l_name]

    itvs_cts_slots = aggregate_itvs(intersec(constraints, itvs_avail))

    if l_name == "resource_id":
        for itv in itvs_cts_slots:
            if (itv[1] - itv[0] + 1) >= n:
                result = [(itv[0], itv[0]+n-1)]
                break

    return result


def find_contiguous_sorted_1h(itvs_avail, hy_res_rqts, hy):
    # NOT FOR PRODUCTION USE
    # Notes support only one resource group and ordered resource_id hierarchy level

    result = []
    hy_level_nbs, constraints = hy_res_rqts[0]  # one resource group
    l_name, n = hy_level_nbs[0]  # one hierarchy level
    # hy_level = hy[l_name]

    itvs_unsorted = aggregate_itvs(intersec(constraints, itvs_avail))
    lg = len(itvs_unsorted)

    ids_sorted = sorted(range(lg), key=lambda k: itvs_unsorted[k][1] - itvs_unsorted[k][0])

    if l_name == "resource_id":
        for i in ids_sorted:
            itv = itvs_unsorted[i]
            if (itv[1] - itv[0] + 1) >= n:
                result = [(itv[0], itv[0]+n-1)]
                break

    return result
#
# LOCAL
#


def find_resource_n_h_local(itvs, hy, rqts, top, h, h_bottom):

    n = rqts[h+1]
    size_bks = []
    avail_bks = []
    for top_itvs in top:
        avail_itvs = intersec(top_itvs, itvs)
        avail_bks.append(avail_itvs)
        size_bks.append(itvs_size(avail_itvs))

    sorted_ids = sorted(range(len(avail_bks)), key=lambda k: size_bks[k])

    for i, idx in enumerate(sorted_ids):
        if size_bks[i] >= n:
            res_itvs = []
            k = 0
            for itv in avail_bks[idx]:
                size_itv = itv[1] - itv[0] + 1
                if (k + size_itv) < n:
                    res_itvs.append(itv)
                else:
                    res_itvs.append((itv[0], itv[0] + (n-k-1)))
                    return res_itvs

    return []


def find_resource_hierarchies_scattered_local(itvs, hy, rqts):
    l_hy = len(hy)
    #    print "find itvs: ", itvs, rqts[0]
    if (l_hy == 1):
        return extract_n_scattered_block_itv(itvs, hy[0], rqts[0])
    else:
        return find_resource_n_h_local(itvs, hy, rqts, hy[0], 0, l_hy)


def find_local(itvs_slots, hy_res_rqts, hy):
    """ 2 Level of Hierarchy supported with sorting by increasing blocks' size"""
    result = []
    for hy_res_rqt in hy_res_rqts:
        (hy_level_nbs, constraints) = hy_res_rqt
        hy_levels = []
        hy_nbs = []
        for hy_l_n in hy_level_nbs:
            (l_name, n) = hy_l_n
            hy_levels.append(hy[l_name])
            hy_nbs.append(n)

        itvs_cts_slots = intersec(constraints, itvs_slots)

        res = find_resource_hierarchies_scattered_local(itvs_cts_slots, hy_levels, hy_nbs)
        if res:
            result.extend(res)
        else:
            return []

    return result


def assign_one_time_find_mld(slots_set, job, hy, min_start_time):
    ''''''
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
            # Next round will moldable we used default find function
            # oar.kao.scheduling.find_resource_hierarchies_job
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


def assign_one_time_find(slots_set, job, hy, min_start_time):
    ''''''
    # NOT FOR PRODUCTION USE

    flag_find = True
    prev_t_finish = 2 ** 32 - 1  # large enough
    prev_res_set = []
    prev_res_rqt = []

    slots = slots_set.slots
    prev_start_time = slots[1].b

    res_rqt = job.mld_res_rqts[0]
    res_rqt_copy = deepcopy(res_rqt)  # to keep set of intervals

    while True:
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
            # Next round will moldable we used default find function
            # oar.kao.scheduling.find_resource_hierarchies_job
            flag_find = False
            job.find = False
            res_rqt = res_rqt_copy
        else:
            break

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
