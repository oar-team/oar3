# coding: utf-8
from typing import Dict, Tuple

from procset import ProcSet

from oar.kao.slot import Slot, SlotSet, intersec_itvs_slots
from oar.lib.hierarchy import find_resource_hierarchies_scattered


def find_resource_hierarchies_job(itvs_slots, hy_res_rqts, hy):
    """
    Find resources in interval for all resource subrequests of a moldable
    instance of a job
    """
    result = ProcSet()
    for hy_res_rqt in hy_res_rqts:
        (hy_level_nbs, constraints) = hy_res_rqt
        hy_levels = []
        hy_nbs = []
        for hy_l_n in hy_level_nbs:
            (l_name, n) = hy_l_n
            hy_levels.append(hy[l_name])
            hy_nbs.append(n)

        itvs_cts_slots = constraints & itvs_slots
        result = result | find_resource_hierarchies_scattered(
            itvs_cts_slots, hy_levels, hy_nbs
        )

    return result


def find_first_suitable_contiguous_slots(
    slots_set: SlotSet, job, res_rqt, hy
) -> Tuple[ProcSet, int, int]:
    """find first_suitable_contiguous_slot"""
    (mld_id, walltime, hy_res_rqts) = res_rqt
    itvs = ProcSet()

    slots = slots_set.slots
    cache = slots_set.cache

    if job.key_cache and (job.key_cache[mld_id] in cache):
        sid_left = cache[job.key_cache[mld_id]]
    else:
        sid_left = 0

    for (slot_begin, slot_end) in slots_set.traverse_with_width(walltime):
        # find next contiguous slots_time
        itvs_avail = intersec_itvs_slots(slots, slot_begin.id, slot_end.id)
        itvs = find_resource_hierarchies_job(itvs_avail, hy_res_rqts, hy)

        if len(itvs) != 0:
            break

    if job.key_cache:
        cache[job.key_cache[mld_id]] = sid_left

    return (itvs, slot_begin.id, slot_end.id)


def assign_resources_mld_job_split_slots(slots_set: SlotSet, job, hy):
    """
    According to a resources a :class:`SlotSet` find the time and the resources to launch a job.
    This function supports the moldable jobs. In case of multiple moldable job corresponding to the request
    it selects the first to finish.

    This function has two side effects.
        - Assign the results directly to the ``job`` (such as start_time, resources etc)
        - Split the slot_set to reflect the new allocation

    One can replace this function using an extension of OAR.

    :param ProcSet slots_set: A :class:`SlotSet` of the current platform
    :param [Job] job: The job to schedule
    :param hy: \
        The description of the resources hierarchy
    """
    prev_t_finish = 2**32 - 1  # large enough
    prev_res_set = ProcSet()
    prev_res_rqt = ProcSet()

    slots = slots_set.slots
    prev_start_time = slots[1].b

    for res_rqt in job.mld_res_rqts:
        (mld_id, walltime, hy_res_rqts) = res_rqt
        (res_set, sid_left, sid_right) = find_first_suitable_contiguous_slots(
            slots_set, job, res_rqt, hy
        )
        # print("after find fisrt suitable")
        t_finish = slots[sid_left].b + walltime
        if t_finish < prev_t_finish:
            prev_start_time = slots[sid_left].b
            prev_t_finish = t_finish
            prev_res_set = res_set
            prev_res_rqt = res_rqt
            (prev_sid_left, prev_sid_right) = slots_set.get_encompassing_range(
                prev_start_time, prev_t_finish
            )

    (mld_id, walltime, hy_res_rqts) = prev_res_rqt
    job.moldable_id = mld_id
    job.res_set = prev_res_set
    job.start_time = prev_start_time
    job.walltime = walltime

    # Take avantage of job.starttime = slots[prev_sid_left].b

    # print prev_sid_left, prev_sid_right, job.moldable_id , job.res_set,
    # job.start_time , job.walltime, job.mld_id

    slots_set.split_slots(prev_sid_left, prev_sid_right, job)


def schedule_id_jobs_ct(slots_sets, jobs, hy, id_jobs, security_time):
    """
    Schedule loop with support for jobs container - can be recursive
    (recursivity has not be tested)
    """

    #    for k,job in iteritems(jobs):
    # print "*********j_id:", k, job.mld_res_rqts[0]

    for jid in id_jobs:
        job = jobs[jid]

        ss_id = "default"
        if "inner" in job.types:
            ss_id = job.types["inner"]

        slots_set = slots_sets[ss_id]

        # slots_set.show_slots()

        assign_resources_mld_job_split_slots(slots_set, job, hy)

        if "container" in job.types:
            Slot(
                1,
                0,
                0,
                job.res_set,
                job.start_time,
                job.start_time + job.walltime - security_time,
            )
