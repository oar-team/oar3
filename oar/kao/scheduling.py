# coding: utf-8
import copy

from procset import ProcSet

from oar.kao.quotas import Quotas
from oar.kao.slot import Slot, SlotSet, intersec_itvs_slots, intersec_ts_ph_itvs_slots
from oar.lib import config, get_logger
from oar.lib.hierarchy import find_resource_hierarchies_scattered
from oar.lib.job_handling import ALLOW, JobPseudo

# for quotas
from oar.lib.resource import ResourceSet

logger = get_logger("oar.kamelot")


def set_slots_with_prev_scheduled_jobs(
    slots_sets,
    jobs,
    job_security_time,
    now=0,
    filter_besteffort=True,
    only_besteffort=False,
):

    jobs_slotsets = {"default": []}

    for job in jobs:
        logger.debug("job.id:" + str(job.id))
        if ((not filter_besteffort) and ("besteffort" in job.types)) or (
            (not only_besteffort) and (not ("besteffort" in job.types))
        ):
            if "container" in job.types:
                # t_e = job.start_time + job.walltime - job_security_time
                # t "job.res_set, job.start_time, t_e", job.res_set,
                # job.start_time, t_e

                if job.types["container"] != "":
                    ss_name = job.types["container"]
                else:
                    ss_name = str(job.id)

                logger.debug("container:" + ss_name)

                if ss_name not in slots_sets:
                    slots_sets[ss_name] = SlotSet((ProcSet(), 1))

                if job.start_time < now:
                    start_time = now
                else:
                    start_time = job.start_time

                j = JobPseudo(
                    id=0,
                    start_time=start_time,
                    walltime=job.walltime - job_security_time,
                    res_set=job.res_set,
                    ts=job.ts,
                    ph=job.ts,
                )

                slots_sets[ss_name].split_slots_jobs([j], False)  # add job's resources

            ss_name = "default"
            if "inner" in job.types:
                ss_name = job.types["inner"]

            if ss_name not in jobs_slotsets:
                jobs_slotsets[ss_name] = []

            jobs_slotsets[ss_name].append(job)

    for ss_name, slot_set in slots_sets.items():
        logger.debug(" slots_sets.items():" + ss_name)
        if ss_name in jobs_slotsets:
            slot_set.split_slots_jobs(jobs_slotsets[ss_name])


def find_resource_hierarchies_job(itvs_slots, hy_res_rqts, hy):
    """find resources in interval for all resource subrequests of a moldable
    instance of a job"""
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
        res = find_resource_hierarchies_scattered(itvs_cts_slots, hy_levels, hy_nbs)
        if res:
            result = result | res
        else:
            return ProcSet()

    return result


def get_encompassing_slots(slots, t_begin, t_end):

    sid_left = 1

    while slots[sid_left].e < t_begin:
        sid_left = slots[sid_left].next

    sid_right = sid_left

    while slots[sid_right].e < t_end:
        sid_right = slots[sid_right].next

    return (sid_left, sid_right)


def find_first_suitable_contiguous_slots(slots_set, job, res_rqt, hy, min_start_time):
    """find first_suitable_contiguous_slot"""

    (mld_id, walltime, hy_res_rqts) = res_rqt

    itvs = ProcSet()

    slots = slots_set.slots
    cache = slots_set.cache
    # flag to control cache update for considered entry
    no_cache = False
    # updated_cache = False
    sid_left = 1
    if min_start_time < 0:
        # to not always begin by the first slots ( O(n^2) )
        # TODO cache_by_container/inner + moldable + time_sharing(?)
        if job.key_cache and (job.key_cache[mld_id] in cache):
            sid_left = cache[job.key_cache[mld_id]]
            # print("cache hit...... ", sid_left)
            # else:
            # print("cache miss :(")

    else:
        while slots[sid_left].b < min_start_time:
            sid_left = slots[sid_left].next
        # satisfy job dependencies converted in min start_time

    # sid_left = 1 # TODO no cache

    sid_right = sid_left
    slot_e = slots[sid_right].e

    # print('first sid_left', sid_left)

    while True:
        # find next contiguous slots_time
        # print("A: job.id:", job.id, "sid_left:", sid_left, "sid_right:",)
        # sid_right

        if sid_left != 0 and sid_right != 0:
            slot_b = slots[sid_left].b
        else:
            # TODO error
            # print("TODO error can't schedule job.id:", job.id)
            logger.info(
                "can't schedule job with id: {}, no suitable resources".format(job.id)
            )
            return (ProcSet(), -1, -1)
        # import pdb; pdb.set_trace()
        if Quotas.calendar and (not job.no_quotas):
            time_limit = slot_b + config["QUOTAS_WINDOW_TIME_LIMIT"]
            while (slot_e - slot_b + 1) < walltime:
                if slot_e > time_limit:
                    logger.info(
                        "can't schedule job with id: {}, QUOTAS_WINDOW_TIME_LIMIT reached {}".format(
                            job.id, walltime
                        )
                    )
                    return (ProcSet(), -1, -1)
                # test next slot need to be temporal_quotas sliced
                if slots[sid_right].quotas_rules_id == -1:
                    # assumption is done that this part is rarely executed (either it's abnormal)
                    t_begin = slots[sid_right].b
                    quotas_rules_id, remaining_duration = Quotas.calendar.rules_at(
                        t_begin
                    )
                    slots_set.temporal_quotas_split_slot(
                        slots[sid_right], quotas_rules_id, remaining_duration
                    )
                    # TODO to long extenion extension here also

                sid_right = slots[sid_right].next
                if sid_right != 0:
                    slot_e = slots[sid_right].e
                else:
                    logger.info(
                        "can't schedule job with id: {}, walltime not satisfied: {}".format(
                            job.id, walltime
                        )
                    )
                    return (ProcSet(), -1, -1)

                if slots[sid_left].quotas_rules_id != slots[sid_right].quotas_rules_id:
                    sid_left = sid_right
                    if sid_left == 0:
                        logger.info(
                            "can't schedule job with id: {}, temporal quotas, no more slot".format(
                                job.id
                            )
                        )
                        return (ProcSet(), -1, -1)
        else:
            while (slot_e - slot_b + 1) < walltime:
                sid_right = slots[sid_right].next
                if sid_right != 0:
                    slot_e = slots[sid_right].e
                else:
                    logger.info(
                        "can't schedule job with id: {}, walltime not satisfied: {}".format(
                            job.id, walltime
                        )
                    )
                    return (ProcSet(), -1, -1)

        #        if not updated_cache and (slots[sid_left].itvs != []):
        #            cache[walltime] = sid_left
        #            updated_cache = True

        if job.ts or (job.ph == ALLOW):
            itvs_avail = intersec_ts_ph_itvs_slots(slots, sid_left, sid_right, job)
        else:
            itvs_avail = intersec_itvs_slots(slots, sid_left, sid_right)
        # print("itvs_avail", itvs_avail, "h_res_req", hy_res_rqts, "hy", hy)
        if job.find:
            beginning_slotset = (
                True if (sid_left == 1) and (slots_set.begin == slots[1].b) else False
            )
            # Use specialized find resource function
            itvs = job.find_func(
                itvs_avail,
                hy_res_rqts,
                hy,
                beginning_slotset,
                *job.find_args,
                **job.find_kwargs
            )
        else:
            itvs = find_resource_hierarchies_job(itvs_avail, hy_res_rqts, hy)

        if len(itvs) != 0:
            if Quotas.enabled and (not job.no_quotas):
                nb_res = len(itvs & ResourceSet.default_itvs)
                res = Quotas.check_slots_quotas(
                    slots, sid_left, sid_right, job, nb_res, walltime
                )
                (quotas_ok, quotas_msg, rule, value) = res
                if not quotas_ok:
                    logger.info(
                        "Quotas limitation reached, job:"
                        + str(job.id)
                        + ", "
                        + quotas_msg
                        + ", rule: "
                        + str(rule)
                        + ", value: "
                        + str(value)
                    )
                    # quotas limitation trigger therefore disable cache update for this entry
                    no_cache = True
                else:
                    break
            else:
                break

        sid_left = slots[sid_left].next

    if job.key_cache and (min_start_time < 0) and (not no_cache):  # and (not job.deps):
        cache[job.key_cache[mld_id]] = sid_left
        #        print("cache: update entry ",  job.key_cache[mld_id], " with ", sid_left)
        # else:
        # print("cache: not updated ", job.key_cache, min_start_time, job.deps)

    return (itvs, sid_left, sid_right)


def assign_resources_mld_job_split_slots(slots_set, job, hy, min_start_time):
    """Assign resources to a job and update by spliting the concerned slots - moldable version"""
    prev_t_finish = 2**32 - 1  # large enough
    prev_res_set = ProcSet()
    prev_res_rqt = ProcSet()

    slots = slots_set.slots
    prev_start_time = slots[1].b

    res_set_nfound = 0

    for res_rqt in job.mld_res_rqts:
        mld_id, walltime, hy_res_rqts = res_rqt
        res_set, sid_left, sid_right = find_first_suitable_contiguous_slots(
            slots_set, job, res_rqt, hy, min_start_time
        )
        if len(res_set) == 0:  # no suitable time*resources found
            res_set_nfound += 1
            continue

        # print("after find fisrt suitable")
        t_finish = slots[sid_left].b + walltime
        if t_finish < prev_t_finish:
            prev_start_time = slots[sid_left].b
            prev_t_finish = t_finish
            prev_res_set = res_set
            prev_res_rqt = res_rqt
            prev_sid_left = sid_left
            prev_sid_right = sid_right

    # no suitable time*resources found for all res_rqt
    if res_set_nfound == len(job.mld_res_rqts):
        job.res_set = ProcSet()
        job.start_time = -1
        job.moldable_id = -1
        return

    (mld_id, walltime, hy_res_rqts) = prev_res_rqt
    job.moldable_id = mld_id
    job.res_set = prev_res_set
    job.start_time = prev_start_time
    job.walltime = walltime

    # Take avantage of job.starttime = slots[prev_sid_left].b
    # logger.debug("ASSIGN " + str(job.moldable_id) + " " + str(job.res_set))
    # job.start_time , job.walltime, job.mld_id

    slots_set.split_slots(prev_sid_left, prev_sid_right, job)
    # returns value other than None value to indicate successful assign
    return prev_sid_left, prev_sid_right, job


def schedule_id_jobs_ct(slots_sets, jobs, hy, id_jobs, job_security_time):
    """Schedule loop with support for jobs container - can be recursive (recursion has not be tested)"""

    #    for k,job in jobs.items():
    # print("*********j_id:", k, job.mld_res_rqts[0])

    for jid in id_jobs:
        logger.debug("Schedule job:" + str(jid))
        job = jobs[jid]

        min_start_time = -1
        to_skip = False
        # Dependencies
        for j_dep in job.deps:
            jid_dep, state, exit_code = j_dep
            if state == "Error":
                logger.info(
                    "job("
                    + str(jid_dep)
                    + ") in dependencies for job("
                    + str(jid)
                    + ") is in error state"
                )
                # TODO  set job to ERROR"
                to_skip = True
                break
            elif state == "Waiting":
                # determine endtime
                if jid_dep in jobs:
                    job_dep = jobs[jid_dep]
                    job_dep_stop_time = job_dep.start_time + job_dep.walltime
                    if job_dep_stop_time > min_start_time:
                        min_start_time = job_dep_stop_time
                else:
                    # TODO
                    to_skip = True
                    break
            elif state == "Terminated" and exit_code == 0:
                next
            else:
                to_skip = True
                break

        if to_skip:
            # Set job as currently not schedulable
            job.start_time = -1
            logger.info("job(" + str(jid) + ") can't be scheduled due to dependencies")
        else:
            ss_name = "default"
            if "inner" in job.types:
                ss_name = job.types["inner"]

            if ss_name not in slots_sets:
                logger.error(
                    "job("
                    + str(jid)
                    + ") can't be scheduled, slots set '"
                    + ss_name
                    + "' is missing. Skip it for this round."
                )
                next

            slots_set = slots_sets[ss_name]

            if job.assign:
                # Use specialized assign function
                job.assign_func(
                    slots_set,
                    job,
                    hy,
                    min_start_time,
                    *job.assign_args,
                    **job.assign_kwargs
                )
            else:
                assign_resources_mld_job_split_slots(slots_set, job, hy, min_start_time)

            if "container" in job.types:
                if job.types["container"] == "":
                    ss_name = str(job.id)
                else:
                    ss_name = job.types["container"]

                if ss_name in slots_sets:
                    j = JobPseudo(
                        id=0,
                        start_time=job.start_time,
                        walltime=job.walltime - job_security_time,
                        res_set=job.res_set,
                        ts=job.ts,
                        ph=job.ts,
                    )
                    slots_sets[ss_name].split_slots_jobs([j], False)

                else:
                    slot = Slot(
                        1,
                        0,
                        0,
                        copy.copy(job.res_set),
                        job.start_time,
                        job.start_time + job.walltime - job_security_time,
                    )
                    # slot.show()
                    slots_sets[ss_name] = SlotSet(slot)
