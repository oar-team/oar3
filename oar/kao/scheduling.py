# coding: utf-8
"""
Scheduling functions used by :py:mod:`oar.kao.kamelot`.
"""
import copy
from typing import Any, Tuple

from procset import ProcSet

from oar.kao.quotas import Quotas
from oar.kao.slot import Slot, SlotSet, intersec_itvs_slots, intersec_ts_ph_itvs_slots
from oar.lib.globals import get_logger, init_oar
from oar.lib.hierarchy import find_resource_hierarchies_scattered
from oar.lib.job_handling import ALLOW, JobPseudo
from oar.lib.models import Job

# for quotas
from oar.lib.resource import ResourceSet

config, db = init_oar(no_db=True)
logger = get_logger("oar.kamelot", forward_stderr=True)


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
    """
    Given a job resource request and a set of resources this function tries to find a matching allocation.

    .. note::
        This` can be override with the oar `extension <../admin/extensions.html#functions-assign-and-find>`_ mechanism.

    :param itvs_slots: A procset of the resources available for the allocation
    :type itvs_slots: :class:`procset.ProcSet`
    :param hy_res_rqts: The job's request
    :param hy: The definition of the resources hierarchy
    :return [ProcSet]: \
            The allocation if found, otherwise an empty :class:`procset.ProcSet`
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
        res = find_resource_hierarchies_scattered(itvs_cts_slots, hy_levels, hy_nbs)
        if res:
            result = result | res
        else:
            return ProcSet()

    return result


def find_first_suitable_contiguous_slots_quotas(
    slots_set: SlotSet, job, res_rqt: Tuple[int, int, Any], hy, min_start_time: int
):
    """
    Loop through time slices from a :py:class:`oar.kao.slot.SlotSet` that are long enough for the job's walltime.
    For each compatible time slice, call the function :py:func:`find_resource_hierarchies_job`
    to find compatible resources allocation for the job, if such allocation is found the function ends.

    :param SlotSet slots_set: Slot set of the current platform
    :param Job job: The job to schedule
    :param res_rqt: The job resource request
    :param hy: The definition of the resources hierarchy
    :param min_start_time: The earliest date at which the job can start
    """

    (mld_id, walltime, hy_res_rqts) = res_rqt

    itvs = ProcSet()

    slots = slots_set.slots
    cache = slots_set.cache

    # flag to control cache update for considered entry
    # no_cache = False

    sid_left = slots_set.first().id
    if min_start_time < 0:
        # to not always begin by the first slots ( O(n^2) )
        # TODO cache_by_container/inner + moldable + time_sharing(?)
        if job.key_cache and (job.key_cache[mld_id] in cache):
            sid_left = cache[job.key_cache[mld_id]]

    else:
        sid_left = slots_set.slot_id_at(min_start_time)

    sid_right = sid_left
    for slot_begin, slot_end in slots_set.traverse_with_width(
        walltime, start_id=sid_left
    ):
        sid_left = slot_begin.id
        sid_right = slot_end.id

        if Quotas.calendar and not job.no_quotas:
            time_limit = slot_begin.b + Quotas.calendar.quotas_window_time_limit
            if slot_end.e > time_limit:
                logger.info(
                    "can't schedule job with id: {}, QUOTAS_WINDOW_TIME_LIMIT reached {}".format(
                        job.id, walltime
                    )
                )
                return (ProcSet(), -1, -1)

            # test next slot need to be temporal_quotas sliced
            if slot_end.quotas_rules_id == -1:
                # assumption is done that this part is rarely executed (either it's abnormal)
                t_begin = slot_end.b
                quotas_rules_id, remaining_duration = Quotas.calendar.rules_at(t_begin)
                slots_set.temporal_quotas_split_slot(
                    slot_end, quotas_rules_id, remaining_duration
                )

        if job.ts or (job.ph == ALLOW):
            itvs_avail = intersec_ts_ph_itvs_slots(slots, sid_left, sid_right, job)
        else:
            itvs_avail = intersec_itvs_slots(slots, sid_left, sid_right)

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
                **job.find_kwargs,
            )
        else:
            itvs = find_resource_hierarchies_job(itvs_avail, hy_res_rqts, hy)

        if len(itvs) != 0:
            nb_res = len(itvs & ResourceSet.default_itvs)
            res = Quotas.check_slots_quotas(
                slots, sid_left, sid_right, job, nb_res, walltime
            )
            (quotas_ok, quotas_msg, rule, value) = res

            if not quotas_ok:
                logger.info(
                    f"Quotas limitation reached, job: {str(job.id)}, {quotas_msg}, rule: {rule}, value: {value}"
                )
                itvs = ProcSet()
            else:
                break

    if len(itvs) == 0:
        logger.info(
            "can't schedule job with id: {}, walltime not satisfied: {}".format(
                job.id, walltime
            )
        )
        return (ProcSet(), -1, -1)

    return (itvs, sid_left, sid_right)


def find_first_suitable_contiguous_slots_no_quotas(
    slots_set: SlotSet, job, res_rqt, hy, min_start_time: int
):
    """
    Loop through time slices from a :py:class:`oar.kao.slot.SlotSet` that are long enough for the job's walltime.
    For each compatible time slice, call the function :py:func:`find_resource_hierarchies_job`
    to find compatible resources allocation for the job, if such allocation is found the function ends.

    :param SlotSet slots_set: Slot set of the current platform
    :param Job job: The job to schedule
    :param res_rqt: The job resource request
    :param hy: The definition of the resources hierarchy
    :param min_start_time: The earliest date at which the job can start
    """

    (mld_id, walltime, hy_res_rqts) = res_rqt

    itvs = ProcSet()

    slots = slots_set.slots
    cache = slots_set.cache

    # flag to control cache update for considered entry
    no_cache = False

    sid_left = slots_set.first().id
    if min_start_time < 0:
        # to not always begin by the first slots ( O(n^2) )
        # TODO cache_by_container/inner + moldable + time_sharing(?)
        if job.key_cache and (job.key_cache[mld_id] in cache):
            sid_left = cache[job.key_cache[mld_id]]

    else:
        sid_left = slots_set.slot_id_at(min_start_time)

    sid_right = sid_left
    for slot_begin, slot_end in slots_set.traverse_with_width(
        walltime, start_id=sid_left
    ):
        sid_left = slot_begin.id
        sid_right = slot_end.id

        if job.ts or (job.ph == ALLOW):
            itvs_avail = intersec_ts_ph_itvs_slots(
                slots, slot_begin.id, slot_end.id, job
            )
        else:
            itvs_avail = intersec_itvs_slots(slots, slot_begin.id, slot_end.id)

        if job.find:
            itvs = job.find_func(
                itvs_avail,
                hy_res_rqts,
                hy,
                # True if this is the first slot
                slot_begin.prev == 0,
                *job.find_args,
                **job.find_kwargs,
            )
        else:
            itvs = find_resource_hierarchies_job(itvs_avail, hy_res_rqts, hy)

        if len(itvs) != 0:
            break

    if len(itvs) == 0:  # TODO error
        # TODO: fill cache also if the job cannot be scheduled
        logger.info(
            "can't schedule job with id: {}, no suitable resources".format(job.id)
        )
        return (ProcSet(), -1, -1)
    else:
        if (
            job.key_cache and (min_start_time < 0) and (not no_cache)
        ):  # and (not job.deps):
            cache[job.key_cache[mld_id]] = sid_left

        return (itvs, sid_left, sid_right)


def find_first_suitable_contiguous_slots(
    slots_set: SlotSet, job, res_rqt, hy, min_start_time: int
):
    """
    Loop through time slices from a :py:class:`oar.kao.slot.SlotSet` that are long enough for the job's walltime.
    For each compatible time slice, call the function :py:func:`find_resource_hierarchies_job`
    to find compatible resources allocation for the job, if such allocation is found the function ends.

    :param SlotSet slots_set: Slot set of the current platform
    :param Job job: The job to schedule
    :param res_rqt: The job resource request
    :param hy: The definition of the resources hierarchy
    :param min_start_time: The earliest date at which the job can start
    """

    if Quotas.enabled and not job.no_quotas:
        return find_first_suitable_contiguous_slots_quotas(
            slots_set, job, res_rqt, hy, min_start_time
        )

    return find_first_suitable_contiguous_slots_no_quotas(
        slots_set, job, res_rqt, hy, min_start_time
    )


def assign_resources_mld_job_split_slots(
    slots_set: SlotSet, job: Job, hy, min_start_time
):
    """
    According to a resources a :class:`SlotSet` find the time and the resources to launch a job.
    This function supports the moldable jobs. In case of multiple moldable job corresponding to the request
    it selects the first to finish.

    This function has two side effects.
        - Assign the results directly to the ``job`` (such as start_time, resources etc)
        - Split the slot_set to reflect the new allocation

    .. note::
        This function can be override with the oar `extension <../admin/extensions.html#functions-assign-and-find>`_ mechanism.

    :param SlotSet slots_set: A :class:`SlotSet` of the current platform
    :param Job job: The job to schedule
    :param hy: \
        The description of the resources hierarchy
    """
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
            (prev_sid_left, prev_sid_right) = slots_set.get_encompassing_slots(
                prev_start_time, prev_t_finish
            )

    # no suitable time*resources found for all res_rqt
    if res_set_nfound == len(job.mld_res_rqts):
        logger.info(f"cannot schedule job {job.id}")
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
    # FIXME: return value not used by kamelot
    return prev_sid_left, prev_sid_right, job


def schedule_id_jobs_ct(slots_sets, jobs, hy, id_jobs, job_security_time):
    """
    Main scheduling loop with support for jobs container - can be recursive (recursion has not been tested)
    Find an allocation for each waiting jobs.

    :param SlotSet slots_sets: A :class:`SlotSet` of the current platform
    :param [Job] jobs: The list of the waiting jobs to schedule
    :param hy: \
        The description of the resources hierarchy
    :param list jobs: the list of job ids
    :param Int job_security_time: The job security time (see `oar.conf <../admin/configuration.html>`_ ``SCHEDULER_JOB_SECURITY_TIME`` variable)
    """

    #    for k,job in jobs.items():
    # print("*********j_id:", k, job.mld_res_rqts[0])

    # logger.debug(f"SlotSet Default (Before):\n{slots_sets['default']}")

    for jid in id_jobs:
        logger.debug("Schedule job:" + str(jid))
        job = jobs[jid]

        min_start_time = -1
        to_skip = False
        # Dependencies
        for j_dep in job.deps:
            jid_dep, state, exit_code = j_dep
            # a dependency with job in Error is ignored (same behavior that OAR2)
            if state == "Error":
                logger.info(
                    "job("
                    + str(jid_dep)
                    + ") in dependencies for job("
                    + str(jid)
                    + ") is in error state, it's ignored"
                )
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
                    **job.assign_kwargs,
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

    # logger.debug(f"SlotSet Default (After):\n{slots_sets['default']}")
    # for jid in id_jobs:
    #     job = jobs[jid]
    #     logger.debug(f"Job id:{jid}, start_time: {job.start_time}, res_set: {job.res_set}")
