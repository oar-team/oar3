# coding: utf-8
import copy
import pickle

from procset import ProcInt, ProcSet

import oar.kao.scheduling
from oar.lib import config
from oar.lib.hierarchy import extract_n_scattered_block_itv

try:
    import zerorpc
except ImportError:
    zerorpc = None


def find_default(itvs_avail, hy_res_rqts, hy, beginning, *find_args, **find_kwargs):
    """Simple wrap function to default function for test purpose"""
    return oar.kao.scheduling.find_resource_hierarchies_job(itvs_avail, hy_res_rqts, hy)


def assign_default(slots_set, job, hy, min_start_time, *assign_args, **assign_kwargs):
    """Simple wrap function to default function for test purpose"""
    return oar.kao.scheduling.assign_resources_mld_job_split_slots(
        slots_set, job, hy, min_start_time
    )


def find_begin(itvs_avail, hy_res_rqts, hy, beginning, *find_args, **find_kwargs):
    """Simple function to test beginning value which is set to True is the slot begins the slotset (slotset.begin == slots[1].b).
    It's only for test/example purpose"""

    if beginning:
        return oar.kao.scheduling.find_resource_hierarchies_job(
            itvs_avail, hy_res_rqts, hy
        )
    else:
        return ProcSet(*[(1, 16)])


def find_contiguous_1h(itvs_avail, hy_res_rqts, hy, beginning):
    # NOT FOR PRODUCTION USE
    # Notes support only one resource group and ordered resource_id hierarchy level
    # Sorted by building

    hy_level_nbs, constraints = hy_res_rqts[0]  # one resource group
    l_name, n = hy_level_nbs[0]  # one hierarchy level
    # hy_level = hy[l_name]

    itvs_cts_slots = constraints & itvs_avail

    if l_name == "resource_id":
        for itv in itvs_cts_slots.intervals():
            if len(itv) >= n:
                return ProcSet(ProcInt(itv.inf, itv.inf + (n - 1)))

    return ProcSet()


def find_contiguous_sorted_1h(itvs_avail, hy_res_rqts, hy, beginning):
    # NOT FOR PRODUCTION USE
    # Notes support only one resource group and ordered resource_id hierarchy level

    hy_level_nbs, constraints = hy_res_rqts[0]  # one resource group
    l_name, n = hy_level_nbs[0]  # one hierarchy level
    # hy_level = hy[l_name]

    itvs_unsorted = [itv for itv in (constraints & itvs_avail).intervals()]
    lg = len(itvs_unsorted)

    ids_sorted = sorted(range(lg), key=lambda k: len(itvs_unsorted[k]))

    if l_name == "resource_id":
        for i in ids_sorted:
            itv = itvs_unsorted[i]
            if len(itv) >= n:
                return ProcSet(ProcInt(itv.inf, itv.inf + (n - 1)))

    return ProcSet()


#
# LOCAL
#


def find_resource_n_h_local(itvs, hy, rqts, top, h, h_bottom):

    n = rqts[h + 1]
    size_bks = []
    avail_bks = []

    for top_itvs in top:
        avail_itvs = top_itvs & itvs
        avail_bks.append(avail_itvs)
        size_bks.append(len(avail_itvs))

    sorted_ids = sorted(range(len(avail_bks)), key=lambda k: size_bks[k])

    for i, idx in enumerate(sorted_ids):
        if size_bks[i] >= n:
            res_itvs = ProcSet()
            k = 0

            for itv in avail_bks[idx].intervals():
                if (k + len(itv)) < n:
                    res_itvs.insert(itv)
                    k += len(itv)
                else:
                    res_itvs.insert(ProcInt(itv.inf, itv.inf + (n - k - 1)))
                    return res_itvs
    return ProcSet()


def find_resource_hierarchies_scattered_local(itvs, hy, rqts):
    l_hy = len(hy)
    #    print "find itvs: ", itvs, rqts[0]
    if l_hy == 1:
        return extract_n_scattered_block_itv(itvs, hy[0], rqts[0])
    else:
        return find_resource_n_h_local(itvs, hy, rqts, hy[0], 0, l_hy)


def find_local(itvs_slots, hy_res_rqts, hy, beginning):
    """2 Level of Hierarchy supported with sorting by increasing blocks' size"""
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
        # import pdb; pdb.set_trace()
        res = find_resource_hierarchies_scattered_local(
            itvs_cts_slots, hy_levels, hy_nbs
        )
        if res:
            result = result | res
        else:
            return ProcSet()

    return result


def assign_one_time_find_mld(slots_set, job, hy, min_start_time):
    """"""
    # NOT FOR PRODUCTION USE

    flag_find = True
    prev_t_finish = 2**32 - 1  # large enough
    prev_res_set = ProcSet()
    prev_res_rqt = ProcSet()

    slots = slots_set.slots
    prev_start_time = slots[1].b

    for res_rqt in job.mld_res_rqts:
        mld_id, walltime, _ = res_rqt
        (
            res_set,
            sid_left,
            sid_right,
        ) = oar.kao.scheduling.find_first_suitable_contiguous_slots(
            slots_set, job, res_rqt, hy, min_start_time
        )
        if len(res_set) == 0:  # no suitable time*resources found
            job.res_set = ProcSet()
            job.start_time = -1
            job.moldable_id = -1
            return
        # print("after find fisrt suitable")
        t_finish = slots[sid_left].b + walltime
        if t_finish < prev_t_finish:
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

    mld_id, walltime, _ = prev_res_rqt
    job.moldable_id = mld_id
    job.res_set = prev_res_set
    job.start_time = prev_start_time
    job.walltime = walltime

    # Take avantage of job.starttime = slots[prev_sid_left].b
    # print(prev_sid_left, prev_sid_right, job.moldable_id , job.res_set,)
    # job.start_time , job.walltime, job.mld_id

    slots_set.split_slots(prev_sid_left, prev_sid_right, job)
    # returns value other than None value to indicate successful assign
    return prev_sid_left, prev_sid_right, job


def assign_one_time_find(slots_set, job, hy, min_start_time):
    """"""
    # NOT FOR PRODUCTION USE

    flag_find = True
    prev_t_finish = 2**32 - 1  # large enough
    prev_res_set = ProcSet()
    prev_res_rqt = ProcSet()

    slots = slots_set.slots
    prev_start_time = slots[1].b

    res_rqt = job.mld_res_rqts[0]
    res_rqts = [(rq[0], copy.copy(rq[1])) for rq in res_rqt[2]]
    res_rqt_copy = (res_rqt[0], res_rqt[1], res_rqts)  # to keep set of intervals

    while True:
        mld_id, walltime, _ = res_rqt
        (
            res_set,
            sid_left,
            sid_right,
        ) = oar.kao.scheduling.find_first_suitable_contiguous_slots(
            slots_set, job, res_rqt, hy, min_start_time
        )
        if len(res_set) == 0:  # no suitable time*resources found
            job.res_set = ProcSet()
            job.start_time = -1
            job.moldable_id = -1
            return
        # print("after find fisrt suitable")
        t_finish = slots[sid_left].b + walltime
        if t_finish < prev_t_finish:
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

    mld_id, walltime, _ = prev_res_rqt
    job.moldable_id = mld_id
    job.res_set = prev_res_set
    job.start_time = prev_start_time
    job.walltime = walltime

    # Take avantage of job.starttime = slots[prev_sid_left].b
    # print(prev_sid_left, prev_sid_right, job.moldable_id , job.res_set,)
    # job.start_time , job.walltime, job.mld_id

    slots_set.split_slots(prev_sid_left, prev_sid_right, job)
    # returns value other than None value to indicate successful assign
    return prev_sid_left, prev_sid_right, job


def find_coorm(itvs_avail, hy_res_rqts, hy, beginning, *find_args, **find_kwargs):
    if zerorpc is None:
        return find_default(itvs_avail, hy_res_rqts, hy, beginning)
    c = zerorpc.Client()
    protocol, ip, port = find_args[:3]
    c.connect("%s://%s:%s" % (protocol, ip, port))
    return c.find_resource_hierarchies(
        itvs_avail,
        hy_res_rqts,
        hy,
    )


def assign_coorm(slots_set, job, hy, min_start_time, *assign_args, **assign_kwargs):
    if zerorpc is None:
        return assign_default(slots_set, job, hy, min_start_time)
    timeout = assign_kwargs.get("timeout", None)
    if timeout is not None and timeout.isdigit():
        default_timeout = int(timeout)
        if config["COORM_DEFAULT_TIMEOUT"] < default_timeout:
            default_timeout = config["COORM_DEFAULT_TIMEOUT"]
    else:
        default_timeout = None
    # Init connetion with COORM application
    c = zerorpc.Client(timeout=default_timeout)
    protocol, ip, port = assign_args[:3]
    c.connect("%s://%s:%s" % (protocol, ip, port))

    # Convert the job to dict, to preserve values after de/serialisation
    missing = object()
    dict_job = job.to_dict()
    for k in ("mld_res_rqts", "key_cache", "ts", "ph", "find"):
        if getattr(job, k, missing) is not missing:
            dict_job[k] = getattr(job, k)

    # Remote call
    prev_sid_left, prev_sid_right, dict_job = c.assign_resources(
        pickle.dumps(slots_set), dict_job, hy, min_start_time
    )

    # Propagate modified job values outside
    for k in ("moldable_id", "res_set", "start_time", "walltime"):
        if k in dict_job:
            setattr(job, k, dict_job.get(k))

    # Split SlotSet to add our reservation
    slots_set.split_slots(prev_sid_left, prev_sid_right, job)
    return prev_sid_left, prev_sid_right, job
