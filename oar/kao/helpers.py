# coding: utf-8
import colorsys
import random

from oar.lib.globals import get_logger

NB_COLORS = 15
HSV_tuples = [(x * 1.0 / NB_COLORS, 0.5, 0.5) for x in range(NB_COLORS)]
RGB_tuples = map(lambda x: colorsys.hsv_to_rgb(*x), HSV_tuples)

logger = get_logger("oar.kamelot", forward_stderr=True)
# TODO remove useless code in profit to Evalys usage


def dump(obj):  # pragma: no cover
    for attr in dir(obj):
        print("obj.%s = %s" % (attr, getattr(obj, attr)))


def annotate(ax, rect, annot):  # pragma: no cover
    rx, ry = rect.get_xy()
    cx = rx + rect.get_width() / 2.0
    cy = ry + rect.get_height() / 2.0

    ax.annotate(annot, (cx, cy), color="black", fontsize=12, ha="center", va="center")


def plot_slots_and_job(slots_set, jobs, nb_res, t_max):  # pragma: no cover
    import matplotlib.patches as mpatch
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots()

    if slots_set:
        for sid, slot in slots_set.slots.items():
            col = "blue"
            if sid % 2:
                col = "red"
            for i, itv in enumerate(slot.itvs):
                (y0, y1) = itv
                # print i, y0,y1, slot.b, slot.e
                # rect =  mpatch.Rectangle((2,2), 8, 2)
                rect = mpatch.Rectangle(
                    (slot.b, y0 - 0.4),
                    slot.e - slot.b,
                    y1 - y0 + 0.9,
                    alpha=0.1,
                    color=col,
                )
                if i == 0:
                    annotate(ax, rect, "s" + str(sid))
                ax.add_artist(rect)

    if jobs:
        for jid, job in jobs.items():
            col = RGB_tuples[random.randint(0, NB_COLORS - 1)]
            duration = job.walltime
            if hasattr(job, "run_time"):
                duration = job.run_time
            for i, itv in enumerate(job.res_set):
                (y0, y1) = itv
                rect = mpatch.Rectangle(
                    (job.start_time, y0 - 0.4), duration, y1 - y0, alpha=0.2, color=col
                )
                if i == 0:
                    annotate(ax, rect, "j" + str(jid))
                ax.add_artist(rect)

    ax.set_xlim((0, t_max))
    ax.set_ylim((0, nb_res))
    #    ax.set_aspect('equal')
    ax.grid(True)
    mng = plt.get_current_fig_manager()
    try:
        mng.resize(*mng.window.maxsize())
        # mng.window.showMaximized()
    except Exception:
        # TODO Handle execption
        pass
    plt.show()
    # mpld3.show()


def slots_2_val_ref(slots):  # pragma: no cover
    """function used to generate reference value for unitest"""
    sid = 1
    while True:
        slot = slots[sid]
        print("(", slot.b, ",", slot.e, ",", slot.itvs, "),")
        sid = slot.next
        if sid == 0:
            break


def slots_all_2_val_ref(slots):  # pragma: no cover
    """function used to generate reference value for unitest"""
    sid = 1
    while True:
        slot = slots[sid]
        print(
            "(",
            slot.id,
            ",",
            slot.prev,
            ",",
            slot.next,
            ",",
            slot.itvs,
            ",",
            slot.b,
            ",",
            slot.e,
            "),",
        )
        sid = slot.next
        if sid == 0:
            break


def extract_find_assign_args(raw_args):
    funcname = raw_args.split(":")[0]
    kwargs = {}
    args = []
    for arg in raw_args.split(":")[1:]:
        item = arg.split("=")
        if len(item) >= 2:
            if item[0] != "":
                kwargs[item[0]] = "=".join(item[1:])
            else:
                args.append(arg)
        else:
            args.append(arg)
    return funcname, args, kwargs


def job_scheduling_record(job, scheduling_time_ms):
    """A small, oarstat-like summary of the job plus its scheduling duration."""
    deps = getattr(job, "deps", None) or []
    res_set = getattr(job, "res_set", None)
    record = {
        "job_id": getattr(job, "id", None),
        "name": getattr(job, "name", None),
        "queue": getattr(job, "queue_name", None),
        "user": getattr(job, "user", None),
        "project": getattr(job, "project", None),
        "types": dict(getattr(job, "types", {}) or {}),
        "dependencies": [dep[0] for dep in deps],
        "walltime": getattr(job, "walltime", None),
        "start_time": getattr(job, "start_time", None),
        "moldable_id": getattr(job, "moldable_id", None),
        "resources": str(res_set) if res_set is not None and len(res_set) > 0 else None,
        "scheduling_time_ms": round(scheduling_time_ms, 3),
    }
    # drop empty/missing fields to keep the yaml compact
    return {k: v for k, v in record.items() if v not in (None, {}, [])}


def write_scheduling_timing_yaml(path, records):
    """Append the per-job timing records of this round as one YAML document."""
    if not records:
        return
    try:
        import yaml

        with open(path, "a") as f:
            yaml.safe_dump(
                records,
                f,
                explicit_start=True,
                default_flow_style=False,
                sort_keys=False,
            )
    except Exception as e:  # pragma: no cover - best effort, never break scheduling
        logger.warning("could not write scheduling timing yaml to %s: %s", path, e)


# j1 = Job(1,"", 10, 10, "", "", "", {}, [(10, 20), (25,30)], 1, [])
# j2 = Job(2,"", 5, 5, "", "", "", {}, [(1, 10), (15,20)], 1, [])
# slots_set = SlotSet(Slot(1, 0, 2, [(1, 32)], 1, 20))
# slots_set.slots[2]=Slot(2,1,0,[(10,15,),(21,30)],21,40)

# j1 = Job(1,"", 5, 10, "", "", "", {}, [(10, 20)], 1, [])
# j2 = Job(2,"", 30, 20, "", "", "", {}, [(5, 15),(20, 28)], 1, [])

# res = [(1, 32)]
# ss = SlotSet(Slot(1, 0, 0, res, 0, 1000))
# all_ss = {0:ss}

# hy = {'node': [ [(1,8)], [(9,16)], [(17,24)], [(25,32)] ] }

# jobs ={}
# j3 = Job(3,"Waiting", 0, 0, "yop", "", "",{}, [], 0, [(1, 60, [  ( [("node", 2)], res)  ])])

# n = 1
# for i in range(1, n+1):
#    jobs[i] = Job(i,"Waiting", 0, 0, "yop", "", "",{}, [], 0, [(1, 60, [  ( [("node", 4)], res) ])])
# j_ids = range(1, n+1)

# j = Job(4,"Waiting", 0, 0, "yop", "", "",{}, [], 0, [(1, 60, [  ( [("node", 2)], res)  ])])
# jobs[4] = j
# j_ids.append(4)

# schedule_id_jobs_ct(all_ss, jobs, hy, j_ids, 10)

# plot_slots_and_job(all_ss[0], jobs, 40, 500)

#######
# assign_resources_mld_job_split_slots(ss, j3, hy)
# set_slots_with_prev_scheduled_jobs(all_ss, {1:j1, 2:j2}, [1,2], 10)

# plot_slots_and_job(all_ss[0], jobs, 40, 500)


# j = Job(1,"", 0 , 10, "", "", "", {}, [(10, 20)], 1, [])
# ss = SlotSet(Slot(1, 0, 0, [(1, 32)], 0, 20))
# ss.split_slots(1,1,j4)
# ss.show_slots()

#############
# v = [ ( 1, 0, 2, [], 0, 59 ),
#      ( 2, 1, 0, [(1, 32)], 60, 1000  ) ]

# ss = SlotSet(None, { i+1: Slot(*a) for i,a in enumerate(v) } )
# j1 = Job(1,"Waiting", 0, 0, "yop", "", "",{}, [], 0, [(1, 60, [  ( [("node", 4)], res)  ])])
# j2 = Job(2,"Waiting", 0, 0, "yop", "", "",{}, [], 0, [(1, 60, [  ( [("node", 4)], res)  ])])

# jobs = {1: j1, 2: j2}


# schedule_id_jobs_ct({0: ss}, jobs, hy, [1, 2], 10)

# plot_slots_and_job(ss, jobs, 40, 500)

##################
# j = Job(5,"Waiting", 0, 0, "yop", "", "",{}, [], 0, [(1, 60, [  ( [("node", 3)], res)  ])])
# schedule_id_jobs_ct(all_ss, {5: j}, hy, [5], 10)

# plot_slots_and_job(ss, jobs, 40, 500)


##################
# v = [ ( 1, 0, 2, [], 0, 59 ), ( 2, 1, 0, [(1, 32)], 60, 2**31  ) ]
# v = [ ( 1, 0, 0, [(1, 32)], 0, 2**31  ) ]

# ss = SlotSet(None, { i+1: Slot(*a) for i,a in enumerate(v) } )

# ss = SlotSet(Slot(1, 0, 0, list(res), 10, 2**31))

# ss.show_slots()

# n = 10
# jobs = {}
# for i in range (1, n+1):
#    jobs[i]= Job(i,"Waiting", 0, 0, "yop", "", "",{}, [], 0, [(1, 60, [  ( [("node", 1)], list(res) )  ])])

# jids = range(1, n+1)

# schedule_id_jobs_ct({0: ss}, jobs, hy, jids, 10)

# plot_slots_and_job(ss, jobs, 40, 10000)
