# coding: utf-8
from oar.lib import config
from functools import reduce

from oar.kao.job import NO_PLACEHOLDER, PLACEHOLDER, ALLOW
from oar.lib.interval import intersec, sub_intervals, add_intervals
import oar.kao.quotas as qts
from copy import deepcopy

MAX_TIME = 2147483648  # (* 2**31 *)


class Slot(object):

    def __init__(self, id, prev, next, itvs, b, e, ts_itvs={}, ph_itvs={}):
        self.id = id
        self.prev = prev
        self.next = next
        self.itvs = itvs
        self.b = b
        self.e = e
        # timesharing ts_itvs: [user] * [job_name] * itvs
        self.ts_itvs = ts_itvs
        self.ph_itvs = ph_itvs  # placeholder ph_itvs: [ph_name] * itvs
        if ('QUOTAS' in config) and (config['QUOTAS'] == 'yes'):
            self.quotas = qts.Quotas()

    def show(self):
        print("%s" % self)

    def __str__(self):
        repr_string = "id=%(id)s, prev=%(prev)s, next=%(next)s, itvs=%(itvs)s, " \
                      "b=%(b)s, e=%(e)s, ts_itvs=%(ts_itvs)s, ph_itvs=%(ph_itvs)s"
        return "Slot(%s)" % (repr_string % vars(self))

    def __repr__(self):
        return "<%s>" % self


def intersec_slots(slots):  # not used TO REMOVE?
    "Return intersection of intervals from a slot list"
    return reduce(lambda itvs_acc, s: intersec(itvs_acc, s.itvs),
                  slots,
                  slots[0].itvs)


def intersec_itvs_slots(slots, sid_left, sid_right):
    sid = sid_left
    itvs_acc = slots[sid].itvs

    while (sid != sid_right):
        sid = slots[sid].next
        itvs_acc = intersec(itvs_acc, slots[sid].itvs)

    return itvs_acc


def intersec_ts_ph_itvs_slots(slots, sid_left, sid_right, job):

    sid = sid_left
    itvs_acc = []
    while True:
        slot = slots[sid]
        itvs = slot.itvs

        if job.ts:
            if "*" in slot.ts_itvs:  # slot.ts_itvs[user][name]
                if "*" in slot.ts_itvs["*"]:
                    itvs = add_intervals(itvs, slot.ts_itvs["*"]["*"])
                elif job.name in slot.ts_itvs["*"]:
                    itvs = add_intervals(itvs, slot.ts_itvs["*"][job.name])
            elif job.user in slot.ts_itvs:
                if "*" in slot.ts_itvs[job.user]:
                    itvs = add_intervals(itvs, slot.ts_itvs[job.user]["*"])
                elif job.name in slot.ts_itvs[job.user]:
                    itvs = add_intervals(
                        itvs, slot.ts_itvs[job.user][job.name])

        if job.ph == ALLOW:
            if job.ph_name in slot.ph_itvs:
                itvs = add_intervals(itvs, slot.ph_itvs[job.ph_name])

        if not itvs_acc:
            itvs_acc = itvs
        else:
            itvs_acc = intersec(itvs_acc, itvs)

        if sid == sid_right:
            break
        sid = slots[sid].next

    return itvs_acc


class SlotSet:

    def __init__(self, slots):
        self.last_id = 1
        # The first (earlier) slot has identifier one.
        if type(slots) == dict:
            self.slots = slots
            s = slots[1]
            self.begin = s.b
            while (s.next != 0):
                s = slots[s.next]
            self.last_id = s.id
        elif type(slots) == tuple:
            itvs, b = slots
            self.begin = b
            self.slots = {1: Slot(1, 0, 0, itvs, b, MAX_TIME)}
        else:
            self.slots = {1: slots}
            self.begin = slots.b

        # cache the last sid_left given for by walltime => not used
        # cache the last sid_left given for same previous job
        #  (same requested resources w/ constraintes)
        self.cache = {}

    def __str__(self):
        lines = []
        for i, slot in self.slots.items():
            lines.append("[%s] %s" % (i, slot))
        max_length = max([len(line) for line in lines])
        lines.append("%s" % ("-" * max_length))
        lines.insert(0, ('{:-^%d}' % max_length).format(' SlotSet '))
        return '\n'.join(lines)

    def __repr__(self):
        return "%s" % self

    def show_slots(self):
        print("%s" % self)

    # Split slot accordingly with job resource assignment *)
    # new slot A + B + C (A, B and C can be null)         *)
    #   -------
    #   |A|B|C|
    #   |A|J|C|
    #   |A|B|C|
    #   -------

    # Generate A slot - slot before job's begin
    def slot_before_job(self, slot, job):
        s_id = slot.id
        self.last_id += 1
        n_id = self.last_id
        a_slot = Slot(s_id, slot.prev, n_id, slot.itvs[:],
                      slot.b, job.start_time - 1,
                      deepcopy(slot.ts_itvs), deepcopy(slot.ph_itvs))
        slot.prev = s_id
        self.slots[s_id] = a_slot
        # slot_id is changed so we have always the rightmost slot (min slot.b)
        # w/ sid = 1 r
        slot.id = n_id
        self.slots[n_id] = slot

        if hasattr(a_slot, 'quotas'):
            a_slot.quotas.deepcopy_from(slot.quotas)

    # Generate B slot (substract job resources)
    def sub_slot_during_job(self, slot, job):
        slot.b = max(slot.b, job.start_time)
        slot.e = min(slot.e, job.start_time + job.walltime - 1)
        slot.itvs = sub_intervals(slot.itvs, job.res_set)
        if job.ts:
            if job.ts_user not in slot.ts_itvs:
                slot.ts_itvs[job.ts_user] = {}

            if job.ts_name not in slot.ts_itvs[job.ts_user]:
                slot.ts_itvs[job.ts_user][job.ts_name] = job.res_set[:]

        if job.ph == ALLOW:
            if job.ph_name in slot.ph_itvs:
                slot.ph_itvs[job.ph_name] = \
                    sub_intervals(slot.ph_itvs[job.ph_name], job.res_set)

        if job.ph == PLACEHOLDER:
            slot.ph_itvs[job.ph_name] = job.res_set[:]

        if hasattr(slot, 'quotas') and not ("container" in job.types):
            slot.quotas.update(job)
            # slot.quotas.show_counters()

    # Generate B slot
    def add_slot_during_job(self, slot, job):
        slot.b = max(slot.b, job.start_time)
        slot.e = min(slot.e, job.start_time + job.walltime - 1)
        if (not job.ts) and (job.ph == NO_PLACEHOLDER):
            slot.itvs = add_intervals(slot.itvs, job.res_set[:])
        if job.ts:
            if job.ts_user not in slot.ts_itvs:
                slot.ts_itvs[job.ts_user] = {}
            if job.ts_name not in slot.ts_itvs[job.ts_user]:
                slot.ts_itvs[job.ts_user][job.ts_name] = job.res_set[:]
            else:
                itvs = slot.ts_itvs[job.ts_user][job.ts_name]
                slot.ts_itvs[job.ts_user][job.ts_name] = add_intervals(itvs, job.res_set[:])

        if job.ph == PLACEHOLDER:
            if job.ph_name in slot.ph_itvs:
                slot.ph_itvs[job.ph_name] = \
                    add_intervals(slot.ph_itvs[job.ph_name], job.res_set)
            else:
                slot.ph_itvs[job.ph_name] = job.res_set[:]

        # PLACEHOLDER / ALLOWED need not to considered in this case

    # Generate C slot - slot after job's end
    def slot_after_job(self, slot, job):
        self.last_id += 1
        s_id = self.last_id
        c_slot = Slot(s_id, slot.id, slot.next, slot.itvs[:],
                      job.start_time + job.walltime, slot.e,
                      deepcopy(slot.ts_itvs), deepcopy(slot.ph_itvs))
        slot.next = s_id
        self.slots[s_id] = c_slot

        if hasattr(c_slot, 'quotas'):
            c_slot.quotas.deepcopy_from(slot.quotas)

    def split_slots(self, sid_left, sid_right, job, sub=True):
        sid = sid_left
        we_will_break = False
        while True:
            slot = self.slots[sid]

            if (sid == sid_right):
                we_will_break = True
            else:
                sid = slot.next

            # print("split", slot.show())
            if job.start_time > slot.b:
                # Generate AB | ABC
                if (job.start_time + job.walltime) > slot.e:
                    # Generate AB
                    self.slot_before_job(slot, job)
                    if sub:
                        # substract resources
                        self.sub_slot_during_job(slot, job)
                    else:
                        # add resources
                        self.add_slot_during_job(slot, job)

                else:
                    # generate ABC
                    self.slot_before_job(slot, job)
                    # generate C before modify slot / B
                    self.slot_after_job(slot, job)
                    if sub:
                        # substract resources
                        self.sub_slot_during_job(slot, job)
                    else:
                        # add resources
                        self.add_slot_during_job(slot, job)

            else:
                # Generate B | BC
                if ((job.start_time + job.walltime) - 1) >= slot.e:
                    # Generate B
                    if sub:
                        # substract resources
                        self.sub_slot_during_job(slot, job)
                    else:
                        # add resources
                        self.add_slot_during_job(slot, job)

                else:
                    # Generate BC
                    # Generate C before modify slot / B
                    self.slot_after_job(slot, job)
                    if sub:
                        # substract resources
                        self.sub_slot_during_job(slot, job)
                    else:
                        # add resources
                        self.add_slot_during_job(slot, job)

            if we_will_break:
                break

    def split_slots_jobs(self, ordered_jobs, sub=True):
        """
        Split slots according to jobs by substracting or adding jobs' assigned resources in slots.
        Jobs must be sorted by start_time.
        It used in to insert previously scheduled jobs in slots or container jobs.
        """
        slot = self.slots[1]  # 1
        left_sid_2_split = 1
        right_sid_2_split = 1

        if not sub:
            # for adding resources we need to inverse the chronological order
            ordered_jobs.reverse()

        for job in ordered_jobs:
            # Find first slot
            while not ((slot.b > job.start_time)
                       or ((slot.b <= job.start_time)
                           and (job.start_time <= slot.e))):
                left_sid_2_split = slot.next
                slot = self.slots[slot.next]

            right_sid_2_split = left_sid_2_split
            # Find slots encompass
            while not (slot.e >= (job.start_time + job.walltime)):
                right_sid_2_split = slot.next
                slot = self.slots[slot.next]

            self.split_slots(left_sid_2_split, right_sid_2_split, job, sub)
