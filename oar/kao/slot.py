# coding: utf-8
"""
This module contains the base scheduling structures :class:`SlotSet` and :class:`Slot` used to represent the available resources through time.
"""

import copy
from typing import Dict, List

from procset import ProcSet

from oar.kao.quotas import Quotas
from oar.lib.job_handling import ALLOW, NO_PLACEHOLDER, PLACEHOLDER
from oar.lib.models import Job
from oar.lib.utils import dict_ps_copy

MAX_TIME = 2147483648  # (* 2**31 *)


class Slot(object):
    """
    Base scheduling class that holds information about available resources `itvs` for a time interval (between `b`  and `e`).
    The Slots is a linked structure as it also holds references on its previous and next :class:`Slot`.
    """

    def __init__(
        self,
        id: int,
        prev: int,
        next: int,
        itvs: ProcSet,
        b: int,
        e: int,
        ts_itvs=None,
        ph_itvs=None,
    ):
        """
        A :class:`Slot` is initialized with the ids of its previous and next Slots,
        the :class:`ProcSet` of resources and the time interval (`b` and `e`).

        :param int id: \
            id of the :class:`Slot`.
        :param int prev: \
            id of the previous :class:`Slot`. `prev` equals to 0 means that it is the first slot.
        :param int next: \
            id of the previous :class:`Slot`. `next` equals to 0 means that it is the last slot.
        :param Procset itvs: \
            An interval set (using :class:`ProcSet`) of available resources.
        :param int b: \
            Beggining time of the :class:`Slot`.
        :param int e: \
            End time of the :class:`Slot`.
        :param ts_itvs: \
            Time sharing interval two-levels hierarchy. The first level is either the name of the user, or "*".
            The second level is either the name of the job or "*". Using "*" means that the time sharing will have no restriction about jobs using this feature.
            For instance, to restrict the time sharing to all the job of the user Alice use `dict { "alice", dict: { "*" } }`.
            see :ref:`FAQ <can-i-perform-a-fix-scheduled-reservation-and-then-launch-several-jobs-in-it>` about how to use it.
            (defaults to `None`).
        :type ts_itvs: dict[str, dict[str, ProcSet]]
        :param dict ph_itvs: \
            Placeholder interval.
            (defaults to `None`)
        """
        self.id = id
        self.prev = prev
        self.next = next
        if type(itvs) == ProcSet:
            self.itvs = itvs
        else:
            self.itvs = ProcSet(*itvs)
        self.b = b
        self.e = e
        # timesharing ts_itvs: [user] * [job_name] * itvs
        if ts_itvs is None:
            self.ts_itvs = {}
        else:
            self.ts_itvs = ts_itvs
        if ph_itvs is None:
            self.ph_itvs = {}
        else:
            self.ph_itvs = ph_itvs  # placeholder ph_itvs: [ph_name] * itvs

        if Quotas.enabled:
            self.quotas = Quotas()
            self.quotas_rules_id = -1

    def show(self):
        """
        Print a :class:`Slot` using the internal :func:`__str__` function.
        """
        print("%s" % self)

    def __str__(self) -> str:
        """
        String representation of a :class:`Slot`.

        Examples:

            >>> print(Slot(1, 0, 0, ProcSet(*[(1, 32)]), 1, 20))
            <Slot(id=1, prev=0, next=0, itvs=1-32, b=1, e=20, ts_itvs={}, ph_itvs={})>
        """
        repr_string = (
            "id=%(id)s, prev=%(prev)s, next=%(next)s, itvs=%(itvs)s, "
            "b=%(b)s, e=%(e)s, ts_itvs=%(ts_itvs)s, ph_itvs=%(ph_itvs)s"
        )
        repr_string += (
            ", quotas_rules_id=%(quotas_rules_id)s"
            if hasattr(self, "quotas_rules_id")
            else ""
        )
        return "Slot(%s)" % (repr_string % vars(self))

    def __repr__(self) -> str:
        return "<%s>" % self


def intersec_itvs_slots(
    slots: Dict[int, Slot], sid_left: int, sid_right: int
) -> ProcSet:
    """
    Return the :class:`ProcSet` which is the intersection of all slots from `sid_left` to `sid_right`.

    :param dict slots: \
        Dict containing the :class:`Slot` indexed by id.
    :param int sid_left: \
        The id of the :class:`Slot` in `slots` from which to begin.
    :param int sid_right: \
        The id of the :class:`Slot` in `slots` from which to end.
    :return: \
        A :class:`ProcSet` containing the intersection of all slots from `sid_left` to `sid_right`.

    Examples:
        >>> s1 = Slot(1, 0, 2, ProcSet(*[(1, 32)]), 1, 10)
        >>> s2 = Slot(2, 1, 3, ProcSet(*[(1, 16), (28, 32)]), 11, 20)
        >>> s3 = Slot(3, 2, 0, ProcSet(*[(1, 8), (30, 32)]), 21, 30)
        >>> slots = {1: s1, 2: s2, 3: s3}
        >>> print(intersec_itvs_slots(slots, 1, 3))
            1-8 30-32
    """
    sid = sid_left
    itvs_acc = slots[sid].itvs

    while sid != sid_right:
        sid = slots[sid].next
        itvs_acc = itvs_acc & slots[sid].itvs

    return itvs_acc


def intersec_ts_ph_itvs_slots(
    slots: Dict[int, Slot], sid_left: int, sid_right: int, job: Job
) -> ProcSet:
    """
    Same as :func:`intersec_itvs_slots`, but depending on the `job` configuration enables to share resources between jobs.
    More precisely, if `job.ts` is `True`, it gathers resources from slot `sid_left` to `sid_right` depending on the slot time sharing configuration (see :class:`Slot`).
    """
    sid = sid_left
    itvs_acc: ProcSet = ProcSet()
    while True:
        slot = slots[sid]
        itvs = slot.itvs

        if job.ts:
            if "*" in slot.ts_itvs:  # slot.ts_itvs[user][name]
                if "*" in slot.ts_itvs["*"]:
                    itvs = itvs | slot.ts_itvs["*"]["*"]
                elif job.name in slot.ts_itvs["*"]:
                    itvs = itvs | slot.ts_itvs["*"][job.name]
            elif job.user in slot.ts_itvs:
                if "*" in slot.ts_itvs[job.user]:
                    itvs = itvs | slot.ts_itvs[job.user]["*"]
                elif job.name in slot.ts_itvs[job.user]:
                    itvs = itvs | slot.ts_itvs[job.user][job.name]

        if job.ph == ALLOW:
            if job.ph_name in slot.ph_itvs:
                itvs = itvs | slot.ph_itvs[job.ph_name]

        if not itvs_acc:
            itvs_acc = itvs
        else:
            itvs_acc = itvs_acc & itvs

        if sid == sid_right:
            break
        sid = slots[sid].next

    return itvs_acc


class SlotSet:
    """
    :class:`SlotSet` holds a linked list of slots and provides utilities for their manipulation.
    """

    def __init__(self, slots):
        """
        :class:`SlotSet` is initialized with a linked list of `slots`.
        :params slots: Either as python `dict`, a `tuple` or with a single :class:`Slot`.

        Examples:
            >>> sset = SlotSet( # Using dict
            ...         {
            ...             1: Slot(1, 0, 2, ProcSet(*[(1, 32)]), 1, 20),
            ...             2: Slot(2, 1, 0, ProcSet(*[(1, 32)]), 21, 25)
            ...         }
            ...     )
            >>> print(sset)
                ---------------------------------- SlotSet ----------------------------------
                [1] Slot(id=1, prev=0, next=2, itvs=1-32, b=1, e=20, ts_itvs={}, ph_itvs={})
                [2] Slot(id=2, prev=1, next=0, itvs=1-32, b=21, e=25, ts_itvs={}, ph_itvs={})
                -----------------------------------------------------------------------------
            >>> print(SlotSet(Slot(1, 0, 0, ProcSet(*[(1, 32)]), 1, 20))) # Using tuple
                --------------------------------- SlotSet ----------------------------------
                [1] Slot(id=1, prev=0, next=0, itvs=1-32, b=1, e=20, ts_itvs={}, ph_itvs={})
                ----------------------------------------------------------------------------
            >>> print(Slot(1, 0, 0, ProcSet(*[(1, 32)]), 1, 20)) # Using single slot
                ------------------------------------- SlotSet -------------------------------------
                [1] Slot(id=1, prev=0, next=0, itvs=1-2, b=0, e=2147483648, ts_itvs={}, ph_itvs={})
                -----------------------------------------------------------------------------------
        """
        self.last_id: int = 1
        # The first (earlier) slot has identifier one.
        if type(slots) == dict:
            self.slots: Dict[int, Slot] = slots
            s = slots[1]
            self.begin: int = s.b
            while s.next != 0:
                s = slots[s.next]
            self.last_id = s.id
        elif type(slots) == tuple:
            itvs, b = slots
            self.begin: int = b
            self.slots: Dict[int, Slot] = {1: Slot(1, 0, 0, itvs, b, MAX_TIME)}
        else:
            # Given slots is, in fact, one slot
            self.slots = {1: slots}
            self.begin = slots.b

        # cache the last sid_left given for by walltime => not used
        # cache the last sid_left given for same previous job
        #  (same requested resources w/ constraintes)
        self.cache = {}

        # Slots must be splitted according to Quotas' calendar if applied and the first has not
        # rules affected
        # import pdb; pdb.set_trace()
        if Quotas.calendar and (self.slots[1].quotas_rules_id == -1):
            i = 1
            quotas_rules_id, remaining_duration = Quotas.calendar.rules_at(self.begin)
            while (
                i and remaining_duration
            ):  # no more slots or quotas_period_end reached
                slot = self.slots[i]
                i = slot.next
                quotas_rules_id, remaining_duration = self.temporal_quotas_split_slot(
                    slot, quotas_rules_id, remaining_duration
                )

    def __str__(self) -> str:
        lines = []
        for i, slot in self.slots.items():
            lines.append("[%s] %s" % (i, slot))
        max_length = max([len(line) for line in lines])
        lines.append("%s" % ("-" * max_length))
        lines.insert(0, ("{:-^%d}" % max_length).format(" SlotSet "))
        return "\n".join(lines)

    def __repr__(self) -> str:
        return "%s" % self

    def show_slots(self):
        print("%s" % self)

    def slot_before_job(self, slot: Slot, job: Job):
        s_id = slot.id
        self.last_id += 1
        next_id = self.last_id
        print("next id:", next_id)
        a_slot = Slot(
            s_id,
            slot.prev,
            next_id,
            copy.copy(slot.itvs),
            slot.b,
            job.start_time - 1,
            dict_ps_copy(slot.ts_itvs),
            dict_ps_copy(slot.ph_itvs),
        )
        slot.prev = s_id
        self.slots[s_id] = a_slot
        # slot_id is changed so we have always the rightmost slot (min slot.b)
        # w/ sid = 1 r
        slot.id = next_id
        self.slots[next_id] = slot

        if hasattr(a_slot, "quotas"):
            a_slot.quotas.deepcopy_from(slot.quotas)
            a_slot.quotas_rules_id = slot.quotas_rules_id
            a_slot.quotas.set_rules(slot.quotas_rules_id)

    # Transform given slot to B slot (substract job resources)
    def sub_slot_during_job(self, slot: Slot, job: Job):
        slot.b = max(slot.b, job.start_time)
        slot.e = min(slot.e, job.start_time + job.walltime - 1)
        slot.itvs = slot.itvs - job.res_set
        if job.ts:
            if job.ts_user not in slot.ts_itvs:
                slot.ts_itvs[job.ts_user] = {}

            if job.ts_name not in slot.ts_itvs[job.ts_user]:
                slot.ts_itvs[job.ts_user][job.ts_name] = copy.copy(job.res_set)

        if job.ph == ALLOW:
            if job.ph_name in slot.ph_itvs:
                slot.ph_itvs[job.ph_name] = slot.ph_itvs[job.ph_name] - job.res_set

        if job.ph == PLACEHOLDER:
            slot.ph_itvs[job.ph_name] = copy.copy(job.res_set)

        if hasattr(slot, "quotas") and not ("container" in job.types):
            slot.quotas.update(job)
            # slot.quotas.show_counters()

    #  Transform given slot to B slot
    def add_slot_during_job(self, slot: Slot, job: Job):
        slot.b = max(slot.b, job.start_time)
        slot.e = min(slot.e, job.start_time + job.walltime - 1)
        if (not job.ts) and (job.ph == NO_PLACEHOLDER):
            slot.itvs = slot.itvs | job.res_set
        if job.ts:
            if job.ts_user not in slot.ts_itvs:
                slot.ts_itvs[job.ts_user] = {}
            if job.ts_name not in slot.ts_itvs[job.ts_user]:
                slot.ts_itvs[job.ts_user][job.ts_name] = copy.copy(job.res_set)
            else:
                itvs = slot.ts_itvs[job.ts_user][job.ts_name]
                slot.ts_itvs[job.ts_user][job.ts_name] = itvs | job.res_set

        if job.ph == PLACEHOLDER:
            if job.ph_name in slot.ph_itvs:
                slot.ph_itvs[job.ph_name] = slot.ph_itvs[job.ph_name] | job.res_set
            else:
                slot.ph_itvs[job.ph_name] = copy.copy(job.res_set)

        # PLACEHOLDER / ALLOWED need not to considered in this case

    # Generate C slot - slot after job's end
    def slot_after_job(self, slot: Slot, job: Job):
        self.last_id += 1
        s_id = self.last_id
        c_slot = Slot(
            s_id,
            slot.id,
            slot.next,
            copy.copy(slot.itvs),
            job.start_time + job.walltime,
            slot.e,
            dict_ps_copy(slot.ts_itvs),
            dict_ps_copy(slot.ph_itvs),
        )
        slot.next = s_id
        self.slots[s_id] = c_slot

        if hasattr(c_slot, "quotas"):
            c_slot.quotas.deepcopy_from(slot.quotas)
            c_slot.quotas_rules_id = slot.quotas_rules_id
            c_slot.quotas.set_rules(slot.quotas_rules_id)

    def split_slots(self, sid_left: int, sid_right: int, job: Job, sub: bool = True):
        """
        Split slot accordingly to a job resource assignment.
        New slot A + B + C (A, B and C can be null)

        +---+---+---+
        | A | B | C |
        +---+---+---+
        | A | J | C |
        +---+---+---+
        | A | B | C |
        +---+---+---+

        Generate A slot - slot before job's begin
        """
        sid = sid_left
        we_will_break = False
        while True:
            slot = self.slots[sid]

            if sid == sid_right:
                we_will_break = True
            else:
                sid = slot.next

            # Found a slot in which the job should execute
            if job.start_time > slot.b:
                # Generate AB | ABC
                # The current slot alone cannot contains the job (i.e. the job walltime ends after the slot).
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
                    # The job's duration is contained in the current slot.
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

    def split_slots_jobs(self, ordered_jobs: List[Job], sub=True):
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
            while not (
                (slot.b > job.start_time)
                or ((slot.b <= job.start_time) and (job.start_time <= slot.e))
            ):
                left_sid_2_split = slot.next
                slot = self.slots[slot.next]

            right_sid_2_split = left_sid_2_split
            # Find slots encompass
            while not (slot.e >= (job.start_time + job.walltime)):
                right_sid_2_split = slot.next
                slot = self.slots[slot.next]

            self.split_slots(left_sid_2_split, right_sid_2_split, job, sub)

    def temporal_quotas_split_slot(
        self, slot: Slot, quotas_rules_id: int, remaining_duration: int
    ):
        # So mypy can infer that this function has been called with Quotas.calendar enabled
        assert Quotas.calendar is not None

        while True:
            # import pdb; pdb.set_trace()
            # slot is included in actual quotas_rules
            slot_duration = (slot.e - slot.b) + 1
            if slot_duration <= remaining_duration:
                slot.quotas_rules_id = quotas_rules_id
                slot.quotas.set_rules(quotas_rules_id)
                return (quotas_rules_id, remaining_duration - slot_duration)
            else:
                # created B slot, modify current A slot according to remaining_duration
                # -----
                # |A|B|
                # -----
                self.last_id += 1
                b_id = self.last_id
                b_slot = Slot(
                    b_id,
                    slot.id,
                    slot.next,
                    copy.copy(slot.itvs),
                    slot.b + remaining_duration,
                    slot.e,
                    dict_ps_copy(slot.ts_itvs),
                    dict_ps_copy(slot.ph_itvs),
                )
                self.slots[b_id] = b_slot
                # modify current A
                slot.next = b_id
                slot.e = slot.b + remaining_duration - 1
                slot.quotas_rules_id = quotas_rules_id
                slot.quotas.set_rules(quotas_rules_id)

                # What is next new rules_id / duration or quatos_period_reached
                quotas_rules_id, remaining_duration = Quotas.calendar.next_rules(
                    b_slot.b
                )
                if not remaining_duration:
                    return (quotas_rules_id, remaining_duration)

                # for next iteration
                slot = b_slot
