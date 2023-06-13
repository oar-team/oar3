# coding: utf-8
"""
This module contains the base scheduling structures :class:`SlotSet` and :class:`Slot` used to represent the available resources through time.
"""

import copy
from typing import Dict, Generator, List, Optional, Tuple

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
        slots = self.slots.values()
        # Get first slot
        slot = [s for s in slots if s.prev == 0][0]

        while slot.next != 0:
            lines.append(f"{slot}")
            slot = self.slots[slot.next]
        # Get last slot
        lines.append(f"{slot}")
        return "\n".join(lines)

    def __repr__(self) -> str:
        return "%s" % self

    def show_slots(self):
        print("%s" % self)

    def new_id(self) -> int:
        """Get a new Id for constructing new slots.

        Returns:
            int: the new id
        """
        self.last_id += 1
        return self.last_id

    def first(self) -> Optional[Slot]:
        """Find the first slot starting the slotset.

        Returns:
            Optional[Slot]: The first slot or None if not found (that should never appends)
        """
        if self.slots:
            return [s for s in self.slots.values() if s.prev == 0][0]

    def last(self) -> Optional[Slot]:
        """Find the last slot ending the slotset.

        Returns:
            Optional[Slot]: Return the last slot.
        """
        # TODO: Maybe the last and first slots can always be referenced, so we won't have to
        # search the whole set each time
        if self.slots:
            return [s for s in self.slots.values() if s.next == 0][0]

    def slot_id_at(self, date: int, starting_id=0) -> int:
        """Return the slot corresponding to the date given in parameter.

        Args:
            insertion_date (int): find slot for this date

        Returns:
            int: return the slot id, or 0 if not found
        """
        assert starting_id == 0 or starting_id in self.slots

        if starting_id != 0:
            current = self.slots[starting_id]
        else:
            current = self.first()

        if current.b > date:
            return 0

        slot_id = current.id

        while current.e < date and current.next != 0:
            current = self.slots[current.next]
            slot_id = current.id

        if current.next == 0 and current.e < date:
            return 0

        return slot_id

    def split_at_before(self, slot_id: int, insertion_date: int) -> Tuple[int, int]:
        """Split a given slot at the insertion date.
          The new slot is created and inserted before the original slot.

          |                     |         |          |          |
          |       Slot 1        | ----->  |  Slot 2  |  Slot 1  |
          |                     |         |          |          |
        --|---------------------|--     --|----------|----------|--
          0                    10         0       a-1|a        10

          If the slot is of size one (ie begin equals end), the slot is not split and the function returns the id of the given slot

          Args:
              slot_id (int): The slot to split
              insertion_date (int): The date at which split the slit (must be within the slot range)

          Returns:
              Tuple[int, int]: return the two slots, starting with the new one.
        """
        if slot_id == 0:
            return (0, 0)

        slot = self.slots[slot_id]

        # The insertion_date must be between the boundaries of the slot
        # Hard fail for the moment
        assert insertion_date >= slot.b and insertion_date <= slot.e

        # Cannot subdivide slot of size 1
        if slot.b == slot.e:
            return (slot_id, slot_id)

        new_slot_end = insertion_date - 1
        if slot.b == insertion_date:
            new_slot_end = slot.b

        new_id = self.new_id()
        new_slot = Slot(new_id, slot.prev, slot.id, ProcSet(), slot.b, new_slot_end)

        if slot.prev != 0:
            self.slots[slot.prev].next = new_id

        slot.prev = new_id

        if slot.b == insertion_date:
            slot.b = slot.b + 1
        else:
            slot.b = insertion_date

        self.slots[new_id] = new_slot

        self.copy_intervals_set(slot.id, new_id)

        return (new_id, slot.id)

    def split_at_after(self, slot_id: int, insertion_date: int) -> Tuple[int, int]:
        """Split a given slot at the insertion date.
          The new slot is created and inserted before the original slot.

          |                     |         |          |          |
          |       Slot 1        | ----->  |  Slot 1  |  Slot 2  |
          |                     |         |          |          |
        --|---------------------|--     --|----------|----------|--
          0                    10         0       a-1|a        10

          If the slot is of size one (ie begin equals end), the slot is not split and the function returns the id of the given slot.

          Args:
              slot_id (int): The slot to split
              insertion_date (int): The date at which split the slit (must be within the slot range)

          Returns:
              Tuple[int, int]: return the two slots, starting with the new one.
        """
        # Cannot split slot_id 0
        if slot_id == 0:
            return (0, 0)

        # Find the slot
        slot = self.slots[slot_id]
        # The insertion_date must be between the boundaries of the slot
        # Hard fail for the moment
        assert insertion_date >= slot.b and insertion_date <= slot.e

        # Cannot subdivide slot of size 1
        if slot.b == slot.e:
            return (slot_id, slot_id)

        new_slot_begin = insertion_date

        # If the slot is splitted at its begin time, it means that the remaining slot will be of
        # size 1. To prevent to go off the slot boundaries we adjust the position
        if slot.b == insertion_date:
            new_slot_begin = slot.b + 1

        new_id = self.new_id()
        new_slot = Slot(new_id, slot.id, slot.next, ProcSet(), new_slot_begin, slot.e)

        if slot.next != 0:
            self.slots[slot.next].prev = new_id

        slot.next = new_id

        if slot.b == insertion_date:
            slot.e = insertion_date
        else:
            slot.e = insertion_date - 1

        self.slots[new_id] = new_slot

        self.copy_intervals_set(slot.id, new_id)

        return (slot.id, new_id)

    def find_and_split_at(self, insertion_date: int) -> Tuple[int, int]:
        slot_id = self.slot_id_at(insertion_date)
        return self.split_at_after(slot_id, insertion_date)

    def get_encompassing_range(self, start: int, end: int) -> Tuple[int, int]:
        assert start <= end

        first_slot = 0
        last_slot = 0

        for slot in self.traverse_id():
            if start >= slot.b and start <= slot.e:
                first_slot = slot.id

            if end >= slot.b and end <= slot.e:
                last_slot = slot.id
                break

        return (first_slot, last_slot)

    def traverse_id(self, start: int = 0, end: int = 0) -> Generator[Slot, None, None]:
        """loop between the slot_id start and slot_id end.
        Note that, the ids are not ordered, so using a slot id for the end argument that is not after start will lead have
        the same result as using end = 0 (i.e looping untill it reaches the end of the structure)

        Args:
            start (int, optional): first id to start the parcour. Defaults to 0.
            end (int, optional): end id. Defaults to 0.

        Yields:
            Generator[Slot, None, None]: _description_
        """
        # Check that the slots exists
        if (start != 0 and start not in self.slots) or (
            end != 0 and end not in self.slots
        ):
            return

        if start != 0:
            slot = self.slots[start]
        else:
            slot = self.first()

        while slot.id != 0 and slot.next != 0 and slot.id != end:
            yield slot
            slot = self.slots[slot.next]

        # yield the last slot
        yield slot

    def traverse_with_width(
        self, width, start_id=0, end_id=0
    ) -> Generator[Tuple[Slot, Slot], None, None]:
        # Width of zero does not exist
        assert width > 0

        for start_slot in self.traverse_id(start=start_id, end=end_id):
            begin_time = start_slot.b
            for end_slot in self.traverse_id(start=start_slot.id, end=end_id):
                size = end_slot.e - begin_time
                if size + 1 >= width:
                    yield (start_slot, end_slot)

                    # If we found a long enough interval from this starting point we don't need to continue
                    # We can skip to the next start
                    break

    def copy_intervals_set(self, id_slot_from: int, id_slot_to: int):
        assert id_slot_from != id_slot_to and id_slot_to != 0

        slot_from = self.slots[id_slot_from]
        slot_to = self.slots[id_slot_to]

        slot_to.itvs = copy.copy(slot_from.itvs)
        slot_to.ts_itvs = dict_ps_copy(slot_from.ts_itvs)
        slot_to.ph_itvs = dict_ps_copy(slot_from.ph_itvs)

        if hasattr(slot_to, "quotas"):
            slot_to.quotas.deepcopy_from(slot_from.quotas)
            slot_to.quotas_rules_id = slot_from.quotas_rules_id
            slot_to.quotas.set_rules(slot_from.quotas_rules_id)

    # Transform given slot to B slot (substract job resources)
    def sub_slot_during_job(self, slot: Slot, job: Job):
        # slot.b = max(slot.b, job.start_time)
        # slot.e = min(slot.e, job.start_time + job.walltime - 1)

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

    def extend(self, date):
        first = self.first()
        last = self.last()

        if date >= first.b and date <= last.e:
            # Lets fail will see later what is best to do
            assert False

        elif date < first.b:
            new_slot = Slot(self.new_id(), 0, first.id, ProcSet(), date, first.b - 1)
            first.prev = new_slot.id
            self.slots[new_slot.id] = new_slot
            self.copy_intervals_set(first.id, new_slot.id)

            return new_slot.id
        elif date > last.e:
            new_slot = Slot(self.new_id(), 0, first.id, ProcSet(), last.e, date)
            first.prev = new_slot.id
            self.slots[new_slot.id] = new_slot
            self.copy_intervals_set(first.id, new_slot.id)

            return new_slot.id

    def add_front(self, date: int, inplace=False) -> int:
        first = self.first()
        assert date < first.b
        if inplace:
            first.b = date
            return first.id

        new_slot = Slot(self.new_id(), 0, first.id, ProcSet(), date, first.b - 1)
        first.prev = new_slot.id
        self.slots[new_slot.id] = new_slot
        self.copy_intervals_set(first.id, new_slot.id)

        return new_slot.id

    def add_back(self, date: int, inplace=False) -> int:
        last = self.last()
        assert date > last.e

        if inplace:
            last.e = date
            return last.id

        new_slot = Slot(self.new_id(), last.id, 0, ProcSet(), last.e, date)
        last.next = new_slot.id

        self.slots[new_slot.id] = new_slot
        self.copy_intervals_set(last.id, new_slot.id)

        return new_slot.id

    def extend_range(
        self, begin: int, end: int, inplace: bool = False
    ) -> Tuple[Optional[int], Optional[int]]:
        """Extend the slot set considering a time range (useful to insert a new job)

        Args:
            begin (int): begin time to insert
            end (int): end time to insert

        Returns:
            Tuple[int, int]: return the newly created slot if any
        """
        first = self.first()
        last = self.last()

        first_id = None
        last_id = None

        assert begin < end

        if end < first.b:
            # both end and begin are before the beginning of the slot set
            # this adding on in front is sufficient
            last_id = first_id = self.add_front(begin, inplace)
        elif begin > last.e:
            # both end and begin are after the end of the slot set
            # this adding on in back is sufficient
            last_id = first_id = self.add_back(end, inplace)
        else:
            # Otherwise we check and add both if needed
            if begin < first.b:
                first_id = self.add_front(begin, inplace)

            if end > last.e:
                last_id = self.add_back(end, inplace)

        return (first_id, last_id)

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

        # First check if we need to increase the size of the slotset
        if sid_left == 0 or sid_right == 0:
            (new_first, new_last) = self.extend_range(
                job.start_time, job.start_time + job.walltime, inplace=True
            )

            if sid_left == 0:
                sid_left = new_first

            if sid_right == 0:
                sid_right = new_last

        if sid_left != 0 and self.slots[sid_left].b != job.start_time:
            (_, sid_left) = self.split_at_before(sid_left, job.start_time)

        if sid_right != 0 and self.slots[sid_right].e != job.start_time + job.walltime:
            (sid_right, _) = self.split_at_after(
                sid_right, job.start_time + job.walltime
            )

        for slot in self.traverse_id(sid_left, sid_right):
            if sub:
                # substract resources
                self.sub_slot_during_job(slot, job)
            else:
                # add resources
                self.add_slot_during_job(slot, job)

    def split_slots_jobs(self, ordered_jobs: List[Job], sub=True):
        """
        Split slots according to jobs by substracting or adding jobs' assigned resources in slots.
        Jobs must be sorted by start_time.
        It used in to insert previously scheduled jobs in slots or container jobs.
        """
        if not sub:
            # for adding resources we need to inverse the chronological order
            ordered_jobs.reverse()

        for job in ordered_jobs:
            # Find first slot
            (left_sid_2_split, right_sid_2_split) = self.get_encompassing_range(
                job.start_time, job.start_time + job.walltime
            )

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
                (_, id_new_slot) = self.split_at_after(
                    slot.id, slot.b + remaining_duration
                )

                slot.quotas_rules_id = quotas_rules_id
                slot.quotas.set_rules(quotas_rules_id)
                b_slot = self.slots[id_new_slot]

                # What is next new rules_id / duration or quatos_period_reached
                quotas_rules_id, remaining_duration = Quotas.calendar.next_rules(
                    b_slot.b
                )
                if not remaining_duration:
                    return (quotas_rules_id, remaining_duration)

                # for next iteration
                slot = b_slot
