# coding: utf-8
import pytest
from procset import ProcSet
from rich import print

from oar.kao.slot import MAX_TIME, Slot, SlotSet, intersec_itvs_slots
from oar.lib.job_handling import JobPseudo


def compare_slots_val_ref(slots: SlotSet, v):
    slot = slots.first()
    i = 0
    for slot in slots.traverse_id():
        # slot = slots[sid]
        (b, e, itvs) = v[i]
        if (slot.b != b) or (slot.e != e) or not (slot.itvs == itvs):
            print("NOT EQUAL", slot.b, b, slot.e, e, slot.itvs, itvs)
            return False
        i += 1
    return True


def test_intersec_itvs_slots():
    s1 = Slot(1, 0, 2, ProcSet(*[(1, 32)]), 1, 10)
    s2 = Slot(2, 1, 3, ProcSet(*[(1, 16), (24, 28)]), 11, 20)
    s3 = Slot(3, 2, 0, ProcSet(*[(1, 8), (12, 26)]), 21, 30)

    slots = {1: s1, 2: s2, 3: s3}

    itvs = intersec_itvs_slots(slots, 1, 3)

    assert itvs == ProcSet(*[(1, 8), (12, 16), (24, 26)])


def test_split_slots_ab():
    v = [(1, 4, ProcSet(*[(1, 32)])), (5, 25, ProcSet(*[(1, 9), (21, 32)]))]

    j1 = JobPseudo(
        id=1,
        start_time=5,
        walltime=20,
        res_set=ProcSet(*[(10, 20)]),
        moldable_id=1,
        ts=False,
        ph=0,
    )

    ss = SlotSet(Slot(1, 0, 0, ProcSet(*[(1, 32)]), 1, 25))

    print()
    print(ss)

    ss.split_slots(1, 1, j1)
    print()
    print(ss)
    assert compare_slots_val_ref(ss, v)


def test_split_slots_abc():
    v = [
        (1, 4, ProcSet(*[(1, 32)])),
        (5, 14, ProcSet(*[(1, 9), (21, 32)])),
        (15, 20, ProcSet(*[(1, 32)])),
    ]

    j1 = JobPseudo(
        id=1,
        start_time=5,
        walltime=10,
        res_set=ProcSet(*[(10, 20)]),
        moldable_id=1,
        ts=False,
        ph=0,
    )

    ss = SlotSet(Slot(1, 0, 0, ProcSet(*[(1, 32)]), 1, 20))
    ss.split_slots(1, 1, j1)
    print()
    print(ss)
    assert compare_slots_val_ref(ss, v)


def test_split_slots_b():
    v = [(1, 21, ProcSet(*[(1, 9), (21, 32)]))]

    j1 = JobPseudo(
        id=1,
        start_time=1,
        walltime=20,
        res_set=ProcSet(*[(10, 20)]),
        moldable_id=1,
        ts=False,
        ph=0,
    )

    ss = SlotSet(Slot(1, 0, 0, ProcSet(*[(1, 32)]), 1, 21))
    print(ss)
    ss.split_slots(1, 1, j1)
    print(ss)
    assert compare_slots_val_ref(ss, v)


def test_split_slots_bc():
    v = [(1, 10, ProcSet(*[(1, 9), (21, 32)])), (11, 20, ProcSet(*[(1, 32)]))]

    j1 = JobPseudo(
        id=1,
        start_time=1,
        walltime=10,
        res_set=ProcSet(*[(10, 20)]),
        moldable_id=1,
        ts=False,
        ph=0,
    )

    ss = SlotSet(Slot(1, 0, 0, ProcSet(*[(1, 32)]), 1, 20))
    ss.split_slots(1, 1, j1)
    assert compare_slots_val_ref(ss, v)


def test_bug_split_slots():
    v = [
        (20, 69, ProcSet(*[(31, 32)])),
        (70, 79, ProcSet(*[(1, 15), (31, 32)])),
        (80, 2147483599, ProcSet(*[(1, 32)])),
        (2147483600, 2147483647, ProcSet()),
    ]

    # res = [(1, 32)]
    s1 = Slot(1, 0, 4, ProcSet(*[(16, 32)]), 20, 69)
    s2 = Slot(2, 1, 0, ProcSet(), 2147483600, 2147483647)
    s4 = Slot(4, 1, 2, ProcSet(*[(1, 32)]), 70, 2147483599)

    slts = dict(((s.id, s) for s in [s1, s2, s4]))
    ss = SlotSet(slts)

    j2 = JobPseudo(
        id=2, start_time=20, walltime=60, res_set=ProcSet(*[(16, 30)]), ts=False, ph=0
    )

    ss.split_slots(1, 4, j2)
    assert compare_slots_val_ref(ss, v)


def test_add_split_slots_jobs_one_job():
    v = [(5, 14, ProcSet(*[(10, 50)])), (15, MAX_TIME, ProcSet())]

    ss = SlotSet((ProcSet(*[]), 10))
    print()
    print(ss)
    j = JobPseudo(
        id=1, start_time=5, walltime=10, res_set=ProcSet(*[(10, 50)]), ts=False, ph=0
    )

    ss.split_slots_jobs([j], False)
    print(ss)

    assert compare_slots_val_ref(ss, v)


def test_add_split_slots_jobs_2_jobs_1():
    # v = [
    #     (10, 19, ProcSet()),
    #     (20, 99, ProcSet(*[(40, 50)])),
    #     (100, 129, ProcSet(*[(10, 20), (40, 50)])),
    #     (130, 219, ProcSet(*[(40, 50)])),
    #     (220, MAX_TIME, ProcSet()),
    # ]

    ss = SlotSet((ProcSet(*[]), 10))

    j1 = JobPseudo(
        id=1, start_time=100, walltime=30, res_set=ProcSet(*[(10, 20)]), ts=False, ph=0
    )

    j2 = JobPseudo(
        id=2, start_time=20, walltime=200, res_set=ProcSet(*[(40, 50)]), ts=False, ph=0
    )

    print(ss)
    ss.split_slots_jobs([j1], False)

    print(ss)
    ss.split_slots_jobs([j2], False)
    ss.split_slots_jobs([j2], False)

    print(ss)
    # assert compare_slots_val_ref(ss.slots, v)


def test_add_split_slots_jobs_2_jobs_2():
    v = [
        (10, 19, ProcSet()),
        (20, 99, ProcSet(*[(40, 50)])),
        (100, 129, ProcSet(*[(10, 20), (40, 50)])),
        (130, 219, ProcSet(*[(40, 50)])),
        (220, MAX_TIME, ProcSet()),
    ]

    ss = SlotSet((ProcSet(*[]), 10))

    j1 = JobPseudo(
        id=1, start_time=100, walltime=30, res_set=ProcSet(*[(10, 20)]), ts=False, ph=0
    )

    j2 = JobPseudo(
        id=2, start_time=20, walltime=200, res_set=ProcSet(*[(40, 50)]), ts=False, ph=0
    )

    ss.split_slots_jobs([j2, j1], False)

    print(ss)
    assert compare_slots_val_ref(ss, v)


def test_split_slots_jobs_same_start_time():
    # v = [
    #     (1, 4, ProcSet(*[(1, 32)])),
    #     (5, 14, ProcSet(*[(1, 9), (21, 32)])),
    #     (15, 20, ProcSet(*[(1, 32)])),
    # ]

    j1 = JobPseudo(
        id=1,
        start_time=5,
        walltime=10,
        res_set=ProcSet(*[(1, 1)]),
    )

    j2 = JobPseudo(
        id=2,
        start_time=7,
        walltime=15,
        res_set=ProcSet(*[(2, 2)]),
    )

    ss = SlotSet(Slot(1, 0, 0, ProcSet(*[(1, 32)]), 1, 30))

    print(ss)
    # ss.split_slots(1, 1, j1)
    print(ss)

    ss.split_slots_jobs([j1, j2])

    print(ss)


def test_split_slots_jobs_same_end_time():
    # v = [
    #     (1, 4, ProcSet(*[(1, 32)])),
    #     (5, 14, ProcSet(*[(1, 9), (21, 32)])),
    #     (15, 20, ProcSet(*[(1, 32)])),
    # ]

    j1 = JobPseudo(
        id=1,
        start_time=5,
        walltime=50,
        res_set=ProcSet(*[(1, 1)]),
    )

    j2 = JobPseudo(
        id=2,
        start_time=5,
        walltime=10,
        res_set=ProcSet(*[(2, 2)]),
    )

    ss = SlotSet(Slot(1, 0, 0, ProcSet(*[(1, 32)]), 1, 300))

    print(ss)

    ss.split_slots_jobs([j1, j2], sub=True)

    print(ss)


@pytest.mark.parametrize(
    "jobs",
    [
        # [
        #     JobPseudo(id=1, start_time=5, walltime=10, res_set=ProcSet(1)),
        #     JobPseudo(id=2, start_time=5, walltime=10, res_set=ProcSet(1)),
        # ],
        # [
        #     JobPseudo(id=1, start_time=5, walltime=10, res_set=ProcSet(1)),
        #     JobPseudo(id=2, start_time=5, walltime=20, res_set=ProcSet(1)),
        # ],
        [
            JobPseudo(id=2, start_time=5, walltime=20, res_set=ProcSet(1)),
            JobPseudo(id=1, start_time=5, walltime=10, res_set=ProcSet(1)),
        ],
    ],
)
def test_slots_and_jobs(jobs):
    ss = SlotSet(Slot(1, 0, 0, ProcSet(*[(1, 32)]), 1, 300))
    ss.split_slots_jobs(jobs, sub=True)

    # Check some properties
    slots = ss.slots.values()
    # Get first slot
    slot = [s for s in slots if s.prev == 0][0]
    prevs = set()
    nexts = set()

    print()
    print(ss)
    # Check the integrity of a slotset without result verification
    while slot.next != 0:
        assert slot.b <= slot.e
        assert slot.next not in nexts and slot.prev not in prevs

        prevs.add(slot.prev)
        nexts.add(slot.next)

        slot = ss.slots[slot.next]

    print()
    for slot in ss.traverse_id():
        print(slot)


@pytest.mark.parametrize(
    "time, answer",
    [(5, 0), (250, 2), (499, 2), (500, 3), (1500, 0), (25, 0)],
)
def test_slot_id_at(time, answer):
    slots = {
        1: Slot(1, 0, 2, ProcSet((1, 32)), 50, 249),
        2: Slot(2, 1, 3, ProcSet((1, 32)), 250, 499),
        3: Slot(3, 2, 0, ProcSet((1, 32)), 500, 1000),
    }

    ss = SlotSet(slots)

    print(f"\n{ss}")
    assert ss.slot_id_at(time) == answer


@pytest.mark.parametrize(
    "time, answer",
    [(5, 0), (250, 2), (499, 2), (500, 3), (1500, 0), (25, 0)],
)
def test_get_encompassing_range(time, answer):
    slots = {
        1: Slot(1, 0, 2, ProcSet((1, 32)), 50, 249),
        2: Slot(2, 1, 3, ProcSet((1, 32)), 250, 499),
        3: Slot(3, 2, 0, ProcSet((1, 32)), 500, 1000),
    }

    ss = SlotSet(slots)

    print(ss)
    assert ss.slot_id_at(time) == answer


def test_traverse():
    slots = {
        1: Slot(1, 0, 2, ProcSet((1, 32)), 50, 249),
        2: Slot(2, 1, 3, ProcSet((1, 32)), 250, 499),
        3: Slot(3, 2, 0, ProcSet((1, 32)), 500, 1000),
    }

    ss = SlotSet(slots)
    print()

    for slot in ss.traverse_id():
        print(slot)

    print()
    for slot in ss.traverse_id(2, 0):
        print(slot)

    print()
    for slot in ss.traverse_id(3, 3):
        print(slot)


def test_traverse_width():
    slots = {
        1: Slot(1, 0, 0, ProcSet((1, 32)), 1, 1000),
    }

    ss = SlotSet(slots)
    ss.find_and_split_at(10)
    ss.find_and_split_at(11)
    ss.find_and_split_at(50)
    ss.find_and_split_at(150)
    ss.find_and_split_at(500)
    ss.find_and_split_at(750)

    print(ss)

    print()
    for (b, e) in ss.traverse_with_width(2000):
        print(b, e)
        print(e.e, "-", b.b, " -> ", e.e - b.b + 1)
        break


def test_split_at_before():
    slots = {
        1: Slot(1, 0, 0, ProcSet((1, 32)), 1, 100),
    }

    ss = SlotSet(slots)

    print()
    ss.split_at_before(1, 1)
    print(ss)

    slots = {
        1: Slot(1, 0, 0, ProcSet((1, 32)), 1, 100),
    }

    ss = SlotSet(slots)

    print()
    ss.split_at_before(1, 100)
    print(ss)

    slots = {
        1: Slot(1, 0, 0, ProcSet((1, 32)), 1, 100),
    }

    ss = SlotSet(slots)

    print()
    ss.split_at_before(1, 50)
    print(ss)


def test_split_at_after():
    slots = {
        1: Slot(1, 0, 0, ProcSet((1, 32)), 1, 100),
    }

    ss = SlotSet(slots)

    print()
    ss.split_at_after(1, 1)
    print(ss)

    slots = {
        1: Slot(1, 0, 0, ProcSet((1, 32)), 1, 100),
    }

    ss = SlotSet(slots)

    print()
    ss.split_at_after(1, 100)
    print(ss)

    slots = {
        1: Slot(1, 0, 0, ProcSet((1, 32)), 1, 100),
    }

    ss = SlotSet(slots)

    print()
    ss.split_at_after(1, 50)
    print(ss)
