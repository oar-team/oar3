# coding: utf-8
"""Parity tests for the optional parallel time-window scan.

The parallel scan must produce exactly the same placements as the sequential
one (it only evaluates windows concurrently).  These tests build a small
in-memory platform where jobs scan many windows, run schedule_id_jobs_ct with
the feature OFF and ON, and compare the resulting placements.
"""
import logging

import pytest
from procset import ProcSet

from oar.kao import parallel_windows
from oar.kao.quotas import Quotas
from oar.kao.scheduling import schedule_id_jobs_ct
from oar.kao.slot import Slot, SlotSet
from oar.lib.globals import init_oar
from oar.lib.job_handling import JobPseudo, set_jobs_cache_keys
from oar.lib.resource import ResourceSet

init_oar(no_db=True)
logging.disable(logging.CRITICAL)

NB_RES = 512


@pytest.fixture(autouse=True)
def reset_state():
    Quotas.enabled = False
    Quotas.calendar = None
    Quotas.default_rules = {}
    Quotas.job_types = ["*"]
    parallel_windows.set_options({"SCHEDULER_PARALLEL_WINDOWS": "no"})
    yield
    Quotas.enabled = False
    Quotas.calendar = None
    parallel_windows.set_options({"SCHEDULER_PARALLEL_WINDOWS": "no"})


def build_slotset(nb_slots, slot_width, nb_res):
    slots = {}
    for i in range(1, nb_slots + 1):
        free = ProcSet((1, nb_res - (i % 8)))
        b = (i - 1) * slot_width
        e = b + slot_width - 1
        slots[i] = Slot(
            i, i - 1 if i > 1 else 0, i + 1 if i < nb_slots else 0, free, b, e
        )
    return SlotSet(slots)


def run(njobs, req, nb_slots=40, slot_width=10, walltime=300, parallel=0):
    ResourceSet.default_itvs = ProcSet((1, NB_RES))
    parallel_windows.set_options(
        {
            "SCHEDULER_PARALLEL_WINDOWS": str(parallel),
            "SCHEDULER_PARALLEL_MIN_WINDOWS": "0",
        }
    )
    all_ss = {"default": build_slotset(nb_slots, slot_width, NB_RES)}
    hy = {"node": [ProcSet((i, i)) for i in range(1, NB_RES + 1)]}
    jobs = {}
    jids = list(range(1, njobs + 1))
    for i in jids:
        jobs[i] = JobPseudo(
            id=i,
            types={},
            deps=[],
            key_cache={},
            queue="default",
            user="u0",
            project="",
            mld_res_rqts=[(i, walltime, [([("node", req)], ProcSet((1, NB_RES)))])],
            ts=False,
            ph=0,
        )
    set_jobs_cache_keys(None, jobs)
    schedule_id_jobs_ct(all_ss, jobs, hy, jids, 10)
    out = {}
    for i in jids:
        job = jobs[i]
        rs = getattr(job, "res_set", None)
        out[i] = (
            getattr(job, "start_time", -1),
            getattr(job, "moldable_id", -1),
            sorted(rs.intervals()) if rs is not None else None,
        )
    return out


@pytest.mark.parametrize("req", [NB_RES, NB_RES - 3, NB_RES // 2])
@pytest.mark.parametrize("njobs", [3, 6])
def test_parallel_matches_sequential(njobs, req):
    import multiprocessing

    if "fork" not in multiprocessing.get_all_start_methods():
        pytest.skip("fork not available")
    sequential = run(njobs, req, parallel=0)
    parallel = run(njobs, req, parallel=6)
    assert sequential == parallel


def test_disabled_by_default_is_sequential():
    # SCHEDULER_PARALLEL_WINDOWS defaults to "no" -> k == 0
    parallel_windows.set_options({})
    assert parallel_windows.get_options()[0] == 0


@pytest.mark.parametrize("nb_slots", [1, 2, 3, 40])
def test_parallel_matches_sequential_nb_slots(nb_slots):
    # Regression: with very few windows the parallel path must still evaluate
    # the remaining windows (it used to return "no fit" without doing so).
    import multiprocessing

    if "fork" not in multiprocessing.get_all_start_methods():
        pytest.skip("fork not available")
    sequential = run(4, NB_RES - 3, nb_slots=nb_slots, parallel=0)
    parallel = run(4, NB_RES - 3, nb_slots=nb_slots, parallel=6)
    assert sequential == parallel
