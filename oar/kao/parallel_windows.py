# coding: utf-8
"""Optional parallel time-window scan for the Python Kamelot scheduler.

The per-window scan in ``find_first_suitable_contiguous_slots_*`` is read-only
and independent across windows; only the final ``split_slots`` commit mutates
the slot set.  On a large platform a "hard" job scans many windows, each
dominated by the hierarchy search.  This module scans a sequential prefix and,
if the job is still not placed, evaluates the remaining windows concurrently in
forked children and returns the earliest suitable window, exactly as the
sequential loop would.

OFF by default.  Enabled only when ``SCHEDULER_PARALLEL_WINDOWS`` is ``auto``
or an integer >= 2, fork is available and the temporal quota calendar is
disabled.  ``SCHEDULER_PARALLEL_MIN_WINDOWS`` (default 200) is the sequential
prefix length: jobs that find a window there never fork.  ``procset`` is pure
Python (GIL), so only fork parallelizes.  Children only read the copy-on-write
slot set; the parent performs the single commit and replays the cache/log side
effects.  A shared flag stops the chunks after the earliest one found a window.
Any fork/child failure falls back to the sequential implementation (returns
``None``).
"""

from __future__ import annotations

import logging
import multiprocessing
import os
import pickle
import struct

from procset import ProcSet

from oar.kao.quotas import Quotas
from oar.kao.slot import intersec_itvs_slots, intersec_ts_ph_itvs_slots
from oar.lib.globals import get_logger
from oar.lib.job_handling import ALLOW
from oar.lib.resource import ResourceSet

logger = get_logger("oar.kamelot")

_K = 0
_MIN_WINDOWS = 200


def set_options(config):
    """Resolve ``(k, min_windows)`` from the OAR configuration; 0 disables."""
    global _K, _MIN_WINDOWS
    raw = str(config.get("SCHEDULER_PARALLEL_WINDOWS", "no") or "no").strip().lower()
    if raw in ("no", "off", "false", "0", ""):
        _K = 0
    elif raw == "auto":
        _K = os.cpu_count() or 1
    else:
        try:
            _K = int(raw)
        except (TypeError, ValueError):
            _K = 0
    if _K < 2 or "fork" not in multiprocessing.get_all_start_methods():
        _K = 0
    try:
        _MIN_WINDOWS = max(
            0, int(str(config.get("SCHEDULER_PARALLEL_MIN_WINDOWS", "200")))
        )
    except (TypeError, ValueError):
        _MIN_WINDOWS = 200
    return _K, _MIN_WINDOWS


def get_options():
    return _K, _MIN_WINDOWS


def _eval_window(
    slots_set, slots, job, hy, walltime, hy_res_rqts, sid_left, sid_right, use_quotas
):
    """(passed, itvs, frontier, precheck_failure) for one window (read-only)."""
    from oar.kao.scheduling import find_resource_hierarchies_job

    if use_quotas and Quotas.enabled and not job.no_quotas:
        failure = Quotas.pre_check_slots_quotas(slots, sid_left, sid_right, job)
        if failure is not None:
            return False, None, True, failure

    if job.ts or (job.ph == ALLOW):
        itvs_avail = intersec_ts_ph_itvs_slots(slots, sid_left, sid_right, job)
    else:
        itvs_avail = intersec_itvs_slots(slots, sid_left, sid_right)

    if job.find:
        if use_quotas:
            beginning_slotset = (sid_left == 1) and (slots_set.begin == slots[1].b)
        else:
            beginning_slotset = slots[sid_left].prev == 0
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

    if len(itvs) == 0:
        return False, None, False, None

    if use_quotas and Quotas.enabled and not job.no_quotas:
        nb_res = len(itvs & ResourceSet.default_itvs)
        res = Quotas.check_slots_quotas(
            slots, sid_left, sid_right, job, nb_res, walltime
        )
        if not res[0]:
            return False, None, True, None

    return True, itvs, True, None


def _child_scan(
    slots_set, job, hy, res_rqt, chunks, use_quotas, write_fd, next_chunk, found
):
    try:
        logging.disable(logging.CRITICAL)
        slots = slots_set.slots
        (_mld_id, walltime, hy_res_rqts) = res_rqt
        n = len(chunks)
        passed = None
        pass_index = None
        frontier = None
        precheck = None
        skipped = 0
        while True:
            with next_chunk.get_lock():
                index = next_chunk.value
                next_chunk.value += 1
            # no more chunks, or a pass was already found at an earlier chunk
            if index >= n or index >= found.value:
                break
            for sid_left, sid_right in chunks[index]:
                # an earlier chunk found a window -> stop this chunk
                if found.value < index:
                    break
                ok, itvs, is_frontier, failure = _eval_window(
                    slots_set,
                    slots,
                    job,
                    hy,
                    walltime,
                    hy_res_rqts,
                    sid_left,
                    sid_right,
                    use_quotas,
                )
                if is_frontier and frontier is None:
                    frontier = sid_left
                if failure is not None:
                    skipped += 1
                    if precheck is None:
                        precheck = failure
                if ok:
                    with found.get_lock():
                        if index < found.value:
                            found.value = index
                    if pass_index is None or index < pass_index:
                        passed = (sid_left, sid_right, itvs)
                        pass_index = index
                    break
        payload = {
            "pass": passed,
            "pass_index": pass_index,
            "frontier": frontier,
            "precheck": precheck,
            "skipped": skipped,
        }
    except BaseException:  # noqa: BLE001
        import traceback

        payload = {"error": traceback.format_exc()}
    data = pickle.dumps(payload, protocol=pickle.HIGHEST_PROTOCOL)
    try:
        os.write(write_fd, struct.pack("!I", len(data)) + data)
        os.close(write_fd)
    finally:
        os._exit(0)


def _read_payload(read_fd):
    chunks = []
    while True:
        block = os.read(read_fd, 65536)
        if not block:
            break
        chunks.append(block)
    os.close(read_fd)
    raw = b"".join(chunks)
    if len(raw) < 4:
        return {"error": "short read"}
    size = struct.unpack("!I", raw[:4])[0]
    return pickle.loads(raw[4 : 4 + size])


def find_first_suitable_parallel(
    slots_set, job, res_rqt, hy, min_start_time, use_quotas
):
    """Parallel counterpart of ``find_first_suitable_contiguous_slots_*``.

    Returns ``(itvs, sid_left, sid_right)`` (replaying cache/log side effects)
    or ``None`` to let the caller use the sequential implementation.
    """
    k, min_windows = get_options()
    if k < 2 or (use_quotas and Quotas.calendar is not None):
        return None

    (mld_id, walltime, hy_res_rqts) = res_rqt
    slots = slots_set.slots
    cache = slots_set.cache
    key = job.key_cache.get(mld_id) if job.key_cache else None

    if min_start_time < 0:
        start_id = slots_set.first().id
        if key is not None and key in cache:
            start_id = cache[key]
    else:
        start_id = slots_set.slot_id_at(min_start_time)

    pairs = [
        (sb.id, se.id)
        for sb, se in slots_set.traverse_with_width(walltime, start_id=start_id)
    ]
    if not pairs:
        return None

    # Sequential prefix: easy jobs never fork, and parity is guaranteed by the
    # same per-window evaluation used by the children.
    frontier = -1
    precheck = None
    prefix_skipped = 0
    prefix_len = min(min_windows, len(pairs))
    for sid_left, sid_right in pairs[:prefix_len]:
        ok, itvs, is_frontier, failure = _eval_window(
            slots_set,
            slots,
            job,
            hy,
            walltime,
            hy_res_rqts,
            sid_left,
            sid_right,
            use_quotas,
        )
        if is_frontier and frontier == -1:
            frontier = sid_left
        if failure is not None:
            prefix_skipped += 1
            if precheck is None:
                precheck = failure
        if ok:
            return _finish(
                use_quotas,
                job,
                cache,
                key,
                min_start_time,
                (sid_left, sid_right, itvs),
                frontier,
                precheck,
                walltime,
                prefix_skipped,
            )

    rest = pairs[prefix_len:]
    if len(rest) < 2 or k < 2:
        # Too few windows (or parallel disabled): keep scanning sequentially
        # rather than returning "no fit" without evaluating them.
        for sid_left, sid_right in rest:
            ok, itvs, is_frontier, failure = _eval_window(
                slots_set,
                slots,
                job,
                hy,
                walltime,
                hy_res_rqts,
                sid_left,
                sid_right,
                use_quotas,
            )
            if is_frontier and frontier == -1:
                frontier = sid_left
            if failure is not None:
                prefix_skipped += 1
                if precheck is None:
                    precheck = failure
            if ok:
                return _finish(
                    use_quotas,
                    job,
                    cache,
                    key,
                    min_start_time,
                    (sid_left, sid_right, itvs),
                    frontier,
                    precheck,
                    walltime,
                    prefix_skipped,
                )
        return _finish(
            use_quotas,
            job,
            cache,
            key,
            min_start_time,
            None,
            frontier,
            precheck,
            walltime,
            prefix_skipped,
        )

    # Fine-grained chunks handed out dynamically (work-stealing) so that a job
    # whose suitable window is early still spreads the preceding windows over
    # all the workers, instead of loading a single contiguous chunk.
    n_chunks = max(1, min(k * 4, len(rest)))
    chunk_size = (len(rest) + n_chunks - 1) // n_chunks
    chunks = [rest[i * chunk_size : (i + 1) * chunk_size] for i in range(n_chunks)]
    chunks = [c for c in chunks if c]
    n_chunks = len(chunks)

    next_chunk = multiprocessing.Value("i", 0)
    found = multiprocessing.Value("i", n_chunks)  # smallest chunk index with a pass
    children = []
    try:
        for _ in range(min(k, n_chunks)):
            read_fd, write_fd = os.pipe()
            pid = os.fork()
            if pid == 0:
                os.close(read_fd)
                _child_scan(
                    slots_set,
                    job,
                    hy,
                    res_rqt,
                    chunks,
                    use_quotas,
                    write_fd,
                    next_chunk,
                    found,
                )
                os._exit(0)
            os.close(write_fd)
            children.append((pid, read_fd))
        results = [_read_payload(rf) for _, rf in children]
        for pid, _ in children:
            try:
                os.waitpid(pid, 0)
            except OSError:
                pass
    except OSError as exc:
        logger.warning("parallel window scan failed (%s); falling back", exc)
        for pid, rf in children:
            try:
                os.close(rf)
            except OSError:
                pass
            try:
                os.waitpid(pid, 0)
            except OSError:
                pass
        return None

    for res in results:
        if "error" in res:
            logger.warning("parallel window scan child error: %s", res["error"])
            return None

    chosen = None
    chosen_index = None
    total_skipped = prefix_skipped
    for res in results:
        worker_frontier = res.get("frontier")
        if worker_frontier is not None and (
            frontier == -1 or worker_frontier < frontier
        ):
            frontier = worker_frontier
        if precheck is None and res.get("precheck") is not None:
            precheck = res["precheck"]
        pass_index = res.get("pass_index")
        if res.get("pass") is not None and (
            chosen_index is None or pass_index < chosen_index
        ):
            chosen = res["pass"]
            chosen_index = pass_index
        total_skipped += res.get("skipped", 0)

    return _finish(
        use_quotas,
        job,
        cache,
        key,
        min_start_time,
        chosen,
        frontier,
        precheck,
        walltime,
        total_skipped,
    )


def _finish(
    use_quotas,
    job,
    cache,
    key,
    min_start_time,
    chosen,
    frontier,
    precheck,
    walltime,
    skipped=0,
):
    if use_quotas:
        if chosen is None:
            if precheck is not None:
                (_ok, quotas_msg, rule, value) = precheck
                logger.info(
                    f"Quotas limitation reached, job: {str(job.id)}, {quotas_msg}, rule: {rule}, value: {value}"
                )
            else:
                logger.info(
                    "can't schedule job with id: {}, walltime not satisfied: {}".format(
                        job.id, walltime
                    )
                )
            return (ProcSet(), -1, -1)
        sid_left, sid_right, itvs = chosen
        if skipped > 0 and precheck is not None:
            # Quota-limited on some windows but scheduled on a later one: report
            # it once (instead of one line per skipped window).
            (_ok, quotas_msg, rule, value) = precheck
            logger.info(
                f"Quotas limitation reached, job: {str(job.id)}, {quotas_msg}, rule: {rule}, value: {value}, scheduled later ({skipped} window(s) skipped)"
            )
        if key is not None and min_start_time < 0:
            cache[key] = frontier if frontier != -1 else sid_left
        return (itvs, sid_left, sid_right)

    # no-quotas path
    if chosen is None:
        logger.info(
            "can't schedule job with id: {}, no suitable resources".format(job.id)
        )
        return (ProcSet(), -1, -1)
    sid_left, sid_right, itvs = chosen
    if key is not None and min_start_time < 0:
        cache[key] = sid_left
    return (itvs, sid_left, sid_right)
