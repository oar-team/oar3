# coding: utf-8
"""
End-to-end benchmark of the OAR3 scheduler, comparing quotas OFF vs ON.

It times `meta_schedule()`, the full scheduling round (resource set loading,
waiting-job retrieval, sorting, slot-set construction, the scheduling loop and
the writing of assignments), against a self-contained SQLite database that the
script creates, seeds with resources and jobs, and tears down. No external
database and no PostgreSQL are required.

Two modes:
  - sweep   (default) measures meta_schedule for a range of job-batch sizes,
            once with quotas disabled and once enabled, and writes a results
            table, a CSV and a PNG plot.
  - profile runs a single configuration under cProfile and prints the functions
            sorted by cumulative time, then saves the raw profile as a .pstats
            file for further inspection.

Outputs (written to --outdir):
  - bench_quotas_metasched.csv   columns: jobs, quotas_off_s, quotas_on_s,
                                 overhead_s, ratio
  - bench_quotas_metasched.png   scheduling time vs job count, OFF vs ON
  - bench_metasched_<on|off>_n<N>.pstats   (profile mode only) raw cProfile dump

Inspecting the .pstats file produced by --mode profile:

  Interactive browser from the shell:
      python3 -m pstats bench_metasched_on_n256.pstats
      # then, at the prompt:
      #   sort cumulative      sort by cumulative time
      #   sort tottime         sort by own (self) time
      #   stats 20             show the top 20 lines
      #   callers check        who calls functions matching "check"
      #   callees schedule     what those functions call
      #   quit

  From Python:
      import pstats
      st = pstats.Stats("bench_metasched_on_n256.pstats")
      st.sort_stats("cumulative").print_stats(25)   # top of the call tree
      st.sort_stats("tottime").print_stats(25)       # heaviest leaf functions
      st.print_callers("check_slots_quotas")         # callers of a function
      st.print_callees("schedule_id_jobs_ct")        # callees of a function

  Several .pstats files can be merged: pstats.Stats("a.pstats", "b.pstats").
  Optional visual tools read the same file, e.g. `snakeviz file.pstats`
  (pip install snakeviz) or gprof2dot piped into Graphviz.

Usage:
    python3 bench/bench_quotas_metasched.py
    python3 bench/bench_quotas_metasched.py --max-k 9 --nb-res 64
    python3 bench/bench_quotas_metasched.py --mode profile --profile-jobs 512 --quotas on
"""
import argparse
import contextlib
import cProfile
import io
import logging
import os
import pstats
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import simplejson as json  # noqa: E402
from alembic.migration import MigrationContext  # noqa: E402
from alembic.operations import Operations  # noqa: E402
from sqlalchemy import Column, Integer, String  # noqa: E402
from sqlalchemy.orm import scoped_session, sessionmaker  # noqa: E402

import oar.lib.tools as tools  # noqa: E402
from oar.kao.meta_sched import meta_schedule  # noqa: E402
from oar.kao.quotas import Quotas  # noqa: E402
from oar.lib.database import ephemeral_session  # noqa: E402
from oar.lib.globals import init_config, init_oar  # noqa: E402
from oar.lib.job_handling import insert_job  # noqa: E402
from oar.lib.models import DeferredReflectionModel, Model, Queue, Resource  # noqa: E402
from tests import DEFAULT_CONFIG  # noqa: E402  (reuse the test config)

logging.disable(logging.CRITICAL)

BIG = 10**9


# ---------------------------------------------------------------------------
# Neutralize network notifications (like the monkeypatch_tools fixture)
# ---------------------------------------------------------------------------
def _silence_tools():
    tools.create_almighty_socket = lambda *a, **k: None
    tools.notify_almighty = lambda *a, **k: True
    tools.notify_tcp_socket = lambda *a, **k: 0
    tools.notify_user = lambda *a, **k: 0
    tools.notify_bipbip_commander = lambda *a, **k: True


# ---------------------------------------------------------------------------
# Build a SQLite database (temporary file) (cf. tests/conftest.py)
# ---------------------------------------------------------------------------
_DB = {}  # cache: schema built only once (global prepare)


def get_engine(quotas_conf_path):
    if _DB:
        _DB["config"]["QUOTAS_CONF_FILE"] = quotas_conf_path
        return _DB["config"], _DB["engine"], _DB["scoped"]
    config = init_config()
    config.update(DEFAULT_CONFIG.copy())
    fd, db_path = tempfile.mkstemp(suffix=".sqlite")
    os.close(fd)
    os.unlink(db_path)
    config["DB_TYPE"] = "sqlite"
    config["DB_BASE_FILE"] = db_path
    config["LOG_FILE"] = os.path.join(tempfile.gettempdir(), "oar_bench.log")
    config["QUOTAS_CONF_FILE"] = quotas_conf_path

    config, engine = init_oar(config=config, no_reflect=True)
    Model.metadata.create_all(bind=engine)

    conn = engine.connect()
    ctx = MigrationContext.configure(conn)
    with ctx.begin_transaction():
        op = Operations(ctx)
        for name, typ in (
            ("core", Integer),
            ("cpu", Integer),
            ("host", String(255)),
            ("mem", Integer),
        ):
            try:
                op.add_column("resources", Column(name, typ, nullable=True))
            except Exception:
                pass
    conn.close()
    DeferredReflectionModel.prepare(engine)  # only once (Resource/Job)

    scoped = scoped_session(sessionmaker(bind=engine))
    _DB.update(config=config, engine=engine, scoped=scoped, path=db_path)
    return config, engine, scoped


def seed(session, nb_res, nb_jobs, req_res, nb_users, nb_projects, cores_per_node):
    Queue.create(
        session, name="default", priority=3, scheduler_policy="kamelot", state="Active"
    )
    Queue.create(
        session,
        name="besteffort",
        priority=0,
        scheduler_policy="kamelot",
        state="Active",
    )
    for i in range(nb_res):
        Resource.create(
            session, network_address="node%d" % (i // max(1, cores_per_node))
        )
    for k in range(nb_jobs):
        insert_job(
            session,
            res=[(60, [("resource_id=%d" % req_res, "")])],
            properties="",
            user="user%d" % (k % nb_users),
            project="proj%d" % (k % nb_projects),
            queue_name="default",
        )


# ---------------------------------------------------------------------------
# Quotas
# ---------------------------------------------------------------------------
def write_quotas_conf():
    """Non-blocking quotas JSON (the machinery runs, nothing gets rejected)."""
    fd, path = tempfile.mkstemp(suffix="_quotas.json")
    with os.fdopen(fd, "w") as f:
        json.dump({"quotas": {"*,*,*,/": [BIG, -1, -1], "*,*,*,*": [-1, -1, -1]}}, f)
    return path


def reset_quotas(on, config):
    Quotas.enabled = False
    Quotas.calendar = None
    Quotas.default_rules = {}
    Quotas.job_types = ["*"]
    config["QUOTAS"] = "yes" if on else "no"


# ---------------------------------------------------------------------------
# One measurement = build DB + seed + timed meta_schedule
# ---------------------------------------------------------------------------
def measure(nb_jobs, args, quotas_on, quotas_conf, profiler=None):
    config, engine, scoped = get_engine(quotas_conf)
    reset_quotas(quotas_on, config)
    secs = None
    with ephemeral_session(scoped, engine, bind=engine) as session:
        seed(
            session,
            args.nb_res,
            nb_jobs,
            args.req_res,
            args.nb_users,
            args.nb_projects,
            args.cores_per_node,
        )
        t0 = time.time()
        if profiler is not None:
            profiler.enable()
        with contextlib.redirect_stdout(io.StringIO()):
            meta_schedule(session, config)
        if profiler is not None:
            profiler.disable()
        secs = time.time() - t0
    return secs


def run_sweep(args):
    quotas_conf = write_quotas_conf()
    print(
        "# meta_schedule end-to-end | cluster=%d res (%d/node) | req=%d res/job | users=%d projects=%d"
        % (
            args.nb_res,
            args.cores_per_node,
            args.req_res,
            args.nb_users,
            args.nb_projects,
        )
    )
    print("# SQLite DB (temporary file) | repeat=%d (best time)\n" % args.repeat)
    hdr = "{:>8}{:>14}{:>14}{:>12}{:>9}".format(
        "jobs", "quotas_off", "quotas_on", "overhead", "ratio"
    )
    print(hdr)
    print("-" * len(hdr))
    rows = []
    for n in args.jobs:
        best_off = best_on = float("inf")
        for _ in range(args.repeat):
            best_off = min(best_off, measure(n, args, False, quotas_conf))
            best_on = min(best_on, measure(n, args, True, quotas_conf))
        overhead = best_on - best_off
        ratio = best_on / best_off if best_off > 0 else float("nan")
        rows.append((n, best_off, best_on, overhead, ratio))
        print(
            "{:>8}{:>14}{:>14}{:>12}{:>9}".format(
                n,
                "%.4f" % best_off,
                "%.4f" % best_on,
                "%.4f" % overhead,
                "%.2fx" % ratio,
            )
        )
    _save_csv(rows, args)
    _save_plot(rows, args)
    return rows


def run_profile(args):
    quotas_conf = write_quotas_conf()
    on = args.quotas == "on"
    n = args.profile_jobs
    pr = cProfile.Profile()
    secs = measure(n, args, on, quotas_conf, profiler=pr)
    print(
        "# PROFILE meta_schedule | jobs=%d | quotas=%s | total=%.3fs\n"
        % (n, args.quotas, secs)
    )
    st = pstats.Stats(pr)
    st.sort_stats("cumulative").print_stats(25)
    out = os.path.join(args.outdir, "bench_metasched_%s_n%d.pstats" % (args.quotas, n))
    st.dump_stats(out)
    print("dump pstats -> %s" % out)


def _save_csv(rows, args):
    path = os.path.join(args.outdir, "bench_quotas_metasched.csv")
    with open(path, "w") as f:
        f.write("jobs,quotas_off_s,quotas_on_s,overhead_s,ratio\n")
        for r in rows:
            f.write("%d,%.6f,%.6f,%.6f,%.4f\n" % r)
    print("\nCSV  -> %s" % path)


def _save_plot(rows, args):
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except Exception as e:  # pragma: no cover
        print("plot skipped (matplotlib unavailable: %s)" % e)
        return
    jobs = [r[0] for r in rows]
    off = [r[1] for r in rows]
    on = [r[2] for r in rows]
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.plot(jobs, off, marker="o", ms=5, color="#2c7", label="quotas OFF")
    ax.plot(jobs, on, marker="s", ms=5, color="#c44", label="quotas ON")
    ax.set_xlabel("number of waiting jobs")
    ax.set_ylabel("meta_schedule time (s)")
    ax.set_title("OAR3 — meta_schedule end-to-end: quotas OFF vs ON")
    ax.grid(True, ls=":", alpha=0.5)
    ax.legend()
    fig.tight_layout()
    p = os.path.join(args.outdir, "bench_quotas_metasched.png")
    fig.savefig(p, dpi=130, bbox_inches="tight")
    print("plot -> %s" % p)


def main():
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("--mode", choices=["sweep", "profile"], default="sweep")
    p.add_argument(
        "--max-k",
        type=int,
        default=8,
        help="N from 2^1 to 2^max_k (def: 8 -> up to 256 jobs)",
    )
    p.add_argument(
        "--jobs",
        nargs="+",
        type=int,
        default=None,
        help="explicit list of sizes (otherwise: powers of 2 via --max-k)",
    )
    p.add_argument(
        "--nb-res", type=int, default=64, help="number of resources in the DB"
    )
    p.add_argument("--cores-per-node", type=int, default=16)
    p.add_argument("--req-res", type=int, default=2, help="resources requested per job")
    p.add_argument("--nb-users", type=int, default=16)
    p.add_argument("--nb-projects", type=int, default=8)
    p.add_argument("--repeat", type=int, default=1)
    p.add_argument(
        "--quotas",
        choices=["on", "off"],
        default="on",
        help="(profile mode) quotas enabled or not",
    )
    p.add_argument("--profile-jobs", type=int, default=256)
    p.add_argument("--outdir", default=".")
    args = p.parse_args()
    os.makedirs(args.outdir, exist_ok=True)
    if not args.jobs:
        args.jobs = [2**k for k in range(1, args.max_k + 1)]
    _silence_tools()
    if args.mode == "sweep":
        run_sweep(args)
    else:
        run_profile(args)


if __name__ == "__main__":
    main()
