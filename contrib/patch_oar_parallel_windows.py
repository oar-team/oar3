#!/usr/bin/env python3
# coding: utf-8
"""Install the optional parallel time-window scan on an installed Debian OAR.

This is the production patch for the ``perf(scheduler): optional parallel
time-window scan for hard jobs`` change.  It is independent from
``patch_oar_quota_scan.py``: the quota patch edits the sequential quota
function, this one adds a new module + the dispatcher/plumbing, so the two
patches can be applied in any order.

It installs/patches:

* a new module ``kao/parallel_windows.py`` (copied from this repository);
* ``kao/scheduling.py``: import it and let the ``find_first_suitable_contiguous_slots``
  dispatcher try the parallel path first;
* ``kao/kamelot.py``: resolve the parallelism options once per scheduling round
  (``parallel_windows.set_options(config)``).

The feature is OFF by default: it only activates when ``oar.conf`` contains
``SCHEDULER_PARALLEL_WINDOWS`` set to ``auto`` or an integer >= 2 (unlike the
other config keys, it does not need a default in configuration.py because OAR
loads arbitrary KEY=VALUE lines from oar.conf):

    SCHEDULER_PARALLEL_WINDOWS="auto"
    SCHEDULER_PARALLEL_MIN_WINDOWS="200"

Same safety rules as the other patch scripts: never imports OAR, verifies every
anchor before writing, idempotent, timestamped backups, dry-run by default,
atomic write, py_compile validation with rollback, and ``--revert``.

Usage
-----
    python3 contrib/patch_oar_parallel_windows.py                 # dry-run
    sudo python3 contrib/patch_oar_parallel_windows.py --apply
    sudo python3 contrib/patch_oar_parallel_windows.py --revert
    python3 contrib/patch_oar_parallel_windows.py --oar-dir /tmp/.../oar
"""

from __future__ import annotations

import argparse
import difflib
import glob
import os
import shutil
import stat
import subprocess
import sys
import tempfile
import time

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PARALLEL_MODULE_SRC = os.path.join(REPO_ROOT, "oar", "kao", "parallel_windows.py")

MARKER_SCHED = "parallel_windows.get_options"
MARKER_KAMELOT = "parallel_windows.set_options"
NEW_FILE_RELPATH = "kao/parallel_windows.py"

SCHED_IMPORT_OLD = (
    "from procset import ProcSet\n"
    "\n"
    "from oar.kao.helpers import job_scheduling_record, write_scheduling_timing_yaml\n"
)
SCHED_IMPORT_NEW = (
    "from procset import ProcSet\n"
    "\n"
    "from oar.kao import parallel_windows\n"
    "from oar.kao.helpers import job_scheduling_record, write_scheduling_timing_yaml\n"
)

SCHED_DISPATCH_OLD = (
    "    if Quotas.enabled and not job.no_quotas:\n"
    "        return find_first_suitable_contiguous_slots_quotas(\n"
    "            slots_set, job, res_rqt, hy, min_start_time\n"
    "        )\n"
    "\n"
    "    return find_first_suitable_contiguous_slots_no_quotas(\n"
    "        slots_set, job, res_rqt, hy, min_start_time\n"
    "    )"
)
SCHED_DISPATCH_NEW = (
    "    use_quotas = Quotas.enabled and not job.no_quotas\n"
    "\n"
    "    if parallel_windows.get_options()[0] >= 2:\n"
    "        parallel_result = parallel_windows.find_first_suitable_parallel(\n"
    "            slots_set, job, res_rqt, hy, min_start_time, use_quotas\n"
    "        )\n"
    "        if parallel_result is not None:\n"
    "            return parallel_result\n"
    "\n"
    "    if use_quotas:\n"
    "        return find_first_suitable_contiguous_slots_quotas(\n"
    "            slots_set, job, res_rqt, hy, min_start_time\n"
    "        )\n"
    "\n"
    "    return find_first_suitable_contiguous_slots_no_quotas(\n"
    "        slots_set, job, res_rqt, hy, min_start_time\n"
    "    )"
)

KAMELOT_OLD = (
    "    resource_set = plt.resource_set(session, config)\n"
    "\n"
    "    #\n"
    "    # Retrieve waiting jobs"
)
KAMELOT_NEW = (
    "    resource_set = plt.resource_set(session, config)\n"
    "\n"
    "    from oar.kao import parallel_windows\n"
    "\n"
    "    parallel_windows.set_options(config)\n"
    "\n"
    "    #\n"
    "    # Retrieve waiting jobs"
)

# (relpath, marker, [(label, old, new), ...])
PATCHED_FILES = [
    (
        "kao/scheduling.py",
        MARKER_SCHED,
        [
            ("import parallel_windows", SCHED_IMPORT_OLD, SCHED_IMPORT_NEW),
            (
                "dispatcher tries the parallel path",
                SCHED_DISPATCH_OLD,
                SCHED_DISPATCH_NEW,
            ),
        ],
    ),
    (
        "kao/kamelot.py",
        MARKER_KAMELOT,
        [("resolve parallelism per round", KAMELOT_OLD, KAMELOT_NEW)],
    ),
]

DEFAULT_GLOBS = [
    "/usr/lib/python3*/dist-packages/oar",
    "/usr/local/lib/python3*/dist-packages/oar",
]

BACKUP_TAG = "oar-parallel-windows"


class PatchError(Exception):
    """Raised when the patch cannot be applied safely."""


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


def find_package_dir(explicit):
    if explicit:
        path = os.path.realpath(os.path.abspath(explicit))
        if not os.path.isfile(os.path.join(path, "kao", "scheduling.py")):
            raise PatchError(
                "{} does not look like the OAR package dir (no kao/scheduling.py)".format(
                    path
                )
            )
        return path
    matches = []
    for pattern in DEFAULT_GLOBS:
        matches.extend(glob.glob(pattern))
    realpaths = sorted(
        {os.path.realpath(m) for m in matches if os.path.isdir(os.path.realpath(m))}
    )
    if not realpaths:
        raise PatchError(
            "no installed OAR package found; looked for:\n  "
            + "\n  ".join(DEFAULT_GLOBS)
            + "\nUse --oar-dir to point at the 'oar' package directory."
        )
    if len(realpaths) > 1:
        raise PatchError(
            "several candidate package dirs found, use --oar-dir:\n  "
            + "\n  ".join(realpaths)
        )
    return realpaths[0]


def read_text(path):
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return fh.read()
    except UnicodeDecodeError as exc:
        raise PatchError("cannot decode {} as UTF-8: {}".format(path, exc))


def apply_edits(content, edits, relpath):
    new = content
    for label, old, replacement in edits:
        count = new.count(old)
        if count != 1:
            raise PatchError(
                "{}: anchor for '{}' matched {} times, expected exactly 1".format(
                    relpath, label, count
                )
            )
        new = new.replace(old, replacement, 1)
    return new


def unified_diff(original, new, path):
    return "".join(
        difflib.unified_diff(
            original.splitlines(keepends=True),
            new.splitlines(keepends=True),
            fromfile="a/" + path,
            tofile="b/" + path,
        )
    )


def _preserve_metadata(src, tmp):
    st = os.stat(src)
    try:
        os.chown(tmp, st.st_uid, st.st_gid)
    except (PermissionError, OSError) as exc:
        eprint(
            "note: could not preserve owner/group of {} ({}); "
            "mode preserved only".format(src, exc)
        )
    os.chmod(tmp, stat.S_IMODE(st.st_mode))


def atomic_write(path, content):
    directory = os.path.dirname(os.path.abspath(path)) or "."
    fd, tmp = tempfile.mkstemp(prefix=".oar-parallel-", suffix=".tmp", dir=directory)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(content)
        _preserve_metadata(path, tmp)
        os.replace(tmp, path)
    except BaseException:
        if os.path.exists(tmp):
            os.unlink(tmp)
        raise


def atomic_create(path, content):
    """Atomically create a new file (no existing metadata to preserve)."""
    directory = os.path.dirname(os.path.abspath(path)) or "."
    fd, tmp = tempfile.mkstemp(prefix=".oar-parallel-", suffix=".tmp", dir=directory)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(content)
        os.chmod(tmp, 0o644)
        os.replace(tmp, path)
    except BaseException:
        if os.path.exists(tmp):
            os.unlink(tmp)
        raise


def make_backup(path):
    ts = time.strftime("%Y%m%d-%H%M%S")
    backup = "{}.{}.{}.bak".format(path, BACKUP_TAG, ts)
    if os.path.exists(backup):
        raise PatchError(
            "backup {} already exists (two runs within the same second?); "
            "refusing to overwrite it".format(backup)
        )
    shutil.copy2(path, backup)
    return backup


def list_backups(path):
    pattern = "{}.{}.????????-??????.bak".format(path, BACKUP_TAG)
    return sorted(glob.glob(pattern))


def validate_compile(path):
    pyc_cache = tempfile.mkdtemp(prefix=".oar-parallel-pyc-")
    env = dict(os.environ)
    env["PYTHONPYCACHEPREFIX"] = pyc_cache
    interpreter = sys.executable or "python3"
    try:
        proc = subprocess.run(
            [interpreter, "-m", "py_compile", path],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            env=env,
            universal_newlines=True,
        )
        return proc.returncode == 0, proc.stdout
    finally:
        shutil.rmtree(pyc_cache, ignore_errors=True)


def _module_source():
    if not os.path.isfile(PARALLEL_MODULE_SRC):
        raise PatchError(
            "parallel_windows.py not found in the repository at {}".format(
                PARALLEL_MODULE_SRC
            )
        )
    return read_text(PARALLEL_MODULE_SRC)


def _installed_new_file(pkg_dir, source):
    dest = os.path.join(pkg_dir, NEW_FILE_RELPATH)
    if not os.path.isfile(dest):
        return False
    try:
        return read_text(dest) == source
    except PatchError:
        return False


def do_apply(pkg_dir, dry_run):
    source = _module_source()
    file_installed = _installed_new_file(pkg_dir, source)

    contents = {}
    states = {}
    for relpath, marker, _ in PATCHED_FILES:
        path = os.path.join(pkg_dir, relpath)
        if not os.path.isfile(path):
            raise PatchError("file not found: {}".format(path))
        content = read_text(path)
        contents[relpath] = content
        states[relpath] = marker in content

    n_done = sum(1 for v in states.values() if v) + (1 if file_installed else 0)
    total = len(PATCHED_FILES) + 1
    if n_done == total:
        print(
            "Already installed: parallel_windows.py + markers present. Nothing to do."
        )
        return 0
    if n_done != 0:
        eprint("ABORT: inconsistent state (partially installed):")
        eprint(
            "  {}: {}".format(
                NEW_FILE_RELPATH, "present" if file_installed else "missing"
            )
        )
        for relpath, _, _ in PATCHED_FILES:
            eprint(
                "  {}: {}".format(
                    relpath, "patched" if states[relpath] else "not patched"
                )
            )
        eprint("Revert first (--revert), then re-apply.")
        return 2

    # Verify every anchor before writing anything.
    problems = []
    for relpath, _, edits in PATCHED_FILES:
        content = contents[relpath]
        for label, old, _ in edits:
            count = content.count(old)
            if count == 0:
                problems.append(
                    (relpath, label, "anchor not found (different OAR version?)")
                )
            elif count > 1:
                problems.append((relpath, label, "anchor found {} times".format(count)))
    if problems:
        eprint("ABORT: refuse to edit; expected anchors are missing/changed:")
        for relpath, label, reason in problems:
            eprint("  - {}: {}: {}".format(relpath, label, reason))
        return 2

    new_contents = {}
    for relpath, _, edits in PATCHED_FILES:
        try:
            new_contents[relpath] = apply_edits(contents[relpath], edits, relpath)
        except PatchError as exc:
            eprint("ABORT: {}".format(exc))
            return 2

    if dry_run:
        print(
            "=== would install kao/parallel_windows.py ({} bytes) ===".format(
                len(source)
            )
        )
        for relpath, _, _ in PATCHED_FILES:
            diff = unified_diff(
                contents[relpath],
                new_contents[relpath],
                os.path.join(pkg_dir, relpath),
            )
            if diff:
                print("=== {} ===".format(relpath))
                print(diff)
        print(
            "Dry run: nothing written. Re-run with --apply (backups will be created)."
        )
        return 0

    new_path = os.path.join(pkg_dir, NEW_FILE_RELPATH)
    backups = {}
    written = []
    try:
        for relpath, _, _ in PATCHED_FILES:
            path = os.path.join(pkg_dir, relpath)
            backups[relpath] = make_backup(path)
            atomic_write(path, new_contents[relpath])
            written.append(relpath)
        # install the new module (backup any pre-existing file)
        if os.path.exists(new_path):
            backups[NEW_FILE_RELPATH] = make_backup(new_path)
            atomic_write(new_path, source)
        else:
            atomic_create(new_path, source)
        written.append(NEW_FILE_RELPATH)
    except (OSError, PatchError) as exc:
        eprint("ABORT: could not write: {}".format(exc))
        eprint("(are you root? /usr is usually not writable by a normal user)")
        for rel in written:
            rel_path = os.path.join(pkg_dir, rel)
            try:
                if rel == NEW_FILE_RELPATH:
                    if rel in backups:
                        atomic_write(rel_path, read_text(backups[rel]))
                    elif os.path.exists(rel_path):
                        os.unlink(rel_path)
                else:
                    atomic_write(rel_path, contents[rel])
            except OSError:
                pass
        eprint("Rolled back {} file(s).".format(len(written)))
        return 2

    failures = []
    for relpath in [r for r, _, _ in PATCHED_FILES] + [NEW_FILE_RELPATH]:
        ok, out = validate_compile(os.path.join(pkg_dir, relpath))
        if not ok:
            failures.append((relpath, out))
    if failures:
        eprint("Validation with py_compile FAILED, restoring every file...")
        for relpath, _, _ in PATCHED_FILES:
            atomic_write(os.path.join(pkg_dir, relpath), contents[relpath])
        if NEW_FILE_RELPATH in backups:
            atomic_write(new_path, read_text(backups[NEW_FILE_RELPATH]))
        else:
            os.unlink(new_path)
        for relpath, out in failures:
            eprint("  {}:\n{}".format(relpath, out))
        return 1

    for relpath, _, _ in PATCHED_FILES:
        print(
            "Patched: {} (backup {})".format(
                os.path.join(pkg_dir, relpath), backups[relpath]
            )
        )
    print(
        "Installed: {} (backup {})".format(
            new_path, backups.get(NEW_FILE_RELPATH, "none")
        )
    )
    print("py_compile: OK for all files")
    print("")
    print("Next steps (run as root):")
    print("  1. enable it in /etc/oar/oar.conf:")
    print('       SCHEDULER_PARALLEL_WINDOWS="auto"')
    print('       SCHEDULER_PARALLEL_MIN_WINDOWS="200"')
    print("  2. systemctl restart oar-server")
    print("")
    print("To revert:")
    print("  sudo {} --revert".format(os.path.basename(sys.argv[0])))
    return 0


def do_revert(pkg_dir):
    rc = 0
    for relpath, _, _ in PATCHED_FILES:
        path = os.path.join(pkg_dir, relpath)
        backups = list_backups(path)
        if not backups:
            eprint("WARNING: no backup found for {}; skipping".format(path))
            rc = 2
            continue
        latest = backups[-1]
        atomic_write(path, read_text(latest))
        ok, out = validate_compile(path)
        print(
            "Reverted {} from {} (py_compile: {})".format(
                path, latest, "OK" if ok else "FAILED"
            )
        )
        if not ok:
            eprint(out)
            rc = 1
    new_path = os.path.join(pkg_dir, NEW_FILE_RELPATH)
    backups = list_backups(new_path)
    if backups:
        atomic_write(new_path, read_text(backups[-1]))
        print("Restored {} from {}".format(new_path, backups[-1]))
    elif os.path.isfile(new_path):
        os.unlink(new_path)
        print("Removed {}".format(new_path))
    return rc


def parse_args(argv):
    parser = argparse.ArgumentParser(
        description=(
            "Install the optional parallel time-window scan on an installed "
            "Debian OAR. Dry-run by default; never imports OAR."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--oar-dir",
        metavar="DIR",
        help="the installed 'oar' package directory (auto-detected otherwise)",
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--dry-run", action="store_true", help="show what would change (default)"
    )
    mode.add_argument(
        "--apply", action="store_true", help="install/patch, then validate"
    )
    mode.add_argument(
        "--revert", action="store_true", help="restore the latest backups"
    )
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(sys.argv[1:] if argv is None else argv)
    dry_run = not (args.apply or args.revert)
    try:
        pkg_dir = find_package_dir(args.oar_dir)
    except PatchError as exc:
        eprint("ABORT: {}".format(exc))
        return 2
    print("OAR package: {}".format(pkg_dir))
    if args.revert:
        return do_revert(pkg_dir)
    try:
        return do_apply(pkg_dir, dry_run)
    except PatchError as exc:
        eprint("ABORT: {}".format(exc))
        return 2
    except OSError as exc:
        eprint("ABORT: {}".format(exc))
        return 2


if __name__ == "__main__":
    sys.exit(main())
