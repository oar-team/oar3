#!/usr/bin/env python3
# coding: utf-8
"""Patch an installed Debian OAR to speed up the quota-aware scheduling scan.

When a job is blocked by a saturated quota rule, the Kamelot scheduler scans
every time window of the slot set.  Historically each window recomputed the
resource intersection from every start slot (O(H*W)), rebuilt the quota rule
tree, and searched the resource hierarchy *before* checking the quota.

This script applies three result-preserving optimisations to an already
installed OAR package (``.../dist-packages/oar/``):

1. ``kao/slot.py`` - ``SlotSet.traverse_with_width`` uses a persistent
   two-pointer (same yielded pairs, same order, O(H)).
2. ``kao/quotas.py`` - the rule tree built by ``init_rule_tree`` is cached by
   rules content, and a new ``pre_check_slots_quotas`` helper rejects a window
   that is *already* over the applicable quota limit.
3. ``kao/scheduling.py`` - the quota pre-check runs before the resource
   intersection/search, so windows that would be rejected anyway are skipped
   entirely (this also removes the per-window "Quotas limitation reached" log
   flood for blocked jobs).

The pre-check is conservative and result-preserving: it is only used when the
job counter is already at or above its limit (adding a job increments it by
exactly one) or a resource/resource-time counter is strictly above its limit,
so it never rejects a window ``check_slots_quotas`` would have accepted.

The script never imports OAR, auto-detects the installed ``oar`` package (or
takes ``--oar-dir``), verifies every anchor is present and unique before
writing, is idempotent, keeps timestamped backups, dry-runs by default, writes
atomically, restores all files if ``py_compile`` fails on any of them, and
supports ``--revert``.

Usage
-----
    python3 contrib/patch_oar_quota_scan.py                  # dry-run
    sudo python3 contrib/patch_oar_quota_scan.py --apply
    sudo python3 contrib/patch_oar_quota_scan.py --revert
    python3 contrib/patch_oar_quota_scan.py --dry-run --oar-dir /tmp/oar/oar
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


def L(*lines):
    """Join ``lines`` with newlines and append a trailing newline."""
    return "\n".join(lines) + "\n"


# --- kao/slot.py: traverse_with_width two-pointer --------------------------
SLOT_OLD = L(
    "    def traverse_with_width(",
    "        self, width, start_id=0, end_id=0",
    "    ) -> Generator[Tuple[Slot, Slot], None, None]:",
    "        # Width of zero does not exist",
    "        assert width > 0",
    "",
    "        for start_slot in self.traverse_id(start=start_id, end=end_id):",
    "            begin_time = start_slot.b",
    "            for end_slot in self.traverse_id(start=start_slot.id, end=end_id):",
    "                size = end_slot.e - begin_time",
    "                if size + 1 >= width:",
    "                    yield (start_slot, end_slot)",
    "",
    "                    # If we found a long enough interval from this starting point we don't need to continue",
    "                    # We can skip to the next start",
    "                    break",
)

SLOT_NEW = L(
    "    def traverse_with_width(",
    "        self, width, start_id=0, end_id=0",
    "    ) -> Generator[Tuple[Slot, Slot], None, None]:",
    "        # Width of zero does not exist",
    "        assert width > 0",
    "",
    "        # Same validity check as traverse_id: an unknown id means an empty traversal",
    "        if (start_id != 0 and start_id not in self.slots) or (",
    "            end_id != 0 and end_id not in self.slots",
    "        ):",
    "            return",
    "",
    "        # Persistent two-pointer: begin_time increases with start_slot, so the",
    "        # first end slot satisfying the width never moves backward.",
    "        end_slot = None",
    "        previous_start_id = None",
    "",
    "        for start_slot in self.traverse_id(start=start_id, end=end_id):",
    "            begin_time = start_slot.b",
    "",
    "            if end_slot is None:",
    "                end_slot = start_slot",
    "            elif end_slot.id == previous_start_id:",
    "                # The end pointer is exactly the previous start, i.e. one slot",
    "                # behind the new start; bring it up to the new start.",
    "                end_slot = start_slot",
    "",
    "            # Advance the end pointer until the width is reached, without ever",
    "            # going past end_id nor the end of the linked list (same terminal",
    "            # condition as traverse_id).",
    "            while (",
    "                end_slot.e - begin_time + 1 < width",
    "                and end_slot.next != 0",
    "                and end_slot.id != end_id",
    "            ):",
    "                end_slot = self.slots[end_slot.next]",
    "",
    "            if end_slot.e - begin_time + 1 >= width:",
    "                yield (start_slot, end_slot)",
    "            else:",
    "                # The width cannot be reached before the traversal end, and",
    "                # later starts begin even later: no more pair can be yielded.",
    "                break",
    "",
    "            previous_start_id = start_slot.id",
)

# --- kao/quotas.py: rule-tree cache ----------------------------------------
Q1_OLD = L(
    "    # Job types are apart, so they can extends the quotas globally ?",
    '    job_types: List[str] = ["*"]',
    "",
    "    @classmethod",
)

Q1_NEW = L(
    "    # Job types are apart, so they can extends the quotas globally ?",
    '    job_types: List[str] = ["*"]',
    "",
    "    # Content-keyed cache of built rule trees.  Quotas.check_slots_quotas builds",
    "    # a fresh Quotas object for every distinct quotas_rules_id on every window;",
    "    # without a cache each construction walks the whole rules dict in",
    "    # init_rule_tree().  The rules attached to a slot (and to a temporal period)",
    "    # are immutable within a scheduling round, so the resulting tree can safely",
    "    # be shared.  The number of distinct rule sets is tiny (the default one plus",
    "    # one per temporal period), so this dict stays small.  The key is a hashable",
    "    # signature of the rules content (see _rules_signature), which makes the",
    "    # cache insensitive to reset_quotas test fixtures swapping default_rules.",
    "    _rule_tree_cache: dict = {}",
    "",
    "    @classmethod",
)

Q2A_OLD = L(
    "        # self.show_counters('combine after')",
    "",
    "    def init_rule_tree(self):",
)

Q2A_NEW = L(
    "        # self.show_counters('combine after')",
    "",
    "    @staticmethod",
    "    def _rules_signature(rules):",
    '        """Return a hashable signature of a rules dict for the tree cache."""',
    "        return frozenset((k, tuple(v)) for k, v in rules.items())",
    "",
    "    @classmethod",
    "    def reset_rule_tree_cache(cls):",
    '        """Drop the shared rule tree cache (useful for tests)."""',
    "        cls._rule_tree_cache.clear()",
    "",
    "    def init_rule_tree(self):",
)

Q2B_OLD = L(
    "        # user:         '/'     '*'       '*'",
    "",
    "        self.rule_tree: dict[str, dict[str, dict[str, (int, int, float)]]] = dict()",
)

Q2B_NEW = L(
    "        # user:         '/'     '*'       '*'",
    "",
    "        signature = self._rules_signature(self.rules)",
    "        cached = Quotas._rule_tree_cache.get(signature)",
    "        if cached is not None:",
    "            self.rule_tree = cached",
    "            return self.rule_tree",
    "",
    "        self.rule_tree: dict[str, dict[str, dict[str, (int, int, float)]]] = dict()",
)

Q2C_OLD = L(
    "            self.rule_tree[queue][project][job_type][user] = rule",
    "",
    "        return self.rule_tree",
)

Q2C_NEW = L(
    "            self.rule_tree[queue][project][job_type][user] = rule",
    "",
    "        Quotas._rule_tree_cache[signature] = self.rule_tree",
    "",
    "        return self.rule_tree",
)

Q3_OLD = L(
    "        # return last one that should be a success anyway",
    "        return res",
    "",
    "    def set_rules(self, rules_id):",
)

Q3_NEW = L(
    "        # return last one that should be a success anyway",
    "        return res",
    "",
    "    @staticmethod",
    "    def pre_check_slots_quotas(",
    "        slots,",
    "        sid_left: int,",
    "        sid_right: int,",
    "        job,",
    "    ):",
    '        """Conservative quota early-out used before the resource search.',
    "",
    "        Returns a ``(False, msg, rule, value)`` tuple (same shape as",
    "        :py:meth:`check`) when the candidate window is *already* over the",
    "        applicable quota limit **before** adding ``job``.  In that case any",
    "        allocation would be rejected by :py:meth:`check_slots_quotas` anyway, so",
    "        the caller can skip the expensive resource intersection and hierarchy",
    "        search.  Returns ``None`` when the window is not already over.",
    "",
    "        ``Quotas.combine`` takes the maximum over the window slots for the",
    "        resource and job counts, and the sum for the resource-time, so a single",
    "        slot exceeding a limit implies the combined window exceeds it: testing",
    "        each slot independently is conservative (never rejects a window that",
    "        would have been accepted).",
    '        """',
    '        if not Quotas.enabled or getattr(job, "no_quotas", False):',
    "            return None",
    "",
    "        # find_applicable_rule only depends on the rule set attached to the",
    "        # slot, which is immutable within a scheduling round; cache it per rule",
    "        # set to avoid re-walking the rule tree for every slot of the window.",
    "        rule_cache = {}",
    "        sid = sid_left",
    "        while True:",
    "            quotas = slots[sid].quotas",
    "            cache_key = id(quotas.rules)",
    "            entry = rule_cache.get(cache_key)",
    "            if entry is None:",
    "                entry = quotas.find_applicable_rule(job)",
    "                rule_cache[cache_key] = entry",
    "            rule, complete_key, rl_quotas = entry",
    "            if rule and complete_key in quotas.counters:",
    "                count = quotas.counters[complete_key]",
    "                rl_nb_resources, rl_nb_jobs, rl_resources_time = rule",
    "                # The job update increments the job counter by exactly one, so a",
    "                # window already *at* the job limit will be over it after the",
    "                # update (``>=`` is the tight, safe test here).  The resource",
    "                # and resource-time counters are only guaranteed to grow by at",
    "                # least one in normal cases, so keep the strict ``<`` for them",
    "                # to stay conservative (no false early-out).",
    "                if (rl_nb_resources > -1) and (rl_nb_resources < count[0]):",
    "                    return (",
    "                        False,",
    '                        "nb resources quotas failed",',
    "                        rl_quotas,",
    "                        rl_nb_resources,",
    "                    )",
    "                if (rl_nb_jobs > -1) and (rl_nb_jobs <= count[1]):",
    '                    return (False, "nb jobs quotas failed", rl_quotas, rl_nb_jobs)',
    "                if (rl_resources_time > -1) and (rl_resources_time < count[2]):",
    "                    return (",
    "                        False,",
    '                        "resources hours quotas failed",',
    "                        rl_quotas,",
    "                        rl_resources_time,",
    "                    )",
    "            if sid == sid_right:",
    "                break",
    "            sid = slots[sid].next",
    "        return None",
    "",
    "    def set_rules(self, rules_id):",
)

# --- kao/scheduling.py: quota pre-check before the resource search ---------
SCHED_OLD = L(
    "                slots_set.temporal_quotas_split_slot(",
    "                    slot_end, quotas_rules_id, remaining_duration",
    "                )",
    "",
    "        if job.ts or (job.ph == ALLOW):",
    "            itvs_avail = intersec_ts_ph_itvs_slots(slots, sid_left, sid_right, job)",
)

SCHED_NEW = L(
    "                slots_set.temporal_quotas_split_slot(",
    "                    slot_end, quotas_rules_id, remaining_duration",
    "                )",
    "",
    "        # Conservative quota early-out: if this window already exceeds the",
    "        # applicable quota limit before adding the job, any allocation would be",
    "        # rejected by check_slots_quotas() below.  Skip the expensive resource",
    "        # intersection and hierarchy search for such windows.",
    "        if Quotas.enabled and not job.no_quotas:",
    "            quotas_failure = Quotas.pre_check_slots_quotas(",
    "                slots, sid_left, sid_right, job",
    "            )",
    "            if quotas_failure is not None:",
    "                if quota_precheck_failure is None:",
    "                    # Remember the first quota reason to emit a single summary",
    "                    # log if the job ends up unscheduled (instead of one line",
    "                    # per scanned window).",
    "                    quota_precheck_failure = quotas_failure",
    "                if sid_left_cache == -1:",
    '                    # Keep the same "resource frontier" hint the regular path',
    "                    # would store; starting the next job at or before the first",
    "                    # resource-feasible window is result-preserving.",
    "                    sid_left_cache = sid_left",
    "                continue",
    "",
    "        if job.ts or (job.ph == ALLOW):",
    "            itvs_avail = intersec_ts_ph_itvs_slots(slots, sid_left, sid_right, job)",
)

# The pre-check needs a place to remember its first failure and a single
# summary log once the job is known to be unscheduled.
SCHED_INIT_OLD = """    sid_right = sid_left
    sid_left_cache = -1
    for slot_begin, slot_end in slots_set.traverse_with_width(
"""

SCHED_INIT_NEW = """    sid_right = sid_left
    sid_left_cache = -1
    # First quota pre-check failure seen while scanning; used to emit a single
    # summary log if the job ends up unscheduled.
    quota_precheck_failure = None
    for slot_begin, slot_end in slots_set.traverse_with_width(
"""

SCHED_END_OLD = """    if len(itvs) == 0:
        logger.info(
            "can't schedule job with id: {}, walltime not satisfied: {}".format(
                job.id, walltime
            )
        )
        return (ProcSet(), -1, -1)
"""

SCHED_END_NEW = """    if len(itvs) == 0:
        if quota_precheck_failure is not None:
            (_quotas_ok, quotas_msg, rule, value) = quota_precheck_failure
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
"""


class FileSpec:
    def __init__(self, relpath, marker, edits):
        self.relpath = relpath
        self.marker = marker
        self.edits = edits  # list of (label, old, new)


FILES = [
    FileSpec(
        "kao/slot.py",
        "previous_start_id",
        [("traverse_with_width: O(H) two-pointer", SLOT_OLD, SLOT_NEW)],
    ),
    FileSpec(
        "kao/quotas.py",
        "_rule_tree_cache",
        [
            ("add rule-tree cache field", Q1_OLD, Q1_NEW),
            ("add signature/reset helpers", Q2A_OLD, Q2A_NEW),
            ("init_rule_tree: use the cache", Q2B_OLD, Q2B_NEW),
            ("init_rule_tree: store the tree", Q2C_OLD, Q2C_NEW),
            ("add pre_check_slots_quotas", Q3_OLD, Q3_NEW),
        ],
    ),
    FileSpec(
        "kao/scheduling.py",
        "pre_check_slots_quotas",
        [
            ("remember the first quota failure", SCHED_INIT_OLD, SCHED_INIT_NEW),
            ("quota pre-check before the resource search", SCHED_OLD, SCHED_NEW),
            ("single quota summary log per job", SCHED_END_OLD, SCHED_END_NEW),
        ],
    ),
]

DEFAULT_GLOBS = [
    "/usr/lib/python3*/dist-packages/oar",
    "/usr/local/lib/python3*/dist-packages/oar",
]

BACKUP_TAG = "oar-quota-scan"


class PatchError(Exception):
    """Raised when the patch cannot be applied safely."""


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


def find_package_dir(explicit):
    """Return the installed ``oar`` package directory to patch.

    The returned path is symlink-resolved so writing to it replaces the real
    file and preserves the Debian ``python3.X -> python3`` symlink layout.
    """
    if explicit:
        path = os.path.realpath(os.path.abspath(explicit))
        if not os.path.isfile(os.path.join(path, "kao", "scheduling.py")):
            raise PatchError(
                "{} does not look like the OAR package dir "
                "(no kao/scheduling.py)".format(path)
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
    """Apply all edits, asserting each anchor is unique."""
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
    # chown first: on Linux, chown(2) clears setuid/setgid, so the mode
    # (including those bits) must be restored afterwards.
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
    fd, tmp = tempfile.mkstemp(prefix=".oar-quota-", suffix=".tmp", dir=directory)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(content)
        _preserve_metadata(path, tmp)
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
    pyc_cache = tempfile.mkdtemp(prefix=".oar-quota-pyc-")
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


def read_all(pkg_dir):
    contents = {}
    states = {}
    for spec in FILES:
        path = os.path.join(pkg_dir, spec.relpath)
        if not os.path.isfile(path):
            raise PatchError("file not found: {}".format(path))
        content = read_text(path)
        contents[spec.relpath] = content
        states[spec.relpath] = spec.marker in content
    return contents, states


def do_apply(pkg_dir, dry_run):
    contents, states = read_all(pkg_dir)

    n_done = sum(1 for v in states.values() if v)
    if n_done == len(FILES):
        print("Already patched: all target files contain their marker. Nothing to do.")
        return 0
    if n_done != 0:
        eprint("ABORT: inconsistent state, some files are patched and others not:")
        for spec in FILES:
            eprint(
                "  {}: {}".format(
                    spec.relpath, "patched" if states[spec.relpath] else "NOT patched"
                )
            )
        eprint("Revert first (--revert), then re-apply.")
        return 2

    # Verify every anchor before writing anything.
    problems = []
    for spec in FILES:
        content = contents[spec.relpath]
        for label, old, _ in spec.edits:
            count = content.count(old)
            if count == 0:
                problems.append(
                    (spec.relpath, label, "anchor not found (different OAR version?)")
                )
            elif count > 1:
                problems.append(
                    (
                        spec.relpath,
                        label,
                        "anchor found {} times (ambiguous)".format(count),
                    )
                )
    if problems:
        eprint("ABORT: refuse to edit; expected anchors are missing/changed:")
        for relpath, label, reason in problems:
            eprint("  - {}: {}: {}".format(relpath, label, reason))
        eprint("")
        eprint(
            "The installed files do not match the version this patch was written for."
        )
        return 2

    new_contents = {}
    for spec in FILES:
        try:
            new_contents[spec.relpath] = apply_edits(
                contents[spec.relpath], spec.edits, spec.relpath
            )
        except PatchError as exc:
            eprint("ABORT: {}".format(exc))
            return 2

    if dry_run:
        for spec in FILES:
            diff = unified_diff(
                contents[spec.relpath],
                new_contents[spec.relpath],
                os.path.join(pkg_dir, spec.relpath),
            )
            if diff:
                print("=== {} ===".format(spec.relpath))
                print(diff)
        print(
            "Dry run: nothing written. Re-run with --apply to patch (backups will be created)."
        )
        return 0

    backups = {}
    written = []
    for spec in FILES:
        path = os.path.join(pkg_dir, spec.relpath)
        try:
            backups[spec.relpath] = make_backup(path)
            atomic_write(path, new_contents[spec.relpath])
            written.append(spec.relpath)
        except (OSError, PatchError) as exc:
            eprint("ABORT: could not write {}: {}".format(path, exc))
            eprint("(are you root? /usr is usually not writable by a normal user)")
            for rel in written:
                try:
                    atomic_write(os.path.join(pkg_dir, rel), contents[rel])
                except OSError:
                    pass
            eprint("Rolled back {} already patched file(s).".format(len(written)))
            return 2

    failures = []
    for spec in FILES:
        path = os.path.join(pkg_dir, spec.relpath)
        ok, out = validate_compile(path)
        if not ok:
            failures.append((spec.relpath, out))

    if failures:
        eprint("Validation with py_compile FAILED, restoring every file...")
        for spec in FILES:
            atomic_write(os.path.join(pkg_dir, spec.relpath), contents[spec.relpath])
        for relpath, out in failures:
            eprint("  {}:\n{}".format(relpath, out))
        eprint("Restored original files from backups.")
        return 1

    for spec in FILES:
        print(
            "Patched: {} (backup {})".format(
                os.path.join(pkg_dir, spec.relpath), backups[spec.relpath]
            )
        )
    print("py_compile: OK for all files")
    print("")
    print("Next steps (run as root):")
    print("  systemctl restart oar-server     # restarts OAR scheduler/almighty")
    print("")
    print("To revert this patch:")
    print("  sudo {} --revert".format(os.path.basename(sys.argv[0])))
    return 0


def do_revert(pkg_dir):
    rc = 0
    for spec in FILES:
        path = os.path.join(pkg_dir, spec.relpath)
        backups = list_backups(path)
        if not backups:
            eprint("WARNING: no backup found for {}; skipping".format(path))
            rc = 2
            continue
        latest = backups[-1]
        try:
            content = read_text(latest)
        except PatchError as exc:
            eprint("ABORT: {}".format(exc))
            return 2
        atomic_write(path, content)
        ok, out = validate_compile(path)
        print(
            "Reverted {} from {} (py_compile: {})".format(
                path, latest, "OK" if ok else "FAILED"
            )
        )
        if not ok:
            eprint(out)
            rc = 1
    return rc


def parse_args(argv):
    parser = argparse.ArgumentParser(
        description=(
            "Patch an installed Debian OAR to speed up the quota-aware "
            "scheduling scan. Dry-run by default; never imports OAR."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--oar-dir",
        metavar="DIR",
        help=(
            "the installed 'oar' package directory. If omitted, auto-detect "
            "under /usr/lib/python3*/dist-packages/oar and "
            "/usr/local/lib/python3*/dist-packages/oar."
        ),
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--dry-run",
        action="store_true",
        help="show the unified diffs and change nothing (default)",
    )
    mode.add_argument(
        "--apply",
        action="store_true",
        help="write the patches atomically, then validate with py_compile",
    )
    mode.add_argument(
        "--revert",
        action="store_true",
        help="restore the most recent backup of each file",
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
