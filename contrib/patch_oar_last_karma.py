#!/usr/bin/env python3
# coding: utf-8
"""Patch an installed Debian OAR to batch ``last_karma`` writes.

OAR stores the fairsharing *karma* of a job in the ``jobs.last_karma``
column.  Before the optimisation, ``job_message()`` called
``set_job_last_karma()`` once per scheduled job, issuing one ``UPDATE`` and
one ``commit`` per job.  The optimisation removed that per-job write and
batches every karma update into a single ``UPDATE ... CASE`` statement inside
``save_assigns()``, alongside the already-batched job message update.

This script applies that optimisation to an already installed OAR package
(``.../dist-packages/oar/lib/job_handling.py``).  It is meant for real-world
testing of the change before it reaches a release, so it is deliberately
conservative:

* it never imports OAR (no dependency on the installed/running code);
* it auto-detects the installed file, or takes ``--file``;
* it checks that every expected anchor is present and aborts otherwise;
* it is idempotent (presence of ``last_karma_updates`` means "already done");
* it keeps a timestamped backup next to the file;
* dry-run is the default and only prints a unified diff;
* ``--apply`` writes atomically and rolls back if ``python3 -m py_compile``
  fails;
* ``--revert`` restores the most recent backup.

The script has no third-party dependency and does not use the network.

Usage
-----
    # inspect what would change (default, no write)
    python3 contrib/patch_oar_last_karma.py

    # apply (needs write permission, usually root for /usr)
    sudo python3 contrib/patch_oar_last_karma.py --apply

    # restore the latest backup
    sudo python3 contrib/patch_oar_last_karma.py --revert

    # target an explicit file (also used for local testing)
    python3 contrib/patch_oar_last_karma.py --dry-run --file /tmp/.../job_handling.py
"""

from __future__ import annotations

import argparse
import difflib
import glob
import os
import re
import shutil
import stat
import subprocess
import sys
import tempfile
import time

# ---------------------------------------------------------------------------
# Patch definition
#
# ``job_handling.py`` contains three identical ``logger.info("save assignements")``
# calls: one in ``job_message`` and two in the ``save_assigns*`` functions.  Every
# anchor below is therefore chosen long enough to be unique in the file.  Each
# anchor is verified with ``str.count(...) == 1`` before anything is written.
# ---------------------------------------------------------------------------

MARKER = "last_karma_updates"

# 1. job_message(): drop the per-job last_karma write, keep the Karma message.
ANCHOR_KARMA_CALL = (
    '    if hasattr(job, "karma"):\n'
    '        message += " " + "(Karma={})".format(job.karma)\n'
    "        set_job_last_karma(session, job.id, job.karma)\n"
)

REPL_KARMA_CALL = (
    '    if hasattr(job, "karma"):\n'
    "        # The last_karma write is batched by save_assigns (alongside the\n"
    "        # message bulk UPDATE) to avoid one UPDATE + commit per job.\n"
    '        message += " " + "(Karma={})".format(job.karma)\n'
)

# 2a. save_assigns(): declare the new accumulator.
ANCHOR_INIT = (
    "        message_updates = {}\n"
    "\n"
    "        for j in jobs.values() if isinstance(jobs, dict) else jobs:\n"
)

REPL_INIT = (
    "        message_updates = {}\n"
    "        last_karma_updates = {}\n"
    "\n"
    "        for j in jobs.values() if isinstance(jobs, dict) else jobs:\n"
)

# 2b. save_assigns(): collect the karma values while looping over jobs.
ANCHOR_COLLECT = (
    "                message_updates[j.id] = msg\n" "\n" "        if message_updates:\n"
)

REPL_COLLECT = (
    "                message_updates[j.id] = msg\n"
    '                if hasattr(j, "karma"):\n'
    "                    last_karma_updates[j.id] = j.karma\n"
    "\n"
    "        if message_updates:\n"
)

# 2c. save_assigns(): bulk-update last_karma before the commit.  The trailing
#     GanttJobsPrediction insert makes this anchor unique to save_assigns().
ANCHOR_BULK = (
    '        logger.info("save assignements")\n'
    "        session.execute(GanttJobsPrediction.__table__.insert(), mld_id_start_time_s)\n"
)

REPL_BULK = (
    "        if last_karma_updates:\n"
    '            logger.info("save job last karma")\n'
    "            session.query(Job).filter(Job.id.in_(last_karma_updates)).update(\n"
    "                {\n"
    "                    Job.last_karma: case(\n"
    "                        last_karma_updates,\n"
    "                        value=Job.id,\n"
    "                    )\n"
    "                },\n"
    "                synchronize_session=False,\n"
    "            )\n"
    "\n"
    '        logger.info("save assignements")\n'
    "        session.execute(GanttJobsPrediction.__table__.insert(), mld_id_start_time_s)\n"
)

# 3. set_job_last_karma(): document that save_assigns no longer calls it.
ANCHOR_DOCSTRING = (
    "def set_job_last_karma(session, job_id, last_karma):\n"
    '    """Update the last_karma value of a job into database\n'
    "    parameter : database ref, job id, karma value\n"
    '    """\n'
)

REPL_DOCSTRING = (
    "def set_job_last_karma(session, job_id, last_karma):\n"
    '    """Update the last_karma value of a job into database\n'
    "    parameter : database ref, job id, karma value\n"
    "\n"
    "    Kept for API compatibility. save_assigns no longer calls this per job:\n"
    "    last_karma is batched into a single bulk UPDATE instead.\n"
    '    """\n'
)

EDITS = [
    (
        "job_message(): remove per-job set_job_last_karma call",
        ANCHOR_KARMA_CALL,
        REPL_KARMA_CALL,
    ),
    ("save_assigns(): declare last_karma_updates", ANCHOR_INIT, REPL_INIT),
    ("save_assigns(): collect per-job karma", ANCHOR_COLLECT, REPL_COLLECT),
    ("save_assigns(): bulk last_karma UPDATE", ANCHOR_BULK, REPL_BULK),
    ("set_job_last_karma(): update docstring", ANCHOR_DOCSTRING, REPL_DOCSTRING),
]

CASE_IMPORT_RE = re.compile(
    r"^\s*(from\s+sqlalchemy\.sql\s+import\s+case|import\s+case)\b", re.M
)

# Auto-detection patterns, checked in order.
DEFAULT_GLOBS = [
    "/usr/lib/python3*/dist-packages/oar/lib/job_handling.py",
    "/usr/local/lib/python3*/dist-packages/oar/lib/job_handling.py",
]

BACKUP_TAG = "oar-lastkarma"


class PatchError(Exception):
    """Raised when the patch cannot be applied safely."""


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


def find_target(explicit):
    """Return the installed file to patch, honouring ``--file`` first.

    The returned path is the real (symlink-resolved) path, so writing to it
    replaces the actual file and preserves the original symlink layout.  On
    Debian ``python3.X/dist-packages`` is usually a symlink to
    ``python3/dist-packages``; replacing the symlink instead of its target
    would be both surprising and wrong.
    """
    if explicit:
        path = os.path.realpath(os.path.abspath(explicit))
        if not os.path.isfile(path):
            raise PatchError("file not found: {}".format(path))
        return path

    # ``python3*`` matches both ``python3`` and ``python3.X``, and on Debian
    # the latter's ``dist-packages`` is typically a symlink to the former's.
    # Deduplicate on the real path so the same file is not reported twice
    # (which used to abort with "several candidates").
    matches = []
    for pattern in DEFAULT_GLOBS:
        matches.extend(glob.glob(pattern))
    realpaths = set()
    for match in matches:
        real = os.path.realpath(match)
        if os.path.isfile(real):
            realpaths.add(real)
    matches = sorted(realpaths)

    if not matches:
        raise PatchError(
            "no installed OAR job_handling.py found; looked for:\n  "
            + "\n  ".join(DEFAULT_GLOBS)
            + "\nUse --file to point at the file explicitly."
        )
    if len(matches) > 1:
        raise PatchError(
            "several candidate files found, use --file to choose one:\n  "
            + "\n  ".join(matches)
        )
    return matches[0]


def read_text(path):
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return fh.read()
    except UnicodeDecodeError as exc:
        # UnicodeDecodeError is a ValueError, not an OSError, so it must be
        # translated explicitly to get a clean abort instead of a traceback.
        raise PatchError("cannot decode {} as UTF-8: {}".format(path, exc))


def already_patched(content):
    return MARKER in content


def verify_anchors(content):
    """Return a list of (label, reason) for problems preventing the patch."""
    problems = []
    for label, anchor, _ in EDITS:
        count = content.count(anchor)
        if count == 0:
            problems.append(
                (label, "anchor not found (file differs from expected version)")
            )
        elif count > 1:
            problems.append((label, "anchor found {} times (ambiguous)".format(count)))
    if not CASE_IMPORT_RE.search(content):
        problems.append(
            (
                "sqlalchemy case import",
                "'case' is not imported; refusing to generate code that needs it",
            )
        )
    return problems


def context_for(anchor, content):
    """Human-readable location hint: search for the anchor's most specific line."""
    lines = [ln for ln in anchor.splitlines() if ln.strip()]
    if not lines:
        return "empty anchor"
    # The longest line is the most distinctive and least likely to appear
    # several times in the file (e.g. the GanttJobsPrediction insert vs. a
    # bare logger.info call).
    needle = max(lines, key=len).strip()
    for lineno, line in enumerate(content.splitlines(), start=1):
        if needle and needle in line:
            return "located near line {}: {!r}".format(lineno, line)
    return "no line resembling {!r}".format(needle)


def patched_content(content):
    """Apply all edits, asserting each anchor is unique."""
    new = content
    for label, anchor, replacement in EDITS:
        count = new.count(anchor)
        if count != 1:
            raise PatchError(
                "anchor for '{}' matched {} times, expected exactly 1".format(
                    label, count
                )
            )
        new = new.replace(anchor, replacement, 1)
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
    # chown first: on Linux, chown(2) clears the setuid/setgid bits of a regular
    # file, so the mode (including those bits) must be restored afterwards.
    try:
        os.chown(tmp, st.st_uid, st.st_gid)
    except (PermissionError, OSError) as exc:
        # Not fatal (e.g. running as non-root); the mode is still preserved.
        eprint(
            "note: could not preserve owner/group of {} ({}); "
            "mode preserved only".format(src, exc)
        )
    os.chmod(tmp, stat.S_IMODE(st.st_mode))


def atomic_write(path, content):
    directory = os.path.dirname(os.path.abspath(path)) or "."
    fd, tmp = tempfile.mkstemp(prefix=".oar-lastkarma-", suffix=".tmp", dir=directory)
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


def validate_compile(path):
    """Run ``python3 -m py_compile`` in an isolated cache directory."""
    pyc_cache = tempfile.mkdtemp(prefix=".oar-lastkarma-pyc-")
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


def list_backups(path):
    pattern = "{}.{}.????????-??????.bak".format(path, BACKUP_TAG)
    return sorted(glob.glob(pattern))


def do_revert(path, content):
    backups = list_backups(path)
    if not backups:
        raise PatchError(
            "no backup found for {} (looked for {}.{}.????????-??????.bak)".format(
                path, path, BACKUP_TAG
            )
        )
    latest = backups[-1]
    backup_content = read_text(latest)
    print("Reverting {} from {}".format(path, latest))
    diff = unified_diff(content, backup_content, path)
    if diff:
        print(diff)
    atomic_write(path, backup_content)
    ok, out = validate_compile(path)
    if not ok:
        eprint("WARNING: reverted file failed py_compile:")
        eprint(out)
    else:
        print("py_compile: OK")
    print("Reverted {} from {}".format(path, latest))
    print("Remaining backups: {}".format("\n  ".join(backups) if backups else "none"))


def do_apply(path, content, dry_run):
    if already_patched(content):
        print("Already patched: {} contains '{}'. Nothing to do.".format(path, MARKER))
        return 0

    problems = verify_anchors(content)
    if problems:
        eprint(
            "ABORT: refuse to edit {}; expected anchors are missing/changed:".format(
                path
            )
        )
        for label, reason in problems:
            eprint("  - {}: {}".format(label, reason))
        eprint("")
        eprint("Anchor context:")
        for label, anchor, _ in EDITS:
            eprint("  {}: {}".format(label, context_for(anchor, content)))
        eprint("")
        eprint(
            "The installed file does not match the version this patch was written for."
        )
        return 2

    try:
        new_content = patched_content(content)
    except PatchError as exc:
        eprint("ABORT: {}".format(exc))
        return 2

    diff = unified_diff(content, new_content, path)
    if not diff:
        print("No change required.")
        return 0

    if dry_run:
        print("Dry run: would patch {}".format(path))
        print(diff)
        print(
            "Nothing written. Re-run with --apply to patch (a backup will be created)."
        )
        return 0

    backup = None
    try:
        backup = make_backup(path)
        print("Backup created: {}".format(backup))
        atomic_write(path, new_content)
    except OSError as exc:
        eprint("ABORT: could not write {}: {}".format(path, exc))
        eprint("(are you root? /usr is usually not writable by a normal user)")
        if backup is not None:
            eprint("Backup left in place: {}".format(backup))
        return 2

    ok, out = validate_compile(path)
    if not ok:
        eprint(
            "Validation with py_compile FAILED, restoring backup {}...".format(backup)
        )
        atomic_write(path, content)
        eprint("Restored original {} from backup.".format(path))
        eprint(out)
        return 1

    print("Patched: {}".format(path))
    print("py_compile: OK")
    print("")
    print("Next steps (run as root):")
    print("  systemctl restart oar-server     # restarts OAR scheduler/almighty")
    print("")
    print("To revert this patch:")
    print("  sudo {} --revert --file {}".format(os.path.basename(sys.argv[0]), path))
    print("  # or restore the backup directly:")
    print("  sudo cp -a {} {}".format(backup, path))
    return 0


def parse_args(argv):
    parser = argparse.ArgumentParser(
        description=(
            "Patch an installed Debian OAR to batch last_karma updates. "
            "Dry-run by default; never imports OAR."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--file",
        metavar="PATH",
        help=(
            "job_handling.py to patch. If omitted, auto-detect under "
            "/usr/lib/python3*/dist-packages/oar/lib/ and "
            "/usr/local/lib/python3*/dist-packages/oar/lib/."
        ),
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--dry-run",
        action="store_true",
        help="show the unified diff and change nothing (default)",
    )
    mode.add_argument(
        "--apply",
        action="store_true",
        help="write the patch atomically, then validate with py_compile",
    )
    mode.add_argument(
        "--revert",
        action="store_true",
        help="restore the most recent timestamped backup",
    )
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(sys.argv[1:] if argv is None else argv)
    dry_run = not (args.apply or args.revert)

    try:
        path = find_target(args.file)
    except PatchError as exc:
        eprint("ABORT: {}".format(exc))
        return 2

    print("Target: {}".format(path))
    try:
        content = read_text(path)
    except PatchError as exc:
        eprint("ABORT: {}".format(exc))
        return 2
    except OSError as exc:
        eprint("ABORT: cannot read {}: {}".format(path, exc))
        return 2

    if args.revert:
        try:
            do_revert(path, content)
        except PatchError as exc:
            eprint("ABORT: {}".format(exc))
            return 2
        return 0

    try:
        return do_apply(path, content, dry_run)
    except PatchError as exc:
        eprint("ABORT: {}".format(exc))
        return 2


if __name__ == "__main__":
    sys.exit(main())
