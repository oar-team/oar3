#!/usr/bin/env python3
# coding: utf-8
"""Patch an installed Debian OAR to cache the hierarchy parent/child relation.

``find_resource_n_h`` (used for every time window of every job) rescanned the
whole next hierarchy level for each block: at the bottom level it computed
``cpu & core`` for all cores of the level for each cpu, and at intermediate
levels ``sub.issubset(level)`` for all children.  On a large platform with a
4-level hierarchy (``cpumodel,network_address,cpu,core``) this is
O(blocks * next_level) per window and dominated the scheduling time.

This patch memoizes the parent -> children relation once per scheduling round
(the hierarchy is static) and looks it up per block, and replaces an
intersection test by ``ProcSet.isdisjoint``.

Result-preserving; measured 86 s -> 31 s single-core on an 8192-resource
4-level workload (~2.7x).  Same safety rules as the other patch scripts:
dry-run by default, anchor checks, timestamped backup, atomic write, py_compile
validation with rollback, ``--revert``.

Usage
-----
    python3 contrib/patch_oar_hierarchy_cache.py                 # dry-run
    sudo python3 contrib/patch_oar_hierarchy_cache.py --apply
    sudo python3 contrib/patch_oar_hierarchy_cache.py --revert
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

TARGET_RELPATH = "lib/hierarchy.py"
MARKER = "_children_map"

KEEP_OLD = (
    "    while i < lr:\n"
    "        x = itvss_ref[i]\n"
    "        if len(x & itvs) != 0:\n"
    "            r_itvss.append(x)\n"
    "        i += 1\n"
    "    return r_itvss"
)
KEEP_NEW = (
    "    while i < lr:\n"
    "        x = itvss_ref[i]\n"
    "        if not x.isdisjoint(itvs):\n"
    "            r_itvss.append(x)\n"
    "        i += 1\n"
    "    return r_itvss"
)

CHILDREN_MAP = (
    "# Memoized parent -> children relation per hierarchy level.  The hierarchy is\n"
    "# static within a scheduling round, so computing it once avoids rescanning the\n"
    "# whole next level for every block and every time window (the dominant cost on\n"
    "# large platforms).\n"
    "_CHILDREN_CACHE = {}\n"
    "\n"
    "\n"
    "def _children_map(parent_list, child_list):\n"
    '    """Return ``{id(parent_block): [child blocks]}`` for two hierarchy levels.\n'
    "\n"
    "    ``parent_list``/``child_list`` are kept alive by the cache so their ``id``\n"
    "    cannot be reused while cached; the identity check guards against a new list\n"
    "    with a recycled id.\n"
    '    """\n'
    "    cached = _CHILDREN_CACHE.get(id(parent_list))\n"
    "    if cached is not None and cached[0] is parent_list and cached[1] is child_list:\n"
    "        return cached[2]\n"
    "    mapping = {\n"
    "        id(blk): [sub for sub in child_list if sub.issubset(blk)] for blk in parent_list\n"
    "    }\n"
    "    if len(_CHILDREN_CACHE) > 16:\n"
    "        _CHILDREN_CACHE.clear()\n"
    "    _CHILDREN_CACHE[id(parent_list)] = (parent_list, child_list, mapping)\n"
    "    return mapping\n"
    "\n"
    "\n"
    "# Same cache for the bottom level, but keeping the exact original semantics:\n"
    "# the intersection ``parent & child`` for every child that overlaps the parent\n"
    "# (not only the children fully contained in it).\n"
    "_BOTTOM_CACHE = {}\n"
    "\n"
    "\n"
    "def _bottom_map(parent_list, child_list):\n"
    '    """Return ``{id(parent_block): [parent_block & child]}`` for the bottom level."""\n'
    "    cached = _BOTTOM_CACHE.get(id(parent_list))\n"
    "    if cached is not None and cached[0] is parent_list and cached[1] is child_list:\n"
    "        return cached[2]\n"
    "    mapping = {}\n"
    "    for blk in parent_list:\n"
    "        intersected = []\n"
    "        for x in child_list:\n"
    "            y = blk & x\n"
    "            if len(y) != 0:\n"
    "                intersected.append(y)\n"
    "        mapping[id(blk)] = intersected\n"
    "    if len(_BOTTOM_CACHE) > 16:\n"
    "        _BOTTOM_CACHE.clear()\n"
    "    _BOTTOM_CACHE[id(parent_list)] = (parent_list, child_list, mapping)\n"
    "    return mapping\n"
    "\n"
    "\n"
)

DEF_N_H = "def find_resource_n_h(itvs, hy, rqts, top, h, h_bottom):"

BOTTOM_OLD = (
    "                avail_sub_bks = [\n"
    "                    (avail_bks[i] & x) for x in hy[h + 1] if len(avail_bks[i] & x) != 0\n"
    "                ]"
)
BOTTOM_NEW = (
    "                # cores of this cpu only (not the whole core level), with the\n"
    "                # exact intersection semantics of the original code\n"
    "                avail_sub_bks = _bottom_map(hy[h], hy[h + 1])[id(avail_bks[i])]"
)

CHILDREN_OLD = (
    "                children = [sub for sub in hy[h + 1] if sub.issubset(level)]"
)
CHILDREN_NEW = "                children = _children_map(hy[h], hy[h + 1])[id(level)]"

EDITS = [
    ("keep_no_empty_scat_bks: use isdisjoint", KEEP_OLD, KEEP_NEW),
    ("add _children_map helper", DEF_N_H, CHILDREN_MAP + DEF_N_H),
    ("bottom level: only this cpu's cores", BOTTOM_OLD, BOTTOM_NEW),
    ("intermediate level: use the cached children", CHILDREN_OLD, CHILDREN_NEW),
]

DEFAULT_GLOBS = [
    "/usr/lib/python3*/dist-packages/oar",
    "/usr/local/lib/python3*/dist-packages/oar",
]

BACKUP_TAG = "oar-hierarchy-cache"


class PatchError(Exception):
    """Raised when the patch cannot be applied safely."""


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


def find_target(explicit, pkg):
    if pkg:
        path = os.path.realpath(os.path.abspath(os.path.join(pkg, TARGET_RELPATH)))
        if not os.path.isfile(path):
            raise PatchError("file not found: {}".format(path))
        return path
    matches = []
    for pattern in DEFAULT_GLOBS:
        matches.extend(glob.glob(os.path.join(pattern, TARGET_RELPATH)))
    realpaths = sorted({os.path.realpath(m) for m in matches if os.path.isfile(m)})
    if not realpaths:
        raise PatchError(
            "no installed OAR hierarchy.py found; looked for:\n  "
            + "\n  ".join(os.path.join(p, TARGET_RELPATH) for p in DEFAULT_GLOBS)
        )
    if len(realpaths) > 1:
        raise PatchError(
            "several candidate files found, use --oar-dir:\n  " + "\n  ".join(realpaths)
        )
    return realpaths[0]


def read_text(path):
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return fh.read()
    except UnicodeDecodeError as exc:
        raise PatchError("cannot decode {} as UTF-8: {}".format(path, exc))


def apply_edits(content):
    new = content
    for label, old, replacement in EDITS:
        count = new.count(old)
        if count != 1:
            raise PatchError(
                "anchor for '{}' matched {} times, expected exactly 1".format(
                    label, count
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
    fd, tmp = tempfile.mkstemp(prefix=".oar-hier-", suffix=".tmp", dir=directory)
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
            "backup {} already exists (two runs within the same second?)".format(backup)
        )
    shutil.copy2(path, backup)
    return backup


def list_backups(path):
    return sorted(glob.glob("{}.{}.????????-??????.bak".format(path, BACKUP_TAG)))


def validate_compile(path):
    pyc_cache = tempfile.mkdtemp(prefix=".oar-hier-pyc-")
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


def do_apply(path, content, dry_run):
    if MARKER in content:
        print("Already patched: {} contains '{}'. Nothing to do.".format(path, MARKER))
        return 0
    problems = [
        (label, content.count(old))
        for label, old, _ in EDITS
        if content.count(old) != 1
    ]
    if problems:
        eprint("ABORT: refuse to edit {}; unexpected anchors:".format(path))
        for label, count in problems:
            eprint("  - {}: matched {} times (expected 1)".format(label, count))
        return 2
    try:
        new_content = apply_edits(content)
    except PatchError as exc:
        eprint("ABORT: {}".format(exc))
        return 2
    diff = unified_diff(content, new_content, path)
    if dry_run:
        print("Dry run: would patch {}".format(path))
        print(diff)
        print("Nothing written. Re-run with --apply (backup will be created).")
        return 0
    backup = None
    try:
        backup = make_backup(path)
        atomic_write(path, new_content)
    except (OSError, PatchError) as exc:
        eprint("ABORT: could not write {}: {}".format(path, exc))
        eprint("(are you root? /usr is usually not writable by a normal user)")
        return 2
    ok, out = validate_compile(path)
    if not ok:
        eprint("py_compile FAILED, restoring {}".format(backup))
        atomic_write(path, content)
        eprint(out)
        return 1
    print("Patched: {} (backup {})".format(path, backup))
    print("py_compile: OK")
    print("")
    print("Next steps (run as root): systemctl restart oar-server")
    print("To revert: sudo {} --revert".format(os.path.basename(sys.argv[0])))
    return 0


def do_revert(path, content):
    backups = list_backups(path)
    if not backups:
        raise PatchError("no backup found for {}".format(path))
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
    return 0


def main(argv=None):
    parser = argparse.ArgumentParser(
        description=(
            "Patch an installed Debian OAR to cache the hierarchy parent/child "
            "relation. Dry-run by default; never imports OAR."
        )
    )
    parser.add_argument("--file", metavar="PATH", help="explicit hierarchy.py to patch")
    parser.add_argument(
        "--oar-dir", metavar="DIR", help="installed 'oar' package directory"
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", help="show diff (default)")
    mode.add_argument("--apply", action="store_true", help="patch and validate")
    mode.add_argument("--revert", action="store_true", help="restore latest backup")
    args = parser.parse_args(sys.argv[1:] if argv is None else argv)
    dry_run = not (args.apply or args.revert)
    try:
        path = (
            os.path.realpath(os.path.abspath(args.file))
            if args.file
            else find_target(args.file, args.oar_dir)
        )
        if args.file and not os.path.isfile(path):
            raise PatchError("file not found: {}".format(path))
    except PatchError as exc:
        eprint("ABORT: {}".format(exc))
        return 2
    print("Target: {}".format(path))
    try:
        content = read_text(path)
    except PatchError as exc:
        eprint("ABORT: {}".format(exc))
        return 2
    if args.revert:
        try:
            return do_revert(path, content)
        except PatchError as exc:
            eprint("ABORT: {}".format(exc))
            return 2
    try:
        return do_apply(path, content, dry_run)
    except PatchError as exc:
        eprint("ABORT: {}".format(exc))
        return 2


if __name__ == "__main__":
    sys.exit(main())
