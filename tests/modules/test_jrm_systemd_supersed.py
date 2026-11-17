# coding: utf-8
"""Tests of the supersed process migration of the systemd job resource managers
(job_resource_manager_systemd.pl and job_resource_manager_systemd_nixos.pl).

The Perl sub ``migrate_superseded_job_processes`` is extracted from the job resource
manager and run in a fake environment: a fake cgroup tree, and fake ``oardodo`` and
``busctl`` commands which record the systemd calls instead of running them.
"""
import os
import re
import shutil
import stat
import subprocess
from pathlib import Path

import pytest

TOOLS = Path(__file__).resolve().parents[2] / "oar" / "tools"
JRMS = ["job_resource_manager_systemd.pl", "job_resource_manager_systemd_nixos.pl"]

SYSTEMD = "org.freedesktop.systemd1 /org/freedesktop/systemd1 org.freedesktop.systemd1.Manager"


def _perl_available():
    if shutil.which("perl") is None:
        return False
    return subprocess.run(["perl", "-MFile::Find", "-e", "1"]).returncode == 0


pytestmark = pytest.mark.skipif(
    not _perl_available(), reason="perl with File::Find is required"
)


def _extract_sub(name, src, jrm):
    # definition only (not the prototype declaration "sub name($);")
    m = re.search(r"^sub " + name + r"\([^)\n]*\) \{\n.*?^\}\n", src, re.S | re.M)
    assert m, f"sub {name} not found in {jrm}"
    return m.group(0)


def _write_executable(path, content):
    path.write_text(content)
    path.chmod(path.stat().st_mode | stat.S_IEXEC)


@pytest.fixture(params=JRMS)
def harness(request, tmp_path):
    """Build the fake environment and return a function running the migration."""
    user_path = tmp_path / "cg" / "oar.slice" / "oar-u1000.slice"
    old_slice = user_path / "oar-u1000-j41.slice"
    (old_slice / "oar-u1000-j41-p1.scope").mkdir(parents=True)
    (old_slice / "oar-u1000-j41-p2.scope").mkdir(parents=True)
    (old_slice / "cgroup.procs").write_text("")
    # head node of job 41: oarexec (100) + user processes (101, 102)
    (old_slice / "oar-u1000-j41-p1.scope" / "cgroup.procs").write_text(
        "100\n101\n102\n"
    )
    # an oarsh connection of job 41
    (old_slice / "oar-u1000-j41-p2.scope" / "cgroup.procs").write_text("200\n")
    pid_file = tmp_path / "pid_of_oarexec_for_jobId_41"
    pid_file.write_text("100\n")

    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    log = tmp_path / "busctl.log"
    _write_executable(bin_dir / "oardodo", '#!/bin/sh\nexec "$@"\n')
    _write_executable(
        bin_dir / "busctl",
        '#!/bin/sh\necho "$*" >> "$BUSCTL_LOG"\n'
        'case "$*" in *FreezeUnit*) [ "$FAIL_FREEZE" = 1 ] && exit 1;; esac\n'
        'case "$*" in *StartTransientUnit*) [ "$FAIL_MOVE" = 1 ] && exit 1;; esac\n'
        "exit 0\n",
    )

    jrm = TOOLS / request.param
    src = jrm.read_text()
    script = tmp_path / "harness.pl"
    script.write_text(
        "use strict; use warnings; use File::Find;\n"
        "my $Log_level = 3;\n"
        "my $Cpuset = { job_id => 42,\n"
        '  supersed_job_id => ($ENV{SUP} eq "none" ? undef : $ENV{SUP}),\n'
        f'  migrate_processes_from_oarexec_pid_file => "{pid_file}" }};\n'
        'my $Systemd_user_slice = "oar-u1000";\n'
        'my $Systemd_job_slice = "oar-u1000-j42";\n'
        f'my $Cgroup_user_path = "{user_path}";\n'
        'sub print_log($$) { my ($l, $s) = @_; print "LOG$l: $s\\n" if $l <= $Log_level }\n'
        'sub exit_myself($$) { my ($c, $s) = @_; print "EXIT$c: $s\\n"; exit($c) }\n'
        "sub migrate_superseded_job_processes();\n"
        + _extract_sub("system_with_log", src, jrm)
        + _extract_sub("migrate_superseded_job_processes", src, jrm)
        + 'migrate_superseded_job_processes();\nprint "DONE\\n";\n'
    )

    def run(supersed_job_id, fail_freeze=False, fail_move=False):
        if log.exists():
            log.unlink()
        env = dict(os.environ)
        env.update(
            {
                "PATH": f"{bin_dir}:{env['PATH']}",
                "BUSCTL_LOG": str(log),
                "SUP": str(supersed_job_id) if supersed_job_id is not None else "none",
                "FAIL_FREEZE": "1" if fail_freeze else "0",
                "FAIL_MOVE": "1" if fail_move else "0",
            }
        )
        proc = subprocess.run(
            ["perl", str(script)], env=env, capture_output=True, text=True
        )
        calls = log.read_text().splitlines() if log.exists() else []
        calls = [c.replace(SYSTEMD, "M") for c in calls if "ListJobs" not in c]
        return proc.returncode, proc.stdout, calls

    return run


def test_migrate_moves_processes_except_oarexec(harness):
    rc, out, calls = harness(41)
    assert rc == 0
    assert "DONE" in out
    assert calls[0] == "call -q M FreezeUnit s oar-u1000-j41.slice"
    move = calls[1]
    assert "StartTransientUnit" in move
    assert re.search(r"oar-u1000-j42-m\d+\.scope", move)
    assert "Slice s oar-u1000-j42.slice" in move
    # oarexec of job 41 (pid 100) is not moved, all the other processes are
    pids = re.search(r"PIDs au (\d+) ([\d ]+) Slice", move)
    assert pids.group(1) == "3"
    assert sorted(pids.group(2).split()) == ["101", "102", "200"]
    assert calls[2] == "call -q M ThawUnit s oar-u1000-j41.slice"
    assert len(calls) == 3


def test_migrate_nothing_on_node_without_superseded_job(harness):
    # node added by the new job: the superseded job 7 has no slice here
    rc, out, calls = harness(7)
    assert rc == 0
    assert "has no cgroup on this node" in out
    assert calls == []


def test_migrate_nothing_without_supersed(harness):
    rc, out, calls = harness(None)
    assert rc == 0
    assert calls == []


def test_migrate_without_freeze(harness):
    # systemd without FreezeUnit support: move anyway, do not thaw
    rc, out, calls = harness(41, fail_freeze=True)
    assert rc == 0
    assert "cannot freeze" in out
    assert any("StartTransientUnit" in c for c in calls)
    assert not any("ThawUnit" in c for c in calls)


def test_migrate_error_thaws_and_fails(harness):
    # the move fails: the superseded job is thawed and the init fails (exit 5)
    rc, out, calls = harness(41, fail_move=True)
    assert rc == 5
    assert "failed to move the processes of job 41" in out
    assert calls[-1] == "call -q M ThawUnit s oar-u1000-j41.slice"
