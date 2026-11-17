Evolving jobs
=============

An evolving job changes its resources while it runs: it grows (more cores or
nodes) or shrinks (fewer), without restarting the application.

.. note::

   Evolving jobs require the cgroup v2 / systemd job resource manager
   (``JOB_RESOURCE_MANAGER_FILE = job_resource_manager_systemd.pl``, the default).

supersed: replace a running job
-------------------------------

A job of type ``supersed=<N>`` replaces your running job *N*:

1. the new job is scheduled on the resources of *N*, plus or minus the difference
   asked for, and starts immediately;
2. on the nodes kept from *N*, the processes of *N* are moved into the new job
   (same PID, no restart); they now run on the cores of the new job;
3. *N* is terminated (``Terminated``, event ``SUPERSEDED``).

What happens on each node:

=================  ======================================================================
node               processes of *N*
=================  ======================================================================
kept               moved into the new job
added              none: start new processes there yourself (``oarsh``, ``$OAR_NODEFILE``)
removed            killed, with the end of *N*
=================  ======================================================================

Rules:

- *N* must be one of your jobs and must be ``Running``;
- processes are moved, not copied: a process never changes node;
- **the new job lives as long as its command**, like any OAR job. When its command
  ends, the processes taken over from *N* are killed with it. The command of the
  new job must therefore wait for the application (see `Keeping the new job alive`_).

Submission
~~~~~~~~~~

::

	jdoe@frontend:~$ oarsub -l core=10,walltime=1:0:0 ./my_app
	OAR_JOB_ID=4930

	# later: grow from 10 to 20 cores
	jdoe@frontend:~$ oarsub -t supersed=4930 -l core=20,walltime=0:50:0 ./wait_and_extend.sh
	OAR_JOB_ID=4931

	jdoe@frontend:~$ oarstat -s -j 4930 -j 4931
	4930: Terminated
	4931: Running
	jdoe@frontend:~$ oarstat -e -j 4930
	  ...  SUPERSEDED                   [4930] Job superseded by job 4931
	  ...  SWITCH_INTO_TERMINATE_STATE  [4930] Ask to change the job state

To shrink, ask for fewer resources (``-l core=5``): the processes running on the
cores or nodes left out are killed.

Checking where your processes are
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The processes taken over from *N* are in a systemd scope ``...-j<new job>-m<pid>.scope``::

	jdoe@frontend:~$ OAR_JOB_ID=4931 oarsh node1
	jdoe@node1:~$ cat /proc/$(pgrep -u $USER my_app)/cgroup
	0::/oar.slice/oar-u1000.slice/oar-u1000-j4931.slice/oar-u1000-j4931-m2386.scope

.. _oarsub-supersed:

Keeping the new job alive
~~~~~~~~~~~~~~~~~~~~~~~~~

``wait_migrated.sh``, the command of the new job: it waits, on each node, until the
processes taken over from *N* are finished::

	#!/bin/bash
	for h in $(sort -u $OAR_NODEFILE); do
	    oarsh $h 'd=$(dirname /sys/fs/cgroup$(sed -n "s/^0:://p" /proc/self/cgroup))
	              while cat $d/*-m*.scope/cgroup.procs 2>/dev/null | grep -q .; do sleep 5; done' &
	done
	wait

``oarsub-supersed <N> <resources>`` submits it with the walltime left to *N*, so
that the new job ends, at the latest, when *N* would have ended::

	#!/bin/bash
	N=$1; RES=$2
	END=$(oarstat -f -J -j $N | python3 -c 'import json,sys; j=next(iter(json.load(sys.stdin).values())); print(j["start_time"]+j["walltime"])')
	LEFT=$(( END - $(date +%s) ))
	oarsub -t supersed=$N -l "$RES,walltime=$((LEFT/3600)):$((LEFT%3600/60)):$((LEFT%60))" ~/wait_migrated.sh

::

	jdoe@frontend:~$ oarsub-supersed 4930 core=20
	OAR_JOB_ID=4931
