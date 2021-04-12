# coding: utf-8
"""Proxy to exploit BatSim compatible scheduler in OAR

 1) Instrastrcuture bootstrap: open batsim session w/ batsim_sched_proxy CLI (option -s A)

 2) Almighty loop:

 2.1) New Metascheduler round (launch new processus at time)

 2.1.1)
    - Create BatsimSchedProxy object: zmq connexion w/ Batsim compatible scheuler
    - Get active ids of active jobs (from DataStore (DS) )
    - ...

 2.1.2) Metascheduler: others steps
    - Advance Reservations
    - ...

 2.1.3) Metascheduler call BatsimSchedProxy.ask_schedule
    - Check_pstate_changes
    - Determine new submitted jobs
    - Determine new completed jobs
    - Send Bastim message w/ submessage:
         - JOB_SUBMITTED
         - JOB_COMPLETED
         - RESOURCE_STATE_CHANGED
         - Nop
    - Wait for Bastim compatible scheduler's answer
    - Handle answer
     - EXECUTE_JOB: Save jobs' allocations in DS (ready to by executed)
     - SET_RESOURCE_STATE: Save pstate to apply in resources_pstate_2_change_str[] 
                          (retrieve by Metasched through retrieve_pstate_changes_to_apply
     - Nop

 2.1.3) Metascheduler manage pstate/energy
    - Call retrieve_pstate_changes_to_apply
    - Trigger pstate changes through Hulot at node granularity

 2.1.4) Metascheduler others steps
    - Resuming/Suspending jobs, 
    - marking jobs to launch
    - ...

 2.1.5) Metascheduler round completed  (processus exits)

 3)  Instrastrcuture shut down: closed batsim session w/ batsim_sched_proxy CLI (option -s Z)

"""

import copy
import json

import click
import zmq
from batsim.batsim import DataStorage
from procset import ProcSet

from oar.kao.scheduling import set_slots_with_prev_scheduled_jobs
from oar.lib import config, get_logger
from oar.lib.job_handling import JobPseudo, get_jobs_ids_in_multiple_states
from oar.lib.node import get_nodes_with_state
from oar.lib.tools import get_date

# Set undefined config value to default one
DEFAULT_CONFIG = {
    "BATSCHED_ENDPOINT": "tcp://localhost:6679",
    "REDIS_HOSTNAME": "localhost",
    "REDIS_PORT": "6379",
    "SCHEDULER_JOB_SECURITY_TIME": "60",
    "WLOAD_BATSIM": "oar3",
    "DS_PREFIX": "oar3",
}

config.setdefault_config(DEFAULT_CONFIG)

logger = get_logger("oar.kao.batsim_sched_proxy", forward_stderr=True)


class BatsimSchedProxy(object):
    def __init__(
        self, plt, scheduled_jobs, all_slot_sets, job_security_time, queue, now
    ):
        self.plt = plt
        self.scheduled_jobs = scheduled_jobs
        self.all_slot_sets = all_slot_sets
        self.job_security_time = int(job_security_time)
        self.queue = queue
        self.now = now
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REQ)
        self.socket.connect(config["BATSCHED_ENDPOINT"])
        self.data_store = DataStorage(config["DS_PREFIX"])
        self.wload = config["WLOAD_BATSIM"]

        self.data_store.redis.set("active_job_ids", json.dumps([]))
        self.resource_set = (
            plt.resource_set()
        )  # Could by obtained directly from metasched

        # Pstate/Energy support
        self.pstate2batsim = {"halt": 0, "wakeup": 2}
        self.resources_pstate_2_change_str = {}  # supported pstate halt/wakup
        self.check_resources_2_change_str = {"halt": "", "wakeup": ""}
        for pstate in ["halt", "wakeup"]:
            self.data_store.redis.set("check_resources_2_" + pstate, "")

    def check_pstate_changes(self):
        """check and notify if previously pstate changes are finished

        *WARNING:* it's assume that there is only one resource by node
        """
        resources_pstate_changed_str = {"halt": "", "wakeup": ""}

        # nodes_2_check = {}
        # retrieve resources changes to check
        for pstate in ["halt", "wakeup"]:
            nodes_2_check = []
            node2roid = {}
            b_check_resources_2_change = self.data_store.redis.get(
                "check_resources_2_" + pstate
            )
            if b_check_resources_2_change:
                resources_2_check = b_check_resources_2_change.decode("utf-8")
                roid_ids = list(resources_2_check)

                for i in roid_ids:
                    node = self.resource_set.roid_2_network_address(i)
                    nodes_2_check.append(node)
                    node2roid[node] = i

            if nodes_2_check:
                # check
                resources_changed = []
                resources_unchanged = []

                for r in get_nodes_with_state(nodes_2_check):
                    if ((pstate == "halt") and (r.state == "Absent")) or (
                        (pstate == "wakeup") and (r.state == "Alive")
                    ):
                        resources_changed.append(node2roid[r.network_address])
                    else:
                        resources_unchanged.append(node2roid[r.network_address])

                resources_changed_str = ",".join(
                    str(i) for i in sorted(resources_changed)
                )
                resources_unchanged_str = ",".join(
                    str(i) for i in sorted(resources_unchanged)
                )

                # check_resources_2_change will be used and save to DS
                # by retrieve_pstate_changes_to_apply after scheduling round
                self.check_resources_2_change_str[pstate] = resources_unchanged_str

                resources_pstate_changed_str[pstate] = resources_changed_str

        # Return the changed resources to be annonced to Batsim compatible scheduler
        return resources_pstate_changed_str

    def retrieve_pstate_changes_to_apply(self):
        # call be metascheduler afer scheduling round
        # *WARNING:* it's assume that there is only one resource per node
        nodes_2_change = {}
        for pstate in ["halt", "wakeup"]:
            if (
                pstate in self.resources_pstate_2_change_str
            ) and self.resources_pstate_2_change_str[pstate]:
                resources_2_change_str = self.resources_pstate_2_change_str[pstate]
                roid_ids = list(resources_2_change_str)
                nodes_2_change[pstate] = [
                    self.resource_set.roid_2_network_address(i) for i in roid_ids
                ]

                # Add resources_pstate to check
                if self.check_resources_2_change_str[pstate]:
                    resources_2_change_str = (
                        self.check_resources_2_change_str[pstate]
                        + ","
                        + resources_2_change_str
                    )

                self.data_store.redis.set(
                    "check_resources_2_" + pstate, resources_2_change_str
                )

        return nodes_2_change

    def ask_schedule(self):
        logger.debug("Start ask_schedule")
        next_active_jids = []
        finished_jids = []
        cached_active_jids = []
        # Retrieve cached list of active id jobs from Redis
        bstr_cached_active_jids = self.data_store.redis.get("active_job_ids")
        # if bstr_cached_active_jids:
        # cached_active_jids = json.loads(bstr_cached_active_jids.decode("utf-8"))
        cached_active_jids = json.loads(bstr_cached_active_jids.decode("utf-8"))

        # Retrieve waiting and running jobs from
        waiting_jobs, waiting_jids, _ = self.plt.get_waiting_jobs(self.queue.name)
        active_jids = get_jobs_ids_in_multiple_states(
            ["Running", "toLaunch", "Launching", "Finishing", "Suspended", "Resuming"]
        )

        # Determine new submitted and finished jobs
        for cached_jid in cached_active_jids:
            if cached_jid in active_jids:
                next_active_jids.append(cached_jid)
            elif cached_jid in waiting_jids:
                next_active_jids.append(cached_jid)
                waiting_jids.remove(cached_jid)
            else:
                finished_jids.append(cached_jid)

        cached_active_jids += waiting_jids

        # Save active_job_ids in redis
        self.data_store.redis.set("active_job_ids", json.dumps(cached_active_jids))

        now = float(self.now)
        now_event = now

        batmsg = {"now": now}

        events = []

        # Check resource/node pstate changes and prepare annoncement if any
        resources_pstate_changed_str = self.check_pstate_changes()
        for pstate in ["halt", "wakeup"]:
            if (
                pstate in resources_pstate_changed_str
            ) and resources_pstate_changed_str[pstate]:
                event = {"timestamp": now_event}
                event["type"] = "RESOURCE_STATE_CHANGED"
                event["data"] = {
                    "resource": resources_pstate_changed_str[pstate],
                    "state": self.pstate2batsim[pstate],
                }
                events.append(event.copy())
                now_event += 0.0001

        if len(finished_jids) > 0:
            for jid in finished_jids:
                event = {"timestamp": now_event}
                event["type"] = "JOB_COMPLETED"
                event["data"] = {
                    "job_id": "{}!{}".format(self.wload, jid),
                    "status": "SUCCESS",
                }  # TODO
                events.append(event.copy())
                now_event += 0.0001

        if len(waiting_jids) > 0:
            self.plt.get_data_jobs(
                waiting_jobs,
                waiting_jids,
                self.resource_set,
                int(config["SCHEDULER_JOB_SECURITY_TIME"]),
            )

            for waiting_jid in waiting_jids:
                # BE CAREFUL: Moldable job is not supported and only the first
                # requested resource number is considered
                # mld_res_rqts=[(1, 60, [([("resource_id", 2)], [])])]
                # walltime: 60
                # res: 2
                mld_res_rqts = waiting_jobs[waiting_jid].mld_res_rqts
                subtime = (waiting_jobs[waiting_jid].submission_time,)
                walltime = mld_res_rqts[0][1]  # walltime
                # take the first requested resource number (2 from above example)
                res = mld_res_rqts[0][2][0][0][0][1]

                self.data_store.set_job(waiting_jid, subtime, walltime, res)

                event = {"timestamp": now_event}
                event["type"] = "JOB_SUBMITTED"
                event["data"] = {"job_id": "{}!{}".format(self.wload, waiting_jid)}
                now_event += 0.0001

        batmsg["events"] = copy.deepcopy(events)

        # send req
        logger.debug("Message sent to Batsim compatible scheduler:\n" + str(batmsg))
        self.socket.send_string(json.dumps(batmsg))

        # recv rep
        logger.debug("Waiting response from scheduler")
        batmsg_rep = json.loads(self.socket.recv().decode("utf-8"))

        logger.debug("Message from scheduler:\n" + str(batmsg_rep))

        events = batmsg_rep["events"]

        resources_2_halt = ""
        resources_2_wakeup = ""

        # sched_time = float(data[1])
        logger.debug("From scheduler")

        for event in events:
            ev_type = event["type"]
            if "data" in event:
                ev_data = event["data"]
            if ev_type == "EXECUTE_JOB":
                jid = int(ev_data["job_id"].split("!")[1])
                res_set = ProcSet.from_str(ev_data["alloc"], "-", ",")
                json_dict = json.loads(self.data_store.get(jid).decode("utf-8"))
                walltime = json_dict["walltime"]

                jobs = [
                    JobPseudo(
                        id=jid,
                        moldable_id=jid,
                        start_time=self.now,
                        walltime=walltime,
                        res_set=res_set,
                    )
                ]

                set_slots_with_prev_scheduled_jobs(
                    self.all_slot_sets, jobs, self.job_security_time
                )
                self.plt.save_assigns(jobs, self.resource_set)

            elif ev_type == "SET_RESOURCE_STATE":
                resources = event["data"]["resources"]
                if event["data"]["state"] == "0":
                    resources_2_halt += resources + ","
                else:
                    resources_2_wakeup += resources + ","

            # TODO elif data[1] == 'R': #job rejection
            #  pass

            else:
                raise Exception("Unsupported submessage type: " + ev_type)

        if resources_2_halt:
            self.resources_pstate_2_change_str["halt"] = resources_2_halt[:1]

        if resources_2_wakeup:
            self.resources_pstate_2_change_str["wakeup"] = resources_2_wakeup[:1]


@click.command()
@click.option(
    "-e",
    "--event",
    default="SIMULATION_BEGINS",
    help="send Batsim event to scheduler. Default event is 'SIMULATION_BEGINS'.",
)
@click.option(
    "-d",
    "--data",
    default="{}",
    help='Data sent with events (ex \'{"nb_resources":4,"config":{}}\'',
)
def cli(event, data):
    """Command to send start/stop sequence to Batsim compatible scheduler"""

    # open zmq socket (REQ/REP)
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.connect(config["BATSCHED_ENDPOINT"])

    # send command
    now = str(get_date())

    msg = {
        "now": now,
        "events": [{"timestamp": now, "type": event, "data": json.loads(data)}],
    }

    logger.info("Batsim_sched_proxy CLI send: " + str(msg))
    socket.send_string(json.dumps(msg))

    msg = json.loads(socket.recv().decode("utf-8"))
    logger.info("Batsim_sched_proxy CLI recv: " + str(msg))
