#!/usr/bin/env python
# coding: utf-8

"""
Process that launches and manages bipbip and leon processes.

..
    OAREXEC_REGEXP   'OAREXEC_(\d+)_(\d+)_(\d+|N)_(\d+)'
    OARRUNJOB_REGEXP  'OARRUNJOB_(\d+)'
    LEONEXTERMINATE_REGEXP   'LEONEXTERMINATE_(\d+)'

Commands:
    - OAREXEC
    - OARRUNJOB
    - LEONEXTERMINATE


TODO: jsonify ?

Example:

.. code-block:: JSON

    {
        "job_id": 5,
        "cmd": "LEONEXTERMINATE"
        "args": [5]
    }

"""  # noqa: W605

import os
import socket
from typing import Any

import zmq

import oar.lib.tools as tools
from oar.lib.globals import get_logger, init_config, init_oar


def bipbip_leon_executor(
    command: dict[str, Any], leon_command: str, bipbip_command: str, logger
):
    job_id = command["job_id"]

    if command["cmd"] == "LEONEXTERMINATE":
        cmd_arg = [leon_command, str(job_id)]
    else:
        cmd_arg = [bipbip_command, str(job_id)] + command["args"]

    logger.debug("Launching: " + str(cmd_arg))

    # TODO returncode,
    tools.call(cmd_arg)


class BipbipCommander(object):
    def __init__(self, config=None):
        if not config:
            config = init_config()

        # Set undefined config value to default one
        DEFAULT_CONFIG = {
            "SERVER_HOSTNAME": "localhost",
            "APPENDICE_SERVER_PORT": "6670",
            "BIPBIP_COMMANDER_SERVER": "localhost",
            "BIPBIP_COMMANDER_PORT": "6671",
            "MAX_CONCURRENT_JOBS_STARTING_OR_TERMINATING": "25",
            "DETACH_JOB_FROM_SERVER": "1",
            "LOG_FILE": "/var/log/oar.log",
        }

        config.setdefault_config(DEFAULT_CONFIG)

        # Max number of concurrent bipbip processes
        self.Max_bipbip_processes = int(
            config["MAX_CONCURRENT_JOBS_STARTING_OR_TERMINATING"]
        )
        self.Detach_oarexec = config["DETACH_JOB_FROM_SERVER"]

        # Maximum duration a a bipbip process (after that time the process is killed)
        self.Max_bipbip_process_duration = 30 * 60

        self.logger = get_logger("oar.modules.bipbip_commander", forward_stderr=True)
        self.logger.info("Start Bipbip Commander")

        if "OARDIR" in os.environ:
            binpath = os.environ["OARDIR"]
        else:
            binpath = "/usr/local/lib/oar/"
            os.environ["OARDIR"] = binpath
            self.logger.warning(
                "OARDIR env variable must be defined, "
                + binpath
                + " is used by default"
            )

        self.leon_command = os.path.join(binpath, "oar-leon")
        self.bipbip_command = os.path.join(binpath, "oar-bipbip")

        # Initialize zeromq context
        self.context = zmq.Context()

        # TODO signal Almighty
        # self.appendice = self.context.socket(zmq.PUSH) # to signal Almighty
        # self.appendice.connect('tcp://' + config['SERVER_HOSTNAME'] + ':' + config['APPENDICE_SERVER_PORT'])

        # IP addr is required when bind function is used on zmq socket
        ip_addr_bipbip_commander = socket.gethostbyname(
            config["BIPBIP_COMMANDER_SERVER"]
        )
        self.notification = self.context.socket(
            zmq.PULL
        )  # receive zmq formatted OAREXEC / OARRUNJOB / LEONEXTERMINATE
        self.notification.bind(
            "tcp://" + ip_addr_bipbip_commander + ":" + config["BIPBIP_COMMANDER_PORT"]
        )

        self.bipbip_leon_commands_to_run = []
        self.bipbip_leon_commands_to_requeue = []
        self.bipbip_leon_executors = {}

    def set_notification_timeout(self, timeout):
        """Set timeout for zmq notification socket"""
        self.notification.RCVTIMEO = timeout

    def run(self, loop=True):
        # TODO: add a shutdown procedure
        while True:
            # add_timeout if bipbip_leon_commands_to_run is not empty
            try:
                command = self.notification.recv_json()
                self.logger.debug(
                    "bipbip commander received notification:" + str(command)
                )
                self.bipbip_leon_commands_to_run.append(command)

            except zmq.error.Again as e:
                self.logger.debug("Timeout on notification:" + str(e))
                if self.bipbip_leon_commands_to_run == []:
                    self.logger.error(
                        "Not queued commands with timeout actived is abnormal"
                    )

            except zmq.ZMQError as e:
                self.logger.error(
                    "Something is wrong with notification reception" + str(e)
                )
                exit(1)

            while (
                len(self.bipbip_leon_commands_to_run) > 0
                and len(self.bipbip_leon_executors.keys()) <= self.Max_bipbip_processes
            ):
                command = self.bipbip_leon_commands_to_run.pop(0)
                job_id = command["job_id"]
                flag_exec = True

                if job_id in self.bipbip_leon_executors:
                    if not self.bipbip_leon_executors[job_id].is_alive():
                        del self.bipbip_leon_executors[job_id]
                    else:
                        flag_exec = False
                        # requeue command
                        self.logger.debug(
                            "A process is already running for the job "
                            + str(job_id)
                            + ". We requeue: "
                            + str(command)
                        )
                        self.bipbip_leon_commands_to_requeue.append(command)

                if flag_exec:
                    # exec
                    executor = tools.Process(
                        target=bipbip_leon_executor,
                        args=(
                            command,
                            self.leon_command,
                            self.bipbip_command,
                            self.logger,
                        ),
                        kwargs=command,
                    )
                    executor.start()
                    self.bipbip_leon_executors[job_id] = executor

            # append commands to requeue
            self.bipbip_leon_commands_to_run += self.bipbip_leon_commands_to_requeue
            self.bipbip_leon_commands_to_requeue = []

            # Remove finished executors:
            for job_id in list(self.bipbip_leon_executors.keys()):
                if not self.bipbip_leon_executors[job_id].is_alive():
                    self.logger.debug(
                        "Executor Exitcode: "
                        + str(self.bipbip_leon_executors[job_id].exitcode)
                    )
                    del self.bipbip_leon_executors[job_id]

            if self.bipbip_leon_commands_to_run == []:
                self.set_notification_timeout(-1)
            else:
                self.set_notification_timeout(500)

            if not loop:
                break


def main():  # pragma: no cover
    bipbip_commander = BipbipCommander()
    bipbip_commander.run()


if __name__ == "__main__":  # pragma: no cover
    main()
