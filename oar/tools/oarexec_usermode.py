#!/usr/bin/env python
import os
import pickle
import signal
import sys
from datetime import datetime

import oar.lib.tools as tools

oarexec_pid_file_name = "pid_of_oarexec_for_jobId_"
command_pid = None


def stop_signal_handler(signum, frame):
    print(f"[{now()}][oarexec] stop signal handler of {signum}")
    # TODO


signal.signal(signal.SIGUSR1, stop_signal_handler)


def checkpoint_signal_handler(signum, frame):
    print(f"[{now()}][oarexec] checkpoint signal handler of {signum}")
    # TODO


signal.signal(signal.SIGUSR2, checkpoint_signal_handler)


def now():
    return datetime.now().strftime("%H:%M:%S")


def quit_oarexec(job_data, exit_value, exit_script_code=""):
    # TODO add retry backoff....
    addr = job_data["almighty_hostname"]
    port = job_data["almighty_port"]
    job_id = job_data["job_id"]
    challenge = job_data["challenge"]
    try:
        tools.notify_tcp_socket(
            addr, port, f"OAREXEC_{job_id}_{exit_value}_{exit_script_code}_{challenge}"
        )
    except Exception as ex:
        print(
            f"[{now()}][oarexec] cannot notify Almighty: server down or network error ({ex})"
        )

    print(f"[{now()}][oarexec] notified Almighty with exit value {exit_value} and exit")


def main():
    with open(sys.argv[1], "rb") as f:
        job_data = pickle.load(f)

    job_id = job_data["job_id"]
    job_user = job_data["job_user"]
    launching_directory = job_data["launching_directory"]

    print(f"[oarexec] user: {job_user}, launch directory: {launching_directory}")

    tmp_directory = job_data["tmp_directory"]
    if not os.path.exists(tmp_directory):
        os.makedirs(tmp_directory)

    # if job_data["debug_mode"] > 0:
    #     sys.stdout = open(f"/tmp/oar_{job_id}.log", "w")
    #     sys.stderr = open(f"/tmp/oar_{job_id}.log", "w")
    # else:
    #     sys.stdout = open("/dev/null", "w")
    #     sys.stderr = open("/dev/null", "w")

    # write Pid do catch and forward Signal (for job delete and job signal)
    with open(f"{tmp_directory}/{oarexec_pid_file_name}{job_id}", "w") as f:
        f.write(f"{os.getpid()}")

    resources = job_data["resources"]
    # create node set file

    with open(f"{tmp_directory}/{job_id}", "w") as node_file:
        tmp_res = []
        tmp_already_there = {}
        for r in resources:
            if (r.get(job_data["node_file_db_fields"], "")) and (
                r["type"] == "default"
            ):
                distinct_value = r.get(
                    job_data["node_file_db_fields_distinct_values"], ""
                )
                if distinct_value and distinct_value not in tmp_already_there:
                    tmp_res.append(r[job_data["node_file_db_fields"]])
                    tmp_already_there[distinct_value] = 1

        for res in sorted(tmp_res):
            node_file.write(res + "\n")

    # create resource set file
    # TODO
    # with  open(f"{tmp_directory}/{job_id}_resources"", "w") as resource_file:

    # write resources file

    env = os.environ.copy()
    env["OAR_JOB_ID"] = str(job_id)
    # env["OAR_JOB_WALLTIME_SECONDS"] = job_data[""] 3600
    # env["OAR_ARRAY_INDEX"] = job_data[""] 1
    # env["OAR_O_WORKDIR"] = job_data[""] /home/orichard
    # env["OAR_WORKING_DIRECTORY"] = job_data[""] /home/orichard
    # env["OAR_JOBID"] = job_data[""] 2363862
    # env["OAR_STDERR"] = job_data[""] <interactive shell>
    # env["OAR_NODE_FILE"] = job_data[""] /var/lib/oar/2363862
    # env["OAR_RESOURCE_FILE"] = job_data[""] /var/lib/oar/2363862
    # env["OAR_STDOUT"] = job_data[""] <interactive shell>
    # env["OAR_ARRAY_ID"] = job_data[""] 2363862
    # env["OAR_RESOURCE_PROPERTIES_FILE"] = job_data[""] /var/lib/oar/2363862_resources
    # env["OAR_KEY"] = job_data[""] 1
    # env["OAR_JOB_ID"] = job_data[""] 2363862
    # env["OAR_FILE_NODES"] = job_data[""] /var/lib/oar/2363862
    # env["OAR_JOB_NAME"] = job_data[""]
    # env["OAR_CPUSET"] = job_data[""] /oar/orichard_2363862
    # env["OAR_JOB_WALLTIME"] = job_data[""] 1:0:0
    # env["OAR_USER"] = job_data[""] orichard
    # env["OAR_PROJECT_NAME"] = job_data[""] datamove
    # env["OAR_ARRAYID"] = job_data[""] 2363862
    # env["OAR_NODEFILE"] = job_data[""] /var/lib/oar/2363862
    # env["OAR_ARRAYINDEX"] = job_data[""] 1
    # env["OAR_WORKDIR"] = job_data[""] /home/orichard

    if job_data["mode"] == "PASSIVE":
        command = job_data["command"]
        stdout_fd = open(f"{launching_directory}/{job_data['stdout_file']}", "w")
        stderr_fd = open(f"{launching_directory}/{job_data['stderr_file']}", "w")

        print(f"[{now()}][oarexec] launch {command}")

        p = tools.Popen(
            command, stdout=stdout_fd, stderr=stderr_fd, shell=True, env=env
        )

        global command_pid
        command_pid = p.pid

        p.wait()
    else:
        # TODO interactive
        print("Sorry not yet implemented")

    print(job_data)

    # clean_all();
    # if ($Kill_myself == 1){
    #     quit_oarexec(3,$Job);
    # }elsif($Stop_signal == 1){
    #     quit_oarexec(34,$Job);
    # }elsif($Checkpoint_signal == 1){
    #     quit_oarexec(40,$Job);
    # }elsif($user_signal == 1){
    #     quit_oarexec(42,$Job);
    # }else{
    #     quit_oarexec(0+$Oarexec_exit_code,$Job);
    # }

    quit_oarexec(job_data, 0, 0)


if __name__ == "__main__":
    main()
