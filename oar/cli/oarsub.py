# -*- coding: utf-8 -*-
import os
import re
import signal
import socket
import sys

import click
from sqlalchemy.orm import scoped_session, sessionmaker

import oar.lib.tools as tools
from oar import VERSION
from oar.cli.oardel import oardel
from oar.lib.database import EngineConnector
from oar.lib.globals import init_oar
from oar.lib.job_handling import (
    get_current_moldable_job,
    get_job,
    get_job_cpuset_name,
    get_job_current_hostnames,
    get_job_types,
    resubmit_job,
)
from oar.lib.models import Job, Model
from oar.lib.submission import JobParameters, Submission, check_reservation, lstrip_none
from oar.lib.tools import get_oarexecuser_script_for_oarsub

from .utils import CommandReturns

click.disable_unicode_literals_warning = True

# Global variable needed for signal handler to trigger job deletion accordingly
job_id_lst = []


def init_tcp_server():
    """Intialize TCP server, to receive job's creation information"""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("0.0.0.0", 0))
    sock.listen(5)
    return sock


def qdel(signalnum, frame):  # pragma: no cover
    """Handle ^C in interactive submission."""
    if job_id_lst:
        if signalnum == signal.SIGINT:
            print("Caught Interrupt (^C), cancelling job(s)...")
        oardel(job_id_lst, None, None, None, None, None, None, None)
    exit(1)


signal.signal(signal.SIGINT, qdel)
signal.signal(signal.SIGHUP, qdel)
signal.signal(signal.SIGPIPE, qdel)


def connect_job(session, config, job_id, stop_oarexec, openssh_cmd, cmd_ret):
    """Connect to a job and give the shell of the user on the remote host."""
    xauth_path = (
        os.environ["OARXAUTHLOCATION"] if "OARXAUTHLOCATION" in os.environ else None
    )
    luser = os.environ["OARDO_USER"] if "OARDO_USER" in os.environ else None

    job = get_job(session, job_id)

    if ((luser == job.user) or (luser == "oar")) and (job.state == "Running"):
        types = get_job_types(session, job_id)
        # No operation job type
        if "noop" in types:
            cmd_ret.warning(" It is not possible to connect to a NOOP job.")
            cmd_ret.exit(17)

        hosts = get_job_current_hostnames(session, job_id)
        host_to_connect_via_ssh = hosts[0]

        # Deploy, cosystem and no host part
        if "cosystem" in types or not hosts:
            host_to_connect_via_ssh = config["COSYSTEM_HOSTNAME"]
        elif "deploy" in types:
            host_to_connect_via_ssh = config["DEPLOY_HOSTNAME"]

        # cpuset part
        cpuset_field = config["JOB_RESOURCE_MANAGER_PROPERTY_DB_FIELD"]
        cpuset_path = config["CPUSET_PATH"]

        if (
            cpuset_field
            and cpuset_path
            and ("cosystem" not in types)
            and ("deploy" not in types)
            and hosts
        ):
            os.environ["OAR_CPUSET"] = (
                cpuset_path + "/" + get_job_cpuset_name(session, job_id)
            )
        else:
            os.environ["OAR_CPUSET"] = ""

        moldable = get_current_moldable_job(session, job.assigned_moldable_job)
        job_user = job.user
        shell = tools.getpwnam(luser).pw_shell

        if (
            not xauth_path
            or not (os.path.isfile(xauth_path) and os.access(xauth_path, os.X_OK))
            or not (
                "DISPLAY" in os.environ
                and re.match(r"^[\w.-]*:\d+(?:\.\d+)?$", os.environ["DISPLAY"])
            )
        ):
            os.environ["DISPLAY"] = ""

        if ("DISPLAY" in os.environ) and os.environ["DISPLAY"]:
            print("Initialize X11 forwarding...")
            # first, get rid of remaining unused .Xautority.{pid} files...
            retcode = tools.call(
                "bash -c 'for f in $HOME/.Xauthority.*; do [ -e \"/proc/${f#$HOME/.Xauthority.}\" ] || rm -f $f; done'",
                shell=True,
            )
            if retcode:
                cmd_ret.error("Error on riding of unused Xautority.{pid} file")
                cmd_ret.exit(retcode)

            new_xauthority = os.environ["HOME"] + "/.Xauthority." + str(os.getpid())
            cmd = (
                "bash -c '[ -x \""
                + xauth_path
                + '" ] && OARDO_BECOME_USER='
                + luser
                + ' oardodo bash --noprofile --norc -c "'
                + xauth_path
                + ' extract - ${DISPLAY/#localhost:/:}" | XAUTHORITY='
                + new_xauthority
                + " "
                + xauth_path
                + " -q merge - 2>/dev/null'"
            )
            try:
                retcode = tools.call(cmd, shell=True)
                if retcode:
                    cmd_ret.error("Error on set new xauthority")
                    cmd_ret.exit(retcode)
            except OSError as e:
                print("Execution failed:", e, file=sys.stderr)

            os.environ["XAUTHORITY"] = new_xauthority

        node_file = config["OAREXEC_DIRECTORY"] + "/" + str(job_id)
        resource_file = node_file + "_resources"
        oarsub_pids = (
            config["OAREXEC_DIRECTORY"]
            + "/"
            + config["OARSUB_FILE_NAME_PREFIX"]
            + str(job_id)
        )

        script = get_oarexecuser_script_for_oarsub(
            config, job, moldable.walltime, node_file, shell, resource_file
        )

        cmd = openssh_cmd
        if ("OAR_CPUSET" in os.environ) and (os.environ["OAR_CPUSET"] != ""):
            cmd += " -oSendEnv=OAR_CPUSET "

        if ("DISPLAY" in os.environ) and os.environ["DISPLAY"]:
            cmd += " -X "
        else:
            cmd += " -x "

        cmd += "-t " + host_to_connect_via_ssh + " "

        if ("DISPLAY" in os.environ) and os.environ["DISPLAY"]:
            cmd += (
                "\"bash -c 'echo \$PPID >> "
                + oarsub_pids
                + " && ("
                + xauth_path
                + " -q extract - \${DISPLAY/#localhost:/:} | OARDO_BECOME_USER="
                + luser
                + " oardodo "
                + xauth_path
                + ' merge -) && [ "'
                + luser
                + '" != "'
                + job.user
                + '" ] && OARDO_BECOME_USER='
                + luser
                + ' oardodo bash --noprofile --norc -c "chmod 660 \\\$HOME/.Xauthority" ;TTY=\$(tty) && test -e \$TTY && oardodo chown '
                + job.user
                + ":oar \$TTY && oardodo chmod 660 \$TTY' && OARDO_BECOME_USER="
                + job.user
                + " oardodo bash --noprofile --norc -c '"
                + script
                + "'\""
            )
            # print('oarsub launchs command (W/ DISPLAY {}) : {}'.format(os.environ['DISPLAY'], cmd))
        else:
            # No X display forwarding
            cmd += (
                "\"bash -c 'echo \$PPID >> "
                + oarsub_pids
                + " && TTY=\$(tty) && test -e \$TTY && oardodo chown "
                + job.user
                + ":oar \$TTY && oardodo chmod 660 \$TTY' && OARDO_BECOME_USER="
                + job.user
                + " oardodo bash --noprofile --norc -c '"
                + script
                + "'\""
            )
        # print('oarsub launchs command : ' + cmd)

        # Essential : you become oar instead of the user
        # Set real to effective uid
        os.setuid(os.geteuid())  # TODO: Do really need to do this ?

        print(
            "Connect to OAR job "
            + str(job_id)
            + " via the node "
            + host_to_connect_via_ssh
        )
        return_code = tools.run(
            "strace -ff -o /tmp/log.txt " + cmd, shell=True
        ).returncode

        exit_value = return_code >> 8
        if exit_value == 2:
            cmd_ret.error("cannot enter working directory: " + job.launching_directory)
            cmd_ret.exit(exit_value)
        elif exit_value == 255:
            cmd_ret.error("Job was terminated.")
            cmd_ret.exit(exit_value)
        elif exit_value != 0:
            cmd_ret.error("an unexpected error: " + str(return_code))
            cmd_ret.exit(exit_value)

        if stop_oarexec > 0:
            tools.signal_oarexec(
                host_to_connect_via_ssh, job_id, "USR1", 0, openssh_cmd
            )
            cmd_ret.info("Disconnected from OAR job " + str(job_id))

    else:
        if job.state != "Running":
            cmd_ret.error(
                "Job "
                + str(job_id)
                + " is not running, current state is "
                + job.state
                + "."
            )

        if (luser != job.user) and (luser != "oar"):
            cmd_ret.error(
                "User mismatch for job "
                + str(job_id)
                + " (job user is "
                + job.user
                + "."
            )
        cmd_ret.exit(20)

    cmd_ret.exit(0)


@click.command()
@click.argument("command", required=False)
@click.option(
    "-I",
    "--interactive",
    is_flag=True,
    help="Interactive mode. Give you a shell on the first reserved node.",
)
@click.option(
    "-q",
    "--queue",
    help="Specifies the destination queue. If not defined the job is enqueued in default queue",
)
@click.option(
    "-l",
    "--resource",
    type=click.STRING,
    multiple=True,
    help="Defines resource list requested for a job: resource=value[,[resource=value]...]\
              Defined resources :\
              nodes : Request number of nodes (special value 'all' is replaced by the number of free\
              nodes corresponding to the weight and properties; special value ``max'' is replaced by\
              the number of free,absent and suspected nodes corresponding to the weight and properties).\
              walltime : Request maximun time. Format is [hour:mn:sec|hour:mn|mn]\
              weight : the weight that you want to reserve on each node",
)
@click.option(
    "-p",
    "--property",
    type=click.STRING,
    help="Add constraints to properties for the job (format is a WHERE clause from the SQL syntax).",
)
@click.option(
    "-r",
    "--reservation",
    type=click.STRING,
    help="Ask for an advance reservation job on the date in argument.",
)
@click.option(
    "-C", "--connect", type=int, help="Connect to a reservation in Running state."
)
@click.option("--array", type=int, help="Specify an array job with 'number' subjobs")
@click.option(
    "--array-param-file",
    type=click.STRING,
    help="Specify an array job on which each subjob will receive one line of the \
              file as parameter",
)
@click.option(
    "-S",
    "--scanscript",
    is_flag=True,
    help="Batch mode only: asks oarsub to scan the given script for OAR directives \
              (#OAR -l ...)",
)
@click.option(
    "--checkpoint",
    type=int,
    default=0,
    help="Specify the number of seconds before the walltime when OAR will send \
              automatically a SIGUSR2 on the process.",
)
@click.option(
    "--signal",
    type=int,
    help="Specify the signal to use when checkpointing Use signal numbers, \
              default is 12 (SIGUSR2)",
)
@click.option(
    "-t",
    "--type",
    type=click.STRING,
    multiple=True,
    help="Specify a specific type (deploy, besteffort, cosystem, checkpoint, timesharing).",
)
@click.option(
    "-d",
    "--directory",
    type=click.STRING,
    help="Specify the directory where to launch the command (default is current directory)",
)
@click.option(
    "--project",
    type=click.STRING,
    help="Specify a name of a project the job belongs to.",
)
@click.option("--name", type=click.STRING, help="Specify an arbitrary name for the job")
@click.option(
    "-a",
    "--after",
    type=click.STRING,
    multiple=True,
    help="Add a dependency to a given job, with optional min and max relative \
              start time constraints (<job id>[,m[,M]]).",
)
@click.option(
    "--notify",
    type=click.STRING,
    help='Specify a notification method (mail or command to execute). Ex: \
              --notify "mail:name\@domain.com"\
              --notify "exec:/path/to/script args"',
)
@click.option(
    "-k", "--use-job-key", is_flag=True, help="Activate the job-key mechanism."
)
@click.option(
    "-i",
    "--import-job-key-from-file",
    type=click.STRING,
    help="Import the job-key to use from a files instead of generating a new one.",
)
@click.option(
    "--import-job-key-inline",
    type=click.STRING,
    help="Import the job-key to use inline instead of generating a new one.",
)
@click.option(
    "-e",
    "--export-job-key-to-file",
    type=click.STRING,
    help="Export the job key to a file. Warning: the\
              file will be overwritten if it already exists.\
              (the %jobid% pattern is automatically replaced)",
)
@click.option(
    "-O",
    "--stdout",
    type=click.STRING,
    help="Specify the file that will store the standard output \
              stream of the job. (the %jobid% pattern is automatically replaced)",
)
@click.option(
    "-E",
    "--stderr",
    type=click.STRING,
    help="Specify the file that will store the standard error stream of the job.",
)
@click.option(
    "--hold",
    is_flag=True,
    help='Set the job state into Hold instead of Waiting,\
              so that it is not scheduled (you must run "oarresume" to turn it into the Waiting state)',
)
@click.option("--resubmit", type=int, help="Resubmit the given job as a new one.")
@click.option("-V", "--version", is_flag=True, help="Print OAR version number.")
@click.pass_context
def cli(
    ctx,
    command,
    interactive,
    queue,
    resource,
    reservation,
    connect,
    type,
    checkpoint,
    property,
    resubmit,
    scanscript,
    project,
    signal,
    directory,
    name,
    after,
    notify,
    array,
    array_param_file,
    use_job_key,
    import_job_key_from_file,
    import_job_key_inline,
    export_job_key_to_file,
    stdout,
    stderr,
    hold,
    version,
):
    """Submit a job to OAR batch scheduler."""

    ctx = click.get_current_context()
    if ctx.obj:
        (session, config) = ctx.obj
    else:
        config, engine, log = init_oar()

        session_factory = sessionmaker(bind=engine)
        scoped = scoped_session(session_factory)
        session = scoped()

    global job_id_lst

    cmd_ret = CommandReturns()

    log_warning = ""  # TODO
    log_error = ""
    log_info = ""
    log_std = ""

    remote_host = config["SERVER_HOSTNAME"]
    remote_port = int(config["APPENDICE_SERVER_PORT"])

    # TODO Deploy_hostname / Cosystem_hostname
    # $Deploy_hostname = get_conf("DEPLOY_HOSTNAME");
    # if (!defined($Deploy_hostname)){
    #    $Deploy_hostname = $remote_host;
    # }

    # $Cosystem_hostname = get_conf("COSYSTEM_HOSTNAME");
    # if (!defined($Cosystem_hostname)){
    #    $Cosystem_hostname = $remote_host;
    # }

    if "OAR_RUNTIME_DIRECTORY" in config:
        pass
    # if (is_conf("OAR_RUNTIME_DIRECTORY")){
    #  OAR::Sub::set_default_oarexec_directory(get_conf("OAR_RUNTIME_DIRECTORY"));
    # }

    # my $default_oar_dir = OAR::Sub::get_default_oarexec_directory();
    # if (!(((-d $default_oar_dir) and (-O $default_oar_dir)) or (mkdir($default_oar_dir)))){
    #    die("# Error: failed to create the OAR directory $default_oar_dir, or bad permissions.\n");
    # }

    binpath = ""
    if "OARDIR" in os.environ:
        binpath = os.environ["OARDIR"] + "/"
    else:
        cmd_ret.error("OARDIR environment variable is not defined.", 0, 1)
        cmd_ret.exit()

    openssh_cmd = config["OPENSSH_CMD"]
    ssh_timeout = int(config["OAR_SSH_CONNECTION_TIMEOUT"])

    # print OAR version
    if version:
        cmd_ret.print_("OAR version : " + VERSION)
        cmd_ret.exit()

    # Check the default name of the key if we have to generate it
    if "OARSUB_FORCE_JOB_KEY" in config and config["OARSUB_FORCE_JOB_KEY"] in [
        "yes",
        "YES",
        "Yes",
        "1",
        1,
    ]:
        use_job_key = True

    # If OAR_JOB_KEY_FILE is set in the shell environment, then imply use_job_key
    # because oarsh will use OAR_JOB_KEY_FILE as well and fail if the job is
    # setup without a job_key
    if "OARSUB_FORCE_JOB_KEY" in os.environ:
        use_job_key = True

    if resubmit:
        cmd_ret.print_("# Resubmitting job " + str(resubmit) + "...")
        error, job_id = resubmit_job(session, resubmit)
        if error[0] == 0:
            print(" done.")
            print("OAR_JOB_ID=" + str(job_id))
            if not tools.notify_almighty("Qsub"):
                error_msg = (
                    "Cannot connect to OAR server (Almighty): "
                    + str(remote_host)
                    + ":"
                    + str(remote_port)
                )
                cmd_ret.error("", 0, (3, error_msg))
        else:
            cmd_ret.error("", 0, error)
        cmd_ret.exit()

    # Strip job's types
    types = [t.lstrip() for t in type]

    properties = lstrip_none(property)
    initial_request = " ".join(sys.argv[1:])
    queue_name = lstrip_none(queue)

    reservation_date = 0
    if reservation:
        (error, reservation_date) = check_reservation(reservation)
        if error[0] != 0:
            cmd_ret.error("", 0, error)
            cmd_ret.exit()

    user = os.environ["OARDO_USER"]

    job_parameters = JobParameters(
        config,
        job_type=None,
        resource=resource,
        command=command,
        info_type=None,
        queue=queue_name,
        properties=properties,
        checkpoint=checkpoint,
        signal=signal,
        notify=notify,
        name=name,
        types=types,
        directory=directory,
        dependencies=after,
        stdout=stdout,
        stderr=stderr,
        hold=hold,
        project=project,
        initial_request=initial_request,
        user=user,
        interactive=interactive,
        reservation_date=reservation_date,
        connect=connect,
        scanscript=scanscript,
        array=array,
        array_param_file=array_param_file,
        use_job_key=use_job_key,
        import_job_key_inline=import_job_key_inline,
        import_job_key_file=import_job_key_from_file,
        export_job_key_file=export_job_key_to_file,
    )

    error = job_parameters.check_parameters()
    if error[0] != 0:
        cmd_ret.error("", 0, error)
        cmd_ret.exit()

    # Connect to a reservation
    if connect:
        exit(connect_job(connect, 0, openssh_cmd, cmd_ret))

    submission = Submission(job_parameters)

    if interactive or reservation:
        socket_server = init_tcp_server()
        (_, server_port) = socket_server.getsockname()
        server = socket.gethostbyname_ex(socket.gethostname())[0]
        job_parameters.info_type = server + ":" + str(server_port)

    if not interactive and command:
        cmd_executor = "Qsub"
        job_parameters.job_type = "PASSIVE"

        if array_param_file:
            error = job_parameters.read_array_param_file()
            if error[0] != 0:
                cmd_ret.error("", 0, error)
                cmd_ret.exit()

        # job_parameters.info_type = "frontend:" #"$Host:$server_port"  # TODO  "$Host:$server_port"

    else:
        cmd_executor = "Qsub -I"
        job_parameters.job_type = "INTERACTIVE"

        # TODO MOVE test to  job_parameters.check_parameters()
        if command:
            cmd_ret.warning(
                "Asking for an interactive job (-I), so ignoring arguments: "
                + command
                + " ."
            )
        else:
            command = ""
            job_parameters.command = command
        if array_param_file:
            cmd_ret.error(
                "An array job with parameters given in a file cannot be interactive.",
                0,
                9,
            )
            cmd_ret.exit()
        if job_parameters.array_nb != 1:
            cmd_ret.error("An array job cannot be interactive.", 0, 8)
            cmd_ret.exit()

    # Launch the checked submission
    (error, job_id_lst) = submission.submit(session, config)

    if error[0] != 0:
        cmd_ret.error("unamed error", 0, error)  # TODO
        cmd_ret.exit()

    oar_array_id = 0

    # Print job_id list
    if len(job_id_lst) == 1:
        print("OAR_JOB_ID=" + str(job_id_lst[0]))
    else:
        job = session.query(Job).filter(Job.id == job_id_lst[0]).one()
        oar_array_id = job.array_id
        for job_id in job_id_lst:
            print("OAR_JOB_ID=" + str(job_id))
        print("OAR_ARRAY_ID=" + str(oar_array_id))
    result = (job_id_lst, oar_array_id)

    # Notify Almigthy
    tools.notify_almighty(cmd_executor)
    # import pdb; pdb.set_trace()
    if reservation:
        # Reservation mode
        cmd_ret.info(
            "Advance reservation request: waiting for approval from the scheduler..."
        )

        (conn, address) = socket_server.accept()
        message = conn.recv(1024)
        message = message[:-1]
        answer = message.decode().rstrip()

        if answer == "GOOD RESERVATION":
            cmd_ret.info("Advance reservation is GRANTED.")
        else:
            cmd_ret.info(answer)
            cmd_ret.info("Advance reservation is REJECTED.")

    elif interactive:
        # Interactive mode
        cmd_ret.info("Interactive mode: waiting...")

        prev_str = ""
        while True:
            (conn, address) = socket_server.accept()
            message = conn.recv(1024)
            message = message[:-1]
            answer = message.decode()

            m = re.search(r"\](.*)$", answer)
            if m and m.group(1) != prev_str:
                cmd_ret.info(answer)
                prev_str = m.group(1)
            elif answer != "GOOD JOB":
                cmd_ret.info(answer)

            if (
                (answer == "GOOD JOB")
                or (answer == "BAD JOB")
                or (answer == "JOB KILL")
                or re.match(r"^ERROR", answer)
            ):
                break

        if answer == "GOOD JOB":
            connect_job(session, config, job_id_lst[0], 1, openssh_cmd, cmd_ret)
        else:
            cmd_ret.exit(11)

    cmd_ret.exit(0)
