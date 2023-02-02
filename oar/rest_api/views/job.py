# -*- coding: utf-8 -*-
import os
import re

from flask import g, url_for

from oar.cli.oardel import oardel
from oar.cli.oarhold import oarhold
from oar.cli.oarresume import oarresume
from oar.lib import Job, db
from oar.lib.submission import JobParameters, Submission, check_reservation

from ..utils import Arg
from . import Blueprint

# DEFAULT_VALUE = {
#    'directory': os.getcwd()
# }

app = Blueprint("jobs", __name__, url_prefix="/jobs")


@app.route("/", methods=["GET"])
@app.route("/<any(details, table):detailed>", methods=["GET"])
# @app.route('/nodes/<string:network_address>', methods=['GET'])  # TODO TOREMOVE ?
# @app.route('/ressources/<string:resource_id>/details', methods=['GET']) # TODO TOREMOVE ?
@app.args(
    {
        "offset": Arg(int, default=0),
        "limit": Arg(int),
        "user": Arg(str),
        "from": Arg(int, dest="start_time"),
        "to": Arg(int, dest="stop_time"),
        "start_time": Arg(int),
        "stop_time": Arg(int),
        "state": Arg([str, ","], dest="states"),
        "array": Arg(int, dest="array_id"),
        "ids": Arg([int, ":"], dest="job_ids"),
    }
)
# TODO network_address & resource_id
# TOREMOVE @app.need_authentication() NOT MANDATORY
# @app.need_authentication()
def index(
    offset, limit, user, start_time, stop_time, states, array_id, job_ids, detailed=None
):
    # import pdb; pdb.set_trace()
    query = db.queries.get_jobs_for_user(
        user, start_time, stop_time, states, job_ids, array_id, None, detailed
    )
    page = query.paginate(offset, limit)
    g.data["total"] = page.total
    g.data["links"] = page.links
    g.data["offset"] = offset
    g.data["items"] = []
    if detailed:
        jobs_resources = db.queries.get_assigned_jobs_resources(page.items)
    for item in page:
        attach_links(item)
        if detailed:
            attach_resources(item, jobs_resources)
            attach_nodes(item, jobs_resources)
        g.data["items"].append(item)


@app.route("/", methods=["POST"])
@app.args(
    {
        "resource": Arg([str]),
        "command": Arg(str),
        "workdir": Arg(str),
        "param_file": Arg(str),
        "array": Arg(int),
        # 'scanscript': Arg(str), TODO to remove ?
        "queue": Arg(str),
        "property": Arg(
            str, dest="properties"
        ),  # TODO add SQL sanatization ? (clause Where)
        "reservation": Arg(str),
        "checkpoint": Arg(int, default=0),
        "signal": Arg(int),
        "type": Arg([str], dest="types"),
        "directory": Arg(str),
        "project": Arg(str),
        "name": Arg(str),
        "after": Arg([int, ","], dest="dependencies"),
        "notify": Arg(str),
        "resubmit": Arg(int),
        "use-job-key": Arg(bool, dest="use_job_key", default=0),
        "import-job-key-from-file": Arg(str, dest="import_job_key_from_file"),
        "import-job-key-inline": Arg(str, dest="import_job_key_inline"),
        "export-job-key-to-file": Arg(str, dest="export_job_key_to_file"),
        "stdout": Arg(str),
        "stderr": Arg(str),
        "hold": Arg(bool),
    }
)
@app.need_authentication()
def submit(
    resource,
    command,
    workdir,
    param_file,
    array,
    queue,
    properties,
    reservation,
    checkpoint,
    signal,
    types,
    directory,
    project,
    name,
    dependencies,
    notify,
    resubmit,
    use_job_key,
    import_job_key_from_file,
    import_job_key_inline,
    export_job_key_to_file,
    stdout,
    stderr,
    hold,
):
    """Job submission

    resource (string): the resources description as required by oar (example: “/nodes=1/cpu=2”)
    command (string): a command name or a script that is executed when the job starts
    workdir (string): the path of the directory from where the job will be submited
    param_file (string): the content of a parameters file, for the submission of an array job.
                         For example: {“resource”:”/nodes=1, “command”:”sleep”, “param_file”:”60n90n30”}

    All other option accepted by the oarsub unix command: every long option that may be passed to
    the oarsub command is known as a key of the input hash. If the option is a toggle (no value),
    you just have to set it to “1” (for example: ‘use-job-key’ => ‘1’). Some options may be arrays
    (for example if you want to specify several ‘types’ for a job)


    array <number>            Specify an array job with 'number' subjobs
    param-file <file>         Specify an array job on which each subjob will
                              receive one line of the file as parameter
    scanscript                Batch mode only: asks oarsub to scan the given
                              script for OAR directives (#OAR -l ...)
    queue                     Set the queue to submit the job to
    property="<list>"         Add constraints to properties for the job.
                              (format is a WHERE clause from the SQL syntax)
    reservation=<date>        Request a job start time reservation,
                              instead of a submission. The date format is
                              "YYYY-MM-DD HH:MM:SS".
    checkpoint=<delay>        Enable the checkpointing for the job. A signal
                              is sent DELAY seconds before the walltime on
                              the first processus of the job
    signal=<#sig>             Specify the signal to use when checkpointing
                              Use signal numbers, default is 12 (SIGUSR2)
    type=<type>               Specify a specific type (deploy, besteffort,
                              cosystem, checkpoint, timesharing)
    directory=<dir>           Specify the directory where to launch the
                              command (default is current directory)
    project=<txt>             Specify a name of a project the job belongs to
    name=<txt>                Specify an arbitrary name for the job
    anterior=<job id>         Anterior job that must be terminated to start
                              this new one
    notify=<txt>              Specify a notification method
                              (mail or command to execute). Ex:
                                  --notify "mail:name@domain.com"
                                  --notify "exec:/path/to/script args"
    resubmit=<job id>         Resubmit the given job as a new one
    use-job-key               Activate the job-key mechanism.
    import-job-key-from-file=<file>
                              Import the job-key to use from a files instead
                              of generating a new one.
    import-job-key-inline=<txt>
                              Import the job-key to use inline instead of
                              generating a new one.
    export-job-key-to-file=<file>
                              Export the job key to a file. Warning: the
                              file will be overwritten if it already exists.
                              (the %jobid% pattern is automatically replaced)
    stdout=<file>             Specify the file that will store the standart
                              output stream of the job.
                              (the %jobid% pattern is automatically replaced)
    stderr=<file>             Specify the file that will store the standart
                              error stream of the job.
                              (the %jobid% pattern is automatically replaced)
    hold                      Set the job state into Hold instead of Waiting,
                              so that it is not scheduled (you must run
                              "oarresume" to turn it into the Waiting state)
    stagein=<dir|tgz>         Set the stagein directory or archive
    stagein-md5sum=<md5sum>   Set the stagein file md5sum


    Input yaml example
    ---
    stdout: /tmp/outfile
    command: /usr/bin/id;echo "OK"
    resource: /nodes=2/cpu=1
    workdir: ~bzizou/tmp
    type:
    - besteffort
    - timesharing
    use-job-key: 1


    Output yaml example
    ---
    api_timestamp: 1332323792
    cmd_output: |
      [ADMISSION RULE] Modify resource description with type constraints
      OAR_JOB_ID=4
    id: 4
    links:
      - href: /oarapi-priv/jobs/4
        rel: self

    Note: up to now yaml is no supported, only json and html form.

    """
    user = g.current_user

    initial_request = ""  # TODO Json version ?

    reservation_date = 0
    if reservation:
        (error, reservation_date) = check_reservation(reservation)
        if error[0] != 0:
            pass  # TODO

    if command and re.match(r".*\$HOME.*", command):
        command = command.replace("$HOME", os.path.expanduser("~" + user))

    if directory and re.match(r".*\$HOME.*", directory):
        directory = directory.replace("$HOME", os.path.expanduser("~" + user))

    if workdir and re.match(r".*\$HOME.*", workdir):
        workdir = workdir.replace("$HOME", os.path.expanduser("~" + user))

    array_params = []
    # array_nb = 1 # TODO
    if param_file:
        array_params = param_file.split("\n")
        # array_nb = len(array_params)
    # if not isinstance(resource, list):
    #    resource = [resource]

    job_parameters = JobParameters(
        job_type="PASSIVE",
        command=command,
        resource=resource,
        workdir=workdir,  # TODO
        array_params=array_params,
        array=array,
        # scanscript=scanscript, TODO
        queue=queue,
        properties=properties,
        reservation=reservation_date,
        checkpoint=checkpoint,
        signal=signal,
        types=types,
        directory=directory,
        project=project,
        initial_request=initial_request,
        user=user,
        name=name,
        dependencies=dependencies,
        notify=notify,
        resubmit=resubmit,
        use_job_key=use_job_key,
        import_job_key_from_file=import_job_key_from_file,
        import_job_key_inline=import_job_key_inline,
        export_job_key_to_file=export_job_key_to_file,
        stdout=stdout,
        stderr=stderr,
        hold=hold,
    )

    # import pdb; pdb.set_trace()

    error = job_parameters.check_parameters()
    if error[0] != 0:
        pass  # TODO

    submission = Submission(job_parameters)

    (error, job_id_lst) = submission.submit()

    # TODO Enhance
    if len(job_id_lst) >= 1:
        job_id = min(
            job_id_lst
        )  # the minimum ids is also the array_id when array of jobs is submitted
        g.data["id"] = job_id
        url = url_for("%s.%s" % (app.name, "show"), job_id=job_id)
        g.data["links"] = [{"rel": "rel", "href": url}]
    else:  # TODO
        pass

    # TODO cmd_output:


@app.route("/<int:job_id>/resources", methods=["GET"])
@app.args({"offset": Arg(int, default=0), "limit": Arg(int)})
def resources(job_id, offset, limit):
    job = Job()
    job.id = job_id
    query = db.queries.get_assigned_one_job_resources([job])
    page = query.paginate(offset, limit)
    g.data["total"] = page.total
    g.data["links"] = page.links
    g.data["offset"] = offset
    g.data["items"] = []
    for item in page:
        attach_links(item)
        g.data["items"].append(item)


@app.route("/<int:job_id>", methods=["GET"])
@app.route("/<int:job_id>/<any(details, table):detailed>", methods=["GET"])
def show(job_id, detailed=None):
    job = db.query(Job).get_or_404(job_id)
    g.data.update(job.asdict())
    if detailed:
        job = Job()
        job.id = job_id
        job_resources = db.queries.get_assigned_jobs_resources([job])
        attach_resources(g.data, job_resources)
        attach_nodes(g.data, job_resources)
    attach_links(g.data)


@app.route("/<int:job_id>/nodes", methods=["GET"])
@app.args({"offset": Arg(int, default=0), "limit": Arg(int)})
def nodes(job_id, offset, limit):
    # TODO
    pass


def attach_links(job):
    rel_map = (
        ("show", "self", "show"),
        ("nodes", "collection", "nodes"),
        ("resources", "collection", "resources"),
    )
    job["links"] = []
    for title, rel, endpoint in rel_map:
        url = url_for("%s.%s" % (app.name, endpoint), job_id=job["id"])
        job["links"].append({"rel": rel, "href": url, "title": title})


def attach_resources(job, jobs_resources):
    job["resources"] = []
    from .resource import attach_links

    for resource in jobs_resources[job["id"]]:
        resource = resource.asdict(ignore_keys=("network_address",))
        attach_links(resource)
        job["resources"].append(resource)


def attach_nodes(job, jobs_resources):
    job["nodes"] = []
    network_addresses = []
    from .resource import attach_links

    for node in jobs_resources[job["id"]]:
        node = node.asdict(ignore_keys=("id",))
        if node["network_address"] not in network_addresses:
            attach_links(node)
            job["nodes"].append(node)
            network_addresses.append(node["network_address"])


@app.route("/<int:job_id>", methods=["DELETE"])
@app.route("/<any(array):array>/<int:job_id>", methods=["DELETE"])
@app.route("/<int:job_id>/deletions/new", methods=["POST", "DELETE"])
@app.route("/<any(array):array>/<int:job_id>/deletions/new", methods=["POST", "DELETE"])
@app.need_authentication()
def delete(job_id, array=None):
    user = g.current_user
    if array:
        cmd_ret = oardel(None, None, None, None, job_id, None, None, None, user, False)
    else:
        cmd_ret = oardel(
            [job_id], None, None, None, None, None, None, None, user, False
        )

    g.data["id"] = job_id
    g.data["cmd_output"] = cmd_ret.to_str()
    g.data["exit_status"] = cmd_ret.get_exit_value()


@app.route("/<int:job_id>/checkpoints/new", methods=["POST"])
@app.route("/<int:job_id>/signal/<int:signal>", methods=["POST"])
@app.need_authentication()
def signal(job_id, signal=None):
    user = g.current_user

    if signal:
        checkpointing = False
    else:
        checkpointing = True

    cmd_ret = oardel(
        [job_id], checkpointing, signal, None, None, None, None, None, user, False
    )

    g.data["id"] = job_id
    g.data["cmd_output"] = cmd_ret.to_str()
    g.data["exit_status"] = cmd_ret.get_exit_value()


@app.route("/<int:job_id>/<any(hold, rhold):hold>", methods=["POST"])
@app.need_authentication()
def hold(job_id, hold):
    user = g.current_user

    running = False
    if hold == "rhold":
        running = True

    cmd_ret = oarhold([job_id], running, None, None, None, user, False)

    g.data["id"] = job_id
    g.data["cmd_output"] = cmd_ret.to_str()
    g.data["exit_status"] = cmd_ret.get_exit_value()


@app.route("/<int:job_id>/resumptions/new", methods=["POST"])
@app.need_authentication()
def resume(job_id):
    """Asks to resume a holded job"""
    user = g.current_user

    cmd_ret = oarresume([job_id], None, None, None, user, False)

    g.data["id"] = job_id
    g.data["cmd_output"] = cmd_ret.to_str()
    g.data["exit_status"] = cmd_ret.get_exit_value()


@app.route("/<int:job_id>/walltime-change/<int:new_walltime>", methods=["POST"])
@app.need_authentication()
def walltime_change(job_id, signal=None):
    user = g.current_user

    if signal:
        checkpointing = False
    else:
        checkpointing = True

    #    request(job_id, user, new_walltime, force, delay_next_jobs):
    cmd_ret = oardel(
        [job_id], checkpointing, signal, None, None, None, None, None, user, False
    )

    g.data["id"] = job_id
    g.data["cmd_output"] = cmd_ret.to_str()
    g.data["exit_status"] = cmd_ret.get_exit_value()
