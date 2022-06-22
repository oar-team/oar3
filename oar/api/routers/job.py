import os
import re
from typing import List, Optional

from fastapi import APIRouter, Body, Depends, HTTPException, Query, Request
from pydantic import BaseModel

from oar.cli.oardel import oardel
from oar.cli.oarhold import oarhold
from oar.cli.oarresume import oarresume
from oar.lib import Job, db
from oar.lib.submission import JobParameters, Submission, check_reservation

from ..dependencies import need_authentication
from ..url_utils import replace_query_params
from . import TimestampRoute

router = APIRouter(
    route_class=TimestampRoute,
    prefix="/jobs",
    tags=["jobs"],
    responses={404: {"description": "Not found"}},
)


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


def attach_links(job):
    rel_map = (
        ("show", "self", "show"),
        ("nodes", "collection", "nodes"),
        ("resources", "collection", "get_resources"),
    )
    job["links"] = []
    for title, rel, endpoint in rel_map:
        url = replace_query_params(
            router.url_path_for(endpoint, job_id=job["id"]), params={}
        )
        # url = url_for("%s.%s" % (app.name, endpoint), job_id=job["id"])
        job["links"].append({"rel": rel, "title": title, "href": url})


@router.get("/")
def index(
    request: Request,
    user: str = None,
    start_time: int = 0,
    stop_time: int = 0,
    states: List[str] = Query([]),
    array: int = None,
    job_ids: List[int] = [],
    details: str = None,
    offset: int = 0,
    limit: int = 500,
):
    # import pdb; pdb.set_trace()
    query = db.queries.get_jobs_for_user(
        user, start_time, stop_time, states, job_ids, array, None, details
    )
    data = {}
    page = query.paginate(request, offset, limit)
    data["total"] = page.total
    data["links"] = page.links
    data["offset"] = offset
    data["items"] = []
    if details:
        jobs_resources = db.queries.get_assigned_jobs_resources(page.items)
        pass
    for item in page:
        attach_links(item)
        if details:
            attach_resources(item, jobs_resources)
            attach_nodes(item, jobs_resources)
        data["items"].append(item)
    return data


@router.get("/{job_id}")
def show(job_id: int, details: Optional[bool] = None):
    job = db.query(Job).get_or_404(job_id)
    data = job.asdict()
    if details:
        job = Job()
        job.id = job_id
        job_resources = db.queries.get_assigned_jobs_resources([job])
        attach_resources(data, job_resources)
        # attach_nodes(data, job_resources)

    attach_links(data)
    return data


@router.get("/{job_id}/nodes")
def nodes(
    job_id,
    limit: int,
    offset: int = 0,
):
    # TODO
    pass


@router.get("/{job_id}/resources")
def get_resources(
    request: Request,
    job_id: int,
    offset: int = 0,
    limit: int = 500,
):
    job = Job()
    job.id = job_id
    query = db.queries.get_assigned_one_job_resources(job)
    page = query.paginate(request, offset, limit)
    data = {}
    data["total"] = page.total
    data["links"] = page.links
    data["offset"] = offset
    data["items"] = []

    for item in page:
        data["items"].append(item)

    return data


class SumbitParameters(BaseModel):
    command: str
    resource: List[str] = Body([])
    workdir: Optional[str] = None
    param_file: Optional[str] = None
    array: Optional[int] = None
    queue: Optional[str] = None
    reservation: Optional[str] = None
    signal: Optional[int] = None
    directory: Optional[str] = None
    project: Optional[str] = None
    name: Optional[str] = None
    notify: Optional[str] = None
    resubmit: Optional[int] = None
    stdout: Optional[str] = None
    stderr: Optional[str] = None
    hold: Optional[bool] = None
    checkpoint: int = 0
    types: Optional[List[str]] = Body(None, alias="type")
    dependencies: Optional[List[int]] = Body(None, alias="after")
    import_job_key_inline: Optional[str] = Body(None, alias="import-job-key-inline")
    export_job_key_to_file: Optional[str] = Body(None, alias="export-job-key-to-file")
    import_job_key_from_file: Optional[str] = Body(
        None, alias="import-job-key-from-file"
    )
    properties: Optional[str] = Body(None, alias="property")
    use_job_key: bool = Body(False, alias="use-job-key")


@router.post("/")
def submit(sp: SumbitParameters, user: str = Depends(need_authentication)):
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

    x_remote_ident
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
    initial_request = ""  # TODO Json version ?

    reservation_date = 0
    if sp.reservation:
        (error, reservation_date) = check_reservation(sp.reservation)
        if error[0] != 0:
            pass  # TODO

    if sp.command and re.match(r".*\$HOME.*", sp.command):
        sp.command = sp.command.replace("$HOME", os.path.expanduser("~" + user))

    if sp.directory and re.match(r".*\$HOME.*", sp.directory):
        sp.directory = sp.directory.replace("$HOME", os.path.expanduser("~" + user))

    if sp.workdir and re.match(r".*\$HOME.*", sp.workdir):
        sp.workdir = sp.workdir.replace("$HOME", os.path.expanduser("~" + user))

    array_params = []
    # array_nb = 1 # TODO
    if sp.param_file:
        array_params = sp.param_file.split("\n")
        # array_nb = len(array_params)
    # if not isinstance(resource, list):
    #    resource = [resource]

    job_parameters = JobParameters(
        job_type="PASSIVE",
        command=sp.command,
        resource=sp.resource,
        workdir=sp.workdir,  # TODO
        array_params=array_params,
        array=sp.array,
        # scanscript=scanscript, TODO
        queue=sp.queue,
        properties=sp.properties,
        reservation=reservation_date,
        checkpoint=sp.checkpoint,
        signal=sp.signal,
        types=sp.types,
        directory=sp.directory,
        project=sp.project,
        initial_request=initial_request,
        user=user,
        name=sp.name,
        dependencies=sp.dependencies,
        notify=sp.notify,
        resubmit=sp.resubmit,
        use_job_key=sp.use_job_key,
        import_job_key_from_file=sp.import_job_key_from_file,
        import_job_key_inline=sp.import_job_key_inline,
        export_job_key_to_file=sp.export_job_key_to_file,
        stdout=sp.stdout,
        stderr=sp.stderr,
        hold=sp.hold,
    )

    # import pdb; pdb.set_trace()

    error = job_parameters.check_parameters()
    if error[0] != 0:
        raise HTTPException(status_code=501)
        pass  # TODO

    submission = Submission(job_parameters)

    (error, job_id_lst) = submission.submit()

    # TODO Enhance
    data = {}
    if len(job_id_lst) >= 1:
        job_id = min(
            job_id_lst
        )  # the minimum ids is also the array_id when array of jobs is submitted
        data["id"] = job_id
        url = router.url_path_for("show", job_id=job_id)
        data["links"] = [{"rel": "rel", "href": url}]
    else:  # TODO
        pass

    return data

    # TODO cmd_output:


# @app.route("/<int:job_id>", methods=["DELETE"])
# @app.route("/<any(array):array>/<int:job_id>", methods=["DELETE"])
# @app.route("/<int:job_id>/deletions/new", methods=["POST", "DELETE"])
# @app.route("/<any(array):array>/<int:job_id>/deletions/new", methods=["POST", "DELETE"])
@router.delete("/{job_id}")
@router.api_route("/{job_id}/deletions/new", methods=["POST", "DELETE"])
def delete(job_id: int, array: bool = False, user: str = Depends(need_authentication)):
    # TODO Get and return error codes ans messages
    if array:
        cmd_ret = oardel(None, None, None, None, job_id, None, None, None, user, False)
    else:
        cmd_ret = oardel(
            [job_id], None, None, None, None, None, None, None, user, False
        )
    data = {}
    data["id"] = job_id
    data["cmd_output"] = cmd_ret.to_str()
    data["exit_status"] = cmd_ret.get_exit_value()
    return data


@router.post("/{job_id}/signal/{signal}")
@router.post("/{job_id}/checkpoints/new")
def signal(
    job_id: int, signal: Optional[int] = None, user: dict = Depends(need_authentication)
):

    if signal:
        checkpointing = False
    else:
        checkpointing = True

    cmd_ret = oardel(
        [job_id], checkpointing, signal, None, None, None, None, None, user, False
    )

    data = {}
    data["id"] = job_id
    data["cmd_output"] = cmd_ret.to_str()
    data["exit_status"] = cmd_ret.get_exit_value()

    return data


@router.post("/{job_id}/resumptions/new")
def resume(job_id: int, user: dict = Depends(need_authentication)):
    """Asks to resume a holded job"""

    cmd_ret = oarresume([job_id], None, None, None, user, False)
    data = {}
    data["id"] = job_id
    data["cmd_output"] = cmd_ret.to_str()
    data["exit_status"] = cmd_ret.get_exit_value()

    return data


@router.post("/{job_id}/{hold}/new")
def hold(
    request: Request,
    job_id: int,
    hold: str = "hold",
    user: dict = Depends(need_authentication),
):
    running = False
    if hold == "rhold":
        running = True

    cmd_ret = oarhold([job_id], running, None, None, None, user, False)

    data = {}
    data["id"] = job_id
    data["cmd_output"] = cmd_ret.to_str()
    data["exit_status"] = cmd_ret.get_exit_value()
    return data
