# -*- coding: utf-8 -*-

# TODO: Evalys integration
# TODO: Complete Job header
# TODO: owf2swf
# Max Queue
# TODO: Job Status field
# Support correctly Job Status field
# 0	Job Failed
# 1	Job completed successfully
# 2	This partial execution will be continued
# 3	This is the last partial execution, job completed
# 4	This is the last partial execution, job failed
# 5	Job was cancelled (either before starting or during run)

import pickle
import time
import uuid
from collections import OrderedDict

import click
from sqlalchemy.sql import distinct, func, or_

from oar.lib.models import AssignedResource, Job, MoldableJobDescription, Resource

click.disable_unicode_literals_warning = True

SWF_COLUMNS = [
    "jobID",
    "submission_time",
    "waiting_time",
    "execution_time",
    "proc_alloc",
    "cpu_used",
    "mem_used",
    "proc_req",
    "user_est",
    "mem_req",
    "status",
    "uid",
    "gid",
    "exe_num",
    "queue",
    "partition",
    "prev_jobs",
    "think_time",
]

OAR_TRACE_COLUMNS = [
    "job_id",
    "submission_time",
    "start_time",
    "stop_time",
    "walltime",
    "nb_default_ressources",
    "nb_extra_ressources",
    "status",
    "user",
    "command",
    "queue",
    "name",
    "array",
    "type",
    "reservation",
    "cigri",
]
OWF_VERSION = "1.0"


class JobMetrics:
    def __init__(self, **entries):
        self.__dict__.update(entries)


class WorkloadMetadata:
    def __init__(
        self,
        db_server,
        db_name,
        first_jobid=None,
        last_jobid=None,
        filename=None,
        uuid=None,
    ):
        if filename:
            return pickle.load(open(filename, "rb"))

        self.db_name = db_name
        self.db_server = db_server
        self.user = {}
        self.name = {}
        self.project = {}
        self.command = {}
        self.queue = {}
        self.resources = db.query(Resource).order_by(Resource.id.asc()).all()
        self.rid2resource = {r.id: r for r in self.resources}
        self.first_jobid = first_jobid
        self.last_jobid = last_jobid
        self.filename = (
            "wkld_metadata_"
            + self.db_server
            + "_"
            + self.db_name
            + "_"
            + str(self.first_jobid)
            + "_"
            + str(self.last_jobid)
            + ".pickle"
        )
        self.OWF_VERSION = OWF_VERSION
        if uuid:
            self.uuid = uuid

    def dump(self, filename=None):
        if not filename:
            filename = self.filename
        pickle.dump(self, open(filename, "wb"))

    def dict2int(self, dictname, key):
        d = getattr(self, dictname)
        if key in d:
            return d[key]
        else:
            value = len(d) + 1
            d[key] = value
            return value

    def user2int(self, user):
        return self.dict2int("user", user)

    def name2int(self, name):
        return self.dict2int("name", name)

    def project2int(self, project):
        return self.dict2int("project", project)

    def command2int(self, command):
        return self.dict2int("command", command)

    def queue2int(self, queue):
        return self.dict2int("queue", queue)


def get_jobs(first_jobid, last_jobid, wkld_metadata):
    jobs_metrics = OrderedDict()
    jobs = (
        db.query(Job)
        .filter(Job.id >= first_jobid)
        .filter(Job.id <= last_jobid)
        .order_by(Job.id)
        .all()
    )

    job_id2job = {}
    # job_id2moldable_id = {}
    moldable_id2job = {}
    assigned_moldable_ids = []
    for job in jobs:
        if job.state == "Terminated" or job.state == "Error":
            status = 1 if job.state == "Terminated" else 0
            assigned_moldable_ids.append(job.assigned_moldable_job)
            job_id2job[job.id] = job
            # job_id2moldable_id[job.id] = job.assigned_moldable_job
            moldable_id2job[job.assigned_moldable_job] = job
            job_metrics = JobMetrics(
                job_id=job.id,
                submission_time=job.submission_time,
                start_time=job.start_time,
                stop_time=job.stop_time,
                walltime=0,
                nb_default_ressources=0,
                nb_extra_ressources=0,
                status=status,
                user=wkld_metadata.user2int(job.user),
                command=wkld_metadata.command2int(job.command),
                queue=wkld_metadata.queue2int(job.queue_name),
                name=wkld_metadata.name2int(job.name),
                array=job.array_id,
                type=0 if job.type == "PASSIVE" else 1,
                reservation=1 if (job.reservation != "None") else 0,
                cigri=job.name.split(".")[1]
                if (job.name and job.name.split(".")[0] == "cigri")
                else "0",
            )
            jobs_metrics[job.id] = job_metrics

    # Determine walltime thanks to assigned moldable id
    assigned_moldable_ids.sort()
    min_mld_id = assigned_moldable_ids[0]
    max_mld_id = assigned_moldable_ids[-1]

    result = (
        db.query(MoldableJobDescription)
        .filter(MoldableJobDescription.id >= min_mld_id)
        .filter(MoldableJobDescription.id <= max_mld_id)
        .all()
    )

    for mld_desc in result:
        if mld_desc.job_id in job_id2job.keys():
            job = job_id2job[mld_desc.job_id]
            if mld_desc.id == job.assigned_moldable_job:
                jobs_metrics[job.id].walltime = mld_desc.walltime

    # Determine nb_default_ressources and nb_extra_ressources for jobs in Terminated or Error state
    result = (
        db.query(AssignedResource)
        .filter(AssignedResource.moldable_id >= min_mld_id)
        .filter(AssignedResource.moldable_id <= max_mld_id)
        .order_by(AssignedResource.moldable_id, AssignedResource.resource_id)
    )

    moldable_id = 0
    nb_default_ressources = 0
    nb_extra_ressources = 0

    for assigned_resource in result:
        # Test if it's the first or a new job(moldable id) (note: moldale_id == 0 doesn't exist)
        if moldable_id != assigned_resource.moldable_id:
            # Not the first so we save the 2 values nb_default_ressources and nb_extra_ressources
            if moldable_id:
                # Test if job is in the list of Terminated or Error ones
                if moldable_id in moldable_id2job:
                    job = moldable_id2job[moldable_id]
                    jobs_metrics[job.id].nb_default_ressources = nb_default_ressources
                    jobs_metrics[job.id].nb_extra_ressources = nb_extra_ressources
            # New job(moldable id)
            moldable_id = assigned_resource.moldable_id
            nb_default_ressources = 0
            nb_extra_ressources = 0

        resource = wkld_metadata.rid2resource[assigned_resource.resource_id]
        if resource.type == "default":
            nb_default_ressources += 1
        else:
            nb_extra_ressources += 1

    # Set value for last job
    if moldable_id in moldable_id2job:
        job = moldable_id2job[moldable_id]
        jobs_metrics[job.id].nb_default_ressources = nb_default_ressources
        jobs_metrics[job.id].nb_extra_ressources = nb_extra_ressources

    return jobs_metrics


def jobs2trace(jobs_metrics, filehandle, unix_start_time, mode, display):
    for _, job_metrics in jobs_metrics.items():
        if display:
            print(
                "id: {job_id} submission: {submission_time} start: {start_time} stop: {stop_time} "
                "walltime: {walltime}  res: {nb_default_ressources} extra: {nb_extra_ressources} cigri: {cigri}".format(
                    **job_metrics.__dict__
                )
            )

        if filehandle:
            if mode == "swf":
                """
                This class is a derived from the SWF format. SWF is the default format
                for parallel workload defined here:
                http://www.cs.huji.ac.il/labs/parallel/workload/swf.html

                the data format is one line per job, with 18 fields:

                1) Job Number, a counter field, starting from
                2) Submit Time, seconds. submittal time
                3) Wait Time, seconds. diff between submit and begin to run
                4) Run Time, seconds. end-time minus start-time
                5) Number of Processors, number of allocated processors
                6) Average CPU Time Used, seconds. user+system. avg over procs
                7) Used Memory, KB. avg over procs.
                8) Requested Number of Processors, requested number of
                processors
                9) Requested Time, seconds. user runtime estimation
                10) Requested Memory, KB. avg over procs.
                11) status (1=completed, 0=killed), 0=fail; 1=completed; 5=canceled
                12) User ID, user id
                13) Group ID, group id
                14) Executable (Application) Number, [1,2..n] n = app#
                appearing in log
                15) Queue Number, [1,2..n] n = queue# in the system
                16) Partition Number, [1,2..n] n = partition# in the systems
                17) Preceding Job Number,  cur job will start only after ...
                18) Think Time from Preceding Job -- this is the number of seconds that
                should elapse between the termination of the preceding job and the
                submittal of this one.
                """

                jobID = job_metrics.job_id
                submission_time = job_metrics.submission_time - unix_start_time
                waiting_time = job_metrics.start_time - job_metrics.submission_time
                execution_time = job_metrics.stop_time - job_metrics.start_time
                proc_alloc = job_metrics.nb_default_ressources
                cpu_used = -1
                mem_used = -1
                proc_req = job_metrics.nb_default_ressources
                user_est = job_metrics.walltime
                mem_req = -1
                status = job_metrics.status
                uid = job_metrics.user
                gid = -1
                exe_num = job_metrics.command
                queue = job_metrics.queue
                partition = -1
                prev_jobs = -1
                think_time = -1

                jm = [
                    jobID,
                    submission_time,
                    waiting_time,
                    execution_time,
                    proc_alloc,
                    cpu_used,
                    mem_used,
                    proc_req,
                    user_est,
                    mem_req,
                    status,
                    uid,
                    gid,
                    exe_num,
                    queue,
                    partition,
                    prev_jobs,
                    think_time,
                ]

                filehandle.write(
                    """{} {} {} {} {} {} {} {} {}"""
                    """ {} {} {} {} {} {} {} {} {}\n""".format(*jm)
                )
            elif mode == "owf":
                job_id = job_metrics.job_id
                submission_time = job_metrics.submission_time - unix_start_time
                start_time = job_metrics.start_time - unix_start_time
                stop_time = job_metrics.stop_time - unix_start_time
                walltime = job_metrics.walltime
                nb_default_ressources = job_metrics.nb_default_ressources
                nb_extra_ressources = job_metrics.nb_extra_ressources
                status = job_metrics.status
                user = job_metrics.user
                command = job_metrics.command
                queue = job_metrics.queue
                name = job_metrics.name
                array = job_metrics.array
                type = job_metrics.type
                reservation = job_metrics.reservation
                cigri = job_metrics.cigri
                jm = [
                    job_id,
                    submission_time,
                    start_time,
                    stop_time,
                    walltime,
                    nb_default_ressources,
                    nb_extra_ressources,
                    status,
                    user,
                    command,
                    queue,
                    name,
                    array,
                    type,
                    reservation,
                    cigri,
                ]
                filehandle.write(
                    """{} {} {} {} {} {} {} {}"""
                    """ {} {} {} {} {} {} {} {}\n""".format(*jm)
                )


def header_values(first_jobid, last_jobid):
    nb_traced_jobs = (
        db.query(Job)
        .filter(Job.id >= first_jobid)
        .filter(Job.id <= last_jobid)
        .filter(or_(Job.state == "Terminated", Job.state == "Error"))
        .count()
    )

    # Resources
    nb_nodes = db.query(func.count(distinct(Resource.network_address))).scalar()

    nb_resources = db.query(Resource).count()
    nb_default_resources = db.query(Resource).filter(Resource.type == "default").count()

    # Time
    unix_start_time = (
        db.query(Job.submission_time).filter(Job.id == first_jobid).one()[0]
    )

    # Max stop time
    max_stop_time = (
        db.query(func.max(Job.stop_time))
        .filter(Job.id >= first_jobid)
        .filter(Job.id <= last_jobid)
        .filter(or_(Job.state == "Terminated", Job.state == "Error"))
        .one()[0]
    )

    # TimeZoneString:
    tz_string = str(time.tzname)
    start_time = time.strftime(
        "%a %b %d %H:%M:%S %Z %Y", time.localtime(unix_start_time)
    )
    end_time = time.strftime("%a %b %d %H:%M:%S %Z %Y", time.localtime(max_stop_time))

    return (
        nb_traced_jobs,
        nb_nodes,
        nb_resources,
        nb_default_resources,
        unix_start_time,
        tz_string,
        start_time,
        end_time,
    )


def file_header(trace_file, wkld_metadata, mode, first_jobid, last_jobid):
    filehandle = open(trace_file, "w")

    if mode == "swf":
        filehandle.write("; WARNING HEADER MISSIING TO BE COMPLETE see : \n;\n")
        filehandle.write(
            ";         http://www.cs.huji.ac.il/labs/parallel/workload/swf.html or\n"
        )
        filehandle.write(
            ";         https://github.com/oar-team/evalys/blob/master/evalys/workload.py\n"
        )
    else:
        filehandle.write("; OAR trace workload file\n;\n")
        filehandle.write("; {:>22}: {}\n".format("OWF Format", OWF_VERSION))

    filehandle.write(";\n")

    if wkld_metadata.uuid:
        filehandle.write("; {:>22}: {}\n".format("Extraction UUDI", wkld_metadata.uuid))

    (
        nb_traced_jobs,
        nb_nodes,
        nb_resources,
        nb_default_resources,
        unix_start_time,
        tz_string,
        start_time,
        end_time,
    ) = header_values(first_jobid, last_jobid)

    filehandle.write("; {:>22}: {}\n".format("MaxJobs", nb_traced_jobs))
    filehandle.write("; {:>22}: {}\n".format("MaxRecords", nb_traced_jobs))
    filehandle.write("; {:>22}: {}\n".format("Preemption", "No"))

    filehandle.write(";\n")

    filehandle.write("; {:>22}: {}\n".format("UnixStartTime", unix_start_time))
    filehandle.write("; {:>22}: {}\n".format("TimeZoneString", tz_string))
    filehandle.write("; {:>22}: {}\n".format("StartTime", start_time))
    filehandle.write("; {:>22}: {}\n".format("EndTime", end_time))

    filehandle.write(";\n")

    filehandle.write("; {:>22}: {}\n".format("MaxNodes", nb_nodes))
    filehandle.write("; {:>22}: {}\n".format("MaxProcs", nb_default_resources))
    filehandle.write("; {:>22}: {}\n".format("Nb Resources", nb_resources))
    filehandle.write(
        "; {:>22}: {}\n".format("Nb Defaults Resource", nb_default_resources)
    )

    filehandle.write(";\n")

    if mode == "owf":
        filehandle.write("; Fields and their position:\n")
        for i, columns in enumerate(OAR_TRACE_COLUMNS):
            filehandle.write("; {:>2}: {}\n".format(i, columns))

    filehandle.write(";\n")

    return (filehandle, unix_start_time)


@click.command()
@click.option(
    "--db-url",
    type=click.STRING,
    help="The url for OAR database (postgresql://oar:PASSWORD@pgsql_server/db_name).",
)
@click.option(
    "-f",
    "--trace-file",
    type=click.STRING,
    help="Trace output file name (SWF by default).",
)
@click.option("-b", "--first-jobid", type=int, default=0, help="First job id to begin.")
@click.option("-e", "--last-jobid", type=int, default=0, help="Last job id to end.")
@click.option("-p", is_flag=True, help="Print metrics on stdout.")
@click.option(
    "-m",
    "--mode",
    type=click.STRING,
    default="swf",
    help="Select trace mode: swf or owf (SWF by default).",
)
@click.option(
    "--chunk-size",
    type=int,
    default=10000,
    help="Number of size retrieve at one time to limit stress on database.",
)
@click.option(
    "--metadata-file",
    type=click.STRING,
    help="Metadata file stores various non-anonymized jobs' information (user, job name, project, command).",
)
def cli(
    db_url, trace_file, first_jobid, last_jobid, chunk_size, metadata_file, p, mode
):
    """This program allows to extract workload traces from OAR RJMS.

    oar2trace --db-url 'postgresql://oar:oar@server/oar' -m owf

    """
    # import pdb; pdb.set_trace()

    display = p
    jobids_range = None

    if (not mode == "swf") and (not mode == "owf"):
        print("Mode must set to swf or owf")
        exit(1)

    if db_url:
        db._cache["uri"] = db_url
        db_name = db_url.split("/")[-1]
        db_server = (db_url.split("/")[-2]).split("@")[-1]
    else:
        db_name = "oar"
        db_server = "localhost"

    try:
        jobids_range = db.query(
            func.max(Job.id).label("max"), func.min(Job.id).label("min")
        ).one()
    except Exception as e:
        print(e)
        exit(1)
    if jobids_range == (None, None):
        exit(1)
    # else:
    #    exit()

    if not first_jobid:
        first_jobid = jobids_range.min

    if not last_jobid:
        last_jobid = jobids_range.max

    if first_jobid > last_jobid:
        print("First job id must be lower then last one.")
        exit(1)

    if not trace_file:
        suffix = "swf" if mode == "swf" else "owf"
        trace_file = "oar_trace_{}_{}_{}_{}.{}".format(
            db_server, db_name, first_jobid, last_jobid, suffix
        )

    # UUID is used to link workload trace file with workload metadata file.
    # One UUID is given per workload generation.
    uuid_str = str(uuid.uuid4())

    wkld_metadata = WorkloadMetadata(
        db_server, db_name, first_jobid, last_jobid, metadata_file, uuid_str
    )

    nb_chunck = int((last_jobid - first_jobid) / chunk_size) + 1

    begin_jobid = first_jobid
    end_jobid = 0
    fh, unix_start_time = file_header(
        trace_file, wkld_metadata, mode, first_jobid, last_jobid
    )
    for chunk in range(nb_chunck):
        if (begin_jobid + chunk_size - 1) > last_jobid:
            end_jobid = last_jobid
        else:
            end_jobid = begin_jobid + chunk_size - 1
        print(
            "# Jobids Range: [{}-{}], Chunck: {}".format(
                begin_jobid, end_jobid, (chunk + 1)
            )
        )

        jobs_metrics = get_jobs(begin_jobid, end_jobid, wkld_metadata)
        jobs2trace(jobs_metrics, fh, unix_start_time, mode, display)

        begin_jobid = end_jobid + 1
    if fh:
        fh.close()
    wkld_metadata.dump()
