# -*- coding: utf-8 -*-
from collections import OrderedDict
from sqlalchemy.sql import func, or_
from oar.lib import db, Job, Resource, MoldableJobDescription, AssignedResource
import pickle
import click
click.disable_unicode_literals_warning = True


class JobMetrics:
    def __init__(self, **entries):
        self.__dict__.update(entries)


class WorkloadMetadata():
    def __init__(self, db_server, db_name, first_jobid=None, last_jobid=None, filename=None):
        if filename:
            return pickle.load(open(filename, 'rb'))

        self.db_name = db_name
        self.db_server = db_server
        self.user = {}
        self.name = {}
        self.project = {}
        self.command = {}
        self.resources = db.query(Resource).order_by(Resource.id.asc()).all()
        self.rid2resource = {r.id: r for r in self.resources}
        self.first_jobid = first_jobid
        self.last_jobid = last_jobid

    def dump(self, filename=None):
        if not filename:
            filename = 'wkld_metadata_' + self.db_server + '_' + self.db_name\
                       + '_' + str(self.first_jobid) + '_' + str(self.last_jobid)\
                       + '.pickle'
        pickle.dump(self, open(filename, 'wb'))

    def dict2int(self, dictname, key):
        d = getattr(self, dictname)
        if key in d:
            return d[key]
        else:
            value = len(d) + 1
            d[key] = value
            return value

    def user2int(self, user):
        return self.dict2int('user')

    def project2int(self, user):
        return self.dict2int('project')

    def command2int(self, command):
        return self.dict2int('command')


def get_jobs(first_jobid, last_jobid, wkld_metadata):
    jobs_metrics = OrderedDict()
    jobs = db.query(Job)\
             .filter(or_(Job.id >= first_jobid, Job.id <= last_jobid))\
             .order_by(Job.id).all()

    job_id2job = {}
    # job_id2moldable_id = {}
    moldable_id2job = {}
    assigned_moldable_ids = []
    for job in jobs:
        if job.state == 'Terminated' or job.state == 'Error':
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
            )
            jobs_metrics[job.id] = job_metrics

    # Determine walltime thanks to assigned moldable id
    assigned_moldable_ids.sort()
    min_mld_id = assigned_moldable_ids[0]
    max_mld_id = assigned_moldable_ids[-1]

    result = db.query(MoldableJobDescription)\
               .filter(or_(MoldableJobDescription.id >= min_mld_id,
                           MoldableJobDescription.id <= max_mld_id))\
               .all()

    for mld_desc in result:
        if mld_desc.job_id in job_id2job.keys():
            job = job_id2job[mld_desc.job_id]
            if mld_desc.id == job.assigned_moldable_job:
                jobs_metrics[job.id].walltime == mld_desc.walltime

    # Determine nb_default_ressources and nb_extra_ressources for jobs in Terminated or Error state
    result = db.query(AssignedResource)\
               .order_by(AssignedResource.moldable_id, AssignedResource.resource_id)

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
        if resource.type == 'default':
            nb_default_ressources += 1
            nb_extra_ressources += 1

    # Set value for last job
    if moldable_id in moldable_id2job:
        job = moldable_id2job[moldable_id]
        jobs_metrics[job.id].nb_default_ressources = nb_default_ressources
        jobs_metrics[job.id].nb_extra_ressources = nb_extra_ressources

    return jobs_metrics


def jobs2swf(jobs_metrics, filename):
    for _, job_metrics in jobs_metrics.items():

        #if not filename:
        print('id: {job_id} submission: {submission_time} start: {start_time} stop: {stop_time} '
              'walltime: {walltime}  res: {nb_default_ressources} extra: {nb_extra_ressources}'
              .format(**job_metrics.__dict__))


@click.command()
@click.option('--db-url', type=click.STRING,
              help='The url for OAR database (postgresql://oar:PASSWORD@pgsql_server/db_name).')
@click.option('-f', '--swf-file', type=click.STRING, help='SWF output file name.')
@click.option('-b', '--first-jobid', type=int, default=0, help='First job id to begin')
@click.option('-e', '--last-jobid', type=int, default=0, help='Last job id to end')
@click.option('--chunk-size', type=int, default=10000,
              help='Number of size retrieve at one time to limit stress on database')
@click.option('--metadata-file', type=click.STRING,
              help="Metadata file stores various non-anonymized jobs' information (user, job name, project, command")
@click.option('-a', '--additional-fields', is_flag=True,
              help='Add field specific to OAR not to SWF format')
def cli(db_url, swf_file, first_jobid, last_jobid, chunk_size, metadata_file, additional_fields):

    jobids_range = None
    
    if additional_fields:
        print('NOT Yet Implemented')

    if db_url:
        db._cache["uri"] = db_url
        try:
            
            jobids_range = db.query(func.max(Job.id).label('max'),
                                    func.min(Job.id).label('min')).one()
        except Exception as e:
            print(e)
            exit()
    else:
        exit()

    db_name = db_url.split('/')[-1]
    db_server = (db_url.split('/')[-2]).split('@')[-1]
        
    if not first_jobid:
        first_jobid = jobids_range.min

    if not last_jobid:
        last_jobid = jobids_range.max

    if not swf_file:
        swf_file = 'oar_trace_{}_{}_{}_{}.swf'.format(db_server, db_name,
                                                      first_jobid, last_jobid)

    wkld_metadata = WorkloadMetadata(db_server, db_name, first_jobid, last_jobid, metadata_file)

    nb_chunck = int((last_jobid - first_jobid) / chunk_size) + 1

    begin_jobid = first_jobid
    end_jobid = 0
    for chunk in range(nb_chunck):
        if (begin_jobid + chunk_size - 1) > last_jobid:
            end_jobid = last_jobid
        else:
            end_jobid = begin_jobid + chunk_size - 1
        print('# Jobids Range: [{}-{}], Chunck: {}'.format(first_jobid, last_jobid, (chunk + 1)))

        jobs_metrics = get_jobs(first_jobid, last_jobid, wkld_metadata)
        jobs2swf(jobs_metrics, swf_file)

        begin_jobid = end_jobid + 1
    wkld_metadata.dump()
