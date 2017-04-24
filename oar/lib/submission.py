# -*- coding: utf-8 -*-
from __future__ import print_function, division, absolute_import, unicode_literals
import re
import os
import sys
import random

from copy import deepcopy
from sqlalchemy import text, exc

from oar.lib import (db, Job, JobType, AdmissionRule, Challenge, Queue,
                     JobDependencie, JobStateLog, MoldableJobDescription,
                     JobResourceGroup, JobResourceDescription, Resource,
                     config)

from oar.lib.resource import ResourceSet
from oar.lib.hierarchy import find_resource_hierarchies_scattered
from oar.lib.interval import intersec, unordered_ids2itvs, itvs_size
from oar.lib.tools import (sql_to_duration, get_date, sql_to_local)


DEFAULT_CONFIG = {
    'SERVER_HOSTNAME': 'localhost',
    'SERVER_PORT': '6666',
    'OPENSSH_CMD': 'ssh',
    'OAR_SSH_CONNECTION_TIMEOUT': '200',
    'STAGEIN_DIR': '/tmp',
    'JOB_RESOURCE_MANAGER_PROPERTY_DB_FIELD': 'cpuset',
    'CPUSET_PATH': '/oar',
    'DEFAULT_JOB_WALLTIME': 3600,
    'OARSUB_DEFAULT_RESOURCES': '/resource_id=1',
    'OARSUB_NODES_RESOURCES': 'resource_id',
    'queue': 'default',
    'project': 'default',
    'signal': 12
}

def default_submission_config(default_value=None):
    if default_value:
        DEFAULT_CONFIG.update(default_value)
    config.setdefault_config(DEFAULT_CONFIG)

def lstrip_none(s):
    if s:
        return s.lstrip()
    else:
        return None

#TODO to remove
def print_error(*objs):
    print('# ERROR: ', *objs, file=sys.stderr)

#TODO to remove
def print_info(*objs):
    print('# INFO: ', *objs, file=sys.stderr)

    
def job_key_management(use_job_key, import_job_key_inline, import_job_key_file,
                       export_job_key_file):
    # TODO job_key_management
    return(0, '', '')

def parse_resource_descriptions(str_resource_request_list, default_resources, nodes_resources):
    """Parse and transform a cli oar resource request in python structure which is manipulated 
    in admission process

    Resource request output composition:

         resource_request = [moldable_instance , ...]
         moldable_instance =  ( resource_desc_lst , walltime)
         walltime = int|None
         resource_desc_lst = [{property: prop, resources: res}]
         property = string|''|None
         resources = [{resource: r, value: v}]
         r = string
         v = int

    Example:

     - oar cli resource request:
         "{ sql1 }/prop1=1/prop2=3+{sql2}/prop3=2/prop4=1/prop5=1+...,walltime=60"

     - str_resource_request_list input:
         ["/switch=2/nodes=10+{lic_type = 'mathlab'}/licence=2, walltime = 60"]
    
     - resource_request output:
         [
            ([{property: '', resources:  [{resource: 'switch', value: 2}, {resource: 'nodes', value: 10}]},
              {property: "lic_type = 'mathlab'", resources: [{resource: 'licence', value: 2}]}
             ], 60)
         ]
    """  

    if not str_resource_request_list:
        str_resource_request_list = [default_resources]

    resource_request = []  # resource_request = [moldable_instance , ...]
    for str_resource_request in str_resource_request_list:
        res_req_walltime = str_resource_request.split(',')
        str_prop_res_req = res_req_walltime[0]
        walltime = None
        if len(res_req_walltime) == 2:
            walltime_desc = res_req_walltime[1].split('=')
            if len(walltime_desc) == 2:
                str_walltime = walltime_desc[1]
                lg_wallime_lst = len(str_walltime.split(':'))
                if lg_wallime_lst == 1:
                    str_walltime += ':00:00'
                elif lg_wallime_lst == 2:
                    str_walltime += ':00'
                walltime = sql_to_duration(str_walltime)

        prop_res_reqs = str_prop_res_req.split('+')

        resource_desc = []  # resource_desc = [{property: prop, resources: res}]
        for prop_res_req in prop_res_reqs:
            # Extract propertie if any
            m = re.search(r'^\{(.+?)\}(.*)$',  prop_res_req)
            if m:
                property = m.group(1)
                str_res_req = m.group(2)
            else:
                property = ''
                str_res_req = prop_res_req

            str_res_value_lst = str_res_req.split('/')

            resources = []  # resources = [{resource: r, value: v}]

            for str_res_value in str_res_value_lst:

                if str_res_value.lstrip():  # to filter first and last / if any "/nodes=1" or "/nodes=1/
                    # remove  first and trailing spaces"
                    str_res_value_wo_spc = str_res_value.lstrip()
                    res_value = str_res_value_wo_spc.split('=')
                    res = res_value[0]
                    value = res_value[1]
                    if res == 'nodes':
                        res = nodes_resources
                    if value == 'ALL':
                        v = -1
                    elif value == 'BESTHALF':
                        v = -2
                    elif value == 'BEST':
                        v = -3
                    else:
                        v = str(value)
                    resources.append({'resource': res, 'value': v})

            resource_desc.append({'property': property, 'resources': resources})

        resource_request.append((resource_desc, walltime))

    return(resource_request)


def estimate_job_nb_resources(resource_request, j_properties):
    '''returns an array with an estimation of the number of resources that can be  used by a job:
    (resources_available, [(nbresources => int, walltime => int)])
    '''
    # estimate_job_nb_resources
    estimated_nb_resources = []
    resource_available = False
    resource_set = ResourceSet()
    resources_itvs = resource_set.roid_itvs

    for mld_idx, mld_resource_request in enumerate(resource_request):

        resource_desc, walltime = mld_resource_request

        if not walltime:
            walltime = str(config['DEFAULT_JOB_WALLTIME'])

        result = []

        for prop_res in resource_desc:
            jrg_grp_property = prop_res['property']
            resource_value_lst = prop_res['resources']

            #
            # determine resource constraints
            #
            if (not j_properties) and \
               (not jrg_grp_property or (jrg_grp_property == "type = 'default'")):
                constraints = deepcopy(resource_set.roid_itvs)
            else:
                if not j_properties or not jrg_grp_property:
                    and_sql = ""
                else:
                    and_sql = " AND "

                sql_constraints = j_properties + and_sql + jrg_grp_property

                try:
                    request_constraints = db.query(Resource.id).filter(text(sql_constraints)).all()
                except exc.SQLAlchemyError:
                    error_code = -5
                    error_msg = 'Bad resource SQL constraints request:' + sql_constraints + '\n' + \
                                'SQLAlchemyError: ' + str(exc)
                    error = (error_code, error_msg)
                    return(error, None, None)

                roids = [resource_set.rid_i2o[int(y[0])] for y in request_constraints]
                constraints = unordered_ids2itvs(roids)

            hy_levels = []
            hy_nbs = []
            for resource_value in resource_value_lst:
                res_name = resource_value['resource']
                value = resource_value['value']
                hy_levels.append(resource_set.hierarchy[res_name])
                hy_nbs.append(int(value))

            cts_resources_itvs = intersec(constraints, resources_itvs)
            res_itvs = find_resource_hierarchies_scattered(cts_resources_itvs, hy_levels, hy_nbs)
            if res_itvs:
                result.extend(res_itvs)
            else:
                result = []
            break

        if result:
            resource_available = True

        estimated_nb_res = itvs_size(result)
        estimated_nb_resources.append((estimated_nb_res, walltime))
        print_info('Moldable instance: ', mld_idx,
                   ' Estimated nb resources: ', estimated_nb_res,
                   ' Walltime: ', walltime)

    if not resource_available:
        error = (-5, "There are not enough resources for your request")
        return (error, None, None)

    return((0, ''), resource_available, estimated_nb_resources)


def add_micheline_subjob(job_parameters,
                         ssh_private_key, ssh_public_key,
                         array_id, array_index,
                         array_commands,
                         properties_applied_after_validation):

    # Estimate_job_nb_resources and incidentally test if properties and resources request are coherent
    # against avalaible resources

    date = get_date()
    properties = job_parameters.properties
    resource_request = job_parameters.resource_request

    # import pdb; pdb.set_trace()
    # TODO
    error, resource_available, estimated_nb_resources = estimate_job_nb_resources(resource_request, properties)
    if error[0] != 0:
        return(error, -1)

    # Add admin properties to the job
    if properties_applied_after_validation:
        if properties:
            properties = '(' + properties + ') AND ' + properties_applied_after_validation
        else:
            properties = properties_applied_after_validation
    job_parameters.properties = properties
    # TODO Verify the content of the ssh keys

    # TODO format job message
    # message = ''

    # my $job_message = format_job_message_text($job_name,$estimated_nb_resources, $estimated_walltime,
    # $jobType, $reservationField, $queue_name, $project, $type_list, '');

    # TODO  job_group
    #
    name = job_parameters.name
    stdout = job_parameters.stdout
    if not stdout:
        stdout = 'OAR'
        if name:
            stdout += '.' + name
        stdout += ".%jobid%.stdout"
    else:
        stdout = re.sub(r'%jobname%', name, stdout)
    job_parameters.stdout = stdout

    stderr = job_parameters.stderr
    if not stderr:
        stderr = 'OAR'
        if name:
            stderr += '.' + name
        stderr += '.%jobid%.stderr'
    else:
        stderr = re.sub(r'%jobname%', name, stderr)
    stderr = job_parameters
    # Insert job

    kwargs = job_parameters.kwargs(array_commands[0], date)
    kwargs['message'] = ''  # TODO message
    kwargs['array_index'] = array_index

    if array_id > 0:
        kwargs['array_id'] = array_id

    ins = Job.__table__.insert().values(**kwargs)
    result = db.session.execute(ins)
    job_id = result.inserted_primary_key[0]

    if array_id <= 0:
        db.query(Job).filter(Job.id == job_id).update({Job.array_id: job_id})
        db.commit()

    random_number = random.randint(1, 1000000000000)
    ins = Challenge.__table__.insert().values(
        {'job_id': job_id, 'challenge': random_number,
         'ssh_private_key': ssh_private_key, 'ssh_public_key': ssh_public_key})
    db.session.execute(ins)

    # print(resource_request)

    # Insert resources request in DB
    mld_jid_walltimes = []
    resource_desc_lst = []
    for moldable_instance in resource_request:
        resource_desc, walltime = moldable_instance
        if not walltime:
            # TODO add nullable=True in MoldableJobDescription@oar.lib.model.py ?
            walltime = 0
        mld_jid_walltimes.append(
            {'moldable_job_id': job_id, 'moldable_walltime': walltime})
        resource_desc_lst.append(resource_desc)

    # Insert MoldableJobDescription job_id and walltime
    # print('mld_jid_walltimes)
    result = db.session.execute(MoldableJobDescription.__table__.insert(),
                                mld_jid_walltimes)

    # Retrieve MoldableJobDescription.ids
    if len(mld_jid_walltimes) == 1:
        mld_ids = [result.inserted_primary_key[0]]
    else:
        res = db.query(MoldableJobDescription.id)\
                .filter(MoldableJobDescription.job_id == job_id).all()
        mld_ids = [e[0] for e in res]
    #
    # print(mld_ids, resource_desc_lst)
    for mld_idx, resource_desc in enumerate(resource_desc_lst):
        # job_resource_groups
        mld_id_property = []
        res_lst = []

        moldable_id = mld_ids[mld_idx]

        for prop_res in resource_desc:
            prop = prop_res['property']
            res = prop_res['resources']

            mld_id_property.append({'res_group_moldable_id': moldable_id,
                                    'res_group_property': prop})

            res_lst.append(res)

        # print(mld_id_property)
        # Insert property for moldable
        db.session.execute(JobResourceGroup.__table__.insert(),
                           mld_id_property)

        if len(mld_id_property) == 1:
            grp_ids = [result.inserted_primary_key[0]]
        else:
            res = db.query(JobResourceGroup.id)\
                    .filter(JobResourceGroup.moldable_id == moldable_id).all()
            grp_ids = [e[0] for e in res]

        # print('grp_ids, res_lst)
        # Insert job_resource_descriptions
        for grp_idx, res in enumerate(res_lst):
            res_description = []
            for idx, res_value in enumerate(res):
                res_description.append({'res_job_group_id': grp_ids[grp_idx],
                                        'res_job_resource_type': res_value['resource'],
                                        'res_job_value': res_value['value'],
                                        'res_job_order': idx})
            # print(res_description)
            db.session.execute(JobResourceDescription.__table__.insert(),
                               res_description)

    # types of job
    types = job_parameters.types
    if types:
        ins = [{'job_id': job_id, 'type': typ} for typ in types]
        db.session.execute(JobType.__table__.insert(), ins)

    # TODO dependencies with min_start_shift and max_start_shift
    dependencies = job_parameters.dependencies
    if dependencies:
        ins = [{'job_id': job_id, 'job_id_required': dep} for dep in dependencies]
        db.session.execute(JobDependencie.__table__.insert(), ins)
    #    foreach my $a (@{$anterior_ref}){
    #    if (my ($j,$min,$max) = $a =~ /^(\d+)(?:,([\[\]][-+]?\d+)?(?:,([\[\]][-+]?\d+)?)?)?$/) {
    #        $dbh->do("  INSERT INTO job_dependencies (job_id,job_id_required,min_start_shift,max_start_shift)
    #                    VALUES ($job_id,$j,'".(defined($min)?$min:"")."','".(defined($max)?$max:"")."')

    if not job_parameters.hold:
        req = db.insert(JobStateLog).values(
            {'job_id': job_id, 'job_state': 'Waiting', 'date_start': date})
        db.session.execute(req)
        db.commit()

        db.query(Job).filter(Job.id == job_id).update({Job.state: 'Waiting'})
        db.commit()
    else:
        req = db.insert(JobStateLog).values(
            {'job_id': job_id, 'job_state': 'Hold', 'date_start': date})
        db.session.execute(req)
        db.commit()

    return((0, ''), job_id)


def add_micheline_simple_array_job(job_parameters,
                                   ssh_private_key, ssh_public_key,
                                   array_id, array_index,
                                   array_commands,
                                   properties_applied_after_validation):

    job_id_list = []
    date = get_date()

    # Check the jobs are no moldable
    resource_request = job_parameters.resource_request
    if len(resource_request) > 1:
        error = (-30, 'array jobs cannot be moldable')
        return(error, [])

    # Estimate_job_nb_resources and incidentally test if properties and resources request are coherent
    # against avalaible resources
    # pdb.set_trace()
    properties = job_parameters.properties
    # TODO
    error, resource_available, estimated_nb_resources = estimate_job_nb_resources(resource_request, properties)

    #TODO
    
    
    # Add admin properties to the job
    if properties_applied_after_validation:
        if properties:
            properties = '(' + properties + ') AND ' + properties_applied_after_validation
        else:
            properties = properties_applied_after_validation
    job_parameters.properties = properties
    # TODO format job message

    # my $job_message = format_job_message_text($job_name,$estimated_nb_resources, $estimated_walltime,
    # $jobType, $reservationField, $queue_name, $project, $type_list, '');

    name = job_parameters.name
    stdout = job_parameters.stdout
    if not stdout:
        stdout = 'OAR'
        if name:
            stdout += '.' + name
        stdout += ".%jobid%.stdout"
    else:
        stdout = re.sub(r'%jobname%', name, stdout)
    job_parameters.stdout = stdout

    stderr = job_parameters.stderr
    if not stderr:
        stderr = 'OAR'
        if name:
            stderr += '.' + name
        stderr += '.%jobid%.stderr'
    else:
        stderr = re.sub(r'%jobname%', name, stderr)
    stderr = job_parameters.stderr

    # Insert job
    kwargs = job_parameters.kwargs(array_commands[0], date)
    kwargs['message'] = ''  # TODO message
    kwargs['array_index'] = array_index

    # print(kwargs)

    ins = Job.__table__.insert().values(**kwargs)
    result = db.session.execute(ins)
    first_job_id = result.inserted_primary_key[0]

    # Update array_id
    array_id = first_job_id
    db.query(Job).filter(Job.id == first_job_id).update({Job.array_id: array_id})
    db.commit()

    # Insert remaining array jobs with array_id
    jobs_data = []
    kwargs['array_id'] = array_id
    for command in array_commands[1:]:
        job_data = kwargs.copy()
        job_data['command'] = command
        jobs_data.append(job_data)

    db.session.execute(Job.__table__.insert(), jobs_data)
    db.commit()

    # Retrieve job_ids thanks to array_id value
    result = db.query(Job.id).filter(Job.array_id == array_id).all()
    job_id_list = [r[0] for r in result]

    # TODO Populate challenges and moldable_job_descriptions tables
    challenges = []
    moldable_job_descriptions = []

    walltime = resource_request[0][1]
    if not walltime:
        walltime = config['DEFAULT_JOB_WALLTIME']

    for job_id in job_id_list:
        random_number = random.randint(1, 1000000000000)
        challenges.append({'job_id': job_id, 'challenge': random_number})
        moldable_job_descriptions.append({'moldable_job_id': job_id, 'moldable_walltime': walltime})

    db.session.execute(Challenge.__table__.insert(), challenges)
    db.session.execute(MoldableJobDescription.__table__.insert(), moldable_job_descriptions)
    db.commit()

    # Retrieve moldable_ids thanks to job_ids
    result = db.query(MoldableJobDescription.id)\
               .filter(MoldableJobDescription.job_id.in_(tuple(job_id_list)))\
               .order_by(MoldableJobDescription.id).all()
    moldable_ids = [r[0] for r in result]

    # Populate job_resource_groups table
    job_resource_groups = []
    resource_desc_lst = resource_request[0][0]

    for moldable_id in moldable_ids:
        for resource_desc in resource_desc_lst:
            prop = resource_desc['property']
            job_resource_groups.append({'res_group_moldable_id': moldable_id,
                                        'res_group_property': prop})

    db.session.execute(JobResourceGroup.__table__.insert(), job_resource_groups)
    db.commit()

    # Retrieve res_group_ids thanks to moldable_ids
    result = db.query(JobResourceGroup.id)\
               .filter(JobResourceGroup.moldable_id.in_(tuple(moldable_ids)))\
               .order_by(JobResourceGroup.id).all()
    res_group_ids = [r[0] for r in result]

    # Populate job_resource_descriptions table
    job_resource_descriptions = []
    k = 0
    for i in range(len(array_commands)):  # Nb jobs
        for resource_desc in resource_desc_lst:
            order = 0
            for res_val in resource_desc['resources']:
                job_resource_descriptions.append({'res_job_group_id': res_group_ids[k],
                                                  'res_job_resource_type': res_val['resource'],
                                                  'res_job_value': res_val['value'],
                                                  'res_job_order': order})
                order += 1
            k += 1

    db.session.execute(JobResourceDescription.__table__.insert(), job_resource_descriptions)
    db.commit()

    # Populate job_types table
    types = job_parameters.types
    if types:
        job_types = []
        for job_id in job_id_list:
            for typ in types:
                job_types.append({'job_id': job_id, 'type': typ})
        db.session.execute(JobType.__table__.insert(), job_types)
        db.commit()

    # TODO Anterior job setting

    # Hold/Waiting management, job_state_log setting
    # Job is inserted with hold state first
    state_log = 'Hold'
    if job_parameters.hold:
        state_log = 'Waiting'
        db.query(Job).filter(Job.array_id == array_id).update({Job.state: state_log})
        db.commit

    # Update array_id field and set job to state if waiting and insert job_state_log
    job_state_logs = [{'job_id': job_id, 'job_state': state_log, 'date_start': date}
                      for job_id in job_id_list]
    db.session.execute(JobStateLog.__table__.insert(), job_state_logs)
    db.commit()

    return((0,''), job_id_list)


def add_micheline_jobs(job_parameters, import_job_key_inline, import_job_key_file, export_job_key_file):
    '''Adds a new job(or multiple in case of array-job) to the table Jobs applying
    the admission rules from the base  parameters : base, jobtype, nbnodes,
    , command, infotype, walltime, queuename, jobproperties,
    startTimeReservation
    return value : ref. of array of created jobids
    side effects : adds an entry to the table Jobs
                 the first jobid is found taking the maximal jobid from
                 jobs in the table plus 1, the next (if any) takes the next
                 jobid. Array-job submission is atomic and array_index are
                 sequential
                 the rules in the base are pieces of python code directly
                 evaluated here, so in theory any side effect is possible
                 in normal use, the unique effect of an admission rule should
                 be to change parameters
    '''


    #import pdb; pdb.set_trace()
    array_id = 0

    if job_parameters.reservation:
        job_parameters.reservation_field = 'toSchedule'
        job_parameters.start_time = job_parameters.reservation

    # job_vars['user'] = os.environ['OARDO_USER']

    # Check the user validity
    if not re.match(r'[a-zA-Z0-9_-]+', job_parameters.user):
        error = (-11, 'invalid username:', job_parameters.user)
        return (error, [])
    # Verify notify syntax
    if job_parameters.notify and not re.match(r'^\s*(\[\s*(.+)\s*\]\s*)?(mail|exec)\s*:.+$',
                                           job_parameters.notify):
        
        error = (-6, 'bad syntax for the notify option.')
        return (error, [])

    # Check the stdout and stderr path validity
    if job_parameters.stdout and not re.match(r'^[a-zA-Z0-9_.\/\-\%\\ ]+$', job_parameters.stdout):
        error = (12, 'invalid stdout file name (bad character)')
        return (error, [])

    if job_parameters.stderr and not re.match(r'^[a-zA-Z0-9_.\/\-\%\\ ]+$', job_parameters.stderr):
        error = (-13, 'invalid stderr file name (bad character)')
        return (error, [])

    # pdb.set_trace()
    # Retrieve Micheline's rules from the table
    rules = db.query(AdmissionRule.rule)\
              .filter(AdmissionRule.enabled == 'YES')\
              .order_by(AdmissionRule.priority, AdmissionRule.id)\
              .all()
    str_rules = '\n'.join([r[0] for r in rules])

    # This variable is used to add some resources properties restrictions but
    # after the validation (job is queued even if there are not enough
    # resources available)
    properties_applied_after_validation = ''

    # Apply rules
    code = compile(str_rules, '<string>', 'exec')

    try:
        #in exec() globals must be a dict,
        exec(code, job_parameters.__dict__)
    except:
        err = sys.exc_info()
        error = (-2, err[1] + ', a failed admission rule prevented submitting the job.') 
        return(error, [])

    # Test if the queue exists
    if not db.query(Queue).filter(Queue.name == job_parameters.queue).all():
        error = (-8, 'queue ' + job_parameters.queue + ' does not exist')
        return(error, [])

    # TODO move to job class ?
    if job_parameters.array_params:
        array_commands = [job_parameters.command + ' ' + params for params in job_parameters.array_params]
    else:
        array_commands = [job_parameters.command * job_parameters.array_nb]

    array_index = 1
    job_id_list = []
    ssh_private_key = ''
    ssh_public_key = ''
    if job_parameters.array_nb > 1 and not job_parameters.use_job_key:
        # TODO Simple array job submissiom
        # Simple array job submission is used
        (error, job_id_list) = add_micheline_simple_array_job(job_parameters,
                                                              ssh_private_key, ssh_public_key,
                                                              array_id, array_index,
                                                              array_commands,
                                                              properties_applied_after_validation)

    else:
        # single job to submit or when job key is used with array job
        for cmd in array_commands:
            (error_code, ssh_private_key, ssh_public_key) = job_key_management(job_parameters.use_job_key,
                                                                               import_job_key_inline,
                                                                               import_job_key_file,
                                                                               export_job_key_file)
            if error_code != 0:
                error = (error_code, 'job key generation and management failed')
                return(error, job_id_list)

            (error, job_id) = add_micheline_subjob(job_parameters,
                                                   ssh_private_key, ssh_public_key,
                                                   array_id, array_index,
                                                   array_commands,
                                                   properties_applied_after_validation)

            if error[0] == 0:
                job_id_list.append(job_id)
            else:
                return(error, job_id_list)

            if array_id <= 0:
                array_id = job_id_list[0]
            array_index += 1

            if job_parameters.use_job_key and export_job_key_file:
                # TODO copy the keys in the directory specified with the right name
                pass

    return((0,''), job_id_list)

def check_reservation(reservation):
    reservation = lstrip_none(reservation)
    if reservation:
        m = re.search(r'^\s*(\d{4}\-\d{1,2}\-\d{1,2})\s+(\d{1,2}:\d{1,2}:\d{1,2})\s*$',
                      reservation)
        if m:
            reservation_date = sql_to_local(m.group(1) + ' ' + m.group(2))
            return (0, reservation_date) 
        else:
            error = (7, 'syntax error for the advance reservation start date \
            specification. Expected format is:"YYYY-MM-DD hh:mm:ss"')
            return (error, None)

class JobParameters():
    def __init__(self, **kwargs):
        for key in ['job_type', 'resource', 'command', 'info_type',
                    'queue', 'properties', 'checkpoint', 'signal',
                    'notify', 'name', 'types', 'directory',
                    'dependencies', 'stdout', 'stderr', 'hold',
                    'project', 'initial_request', 'user',
                    'interactive', 'reservation', 'connect', 'scanscript',
                    'array', 'array_params', 'array_param_file',
                    'use_job_key', 'import_job_key_inline',
                    'import_job_key_file', 'export_job_key_file']:
            if key in kwargs:
                setattr(self, key, kwargs[key])
            else:
                setattr(self, key, None)

        #import pdb; pdb.set_trace()

        if not self.initial_request:
            self.initial_request = self.command

        if self.array:
            self.array_nb = self.array
        else:
            self.array_nb = 1

        if not self.queue:
            self.queue = config['queue']
            
        if not self.project:
            self.project = config['project']

        if not self.signal:
            self.signal = config['signal']

        if self.directory:
            self.launching_directory = self.directory
        else:
            self.launching_directory = config['directory']

        self.array_id = 0
        self.start_time = 0
        self.reservation_field = 'None'

        # prepare and build resource_request
        default_resources = config['OARSUB_DEFAULT_RESOURCES']
        nodes_resources = config['OARSUB_NODES_RESOURCES']
        self.resource_request = parse_resource_descriptions(self.resource, default_resources, nodes_resources)

        # Check the default name of the key if we have to generate it
        try:
            getattr(self, 'use_job_key')
        except AttributeError:
            if ('OARSUB_FORCE_JOB_KEY' in config) and (config['OARSUB_FORCE_JOB_KEY'] == 'yes'):
                self.use_job_key = True
            else:
                self.use_job_key = False
                
    def check_parameters(self):
        if not self.command and not self.interactive and not self.reservation and not self.connect:
            return (5, 'Command or interactive flag or advance reservation time or connection directive must be provided')

        if self.interactive and self.reservation:
            return (7, 'An advance reservation cannot be interactive.')

        if self.interactive and any(re.match(r'^desktop_computing$', t) for t in self.types):
            return (17, 'A desktop computing job cannot be interactive')

        if any(re.match(r'^noop$', t) for t in self.types):
            if self.interactive:
                return (17, 'a NOOP job cannot be interactive.')
            elif self.connect:
                return(17, 'A NOOP job does not have a shell to connect to.')

        # notify : check insecure character
        if self.notify and re.match(r'^.*exec\s*:.+$', self.notify):
            m = re.search(r'.*exec\s*:([a-zA-Z0-9_.\/ -]+)$', self.notify)
            if not m:
                return(16, 'insecure characters found in the notification method (the allowed regexp is: [a-zA-Z0-9_.\/ -]+).')

        return (0, '')


    def kwargs(self, command, date):
        kwargs = {}
        kwargs['submission_time'] = date
        kwargs['command'] = command
        kwargs['state'] = 'Hold'
        
        for key in ['job_type', 'info_type', 'properties', 'launching_directory',
                     'start_time', 'checkpoint', 'notify', 'project', 'initial_request',
                     'array_id']:
            # TODO DEBUG ('stdout', (''"' + stdout + '"'
            # TODO DEBUG kwargs['stderr', (''"' + stderr + '"'
            
            kwargs[key] = getattr(self, key)


        kwargs['job_user'] = self.user
        kwargs['queue_name'] = self.queue
        kwargs['job_name'] = self.name
        kwargs['checkpoint_signal'] = self.signal
        kwargs['reservation'] = self.reservation_field
            
        # print(kwargs)
        return kwargs

class Submission():
    def __init__(self, job_parameters):
        self.job_parameters = job_parameters

    def submit(self):
        import_job_key_inline = self.job_parameters.import_job_key_inline
        import_job_key_file = self.job_parameters.import_job_key_file
        export_job_key_file = self.job_parameters.export_job_key_file

        (err, job_id_lst) = add_micheline_jobs(self.job_parameters, import_job_key_inline, \
                                               import_job_key_file, export_job_key_file)
        return(err, job_id_lst)
