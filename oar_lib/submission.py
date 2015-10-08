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
from oar.lib.tools import (sql_to_duration, get_date)

not_cli = False


DEFAULT_CONFIG = {
    'DEFAULT_JOB_WALLTIME': '3600'
    }
config.setdefault_config(DEFAULT_CONFIG)

# Default value for walltime when it's not provided
default_job_walltime = str(config['DEFAULT_JOB_WALLTIME'])


def set_not_cli():
    global not_cli
    not_cli = True


def print_warning(*objs):
    print('# WARNING: ', *objs, file=sys.stderr)


def print_error(*objs):
    print('# ERROR: ', *objs, file=sys.stderr)


def print_info(*objs):
    print('# INFO: ', *objs, file=sys.stderr)


def sub_exit(num, result=''):
    if not_cli:
        return (num, result)
    else:
        if result:
            print(result)
        exit(num)


def job_key_management(use_job_key, import_job_key_inline, import_job_key_file,
                       export_job_key_file):
    # TODO job_key_management
    return(0, '', '')


def job_kwargs(job_vars, command, date):
    kwargs = {}
    kwargs['submission_time'] = date
    kwargs['command'] = command
    kwargs['state'] = 'Hold'

    kwargs = {}
    for keys in [('job_type', 'job_type'), ('info_type', 'info_type'), ('job_user', 'user'),
                 ('queue_name', 'queue_name'), ('properties', 'properties'),
                 ('launching_directory', 'launching_directory'),
                 ('start_time', 'start_time'),
                 ('checkpoint', 'checkpoint'), ('job_name', 'name'),
                 ('notify', 'notify'), ('checkpoint_signal', 'signal'),
                 ('project', 'project'), ('initial_request', 'initial_request'),
                 ('array_id', 'array_id')]:
        # TODO DEBUG ('stdout', (''"' + stdout + '"'
        # TODO DEBUG kwargs['stderr', (''"' + stderr + '"'
        k1, k2 = keys
        kwargs[k1] = job_vars[k2]
    if job_vars['reservation_field']:
        kwargs['reservation'] = job_vars['reservation_field']
        # print(kwargs)
    return kwargs


def parse_resource_descriptions(str_resource_request_list, default_resources, nodes_resources):
    # "{ sql1 }/prop1=1/prop2=3+{sql2}/prop3=2/prop4=1/prop5=1+...,walltime=60"
    # transform to 'à la' perl structure use in admission rules
    #
    # resource_request = [moldable_instance , ...]
    # moldable_instance =  ( resource_desc_lst , walltime)
    # walltime = int|None
    # resource_desc_lst = [{property: prop, resources: res}]
    # property = string|''|None
    # resources = [{resource: r, value: v}]
    # r = string
    # v = int
    #
    #   "{ sql1 }/prop1=1/prop2=3+{sql2}/prop3=2/prop4=1/prop5=1+...,walltime=60"
    #
    #  str_resource_request_list = ["/switch=2/nodes=10+{lic_type = 'mathlab'}/licence=2, walltime = 60"]
    #
    #  resource_request = [
    #                      ([{property: '', resources:  [{resource: 'switch', value: 2},
    #                                                    {resource: 'nodes', value: 10}]},
    #                        {property: "lic_type = 'mathlab'", resources: [{resource: 'licence', value: 2}]}
    #                       ], 60)
    #                     ]

    # "{gpu='YES'}/nodes=2/core=4+{gpu='NO'}/core=20"

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
            walltime = default_job_walltime

        result = []

        for prop_res in resource_desc:
            jrg_grp_property = prop_res['property']
            resource_value_lst = prop_res['resources']

            #
            # determine resource constraints
            #
            if (not j_properties) and (not jrg_grp_property or (jrg_grp_property == "type = 'default'")):
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
                    print_error('Bad resource SQL constraints request:', sql_constraints)
                    print_error('SQLAlchemyError: ', exc)
                    result = []
                    break

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
        print_info('Modlable instance: ', mld_idx,
                   ' Estimated nb resources: ', estimated_nb_res,
                   ' Walltime: ', walltime)

    if not resource_available:
        print_error("There are not enough resources for your request")
        sub_exit(-5)

    return(resource_available, estimated_nb_resources)


def add_micheline_subjob(job_vars,
                         ssh_private_key, ssh_public_key,
                         array_id, array_index,
                         array_commands,
                         properties_applied_after_validation):

    # Estimate_job_nb_resources and incidentally test if properties and resources request are coherent
    # against avalaible resources
    # pdb.set_trace()
    date = get_date()
    properties = job_vars['properties']
    resource_request = job_vars['resource_request']
    resource_available, estimated_nb_resources = estimate_job_nb_resources(resource_request, properties)

    # Add admin properties to the job
    if properties_applied_after_validation:
        if properties:
            properties = '(' + properties + ') AND ' + properties_applied_after_validation
        else:
            properties = properties_applied_after_validation
    job_vars['properties'] = properties
    # TODO Verify the content of the ssh keys

    # TODO format job message
    # message = ''

    # my $job_message = format_job_message_text($job_name,$estimated_nb_resources, $estimated_walltime,
    # $jobType, $reservationField, $queue_name, $project, $type_list, '');

    # TODO  job_group
    #
    name = job_vars['name']
    stdout = job_vars['stdout']
    if not stdout:
        stdout = 'OAR'
        if name:
            stdout += '.' + name
        stdout += ".%jobid%.stdout"
    else:
        stdout = re.sub(r'%jobname%', name, stdout)
    job_vars['stdout'] = stdout

    stderr = job_vars['stderr']
    if not stderr:
        stderr = 'OAR'
        if name:
            stderr += '.' + name
        stderr += '.%jobid%.stderr'
    else:
        stderr = re.sub(r'%jobname%', name, stderr)
    stderr = job_vars['stderr']

    # Insert job

    kwargs = job_kwargs(job_vars, array_commands[0], date)
    kwargs['message'] = ''  # TODO message
    kwargs['array_index'] = array_index

    if array_id > 0:
        kwargs['array_id'] = array_id

    ins = Job.__table__.insert().values(**kwargs)
    result = db.engine.execute(ins)
    job_id = result.inserted_primary_key[0]

    if array_id <= 0:
        db.query(Job).filter(Job.id == job_id).update({Job.array_id: job_id})
        db.commit()

    random_number = random.randint(1, 1000000000000)
    ins = Challenge.__table__.insert().values(
        {'job_id': job_id, 'challenge': random_number,
         'ssh_private_key': ssh_private_key, 'ssh_public_key': ssh_public_key})
    db.engine.execute(ins)

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
    db.engine.execute(MoldableJobDescription.__table__.insert(),
                      mld_jid_walltimes)

    # Retrieve MoldableJobDescription.ids
    if len(mld_jid_walltimes) == 1:
        mld_ids = [result.inserted_primary_key[0]]
    else:
        r = db.query(MoldableJobDescription.id)\
              .filter(MoldableJobDescription.job_id == job_id).all()
        mld_ids = [e[0] for e in r]
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
        db.engine.execute(JobResourceGroup.__table__.insert(),
                          mld_id_property)

        if len(mld_id_property) == 1:
            grp_ids = [result.inserted_primary_key[0]]
        else:
            r = db.query(JobResourceGroup.id)\
                  .filter(JobResourceGroup.moldable_id == moldable_id).all()
            grp_ids = [e[0] for e in r]

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
            db.engine.execute(JobResourceDescription.__table__.insert(),
                              res_description)

    # types of job
    types = job_vars['types']
    if types:
        ins = [{'job_id': job_id, 'type': typ} for typ in types]
        db.engine.execute(JobType.__table__.insert(), ins)

    # TODO dependencies with min_start_shift and max_start_shift
    dependencies = job_vars['dependencies']
    if dependencies:
        ins = [{'job_id': job_id, 'job_id_required': dep} for dep in dependencies]
        db.engine.execute(JobDependencie.__table__.insert(), ins)
    #    foreach my $a (@{$anterior_ref}){
    #    if (my ($j,$min,$max) = $a =~ /^(\d+)(?:,([\[\]][-+]?\d+)?(?:,([\[\]][-+]?\d+)?)?)?$/) {
    #        $dbh->do("  INSERT INTO job_dependencies (job_id,job_id_required,min_start_shift,max_start_shift)
    #                    VALUES ($job_id,$j,'".(defined($min)?$min:"")."','".(defined($max)?$max:"")."')

    if not job_vars['hold']:
        req = db.insert(JobStateLog).values(
            {'job_id': job_id, 'job_state': 'Waiting', 'date_start': date})
        db.engine.execute(req)
        db.commit()

        db.query(Job).filter(Job.id == job_id).update({Job.state: 'Waiting'})
        db.commit()
    else:
        req = db.insert(JobStateLog).values(
            {'job_id': job_id, 'job_state': 'Hold', 'date_start': date})
        db.engine.execute(req)
        db.commit()

    return(0, job_id)


def add_micheline_simple_array_job(job_vars,
                                   ssh_private_key, ssh_public_key,
                                   array_id, array_index,
                                   array_commands,
                                   properties_applied_after_validation):

    job_id_list = []
    date = get_date()

    # Check the jobs are no moldable
    resource_request = job_vars['resource_request']
    if len(resource_request) > 1:
        print_error('array jobs cannot be moldable')
        sub_exit(-30)

    # Estimate_job_nb_resources and incidentally test if properties and resources request are coherent
    # against avalaible resources
    # pdb.set_trace()
    properties = job_vars['properties']
    resource_available, estimated_nb_resources = estimate_job_nb_resources(resource_request, properties)

    # Add admin properties to the job
    if properties_applied_after_validation:
        if properties:
            properties = '(' + properties + ') AND ' + properties_applied_after_validation
        else:
            properties = properties_applied_after_validation
    job_vars['properties'] = properties
    # TODO format job message

    # my $job_message = format_job_message_text($job_name,$estimated_nb_resources, $estimated_walltime,
    # $jobType, $reservationField, $queue_name, $project, $type_list, '');

    name = job_vars['name']
    stdout = job_vars['stdout']
    if not stdout:
        stdout = 'OAR'
        if name:
            stdout += '.' + name
        stdout += ".%jobid%.stdout"
    else:
        stdout = re.sub(r'%jobname%', name, stdout)
    job_vars['stdout'] = stdout

    stderr = job_vars['stderr']
    if not stderr:
        stderr = 'OAR'
        if name:
            stderr += '.' + name
        stderr += '.%jobid%.stderr'
    else:
        stderr = re.sub(r'%jobname%', name, stderr)
    stderr = job_vars['stderr']

    # Insert job
    kwargs = job_kwargs(job_vars, array_commands[0], date)
    kwargs['message'] = ''  # TODO message
    kwargs['array_index'] = array_index

    # print(kwargs)

    ins = Job.__table__.insert().values(**kwargs)
    result = db.engine.execute(ins)
    first_job_id = result.inserted_primary_key[0]

    # Update array_id
    array_id = first_job_id
    db.query(Job).filter(Job.id == first_job_id).update({Job.array_id: array_id})
    db.commit()

    # Insert remaining array jobs with array_id
    jobs_data = []
    for command in array_commands[1:]:
        job_data = kwargs.copy()
        job_data['command'] = command
        jobs_data.append(job_data)

    db.engine.execute(Job.__table__.insert(), jobs_data)
    db.commit()

    # Retreive job_ids thanks to array_id value
    result = db.query(Job.id).filter(Job.array_id == array_id).all()
    job_id_list = [r[0] for r in result]

    # TODO Populate challenges and moldable_job_descriptions tables
    challenges = []
    moldable_job_descriptions = []

    walltime = resource_request[0][1]
    if not walltime:
        walltime = default_job_walltime

    for job_id in job_id_list:
        random_number = random.randint(1, 1000000000000)
        challenges.append({'job_id': job_id, 'challenge': random_number})
        moldable_job_descriptions.append({'moldable_job_id': job_id, 'moldable_walltime': walltime})

    db.engine.execute(Challenge.__table__.insert(), challenges)
    db.engine.execute(MoldableJobDescription.__table__.insert(), moldable_job_descriptions)
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

    db.engine.execute(JobResourceGroup.__table__.insert(), job_resource_groups)
    db.commit()

    # Retrieve res_group_ids thanks to moldable_ids
    result = db.query(JobResourceGroup.id)\
               .filter(JobResourceGroup.moldable_id.in_(tuple(moldable_ids)))\
               .order_by(JobResourceGroup.id).all()
    res_group_ids = [r[0] for r in result]

    # Populate job_resource_descriptions table
    k = 0
    job_resource_descriptions = []
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

    db.engine.execute(JobResourceDescription.__table__.insert(), job_resource_descriptions)
    db.commit()

    # Populate job_types table
    types = job_vars['types']
    if types:
        job_types = []
        for job_id in job_id_list:
            for typ in types:
                job_types.append({'job_id': job_id, 'type': typ})
        db.engine.execute(JobType.__table__.insert(), job_types)
        db.commit()

    # TODO Anterior job setting

    # Hold/Waiting management, job_state_log setting
    # Job is inserted with hold state first
    state_log = 'Hold'
    if job_vars['hold']:
        state_log = 'Waiting'
        db.query(Job).filter(Job.array_id == array_id).update({Job.state: state_log})
        db.commit

    # Update array_id field and set job to state if waiting and insert job_state_log
    job_state_logs = [{'job_id': job_id, 'job_state': state_log, 'date_start': date}
                      for job_id in job_id_list]
    db.engine.execute(JobStateLog.__table__.insert(), job_state_logs)
    db.commit()

    return(0, job_id_list)


def add_micheline_jobs(job_vars, reservation_date, use_job_key,
                       import_job_key_inline, import_job_key_file,
                       export_job_key_file, initial_request, array_nb, array_params):
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

    # pdb.set_trace()
    array_id = 0

    if reservation_date:
        job_vars['reservation_field'] = 'toSchedule'
        job_vars['start_time'] = reservation_date

    # job_vars['user'] = os.environ['OARDO_USER']

    # Check the user validity
    if not re.match(r'[a-zA-Z0-9_-]+', job_vars['user']):
        print_error('invalid username:', job_vars['user'])
        sub_exit(-11)

    # TVerify notify syntax
    if job_vars['notify'] and not re.match(r'^\s*(\[\s*(.+)\s*\]\s*)?(mail|exec)\s*:.+$',
                                           job_vars['notify']):
        print_error('bad syntax for the notify option.')
        return (-6, [])

    # Check the stdout and stderr path validity
    if job_vars['stdout'] and not re.match(r'^[a-zA-Z0-9_.\/\-\%\\ ]+$', job_vars['stdout']):
        print_error('invalid stdout file name (bad character)')
        return (-12, [])

    if job_vars['stderr'] and not re.match(r'^[a-zA-Z0-9_.\/\-\%\\ ]+$', job_vars['stderr']):
        print_error('invalid stderr file name (bad character)')
        return (-13, [])

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
        exec(code, job_vars)
    except:
        err = sys.exc_info()
        print_error(err[1])
        print_error('a failed admission rule prevented submitting the job.')
        sub_exit(-2)

    # Test if the queue exists
    if not db.query(Queue).filter(Queue.name == job_vars['queue_name']).all():
        print_error('queue ', job_vars['queue_name'], ' does not exist')
        sub_exit(-8)

    if array_params:
        array_commands = [job_vars['command'] + ' ' + params for params in array_params]
    else:
        array_commands = [job_vars['command'] * array_nb]

    array_index = 1
    job_id_list = []
    ssh_private_key = ''
    ssh_public_key = ''
    if array_nb > 1 and not use_job_key:
        # TODO Simple array job submissiom
        # Simple array job submission is used
        (error, job_id) = add_micheline_simple_array_job(job_vars,
                                                         ssh_private_key, ssh_public_key,
                                                         array_id, array_index,
                                                         array_commands,
                                                         properties_applied_after_validation)

    else:
        # single job to submit or when job key is used with array job
        for cmd in array_commands:
            (error, ssh_private_key, ssh_public_key) = job_key_management(use_job_key,
                                                                          import_job_key_inline,
                                                                          import_job_key_file,
                                                                          export_job_key_file)
            if error != 0:
                print_error('job key generation and management failed :' + str(err))
                return(err, job_id_list)

            (error, job_id) = add_micheline_subjob(job_vars,
                                                   ssh_private_key, ssh_public_key,
                                                   array_id, array_index,
                                                   array_commands,
                                                   properties_applied_after_validation)

            if error == 0:
                job_id_list.append(job_id)
            else:
                return(error, job_id_list)

            if array_id <= 0:
                array_id = job_id_list[0]
            array_index += 1

            if use_job_key and export_job_key_file:
                # TODO copy the keys in the directory specified with the right name
                pass

    return(0, job_id_list)
