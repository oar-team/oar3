# -*- coding: utf-8 -*-
from __future__ import print_function, division, absolute_import, unicode_literals
import re
import os
import sys
import socket
import signal
import click
click.disable_unicode_literals_warning = True

from oar.lib import config

from oar.lib import (db, Job)

from oar.lib.submission import (print_warning, print_error, print_info, sub_exit,
                                parse_resource_descriptions, add_micheline_jobs)

from oar.lib.tools import sql_to_local
import oar.lib.tools as tools


DEFAULT_VALUE = {
    'directory': os.getcwd(),
    'project': 'default',
    'signal': 12
}

DEFAULT_CONFIG = {
    'SERVER_HOSTNAME': 'localhost',
    'SERVER_PORT': '6666',
    'OPENSSH_CMD': 'ssh',
    'OAR_SSH_CONNECTION_TIMEOUT': '200',
    'STAGEIN_DIR': '/tmp',
    'JOB_RESOURCE_MANAGER_PROPERTY_DB_FIELD': 'cpuset',
    'CPUSET_PATH': '/oar',
    'DEFAULT_JOB_WALLTIME': '3600'
}
config.setdefault_config(DEFAULT_CONFIG)


# When the walltime of a job is not defined
default_job_walltime = str(config['DEFAULT_JOB_WALLTIME'])

log_warning = ''  # TODO
log_error = ''
log_info = ''
log_std = ''


def lstrip_none(str):
    if str:
        return str.lstrip()
    else:
        return None


def init_tcp_server():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((socket.getfqdn(), 0))
    s.listen(5)
    return s


def qdel(signal, frame):
    '''To address ^C in interactive submission.'''
    # TODO launch a qdel
    print('TODO launch a qdel')

signal.signal(signal.SIGINT, qdel)
signal.signal(signal.SIGHUP, qdel)
signal.signal(signal.SIGPIPE, qdel)


def connect_job(job_id, stop_oarexec, openssh_cmd):
    '''Connect to a job and give the shell of the user on the remote host.'''
    # TODO connect_job
    print('TODO connect_job')


def resubmit_job(job_id):
    print('TODO resubmit_job')
    # TODO resubmit_job


def usage():
    '''Print usage message.'''
    ctx = click.get_current_context()
    click.echo(ctx.get_help())


# Move to oar.lib.tool

def signal_almighty(remote_host, remote_port, msg):
    print('TODO signal_almighty')
    return 1


@click.command()
@click.argument('command', required=False)
@click.option('-I', '--interactive', is_flag=True,
              help='Interactive mode. Give you a shell on the first reserved node.')
@click.option('-q', '--queue', default='default',
              help='Specifies the destination queue. If not defined the job is enqueued in default queue')
@click.option('-l', '--resource', type=click.STRING, multiple=True,
              help="Defines resource list requested for a job: resource=value[,[resource=value]...]\
              Defined resources :\
              nodes : Request number of nodes (special value 'all' is replaced by the number of free\
              nodes corresponding to the weight and properties; special value ``max'' is replaced by\
              the number of free,absent and suspected nodes corresponding to the weight and properties).\
              walltime : Request maximun time. Format is [hour:mn:sec|hour:mn|mn]\
              weight : the weight that you want to reserve on each node")
@click.option('-p', '--property', type=click.STRING,
              help='Specify with SQL syntax reservation properties.')
@click.option('-r', '--reservation', type=click.STRING,
              help='Ask for an advance reservation job on the date in argument.')
@click.option('-C', '--connect', type=int,
              help='Connect to a reservation in Running state.')
@click.option('--array', type=int,
              help="Specify an array job with 'number' subjobs")
@click.option('--array-param-file', type=click.STRING,
              help='Specify an array job on which each subjob will receive one line of the \
              file as parameter')
@click.option('-S', '--scanscript', is_flag=True,
              help='Batch mode only: asks oarsub to scan the given script for OAR directives \
              (#OAR -l ...)')
@click.option('-k', '--checkpoint', type=int, default=0,
              help='Specify the number of seconds before the walltime when OAR will send \
              automatically a SIGUSR2 on the process.')
@click.option('--signal', type=int,
              help='Specify the signal to use when checkpointing Use signal numbers, \
              default is 12 (SIGUSR2)')
@click.option('-t', '--type', type=click.STRING, multiple=True,
              help='Specify a specific type (deploy, besteffort, cosystem, checkpoint, timesharing).')
@click.option('-d', '--directory', type=click.STRING,
              help='Specify the directory where to launch the command (default is current directory)')
@click.option('--project', type=click.STRING,
              help='Specify a name of a project the job belongs to.')
@click.option('--name', type=click.STRING,
              help='Specify an arbitrary name for the job')
@click.option('-a', '--after', type=click.STRING, multiple=True,
              help='Add a dependency to a given job, with optional min and max relative \
              start time constraints (<job id>[,m[,M]]).')
@click.option('--notify', type=click.STRING,
              help='Specify a notification method (mail or command to execute). Ex: \
              --notify "mail:name\@domain.com"\
              --notify "exec:/path/to/script args"')
@click.option('-k', '--use-job-key', is_flag=True,
              help='Activate the job-key mechanism.')
@click.option('-i', '--import-job-key-from-file', type=click.STRING,
              help='Import the job-key to use from a files instead of generating a new one.')
@click.option('--import-job-key-inline', type=click.STRING,
              help='Import the job-key to use inline instead of generating a new one.')
@click.option('-e', '--export-job-key-to-file', type=click.STRING,
              help='Export the job key to a file. Warning: the\
              file will be overwritten if it already exists.\
              (the %jobid% pattern is automatically replaced)')
@click.option('-O', '--stdout', type=click.STRING,
              help='Specify the file that will store the standart output \
              stream of the job. (the %jobid% pattern is automatically replaced)')
@click.option('-E', '--stderr', type=click.STRING,
              help='Specify the file that will store the standart error stream of the job.')
@click.option('--hold', is_flag=True,
              help='Set the job state into Hold instead of Waiting,\
              so that it is not scheduled (you must run "oarresume" to turn it into the Waiting state)')
@click.option('--resubmit', type=int,
              help='Resubmit the given job as a new one.')
@click.option('-V', '--version', is_flag=True,
              help='Print OAR version number.')
def cli(command, interactive, queue, resource, reservation, connect,
        type, checkpoint, property, resubmit, scanscript, project, signal,
        directory, name, after, notify, array, array_param_file,
        use_job_key, import_job_key_from_file, import_job_key_inline, export_job_key_to_file,
        stdout, stderr, hold, version):
    """Submit a job to OAR batch scheduler."""

    # pdb.set_trace()
    print(resource)

    remote_host = config['SERVER_HOSTNAME']
    remote_port = int(config['SERVER_PORT'])

    if 'OARSUB_DEFAULT_RESOURCES' in config:
        default_resources = config['OARSUB_DEFAULT_RESOURCES']
    else:
        default_resources = '/resource_id=1'

    if 'OARSUB_NODES_RESOURCES' in config:
        nodes_resources = config['OARSUB_NODES_RESOURCES']
    else:
        nodes_resources = 'resource_id'

    # TODO Deploy_hostname / Cosystem_hostname
    # $Deploy_hostname = get_conf("DEPLOY_HOSTNAME");
    # if (!defined($Deploy_hostname)){
    #    $Deploy_hostname = $remote_host;
    # }

    # $Cosystem_hostname = get_conf("COSYSTEM_HOSTNAME");
    # if (!defined($Cosystem_hostname)){
    #    $Cosystem_hostname = $remote_host;
    # }

    cpuset_field = config['JOB_RESOURCE_MANAGER_PROPERTY_DB_FIELD']
    cpuset_path = config['CPUSET_PATH']

    if 'OAR_RUNTIME_DIRECTORY' in config:
        pass
    # if (is_conf("OAR_RUNTIME_DIRECTORY")){
    #  OAR::Sub::set_default_oarexec_directory(get_conf("OAR_RUNTIME_DIRECTORY"));
    # }

    # my $default_oar_dir = OAR::Sub::get_default_oarexec_directory();
    # if (!(((-d $default_oar_dir) and (-O $default_oar_dir)) or (mkdir($default_oar_dir)))){
    #    die("# Error: failed to create the OAR directory $default_oar_dir, or bad permissions.\n");
    # }

    binpath = ''
    if 'OARDIR' in os.environ:
        binpath = os.environ['OARDIR'] + '/'
    else:
        print_error('OARDIR environment variable is not defined.')
        sub_exit(1)

    openssh_cmd = config['OPENSSH_CMD']
    ssh_timeout = int(config['OAR_SSH_CONNECTION_TIMEOUT'])

    # if (is_conf("OAR_SSH_CONNECTION_TIMEOUT")){
    #    OAR::Sub::set_ssh_timeout(get_conf("OAR_SSH_CONNECTION_TIMEOUT"));
    # }

    # OAR version
    # TODO: OAR is now a set of composition...

    #
    types = type

    properties = lstrip_none(property)
    if not directory:
        launching_directory = ''
    else:
        launching_directory = lstrip_none(directory)

    initial_request = ' '.join(sys.argv[1:])
    queue_name = lstrip_none(queue)
    reservation_date = lstrip_none(reservation)

    if reservation_date:
        m = re.search(r'^\s*(\d{4}\-\d{1,2}\-\d{1,2})\s+(\d{1,2}:\d{1,2}:\d{1,2})\s*$',
                      reservation)
        if m:
            reservation_date = sql_to_local(m.group(1) + ' ' + m.group(2))
        else:
            print_error('syntax error for the advance reservation start date \
            specification. Expected format is:"YYYY-MM-DD hh:mm:ss"')
            sub_exit(7)

    if array:
        array_nb = array
    else:
        array_nb = 1

    # Check the default name of the key if we have to generate it
    if ('OARSUB_FORCE_JOB_KEY' in config) and config['OARSUB_FORCE_JOB_KEY'] == 'yes':
        use_job_key = True
    else:
        use_job_key = False

    # TODO ssh_private_key, ssh_public_key,
    # ssh_private_key = ''
    # ssh_public_key = ''

    # TODO import_job_key_file, export_job_key_file
    import_job_key_file = ''
    export_job_key_file = ''

    if resubmit:
        print('# Resubmitting job ', resubmit, '...')
        ret = resubmit_job(resubmit)
        if ret > 0:
            job_id = ret
            print(' done.\n')
            print('OAR_JOB_ID=' + str(job_id))
            if signal_almighty(remote_host, remote_port, 'Qsub') > 0:
                print_error('cannot connect to executor ' + str(remote_host) + ':' +
                            str(remote_port) + '. OAR server might be down.')
                sub_exit(3)
            else:
                sub_exit(0)
        else:
            print(' error.')
            if ret == -1:
                print_error('interactive jobs and advance reservations cannot be resubmitted.')
            elif ret == -2:
                print_error('only jobs in the Error or Terminated state can be resubmitted.')
            elif ret == -3:
                print_error('resubmitted job user mismatch.')
            elif ret == -4:
                print_error('another active job is using the same job key.')
            else:
                print_error('unknown error.')
            sub_exit(4)

    if not command and not interactive and not reservation and not connect:
        usage()
        sub_exit(5)

    if interactive and reservation:
        print_error('an advance reservation cannot be interactive.')
        usage()
        sub_exit(7)

    if interactive and any(re.match(r'^desktop_computing$', t) for t in type):
        print_error(' a desktop computing job cannot be interactive')
        usage()
        sub_exit(17)

    if any(re.match(r'^noop$', t) for t in type):
        if interactive:
            print_error('a NOOP job cannot be interactive.')
            sub_exit(17)
        elif connect:
            print_error('a NOOP job does not have a shell to connect to.')
            sub_exit(17)

    # notify : check insecure character
    if notify and re.match(r'^.*exec\s*:.+$'):
        m = re.search(r'.*exec\s*:([a-zA-Z0-9_.\/ -]+)$', notify)
        if not m:
            print_error('insecure characters found in the notification method \
            (the allowed regexp is: [a-zA-Z0-9_.\/ -]+).')
            sub_exit(16)

    # TODO   Connect to a reservation
    # Connect to a reservation
    # if (defined($connect_job)){
    # Do not kill the job if the user close the window
    #  $SIG{HUP} = 'DEFAULT';
    #  OAR::Sub::close_db_connection(); exit(connect_job($connect_job,0,$Openssh_cmd));
    # }

    if not project:
        project = DEFAULT_VALUE['project']
    if not signal:
        signal = DEFAULT_VALUE['signal']
    if not directory:
        directory = DEFAULT_VALUE['directory']

    resource_request = parse_resource_descriptions(resource, default_resources, nodes_resources)

    job_vars = {
        'job_type': None,
        'resource_request': resource_request,
        'command': command,
        'info_type': None,
        'queue_name': queue_name,
        'properties': properties,
        'checkpoint': checkpoint,
        'signal': signal,
        'notify': notify,
        'name': name,
        'types': types,
        'launching_directory': launching_directory,
        'dependencies': after,
        'stdout': stdout,
        'stderr': stderr,
        'hold': hold,
        'project': project,
        'initial_request': initial_request,
        'user': os.environ['OARDO_USER'],
        'array_id': 0,
        'start_time': '0',
        'reservation_field': None,
    }

    if not interactive and command:

        cmd_executor = 'Qsub'

        if scanscript:
            # TODO scanscript
            pass

        array_params = []
        if array_param_file:
            pass
        # TODO
        # $array_params_ref = OAR::Sub::read_array_param_file($array_param_file);
        # $array_nb = scalar @{$array_params_ref};

        if array_nb == 0:
            print_error('an array of job must have a number of sub-jobs greater than 0.')
            usage()
            sub_exit(6)

        job_vars['info_type'] = "$Host:$server_port"  # TODO  "$Host:$server_port"
        job_vars['job_type'] = 'PASSIVE'
        (err, job_id_lst) = add_micheline_jobs(job_vars, reservation_date, use_job_key,
                                               import_job_key_inline, import_job_key_file,
                                               export_job_key_file,
                                               initial_request, array_nb, array_params)
    else:
        # TODO interactive
        if command:
            print_warning('asking for an interactive job (-I), so ignoring arguments: ' + command + ' .')

        cmd_executor = 'Qsub -I'

        if array_param_file:
            print_error('a array job with parameters given in a file cannot be interactive.')
            usage()
            sub_exit(9)

        if array_nb != 1:
            print_error('an array job cannot be interactive.')
            usage()
            sub_exit(8)

        if reservation:
            # Test if this job is a reservation and the syntax is right
            # TODO Pass
            pass
        socket_server = init_tcp_server()
        (server, server_port) = socket_server.getsockname()
        job_vars['info_type'] = server + ':' + str(server_port)
        job_vars['job_type'] = 'INTERACTIVE'
        (err, job_id_lst) = add_micheline_jobs(job_vars, reservation_date, use_job_key,
                                               import_job_key_inline,
                                               import_job_key_file, export_job_key_file,
                                               initial_request, array_nb, array_params)

    # pdb.set_trace()

    if err != 0:
        print_error('command failed, please verify your syntax.')
        sub_exit(err, '')

    oar_array_id = 0

    # Print job_id list
    if len(job_id_lst) == 1:
        print('OAR_JOB_ID=', job_id_lst[0])
    else:
        job = db['Job'].query.filter(Job.id == job_id_lst[0]).one()
        oar_array_id = job.array_id
        for job_id in job_id_lst:
            print('OAR_JOB_ID=', job_id)

    result = (job_id_lst, oar_array_id)

    # Notify Almigthy
    tools.create_almighty_socket()
    tools.notify_almighty(cmd_executor)

    if reservation:
        # Reservation mode
        print_info("advance reservation request: waiting for approval from the scheduler...")
        (conn, address) = socket_server.accept()
        answer = conn.recv(1024)
        if answer[:-1] == 'GOOD RESERVATION':
            print_info('advance reservation is GRANTED.')
        else:
            print_info('advance reservation is REJECTED ', answer[:-1])
            sub_exit(10)
    elif interactive:
        # Interactive mode
        print_info('interactive mode: waiting...')

        prev_str = ''
        while True:
            (conn, address) = socket_server.accept()
            answer = conn.recv(1024)
            answer = answer[:-1]

            m = re.search(r'\](.*)$', answer)
            if m and m.group(1) != prev_str:
                print_info(answer)
                prev_str = m.group(1)
            elif answer != 'GOOD JOB':
                print_info(answer)

            if (answer == 'GOOD JOB') or (answer == 'BAD JOB') or\
               (answer == 'JOB KILLE') or re.match(r'^ERROR', answer):
                break

        if (answer == 'GOOD JOB'):
            # TODO exit(connect_job($Job_id_list_ref->[0],1,$Openssh_cmd));
            pass
        else:
            sub_exit(11)

    sub_exit(0, result)
