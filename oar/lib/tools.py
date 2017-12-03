# coding: utf-8
import sys
import pwd
import time
import re
import os
import socket
from sqlalchemy import distinct
from oar.lib import (db, config, get_logger, Resource, AssignedResource)

import signal, psutil
from subprocess import (Popen, call, PIPE, check_output, CalledProcessError, TimeoutExpired, STDOUT)
from pexpect import (spawn, exceptions) 


# Constants
DEFAULT_CONFIG = {
    'LEON_SOFT_WALLTIME': 20,
    'LEON_WALLTIME': 300,
    'TIMEOUT_SSH': 120,
    'OAR_SSH_CONNECTION_TIMEOUT': 120,
    'SERVER_PROLOGUE_EPILOGUE_TIMEOUT': 60,
    'SERVER_PROLOGUE_EXEC_FILE': '',
    'SERVER_EPILOGUE_EXEC_FILE': '',
    'BIPBIP_OAREXEC_HASHTABLE_SEND_TIMEOUT': 30,
    'DEAD_SWITCH_TIME': 0,
    'OAREXEC_DIRECTORY': '/var/lib/oar',
    'OAREXEC_PID_FILE_NAME': 'pid_of_oarexec_for_jobId_',
    'OARSUB_FILE_NAME_PREFIX': 'oarsub_connections_',
    'PROLOGUE_EPILOGUE_TIMEOUT': 60,
    'PROLOGUE_EXEC_FILE': '',
    'EPILOGUE_EXEC_FILE': '',
    'SUSPEND_RESUME_SCRIPT_TIMEOUT': 60,
    'SSH_RENDEZ_VOUS': 'oarexec is initialized and ready to do the job',
    'OPENSSH_CMD': 'ssh',
    'CPUSET_FILE_MANAGER': '/etc/oar/job_resource_manager.pl',
    'MONITOR_FILE_SENSOR': '/etc/oar/oarmonitor_sensor.pl',
    'SUSPEND_RESUME_FILE_MANAGER': '/etc/oar/suspend_resume_manager.pl',
    'OAR_SSH_CONNECTION_TIMEOUT': 120,
    'OAR_SSH_AUTHORIZED_KEYS_FILE': '.ssh/authorized_keys',
    'NODE_FILE_DB_FIELD': 'network_address',
    'NODE_FILE_DB_FIELD_DISTINCT_VALUES': 'resource_id',
    'NOTIFY_TCP_SOCKET_ENABLED': 1,
    'SUSPECTED_HEALING_TIMEOUT': 10, 
    'SUSPECTED_HEALING_EXEC_FILE': None
    }

logger = get_logger("oar.lib.tools")

almighty_socket = None

notification_user_socket = None


def init_judas_notify_user():  # pragma: no cover

    logger.debug("init judas_notify_user (launch judas_notify_user.pl)")

    global notification_user_socket
    uds_name = "/tmp/judas_notify_user.sock"
    if not os.path.exists(uds_name):
        binary = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                              "judas_notify_user.pl")
        os.system("%s &" % binary)

        while(not os.path.exists(uds_name)):
            time.sleep(0.1)

        notification_user_socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        notification_user_socket.connect(uds_name)
        

def notify_user(job, state, msg):  # pragma: no cover
    return () # TODO remove init_judas_notify_user()

    global notification_user_socket
    # Currently it uses a unix domain sockey to communication to a perl script
    # TODO need to define and develop the next notification system
    # see OAR::Modules::Judas::notify_user

    logger.debug("notify_user uses the perl script: judas_notify_user.pl !!! ("
                 + state + ", " + msg + ")")

    # OAR::Modules::Judas::notify_user($base,notify,$addr,$user,$jid,$name,$state,$msg);
    # OAR::Modules::Judas::notify_user($dbh,$job->{notify},$addr,$job->{job_user},$job->{job_id},$job->{job_name},"SUSPENDED","Job
    # is suspended."
    addr, port = job.info_type.split(':')

    notify = ''
    if job.notify:
        notify = job.notify
    
    name = ''
    if job.name:
        name = job.name
    
    msg_uds = notify + "°" + addr + "°" + job.user + "°" + str(job.id) + "°" +\
              name + "°" + state + "°" + msg + "\n"

    if not notification_user_socket:
        init_judas_notify_user()

    nb_sent = notification_user_socket.send(msg_uds.encode())

    if nb_sent == 0:
        logger.error("notify_user: socket error")


def create_almighty_socket():  # pragma: no cover
    global almighty_socket
    almighty_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server = config["SERVER_HOSTNAME"]
    port = config["SERVER_PORT"]
    try:
        almighty_socket.connect((server, port))
    except socket.error as exc:
        logger.error("Connection to Almighty" + server + ":" + str(port) +
                     " raised exception socket.error: " + str(exc))
        sys.exit(1)


# TODO: refactor to use zmq
def notify_almighty(message):  # pragma: no cover
    if not almighty_socket:
        create_almighty_socket()
    return almighty_socket.send(message.encode())

def notify_interactif_user(job, message):
    addr, port = job.info_type.split(':')
    return notify_tcp_socket(addr, port, message)

# TODO: refactor to use zmq,  TO CARE of notify_interactif_user 
def notify_tcp_socket(addr, port, message):  # pragma: no cover
    tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    logger.debug('notify_tcp_socket:' + addr + ":" + port + ', msg:' + message)
    try:
        tcp_socket.connect((addr, int(port)))
    except socket.error as exc:
        logger.error("notify_tcp_socket: Connection to " + addr + ":" + port +
                     " raised exception socket.error: " + str(exc))
        return 0
    nb_sent = tcp_socket.send(message.encode())
    tcp_socket.close()
    return nb_sent


def pingchecker(hosts):
    #raise NotImplementedError("TODO")
    return []

def send_log_by_email(title, message):
    raise NotImplementedError("TODO")


def exec_with_timeout(cmd, timeout=DEFAULT_CONFIG['TIMEOUT_SSH']):
    # Launch admin script
    error_msg = ''
    try:
        check_output(cmd, stderr=STDOUT, timeout=timeout)
    except CalledProcessError as e:
        error_msg = str(e.output) + '. Return code: ' + str(e.returncode)
    except TimeoutExpired as e:
        error_msg = str(e.output)

    return error_msg

def kill_child_processes(parent_pid, sig=signal.SIGTERM):
    """from: https://stackoverflow.com/questions/3332043/obtaining-pid-of-child-process"""
    try:
      parent = psutil.Process(parent_pid)
    except psutil.NoSuchProcess:
      return
    children = parent.children(recursive=True)
    for process in children:
      process.send_signal(sig)


def fork_and_feed_stdin(healing_exec_file, timeout, resources_to_heal):
    raise NotImplementedError("TODO")

def get_oar_pid_file_name(job_id):
    """Get the name of the file which contains the pid of oarexec"""
    return config['OAREXEC_DIRECTORY'] + '/' + config['OAREXEC_PID_FILE_NAME'] + str(job_id)

def get_oar_user_signal_file_name(job_id):
    """Get the name of the file which contains the signal given by the user"""
    return config['OAREXEC_DIRECTORY'] + '/USER_SIGNAL_' + str(job_id)

def signal_oarexec(host, job_id, signal, wait, ssh_cmd, user_signal=None):
    """Send the given signal to the right oarexec process
    args : host name, job id, signal, wait or not (0 or 1), 
    DB ref (to close it in the child process), ssh cmd, user defined signal 
    for oardel -s (null by default if not used)
    return an array with exit values.
    """
    filename = get_oar_pid_file_name(job_id);
    cmd = ssh_cmd.split()
    cmd += ['-x', '-T', host]
    if user_signal:
        signal_file = get_oar_user_signal_file_name(job_id)
        cmd.append("bash -c 'echo " + user_signal + " > " + signal_file + " && test -e " + filename + " && PROC=$(cat "\
                   + filename + ") && kill -s CONT $PROC && kill -s " + signal + " $PROC'")
    else:
        cmd.append("bash -c 'test -e " + filename + " && PROC=$(cat " + filename + ") && kill -s CONT $PROC && kill -s "\
                   + signal + " $PROC'")

    comment = None
    if wait:
        try: 
            check_output(cmd, stderr=STDOUT, timeout=DEFAULT_CONFIG['TIMEOUT_SSH'])
        except CalledProcessError as e:
            comment = 'The kill command return a bad exit code (' + str(e.returncode)\
                      + 'for the job ' + str(job_id) +  'on the node ' + head_host\
                      + ', output: ' + str(e.output)
        except TimeoutExpired as e:
            comment = 'Cannot contact ' + head_host + ', operation timouted. Cannot send kill signal to the job '\
                      + str(job.id) + ' on ' + head_host + ' node'
    else:
        # TODO kill after timeout, note Popen launchs process in background
        Popen(cmd)

    return comment

# get_date
# returns the current time in the format used by the sql database

# TODO
def send_to_hulot(cmd, data):
    config.setdefault_config({"FIFO_HULOT": "/tmp/oar_hulot_pipe"})
    fifoname = config["FIFO_HULOT"]
    try:
        with open(fifoname, 'w') as fifo:
            fifo.write('HALT:%s\n' % data)
            fifo.flush()
    except IOError as e:
        e.strerror = 'Unable to communication with Hulot: %s (%s)' % fifoname % e.strerror
        logger.error(e.strerror)
        return 1
    return 0

def get_default_suspend_resume_file():
    raise NotImplementedError("TODO")

def manage_remote_commands(hosts, data_str, manage_file, action, ssh_command, taktuk_cmd):
    
    # args : array of host to connect to, hashtable to transfer, name of the file containing the perl script, action to perform (start or stop), SSH command to use, taktuk cmd or undef
    # TODO
    
    #my $connect_hosts = shift;
    #my $data_hash = shift;
    #my $manage_file = shift;
    #my $action = shift;
    #my $ssh_cmd = shift;
    #my $taktuk_cmd = shift;
    

    return(1, [])
    
    

def get_date():
    if db.engine.dialect.name == 'sqlite':
        req = "SELECT strftime('%s','now')"
    else:   # pragma: no cover
        req = "SELECT EXTRACT(EPOCH FROM current_timestamp)"

    result = db.session.execute(req).scalar()
    return int(result)

def sql_to_local(date):
    """Converts a date specified in the format used by the sql database to an
    integer local time format
    Date 'year mon mday hour min sec' """
    date = ' '.join(re.findall(r"[\d']+", date))
    t = time.strptime(date, "%Y %m %d %H %m %s")
    return int(time.mktime(t))

def local_to_sql(local):
    """Converts a date specified in an integer local time format to the format used
    by the sql database"""
    return time.strftime("%F %T", time.localtime(local))

def sql_to_hms(t):
    """Converts a date specified in the format used by the sql database to hours,
    minutes, secondes values"""
    hms = t.split(':')
    return (hms[0], hms[1], hms[2])

def hms_to_sql(hour, min, sec):
    """Converts a date specified in hours, minutes, secondes values to the format
    used by the sql database"""
    return(str(hour) + ":" + str(min) + ":" + str(sec))

def hms_to_duration(hour, min, sec):
    """Converts a date specified in hours, minutes, secondes values to a duration
    in seconds."""
    return int(hour) * 3600 + int(min) * 60 + int(sec)

def duration_to_hms(t):
    """Converts a date specified as a duration in seconds to hours, minutes,
    secondes values"""
    sec = t % 60
    t /= 60
    min = t % 60
    hour = int(t / 60)

    return (hour, min, sec)

def duration_to_sql(t):
    """converts a date specified as a duration in seconds to the format used by the
    sql database"""
    hour, min, sec = duration_to_hms(t)
    return hms_to_sql(hour, min, sec)

def sql_to_duration(t):
    """Converts a date specified in the format used by the sql database to a
    duration in seconds."""
    (hour, min, sec) = sql_to_hms(t)
    return hms_to_duration(hour, min, sec)

def send_checkpoint_signal(job):
    raise NotImplementedError("TODO")
    logger.debug("Send checkpoint signal to the job " + str(job.id))
    logger.warning("Send checkpoint signal NOT YET IMPLEMENTED ")
    # Have a look to  check_jobs_to_kill/oar_meta_sched.pl

def get_username(): # NOTUSED
    return pwd.getpwuid( os.getuid() ).pw_name


def format_ssh_pub_key(key, cpuset, user, job_user=None):
    """Add right environment variables to the given public key"""
    if not job_user:
        job_user = user
    if not cpuset:
        cpuset = 'undef'

    formated_key = 'environment="OAR_CPUSET=' + cpuset + '",environment="OAR_JOB_USER='\
                                 + job_user + '" ' + key + "\n"
    return formated_key


def get_private_ssh_key_file_name(cpuset_name):
    """Get the name of the file of the private ssh key for the given cpuset name"""
    return(config['OAREXEC_DIRECTORY'] + '/' + cpuset_name + '.jobkey')


def limited_dict2hash_perl(d):
    """Serialize python dictionnary to string hash perl representaion"""
    s = '{'
    for k,v in d.items():
        s = s + "'" + k + "' => " 
        if isinstance(v, dict):
            s = s + limited_dict2hash_perl(v)
        elif isinstance(v, str):
            s = s + "'" + str(v) + "'"
        else:
            s = s + str(v)
        s = s + ','
    return s[:-1] + '}'
