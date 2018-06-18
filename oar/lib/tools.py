# coding: utf-8
import sys
import pwd
import time
import re
import os
import zmq
import random
import string
import socket
from socket import gethostname
from sqlalchemy import distinct
from oar.lib import (db, config, get_logger, Resource, AssignedResource)
from oar.lib import logger as log
import signal, psutil
from subprocess import (Popen, run, call, PIPE, check_output, CalledProcessError, TimeoutExpired, STDOUT)

from multiprocessing import Process
from os import kill


# Constants
DEFAULT_CONFIG = {
    'SERVER_HOSTNAME': 'localhost',
    'SERVER_PORT': '6666',
    'APPENDICE_SERVER_PORT': '6668',
    'BIPBIP_COMMANDER_SERVER': 'localhost',
    'BIPBIP_COMMANDER_PORT': '6669',
    'LEON_SOFT_WALLTIME': 20,
    'LEON_WALLTIME': 300,
    'TIMEOUT_SSH': 120,
    'OAR_SSH_CONNECTION_TIMEOUT': 120,
    'SERVER_PROLOGUE_EPILOGUE_TIMEOUT': 60,
    'SERVER_PROLOGUE_EXEC_FILE': None,
    'SERVER_EPILOGUE_EXEC_FILE': None,
    'BIPBIP_OAREXEC_HASHTABLE_SEND_TIMEOUT': 30,
    'DEAD_SWITCH_TIME': 0,
    'OAREXEC_DIRECTORY': '/var/lib/oar',
    'OAREXEC_PID_FILE_NAME': 'pid_of_oarexec_for_jobId_',
    'OARSUB_FILE_NAME_PREFIX': 'oarsub_connections_',
    'PROLOGUE_EPILOGUE_TIMEOUT': 60,
    'PROLOGUE_EXEC_FILE': None,
    'EPILOGUE_EXEC_FILE': None,
    'SUSPEND_RESUME_SCRIPT_TIMEOUT': 60,
    'SSH_RENDEZ_VOUS': 'oarexec is initialized and ready to do the job',
    'OPENSSH_CMD': 'ssh',
    'JOB_RESOURCE_MANAGER_FILE': '/etc/oar/job_resource_manager_cgroups.pl',
    'MONITOR_FILE_SENSOR': '/etc/oar/oarmonitor_sensor.pl',
    'SUSPEND_RESUME_FILE_MANAGER': '/etc/oar/suspend_resume_manager.pl',
    'OAR_SSH_CONNECTION_TIMEOUT': 120,
    'OAR_SSH_AUTHORIZED_KEYS_FILE': '.ssh/authorized_keys',
    'NODE_FILE_DB_FIELD': 'network_address',
    'NODE_FILE_DB_FIELD_DISTINCT_VALUES': 'resource_id',
    'NOTIFY_TCP_SOCKET_ENABLED': 1,
    'SUSPECTED_HEALING_TIMEOUT': 10, 
    'SUSPECTED_HEALING_EXEC_FILE': None,
    'DEBUG_REMOTE_COMMANDS': 'yes',
    'COSYSTEM_HOSTNAME': '127.0.0.1',
    'DEPLOY_HOSTNAME': '127.0.0.1',
    'OPENSSH_CMD': '/usr/bin/ssh -p 6667',
    'OAREXEC_DEBUG_MODE' : '1'
    }

logger = get_logger("oar.lib.tools")

zmq_context = None
almighty_socket = None
bipbip_commander_socket = None

def notify_user(job, state, msg):  # pragma: no cover
    
    if job.notify:
        tags = ['RUNNING', 'END', 'ERROR', 'INFO', 'SUSPENDED', 'RESUMING']
        m = re.match(r'^\s*\[\s*(.+)\s*\]\s*(mail|exec)\s*:.+$', job.notify)
        if m:
            tags = m.group(1).split(',')
            
        if state in tags:
            m = re.match(r'^.*mail\s*:(.+)$', job.notify)
            if m:
                mail_address = m.group(1)
                add_new_event('USER_MAIL_NOTIFICATION', job.id,
                              'Send a mail to {}: state: {}'.format(mail_address, state))
                jname = '({})'.format(job.name) if job.name else ''
                send_mail(job, mail_address,
                          '*OAR* [{}] {} on {}'.format(state, jname, gethostname()),
                          msg)
            else:
                m = re.match(r'^.*exec\s*:([a-zA-Z0-9_.\/ -]+)$', job.notify)
                if m:
                    host = job.info_type.split(':')[0]
                    
                    cmd = '{} -x -T {} OARDO_BECOME_USER={} oardodo {} {} {} {} "{}"  > /dev/null 2>&1'.\
                                                          format(config['OPENSSH_CMD'], host,
                                                                 job.user, m.group(1), job.id, job.name,
                                                                 state, msg)
                    logger.error(cmd)
                    try:
                        p = check_output(cmd, stderr=STDOUT, shell=True,
                                         timeout=config['OAR_SSH_CONNECTION_TIMEOUT'])
                    except CalledProcessError as e:
                        logger.error('User notification failed: ' + e.output)
                        return False
                    except TimeoutExpired as e:
                        p.kill()
                        msg = 'User notification failed: ssh timeout (cmd: ' +\
                              cmd + ')'
                        logger.error(msg)
                        add_new_event('USER_EXEC_NOTIFICATION_ERROR', job.id, msg)
                        return False

                    msg = 'Launched user notification command : ' + cmd
                    add_new_event('USER_EXEC_NOTIFICATION', job.id, msg)

    return True


def send_mail(job, mail_address, subject, msg_content): # pragma: no cover
    import smtplib
    from email.message import EmailMessage
    
    msg = EmailMessage()
    msg.set_content(msg_content)

    msg['Subject'] = subject
    msg['From'] = config['MAIL_SENDER']
    msg['To'] = mail_address

    s = smtplib.SMTP(config['MAIL_SMTP_SERVER'])
    s.send_message(msg)
    s.quit()

    
def create_almighty_socket():  # pragma: no cover
    global zmq_context
    global almighty_socket
    
    config.setdefault_config(DEFAULT_CONFIG)
    
    if not zmq_context:
        zmq_context = zmq.Context()
        
    almighty_socket = zmq_context.socket(zmq.PUSH)
    almighty_socket.connect('tcp://' + config['SERVER_HOSTNAME'] + ':'
                             + config['APPENDICE_SERVER_PORT'])
    
    #global almighty_socket
    #almighty_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    #server = config["SERVER_HOSTNAME"]
    #port = config["SERVER_PORT"]
    #try:
    #    almighty_socket.connect((server, port))
    #except socket.error as exc:
    #    logger.error("Connection to Almighty" + server + ":" + str(port) +
    #                 " raised exception socket.error: " + str(exc))
    #    sys.exit(1)   

# TODO: refactor to use zmq and/or conserve notification through TCP (for oarsub by example ???)
def notify_almighty(cmd, job_id=None, args=None):  # pragma: no cover
    if not almighty_socket:
        create_almighty_socket()

    message = {'cmd': cmd}
    if job_id:
        message['job_id'] = job_id
    if args:
        message['args'] = args
        
    completed = True
    try:
        almighty_socket.send_json(message)
    except zmq.ZMQError:
        completed = False
    return completed

def create_bipbip_commander_socket():  # pragma: no cover
    global zmq_context
    global bipbip_commander_socket
    
    config.setdefault_config(DEFAULT_CONFIG)
    if not zmq_context:
        zmq_context = zmq.Context()
    bipbip_commander_socket = zmq_context.socket(zmq.PUSH)
    bipbip_commander_socket.connect('tcp://' + config['BIPBIP_COMMANDER_SERVER']\
                                    + ':' + config['BIPBIP_COMMANDER_PORT'])
    
def notify_bipbip_commander(message):  # pragma: no cover
    
    if not bipbip_commander_socket:
        create_bipbip_commander_socket()
        
    completed = True
    try:
        bipbip_commander_socket.send_json(message)
    except zmq.ZMQError:
        completed = False
    return completed

def notify_interactif_user(job, message): # pragma: no cover
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
    message += '\n'
    nb_sent = tcp_socket.send(message.encode('utf8'))
    
    tcp_socket.close()
    return nb_sent

def pingchecker(hosts): # pragma: no cover
    """Check compute nodes remotely accordindly to method specified in oar.conf"""
    cmd = ''
    ip2hostname = {}
    pipe_hosts = None
    add_bad_hosts = False

    if 'PINGCHECKER_TAKTUK_ARG_COMMAND' in config and 'TAKTUK_CMD' in config :
        cmd = config['TAKTUK_CMD']\
              + " -c '" + config['OPENSSH_CMD'] + "'"\
              + ' -o status=\'"STATUS $host $line\\n"\''\
              + ' -f - ' + config['PINGCHECKER_TAKTUK_ARG_COMMAND']
        pipe_hosts = ('\n'.join(hosts) + '\n').encode('utf-8')
        def filter_output(line, _):
            m = re.match(r'^STATUS ([\w\.\-\d]+) (\d+)$', line)
            if m and m.group(2) == '0':
                return m.group(1)

    elif 'PINGCHECKER_SENTINELLE_SCRIPT_COMMAND' in config:
        add_bad_hosts = True
        cmd = config['PINGCHECKER_SENTINELLE_SCRIPT_COMMAND']\
              + " -c '" + config['OPENSSH_CMD'] + "' -f -  "
        pipe_hosts = ('\n'.join(hosts) + '\n').encode('utf-8')
        def filter_output(line, _):
             m = re.match(r'^([\w\.\-]+)\s:\sBAD\s.*$', line)
             if m and m.group(1):
                 return m.group(1)

    elif 'PINGCHECKER_NMAP_COMMAND' in config:
        cmd = config['PINGCHECKER_NMAP_COMMAND'] + ' -oG - ' + ' '.join(hosts)
        ip2hostname = {socket.gethostbyname(h): h for h in hosts}
        def filter_output(line, ip2hostname):
            m = re.match(r'^Host:\s(\d+\.\d+\.\d+\.\d+).*/open/.*$', line)
            if m and m.group(1):
                return ip2hostname[m.group(1)]
    elif 'PINGCHECKER_GENERIC_COMMAND' in config:
        add_bad_hosts = True 
        cmd = config['PINGCHECKER_GENERIC_COMMAND'] + " ".join(hosts)
        def filter_output(line, _):
            m = re.match(r'^\s*([\w\.\-]+)\s*$', line)
            if m and m.group(1):
                return m.group(1)

    elif 'PINGCHECKER_FPING_COMMAND' in config:
        add_bad_hosts = True
        cmd = config['PINGCHECKER_FPING_COMMAND'] + ' -u ' + ' '.join(hosts)
        def filter_output(line, _): 
            m = re.match(r'^\s*([\w\.\d-]+)\s*(.*)$', line) 
            if m and m.group(1) and 'alive' in m.group(2):
                return m.group(1)
    else:
        logger.debug('[PingChecker] no PINGCHECKER configuration found')

    return pingchecker_exec_command(cmd, hosts, filter_output, ip2hostname, pipe_hosts, add_bad_hosts) 

def pingchecker_exec_command(cmd, hosts, filter_output, ip2hostname, pipe_hosts, add_bad_hosts, log=log): # pragma: no cover
    log.debug('[PingChecker] command to run : {}'.format(cmd))
    
    if add_bad_hosts:
        bad_hosts_list = []
    else: 
        bad_hosts = {h: True for h in hosts}
  
    env = os.environ.copy()
    env['ENV'] = ''
    env['IFS'] = ''
    # Launch taktuk
    p = Popen(cmd, stdin=PIPE, stdout=PIPE, stderr=PIPE, shell=True, env=env)

    try: 
        out, err = p.communicate(pipe_hosts, 2*DEFAULT_CONFIG['TIMEOUT_SSH'])
    except TimeoutExpired:
        p.kill()
        log.debug('[PingChecker] TimeoutExpired')
        return (0, [])

    output = out.decode()
    error = err.decode()
    
    for line in output.split('\n'):
        host = filter_output(*(line,ip2hostname))
        if host and host in bad_hosts:
            if add_bad_hosts:
                bad_hosts_list.append(host)
            else:
                del bad_hosts[host]

    if add_bad_hosts:
        return (1, bad_hosts_list)
    else:
        return (1, list(bad_hosts.keys()))


def send_log_by_email(title, message): # pragma: no cover
    #raise NotImplementedError("TODO")
    return

def exec_with_timeout(cmd, timeout=DEFAULT_CONFIG['TIMEOUT_SSH']): # pragma: no cover
    # Launch admin script
    error_msg = ''
    try:
        check_output(cmd, stderr=STDOUT, timeout=timeout)
    except CalledProcessError as e:
        error_msg = str(e.output) + '. Return code: ' + str(e.returncode)
    except TimeoutExpired as e:
        error_msg = str(e.output)

    return error_msg

def kill_child_processes(parent_pid, sig=signal.SIGTERM): # pragma: no cover
    """from: https://stackoverflow.com/questions/3332043/obtaining-pid-of-child-process"""
    try:
      parent = psutil.Process(parent_pid)
    except psutil.NoSuchProcess:
      return
    children = parent.children(recursive=True)
    for process in children:
      process.send_signal(sig)


def fork_and_feed_stdin(healing_exec_file, timeout, resources_to_heal): # pragma: no cover
    raise NotImplementedError("TODO")

def get_oar_pid_file_name(job_id):
    """Get the name of the file which contains the pid of oarexec"""
    return config['OAREXEC_DIRECTORY'] + '/' + config['OAREXEC_PID_FILE_NAME'] + str(job_id)

def get_oar_user_signal_file_name(job_id): # pragma: no cover
    """Get the name of the file which contains the signal given by the user"""
    return config['OAREXEC_DIRECTORY'] + '/USER_SIGNAL_' + str(job_id)

def signal_oarexec(host, job_id, signal, wait, ssh_cmd, user_signal=None): # pragma: no cover
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
    except IOError as e: # pragma: no cover
        e.strerror = 'Unable to communication with Hulot: %s (%s)' % fifoname % e.strerror
        logger.error(e.strerror)
        return 1
    return 0

def get_default_suspend_resume_file():
    raise NotImplementedError("TODO")


def launch_oarexec(cmd, data_str, oarexec_files): # pragma: no cover
    # Prepare string to transfer to perl interpreter on head node
      
    str_to_transfer = ''
    for oarexec_file in oarexec_files:
        with open(oarexec_file, 'r') as mg_file:
            str_to_transfer += mg_file.read()
    str_to_transfer += '__END__\n' + data_str

    # Launch perl interpreter on remote
    p = Popen(cmd, stdin=PIPE, stdout=PIPE, stderr=PIPE, shell=True)

    try: 
        out, err = p.communicate(str_to_transfer.encode('utf8'), timeout=2*DEFAULT_CONFIG['TIMEOUT_SSH'])
    except TimeoutExpired:
        p.kill()
        logger.debug("Oarexec's Launching TimeoutExpired")
        return False

    output = out.decode()
    error = err.decode()

    #print(output)
    #print(error)
    
    if config['OAREXEC_DEBUG_MODE'] in ['1', 1, 'yes', 'YES']:
        logger.debug('SSH output: ' + output)
        logger.debug('SSH error: ' + error)
    
    regex_ssh_rdv = re.compile('^' + config['SSH_RENDEZ_VOUS'] + '$')
    for line in output.split('\n'):
        if re.match(regex_ssh_rdv, line):
            return True
    return False

def manage_remote_commands(hosts, data_str, manage_file, action, ssh_command, taktuk_cmd=None): # pragma: no cover
    # args : array of host to connect to, hashtable to transfer, name of the file containing the perl script,
    # action to perform (start or stop), SSH command to use, taktuk cmd or undef

    str_to_transfer = ''
    with open(manage_file, 'r') as mg_file:
         str_to_transfer = mg_file.read()
    str_to_transfer += '__END__\n' + data_str

    if not taktuk_cmd:
        raise NotImplementedError("Taktuk must be used...")
    else:
        fifoname = '/tmp/tmp_' + ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(10))
        os.mkfifo(fifoname)

        output_connector_tpl = ' -o output -o connector'
        if config['DEBUG_REMOTE_COMMANDS'] != 'no':
            output_connector_tpl = ''

        cmd = taktuk_cmd\
              + " -c '" + ssh_command + "'"\
              + ' -o status=\'"STATUS $host $line\\n"\''\
              + output_connector_tpl\
              + ' -f '\
              + fifoname\
              + ' broadcast exec [ perl - '\
              + action\
              + ' ], broadcast input file [ - ], broadcast input close'
        # print(cmd)
        
        # Launch taktuk
        p = Popen(cmd, stdin=PIPE, stdout=PIPE, stderr=PIPE, shell=True)

        bad_hosts = {}
        # Send hosts to address 
        w = open(fifoname, 'w')
        for host in hosts:
            bad_hosts[host] = True
            w.write(host + '\n')
        w.close()
        os.remove(fifoname)

        try: 
            out, err = p.communicate(str_to_transfer.encode('utf8'),
                                     timeout=2*DEFAULT_CONFIG['TIMEOUT_SSH'])
        except TimeoutExpired:
            p.kill()
            # TODO
            print('TimeoutExpired')
            #m = re.match(br'^STATUS ([\w\.\-\d]+) (\d+)$', out)
            return (0, [])
        
        output = out.decode()
        error = err.decode()
        
        #import pdb; pdb.set_trace()
        if config['DEBUG_REMOTE_COMMANDS'] in ['1', 1, 'yes', 'YES']:
            logger.debug('Taktuk output: ' + output)
            if error:
                logger.debug('Taktuk error: ' + error)
            
        for line in output.split('\n'):
            m = re.match(r'^STATUS ([\w\.\-\d]+) (\d+)$', line) 
            if m:
                if m.group(2) == '0':
                    # Host is OK
                    if m.group(1) in bad_hosts:
                        del bad_hosts[m.group(1)]

        return (1, list(bad_hosts.keys()))
    return (0, []) 


def get_date(): # pragma: no cover
    if db.engine.dialect.name == 'sqlite':
        req = "SELECT strftime('%s','now')"
    else:
        req = "SELECT EXTRACT(EPOCH FROM current_timestamp)"

    result = db.session.execute(req).scalar()
    return int(result)

def get_time(): # pragma: no cover
    return(time.time())

def sql_to_local(date):
    """Converts a date specified in the format used by the sql database to an
    integer local time format
    Date 'year mon mday hour min sec' """
    date = ' '.join(re.findall(r"[\d']+", date))
    t = time.strptime(date, "%Y %m %d %H %M %S")
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

def duration_to_hms(t_sec):
    """Converts a date specified as a duration in seconds to hours, minutes,
    secondes values"""
    hour = t_sec // 3600
    min = (t_sec - hour * 3600) // 60 
    sec = t_sec % 60

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

def get_duration(seconds):
    """Convert seconds to compound duration taken from rosettacode site
    https://rosettacode.org/wiki/Convert_seconds_to_compound_duration#Python
    """
    t= []
    for dm in (60, 60, 24, 7):
        seconds, m = divmod(seconds, dm)
        t.append(m)
    t.append(seconds)
    return ', '.join('%d %s' % (num, unit)
                     for num, unit in zip(t[::-1], 'wk d hr min sec'.split())
                     if num)


def send_checkpoint_signal(job): # pragma: no cover
    raise NotImplementedError("TODO")
    logger.debug("Send checkpoint signal to the job " + str(job.id))
    logger.warning("Send checkpoint signal NOT YET IMPLEMENTED ")
    # Have a look to  check_jobs_to_kill/oar_meta_sched.pl


def get_username(): # pragma: no cover
    #return pwd.getpwuid( os.getuid() ).pw_name
    return os.environ['OARDO_USER']

def check_resource_property(prop):
    """ Check if a property can be deleted or created by a user
    return 0 if all is good otherwise return 1
    """
    if prop in ['resource_id', 'network_address', 'state', 'state_num', 'next_state',
           'finaud_decision', 'next_finaud_decision', 'besteffort', 'desktop_computing',
           'deploy', 'expiry_date', 'last_job_date', 'available_upto', 'last_available_upto',
           'walltime', 'nodes', 'type', 'suspended_jobs', 'scheduler_priority', 'cpuset', 'drain']:
        return True
    else:
        return False

def check_resource_system_property(prop):
    """Check if a property can be manipulated by a user
    return 0 if all is good otherwise return 1
    """
    if prop in ['resource_id', 'state', 'state_num', 'next_state', 'finaud_decision',
                'next_finaud_decision', 'last_job_date', 'suspended_jobs', 'expiry_date',
                'scheduler_priority']:
        return True
    else:
        return False

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


def format_job_message_text(job_name, estimated_nb_resources, estimated_walltime, job_type,
                            reservation, queue, project, types_list, additional_msg):
    job_mode = 'B'
    if reservation:
        job_mode = 'R'
    elif job_type == 'INTERACTIVE':
        job_mode = 'I'
        
    types_to_text = ''
    if types_list:
        types_to_text = 'T=' + '|'.join(types_list) + ','

    job_message = 'R=' + str(estimated_nb_resources) + ',W='
    job_message += duration_to_sql(int(estimated_walltime)) + ',J=' + job_mode + ','
    if job_name:
        job_message += 'N=' + job_name + ','
    if queue != 'default' and queue != 'besteffort':
        job_message += 'Q=' + queue + ','
    if project != 'default':
        job_message += 'P=' + project + ','
    job_message += types_to_text

    job_message = job_message[:-1]

    if additional_msg:
        job_message += ' (' + additional_msg + ')'
    
    return(job_message)



def limited_dict2hash_perl(d):
    """Serialize python dictionnary to string hash perl representaion"""
    s = '{'
    for k,v in d.items():
        s = s + "'" + k + "' => "
        #print (s + ' - ' + str(v) + ' - ' + str(type(v)))
        if v is None:
            s = s + 'undef'  
        elif isinstance(v, dict):
            if not v:
                s = s + '{}'
            else:
                s = s + limited_dict2hash_perl(v)
        elif isinstance(v, str):
            s = s + "'" + v + "'"
        elif isinstance(v, bool):
            val = '1' if v else '0'
            s = s + val
        else:
            s = s + str(v)
        s = s + ','
    return s[:-1] + '}'

def resources2dump_perl(resources):
    a = '['
    for resource in resources:
        a = a + limited_dict2hash_perl(resource.to_dict()) + ','
    return a[:-1] + ']'
    # TODO selection only needed resource fields
    # def resource2hash_perl(resouces):
    #     h = '{'
    #     for k,v in d.to_dict().items():
    #         h = h + "'" + k + "' => "
    #         if isinstance(v, str):
    #             h = h + "'" + v + "'"
    #         else:
    #             h = h + str(v)
    #         s = s + ',

def get_oarexecuser_script_for_oarsub(job, job_walltime, node_file, shell, resource_file): # pragma: no cover
    """ Create the shell script used to execute right command for the user
    The resulting script can be launched with : bash -c 'script'
    """           
    script = "if [ \"a\$TERM\" == \"a\" ] || [ \"x\$TERM\" == \"xunknown\" ]; then export TERM=xterm; fi;"\
              + (job.env if job.env else '')\
              + "export OAR_FILE_NODES=\"" + node_file + "\";"\
              + "export OAR_JOBID=" + str(job.id) + ";"\
              + "export OAR_ARRAYID=" + str(job.array_id) + ";"\
              + "export OAR_ARRAYINDEX=" + str(job.array_index) + ";"\
              + "export OAR_USER=\"" + job.user + "\";"\
              + "export OAR_WORKDIR=\"" + job.launching_directory + "\";"\
              + "export OAR_RESOURCE_PROPERTIES_FILE=\"" + resource_file + "\";"\
              + "export OAR_NODEFILE=\$OAR_FILE_NODES;"\
              + "export OAR_O_WORKDIR=\$OAR_WORKDIR;"\
              + "export OAR_NODE_FILE=\$OAR_FILE_NODES;"\
              + "export OAR_RESOURCE_FILE=\$OAR_FILE_NODES;"\
              + "export OAR_WORKING_DIRECTORY=\$OAR_WORKDIR;"\
              + "export OAR_JOB_ID=\$OAR_JOBID;"\
              + "export OAR_ARRAY_ID=\$OAR_ARRAYID;"\
              + "export OAR_ARRAY_INDEX=\$OAR_ARRAYINDEX;"\
              + "export OAR_JOB_NAME=\"" + (job.name if job.name else '') + "\";"\
              + "export OAR_PROJECT_NAME=\"" + job.project + "\";"\
              + "export OAR_JOB_WALLTIME=\"" + duration_to_sql(job_walltime) + "\";"\
              + "export OAR_JOB_WALLTIME_SECONDS=" + str(job_walltime) + ";"\
              + "export SHELL=\"" + shell + "\";"\
              + " export SUDO_COMMAND=OAR;"\
              + " SHLVL=1;"\
              + " if ( cd \"\$OAR_WORKING_DIRECTORY\" &> /dev/null );"\
              + " then"\
              + "     cd \"\$OAR_WORKING_DIRECTORY\";"\
              + " else"\
              + "     exit 2;"\
              + " fi;"\
              + " (exec -a -\${SHELL##*/} \$SHELL);"\
              + " exit 0"
    
    return(script)


