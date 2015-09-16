# coding: utf-8
from __future__ import unicode_literals, print_function
import sys
import time
import re
import os
import socket
from sqlalchemy import func, distinct
from oar.lib import (db, config, get_logger, Resource, AssignedResource,
                     EventLog)
from subprocess import (Popen, call, TimeoutExpired)

logger = get_logger("oar.kao.utils")

almighty_socket = None

notification_user_socket = None


def init_judas_notify_user():  # pragma: no cover

    logger.debug("init judas_notify_user (launch judas_notify_user.pl)")

    global notify_user_socket
    uds_name = "/tmp/judas_notify_user.sock"
    if not os.path.exists(uds_name):
        if "OARDIR" in os.environ:
            binpath = os.environ["OARDIR"] + "/"
        else:
            binpath = "/usr/local/lib/oar/"
        os.system(binpath + "judas_notify_user.pl &")

        while(not os.path.exists(uds_name)):
            time.sleep(0.1)

        notification_user_socket = socket.socket(
            socket.AF_UNIX, socket.SOCK_STREAM)
        notification_user_socket.connect(uds_name)


def notify_user(job, state, msg):  # pragma: no cover
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
    msg_uds = job.notify + "°" + addr + "°" + job.user + "°" + job.id + "°" +\
        job.name + "°" + state + "°" + msg + "\n"
    nb_sent = notification_user_socket.send(msg_uds)

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


def notify_almighty(message):  # pragma: no cover
    return almighty_socket.send(message)


def notify_tcp_socket(addr, port, message):  # pragma: no cover
    tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    logger.debug('notify_tcp_socket:' + addr + ":" + port + ', msg:' + message)
    try:
        tcp_socket.connect((addr, int(port)))
    except socket.error as exc:
        logger.error("notify_tcp_socket: Connection to " + addr + ":" + port +
                     " raised exception socket.error: " + str(exc))
        return 0
    nb_sent = tcp_socket.send(message)
    tcp_socket.close()
    return nb_sent

# get_date
# returns the current time in the format used by the sql database


def get_date():

    if db.engine.dialect.name == 'sqlite':
        req = "SELECT strftime('%s','now')"
    else:   # pragma: no cover
        req = "SELECT EXTRACT(EPOCH FROM current_timestamp)"

    result = db.engine.execute(req).scalar()
    return int(result)


# sql_to_local
# converts a date specified in the format used by the sql database to an
# integer local time format
# parameters : date string
# return value : date integer
# side effects : /


def sql_to_local(date):
    # Date "year mon mday hour min sec"
    date = ' '.join(re.findall(r"[\d']+", date))
    t = time.strptime(date, "%Y %m %d %H %m %s")
    return int(time.mktime(t))


# local_to_sql
# converts a date specified in an integer local time format to the format used
# by the sql database
# parameters : date integer
# return value : date string
# side effects : /

def local_to_sql(local):
    return time.strftime("%F %T", time.localtime(local))

# sql_to_hms
# converts a date specified in the format used by the sql database to hours,
# minutes, secondes values
# parameters : date string
# return value : hours, minutes, secondes
# side effects : /


def sql_to_hms(t):
    hms = t.split(':')
    return (hms[0], hms[1], hms[2])

# hms_to_sql
# converts a date specified in hours, minutes, secondes values to the format
# used by the sql database
# parameters : hours, minutes, secondes
# return value : date string
# side effects : /


def hms_to_sql(hour, min, sec):

    return(str(hour) + ":" + str(min) + ":" + str(sec))
# hms_to_duration
# converts a date specified in hours, minutes, secondes values to a duration
# in seconds
# parameters : hours, minutes, secondes
# return value : duration
# side effects : /


def hms_to_duration(hour, min, sec):
    return int(hour)*3600 + int(min) * 60 + int(sec)


# duration_to_hms
# converts a date specified as a duration in seconds to hours, minutes,
# secondes values
# parameters : duration
# return value : hours, minutes, secondes
# side effects : /


def duration_to_hms(t):

    sec = t % 60
    t /= 60
    min = t % 60
    hour = int(t / 60)

    return (hour, min, sec)

# duration_to_sql
# converts a date specified as a duration in seconds to the format used by the
# sql database
# parameters : duration
# return value : date string
# side effects : /


def duration_to_sql(t):

    hour, min, sec = duration_to_hms(t)

    return hms_to_sql(hour, min, sec)


# sql_to_duration
# converts a date specified in the format used by the sql database to a
# duration in seconds
# parameters : date string
# return value : duration
# side effects : /


def sql_to_duration(t):
    (hour, min, sec) = sql_to_hms(t)
    return hms_to_duration(hour, min, sec)

# update_current_scheduler_priority
# Update the scheduler_priority field of the table resources


def update_current_scheduler_priority(job, value, state):
    """Update the scheduler_priority field of the table resources
    """

    # TO FINISH
    # TODO: MOVE TO resource.py ???

    logger.info("update_current_scheduler_priority " +
                " job.id: " + str(job.id) + ", state: " + state + ", value: "
                + str(value))

    if "SCHEDULER_PRIORITY_HIERARCHY_ORDER" in config:
        sched_priority = config["SCHEDULER_PRIORITY_HIERARCHY_ORDER"]
        if ((('besteffort' in job.types) or ('timesharing' in job.types)) and
           (((state == 'START') and
             is_an_event_exists(job.id, "SCHEDULER_PRIORITY_UPDATED_START") <= 0) or
           ((state == 'STOP') and is_an_event_exists(job.id, "SCHEDULER_PRIORITY_UPDATED_START") > 0))):

            coeff = 1
            if ('besteffort' in job.types) and (not ('timesharing' in job.types)):
                coeff = 10

            index = 0
            for f in sched_priority.split('/'):
                if f == '':
                    continue
                elif f == 'resource_id':
                    f = 'id'

                index += 1

                res = db.query(distinct(getattr(Resource, f)))\
                        .filter(AssignedResource.assigned_resource_index == 'CURRENT')\
                        .filter(AssignedResource.moldable_id == job.assigned_moldable_job)\
                        .filter(AssignedResource.resource_id == Resource.id)\
                        .all()

                resources = tuple(r[0] for r in res)

                if resources == ():
                    return

                incr_priority = int(value) * index * coeff
                db.query(Resource)\
                  .filter((getattr(Resource, f)).in_(resources))\
                  .update({Resource.scheduler_priority: incr_priority}, synchronize_session=False)

            add_new_event('SCHEDULER_PRIORITY_UPDATED_' + state, job.id,
                          'Scheduler priority for job ' + str(job.id) +
                          'updated (' + sched_priority + ')')


def update_scheduler_last_job_date(date, moldable_id):
    db.query(Resource).filter(AssignedResource.Moldable_job_id == moldable_id)\
                      .filter(AssignedResource.Resource_id == Resource.resource_id)\
                      .update({Resource.last_job_date: date})


# EVENTS LOG MANAGEMENT

# add a new entry in event_log table
# args : database ref, event type, job_id , description
def add_new_event(type, job_id, description):
    event_data = EventLog(type=type, job_id=job_id, date=get_date(),
                          description=description[:255])
    db.add(event_data)


def is_an_event_exists(job_id, event):
    res = db.query(func.count(EventLog.id)).filter(EventLog.job_id == job_id)\
                                           .filter(EventLog.type == event)\
                                           .scalar()
    return res


def get_job_events(job_id):
    '''Get events for the specified job
    '''
    result = db.query(EventLog).filter(EventLog.job_id == job_id).all()
    return result


def send_checkpoint_signal(job):
    logger.debug("Send checkpoint signal to the job " + str(job.id))
    logger.warn("Send checkpoint signal NOT YET IMPLEMENTED ")
    # Have a look to  check_jobs_to_kill/oar_meta_sched.pl
