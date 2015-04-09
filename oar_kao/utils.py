# -*- coding: utf-8 -*-
import sys
import time
import os
import socket
from oar.lib import db, config, get_logger, Resource, AssignedResource

log = get_logger("oar.kao.utils")

almighty_socket = None

notification_user_socket = None


def init_judas_notify_user():

    log.debug("init judas_notify_user (launch judas_notify_user.pl)")

    global notify_user_socket
    uds_name = "/tmp/judas_notify_user.sock"
    if not os.path.exists(uds_name):
        if "OARDIR" in os.environ:
            binpath = os.environ["OARDIR"] + "/"
        else:
            binpath = "/usr/local/lib/oar/"
        os.system(binpath +"judas_notify_user.pl &")
        
        while(not os.path.exists(uds_name)):
            time.sleep(0.1)

        notification_user_socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        notification_user_socket.connect(uds_name)


def notify_user(job, state, msg):
    global notification_user_socket
    #Currently it uses a unix domain sockey to communication to a perl script
    #TODO need to define and develop the next notification system
    # see OAR::Modules::Judas::notify_user
    
    log.debug("notify_user uses the perl script: judas_notify_user.pl !!! (" + state + ", " + msg + ")")

    #OAR::Modules::Judas::notify_user($base,notify,$addr,$user,$jid,$name,$state,$msg);
    #OAR::Modules::Judas::notify_user($dbh,$job->{notify},$addr,$job->{job_user},$job->{job_id},$job->{job_name},"SUSPENDED","Job is suspended."
    addr, port = job.info_type.split(':')
    msg_uds = job.notify + "°" + addr + "°" + job.user + "°" + job.id + "°" +\
              job.name + "°" + state + "°" + msg + "\n"
    nb_sent = notification_user_socket.send(msg_uds)

    if nb_sent==0:
        log.error("notify_user: socket error" )

def create_almighty_socket():
    global almighty_socket
    almighty_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server = config["SERVER_HOSTNAME"]
    port = config["SERVER_PORT"]
    try:
        almighty_socket.connect((server, port))
    except socket.error, exc:
        log.error("Connection to Almighty" + server + ":" + str(port) +
                  " raised exception socket.error: " + exc)
        sys.exit(1)


def notify_almighty(message):
    return almighty_socket.send(message)


def notify_tcp_socket(addr, port, message):
    tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        tcp_socket.connect((addr, int(port)))
    except socket.error, exc:
        log.error("notify_tcp_socket: Connection to " + addr + ":" + str(port) +
                  " raised exception socket.error: " + exc)
        return 0
    nb_sent = tcp_socket.send(message)
    tcp_socket.close()
    return nb_sent

# get_date
# returns the current time in the format used by the sql database
def get_date():
    result = db.engine.execute("select EXTRACT(EPOCH FROM current_timestamp)").scalar()
    return int(result)


# local_to_sql
# converts a date specified in an integer local time format to the format used
# by the sql database
# parameters : date integer
# return value : date string
# side effects : /
def local_to_sql(local):
    return time.strftime("%F %T", time.localtime(local))


def notify_user(job, state, msg):
    #TODO see OAR::Modules::Judas::notify_user
    log.info("notify_user not yet implemented !!!! (" + state + ", " + msg + ")")


# update_current_scheduler_priority
# Update the scheduler_priority field of the table resources
def update_current_scheduler_priority(job, value, state):
    log.info("update_current_scheduler_priority not yet implemented !!!! job.id: " +
             str(job.id) + ", state: " + state + ", value: " + str(value))
#  # code from IO.pm update_current_scheduler_priority
#
#     my $dbh = shift;
#     my $job_id = shift;
#     my $moldable_id = shift;
#     my $value = shift;
#     my $state = shift;
#
#     $state = "STOP" if ($state ne "START");
#
#     if (is_conf("SCHEDULER_PRIORITY_HIERARCHY_ORDER")){
#         my $types = OAR::IO::get_job_types_hash($dbh,$job_id);
#         if (((defined($types->{besteffort})) or (defined($types->{timesharing})))
#             and (($state eq "START" and (is_an_event_exists($dbh,$job_id,"SCHEDULER_PRIORITY_UPDATED_START") <= 0))
#                 or (($state eq "STOP") and (is_an_event_exists($dbh,$job_id,"SCHEDULER_PRIORITY_UPDATED_START") > 0)))
#            ){
#             my $coeff = 1;
#             if ((defined($types->{timesharing})) and !(defined($types->{besteffort}))){
#                 $coeff = 10;
#             }
#             my $index = 0;
#             foreach my $f (split('/',get_conf("SCHEDULER_PRIORITY_HIERARCHY_ORDER"))){
#                 next if ($f eq "");
#                 $index++;

#                 my $sth = $dbh->prepare("   SELECT distinct(resources.$f)
#                                             FROM assigned_resources, resources
#                                             WHERE
#                                                 assigned_resource_index = \'CURRENT\' AND
#                                                 moldable_job_id = $moldable_id AND
#                                                 assigned_resources.resource_id = resources.resource_id
#                                         ");
#                 $sth->execute();
#                 my $value_str;
#                 while (my @ref = $sth->fetchrow_array()){
#                     $value_str .= $dbh->quote($ref[0]);
#                     $value_str .= ',';
#                 }
#                 $sth->finish();
#                 return if (!defined($value_str));
#                 chop($value_str);
#                 my $req =  "UPDATE resources
#                             SET scheduler_priority = scheduler_priority + ($value * $index * $coeff)
#                             WHERE
#                                 $f IN (".$value_str.")
#                            ";
#                 $dbh->do($req);
#             }
#             add_new_event($dbh,"SCHEDULER_PRIORITY_UPDATED_$state",$job_id,"Scheduler priority for job $job_id updated (".get_conf("SCHEDULER_PRIORITY_HIERARCHY_ORDER").")");
#         }
#     }
# }


def update_scheduler_last_job_date(date, moldable_id):
    db.query(Resource).filter(AssignedResource.Moldable_job_id == moldable_id)\
                      .filter(AssignedResource.Resource_id == Resource.resource_id)\
                      .update({Resource.last_job_date: date})
    db.commit()

# to remove
init_judas_notify_user()
