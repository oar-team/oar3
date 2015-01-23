from oar.lib import db

notification_socket = None

def create_tcp_notification_socket():
    global notification_socket
    notification_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server = config["SERVER_HOSTNAME"]
    port =  config["SERVER_PORT"]
    try:
        notification_socket.connect( (server, port) )
    except socket.error, exc:
        log.error("Connection to " + server + ":" + port + " raised exception socket.error: " + exc)
        sys.exit(1)

# get_date
# returns the current time in the format used by the sql database
def get_date():
    result = db.engine.execute("select EXTRACT(EPOCH FROM current_timestamp)").scalar()
    return int(result)

def notify_user(job, state, msg):
    #TODO see OAR::Modules::Judas::notify_user
    log.info("notify_user not yet implemented !!!! (" + state + ", " + msg + ")" )

# update_current_scheduler_priority
# Update the scheduler_priority field of the table resources
def update_current_scheduler_priority(job, value, state):
    log.info("update_current_scheduler_priority not yet implemented !!!! job.id: " + str(job.id) + ", state: " + state + ", value: " + str(value) )
#  # code from IO.pm update_current_scheduler_priority  
#
#     my $dbh = shift;
#     my $job_id = shift;
#     my $moldable_id = shift;
#     my $value = shift;
#     my $state = shift;
    
#     $state = "STOP" if ($state ne "START");

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
    req = db.query(Resource).update({Resource.last_job_date: date}).filter(AssignedResourcesMoldable_job_id == moldable_id)\
                                                                   .filter(AssignedResources.Resource_id == Resources.resource_id)
    db.engine.execute(req)
    db.commit()
