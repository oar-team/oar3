import struct
import socket
import sys
import os
import json

from oar.lib import config
from oar.kao.simsim import ResourceSetSimu, JobSimu
from oar.kao.helpers import plot_slots_and_job

config['LOG_FILE'] = '/dev/stdout'

jobs = {}
jobs_completed = []
jobs_waiting = []

sched_delay = 5.0

nb_completed_jobs = 0
nb_jobs = 0
nb_res = 0

def create_uds(uds_name):
    # Make sure the socket does not already exist
    try:
        os.unlink(uds_name)
    except OSError:
        if os.path.exists(uds_name):
            raise

    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    
    # Bind the socket to the port
    print >>sys.stderr, 'starting up on %s' % uds_name
    sock.bind(uds_name)

    # Listen for incoming connections
    sock.listen(1)

    return sock

def read_bat_msg(connection):
    lg_str = connection.recv(4)
    
    if not lg_str:
        print "connection is closed by batsim core"
        exit(1)
        
    print 'from client (lg_str): %r' % lg_str
    lg = struct.unpack("i",lg_str)[0]
    print 'size msg to recv %d' % lg
    msg = connection.recv(lg)
    print 'from client: %r' % msg
    sub_msgs = msg.split('|')
    data = sub_msgs[-1].split(":")
    if data[2] != 'T':
        raise Exception("Terminal submessage must be T type")
    time = float(data[1])

    jobs_submitted = []
    jobs_completed = []
    for i in range(len(sub_msgs)-1):
        data = sub_msgs[i].split(':')
        if data[2] == 'S':
            jobs_submitted.append( int(data[3]) )
        elif data[2] == 'C':
            jobs_completed.append( int(data[3]) )
        else:
            raise Exception("Unknow submessage type" + data[2] )  

    return (time, jobs_submitted, jobs_completed)

def send_bat_msg(connection, now, jids_toLaunch):
    msg = "0:" + str(time)
    if jids_toLaunch:
        msg += ":J:" 
        for jid in jids_toLaunch:
            msg += str(jid) + "="
            for r in jobs[jid].res_set:
               msg += str(r) + ","
            msg = msg[:-1] + ";" # replace last comma by semicolon separtor between jobs
        msg = msg[:-1] # remove last semicolon

    else: #Do nothing        
        msg += ":N"

    print msg
    lg = struct.pack("i",int(len(msg)))
    connection.sendall(lg)
    connection.sendall(msg)

def load_json_workload_profile(filename):
    wkp_file = open("filname")
    wkp = json.load(wkp_file)
    return wkp["jobs"], wkp["nb_res"] 

class BatEnv:
    pass

class BatSched:
    def __init__(self, res_set, jobs, uds_name = '/tmp/bat_socket', mode_platform = "batsim"):
        self.env = BatEnv
        self.platform = Platform(mode_platform, env=self.env, resource_set=res_set, jobs=jobs )

        self.sock = create_uds(uds_name)
        print >>sys.stderr, 'waiting for a connection'
        self.connection, self.client_address = sock.accept()
        
    def sched_loop(self):
        while True: #ADD (nb_completed < nb_jobs)

            now_str, jobs_submitted, new_jobs_completed = read_sched_msg(self.connection)
            
            nb_completed_jobs += len(new_jobs_completed)
            
            if nb_completed_jobs == nb_jobs:
                break;

            now = int(now_str)

            print "call schedule_cycle.... ", now
            schedule_cycle(self.platform,now, "test")

            #retrieve jobs to launch
            jids_toLaunch = [] 
            for jid, job in self.platform.assigned_jobs.iteritems():
                if job.start_time == now:
                    self.waiting_jids.remove(jid)
                    jobs_tolaunch.append(jid)
                    job.state = "Running"
                    print "tolaunch:", jid
                    self.platform.running_jids.append(jid)

            now += self.sched_delay

            send_bat_msg(self.connection, now, jids_toLaunch)
        
            #print >>sys.stderr, 'received %d' % data
            #if data:
            #    print >>sys.stderr, 'sending data back to the client'
            #    connection.sendall(data)
            #else:
            #    print >>sys.stderr, 'no more data from', client_address
            #    break
    def run(self):
        self. sched_loop()
            
##############

#
# Load workload
#

json_jobs, nb_res = load_json_workload_profile(sys.argv[1])

#
# generate ResourceSet
#
hy_resource_id = [[(i,i)] for i in range(1,nb_res+1)]
res_set = ResourceSetSimu(
    rid_i2o = range(nb_res+1),
    rid_o2i = range(nb_res+1),
    roid_itvs = [(1,nb_res)],
    hierarchy = {'resource_id': hy_resource_id},
    available_upto = {2147483600:[(1,nb_res)]}
)

#
# generate jobs
#

for j in json_jobs:
    jobs[i] = JobSimu( id = int(j["id"]),
                       state = "Waiting",
                       queue = "test",
                       start_time = j["subtime"],
                       walltime = j["walltime"],
                       types = {},
                       res_set = [],
                       moldable_id = 0,
                       mld_res_rqts =  [(i, 60, [([("resource_id", j["res"])], [(0,nb_res-1)])])],
                       run_time = 0,
                       key_cache = "",
                       ts=False, ph=0
                       )

batsched = BatSched(res_set, jobs)
batsched.run()
