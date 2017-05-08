#!/usr/bin/env python
# coding: utf-8
"""

Examples:
{
 "cmd": "WAKEUP",
 "nodes": ["node1", "node2" ] 
}


"""


from multiprocessing import Process
from oar.lib.compat import iterkeys
from oar.lib import (config, get_logger)
from oar.kao.node import (get_alive_nodes_with_jobs)
import zmq

# Set undefined config value to default one
DEFAULT_CONFIG = {
    'HULOT_SERVER': 'localhost',
    'HULOT_PORT' : 6670,
    'ENERGY_SAVING_WINDOW_SIZE': 25,
    'ENERGY_SAVING_WINDOW_TIME': 60,
    'ENERGY_SAVING_WINDOW_TIMEOUT': 120,
    'ENERGY_SAVING_NODE_MANAGER_WAKEUP_TIMEOUT': 900,
    'ENERGY_MAX_CYCLES_UNTIL_REFRESH': 5000,
    'OAR_RUNTIME_DIRECTORY': '/tmp/oar_runtime',
    'ENERGY_SAVING_NODES_KEEPALIVE': "type='default':0",
    'ENERGY_SAVING_WINDOW_FORKER_BYPASS': 'yes',
    'ENERGY_SAVING_WINDOW_FORKER_SIZE': 20
}

config.setdefault_config(DEFAULT_CONFIG)

logger = get_logger("oar.modules.hulot", forward_stderr=True)

def execute_action(command, nodes, cmd, forker_type):
    if nodes:
        




# Sends 'check' signal on the named pipe to Hulot
def check():
    #my @tab = ();
    #return send_cmd_to_fifo( \@tab, "CHECK" );
    pass

def halt_nodes(nodes):
    # return send_cmd_to_fifo( $nodes, "HALT" );
    pass

def wake_up_nodes(nodes):
pass

#Fill the timeouts hash with the different timeouts
def fill_timeouts(str_timeouts):

    # Timeout to consider a node broken (suspected) if it has not woken up
    # The value can be an integer of seconds or a set of pairs.
    # For example, "1:500 11:1000 21:2000" will produce a timeout of 500
    # seconds if 1 to 10 nodes have to wakeup, 1000 seconds if 11 t 20 nodes
    # have to wake up and 2000 seconds otherwise.
    #ENERGY_SAVING_NODE_MANAGER_WAKEUP_TIMEOUT="900"

    timeouts = {}
    if re.match(r'^\s*\d+\s*$', str_timeouts):
        timeouts[1] = int(str_timeouts)
    else:
        #Remove front and final spaces
        str_timeouts = re.sub(r'^\s+|\s+$', '', str_timeouts)
        for str_nb_timeout in re.split(r'\s+', str_timeouts):
            #Each couple of values is only composed of digits separated by colon
            if re.match(r'^\d+:\d+$', str_nb_timeout):
                nb_timeout = re.split(r':', str_nb_timeout)
                timeouts[int(nb_timeout[0])] = int(nb_timeout[1])
            else:
                logger.warning(nb_timeout + " is not a valid couple for a timeout")
    if not timeouts:
        timeouts[1] = 900
        logger.warning('Timeout not properly defined, using default value: ' + str(timeouts[1]))
        
    return timeouts

#Choose a timeout based on the number of nodes to wake up
def get_timeout(timeouts, nb_nodes):

    timeout = timeouts[1]
    #Search for the timeout of the corresponding interval
    for nb in iterkeys(timeouts).sort():
        if nb_nodes < nb:
            break
        timeout = timeouts[nb]
    return timeout


class HulotClient(object):
    def __init__(self):
        # Initialize zeromq context
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.PUSH) # to signal Almighty
        try:
            self.appendice.connect('tcp://' + config['HULOT_SERVER'] + ':' + config['HULOT_PORT'])
        except:
            logger.error('Failed to connect to Hulot')
            exit(1)

class Hulot(object):

    def __init__(self):
        logger.info('Initiating Hulot, the energy saving module');
        
        # Intialize zeromq context
        self.context = zmq.Context()
        # IP addr is required when bind function is used on zmq socket
        ip_addr_hulot = socket.gethostbyname(config['HULOT_SERVER'])
        self.socket = self.context.socket(zmq.PULL)
        try:
            self.socket.bind('tcp://' + ip_addr_hulot + ':' + config['HULOT_PORT'])
         except:
            logger.error('Failed to bind Hulot endpoint')
            exit(1)
        
        self.timeouts = fill_timeouts(config('ENERGY_SAVING_NODE_MANAGER_WAKEUP_TIMEOUT'))

        self.executors = []
        if (config('ENERGY_SAVING_WINDOW_FORKER_BYPASS') == 'no'):
            self.max_executors = -1
        else:
            self.max_executors = int(config('ENERGY_SAVING_WINDOW_FORKER_SIZE'))
        
        # Load state if exists
        
        #    if (-s "$runtime_directory/hulot_status.dump") {
        #      my $ref = do "$runtime_directory/hulot_status.dump";
        #      if ($ref) {
        #        if (defined($ref->[0]) && defined($ref->[1]) &&
        #            ref($ref->[0]) eq "HASH" && ref($ref->[1]) eq "HASH") {
        #          oar_debug("[Hulot] State file found, loading it\n");
        #          %nodes_list_running = %{$ref->[0]};
        #          %nodes_list_to_remind = %{$ref->[1]};
        #        }
        #      }
        #    }
        #    unlink "$runtime_directory/hulot_status.dump";
        #

        ############################################
        
        # Init keepalive values ie construct a hash:
        #      sql properties => number of nodes to keepalive
        # given the ENERGY_SAVING_NODES_KEEPALIVE variable such as:
        # "cluster=paradent:nodes=4,cluster=paraquad:nodes=6"
        
        # Number of nodes to keepalive per properties:
        #     $keepalive{<properties>}{"min"}=int
        # Number of nodes currently alive and with no jobs, per properties:
        #     $keepalive{<properties>}{"cur_idle"}=int 
        # List of nodes corresponding to properties:
        #     $keepalive{<properties>}{"nodes"}=@;

        
        # Creates the fifo if it doesn't exist
        #unless ( -p $FIFO ) {
        #    unlink $FIFO;
        #    system( 'mknod', '-m', '600', $FIFO, 'p' );
        #}
        #
        ## Test if the FIFO has been correctly created
        #unless ( -p $FIFO ) {
        #    oar_error("[Hulot] Could not create the fifo $FIFO!\n");
        #    exit(1);
        #}
        #
        ## Create message queue for Inter Processus Communication
        #$id_msg_hulot = msgget( IPC_PRIVATE, IPC_CREAT | S_IRUSR | S_IWUSR );
        #if ( !defined $id_msg_hulot ) {
        #        oar_error("[Hulot] Cannot create message queue : msgget failed\n");
        #        exit(1);
        #}
        #
        #my $count_cycles;
        #

        
    def run(self, loop=True):
        logger.info("Starting Hulot's main loop")

        while True:

            message = self.socket.recv_json().decode('utf-8')

            command = message['cmd']
            nodes = []
            if 'nodes' in message:
                nodes = message['nodes']

            if cmd == 'CHECK':
                logger.debug('Got request: ' + cmd) 
            else:
                logger.debug('Got request: ' + cmd + ' for nodes: ' + str(nodes))


            # Identify idle and occupied nodes 
            all_occupied_nodes = get_alive_nodes_with_jobs()
            nodes_that_can_be_waked_up = get_nodes_that_can_be_waked_up(get_date())


            #for properties in iterkeys(keepalive):
                
            
            # Identify idle and occupied nodes           
 #           my @all_occupied_nodes=OAR::IO::get_alive_nodes_with_jobs($base);
 #           my @nodes_that_can_be_waked_up=OAR::IO::get_nodes_that_can_be_waked_up($base,OAR::IO::get_date($base));
 #           foreach my $properties (keys %keepalive) {
 #             my @occupied_nodes;
 #             my @idle_nodes;
 #             $keepalive{$properties}{"nodes"} =
 #                [ OAR::IO::get_nodes_with_given_sql($base,$properties) ];
 #             $keepalive{$properties}{"cur_idle"}=0;
 #             foreach my $alive_node (OAR::IO::get_nodes_with_given_sql($base,
 #                                       $properties. " and (state='Alive' or next_state='Alive')")) {
 #               if (grep(/^$alive_node$/,@all_occupied_nodes)) {
 #                 push(@occupied_nodes,$alive_node);
 #               }else{
 #                 $keepalive{$properties}{"cur_idle"}+=1;
 #                 push(@idle_nodes,$alive_node);
 #               }
 #             }
 #             #oar_debug("[Hulot] cur_idle($properties) => "
 #             #     .$keepalive{$properties}{"cur_idle"}."\n");
 #
 #             #print DUMP "point 3:"; print `ps -p $pid -o rss h >> /tmp/hulot_dump`; 
 #
 #             # Wake up some nodes corresponding to properties if needed
 #             my $ok_nodes=$keepalive{$properties}{"cur_idle"}
 #                           - $keepalive{$properties}{"min"};
 #             my $wakeable_nodes=@{$keepalive{$properties}{"nodes"}}
 #                           - @idle_nodes - @occupied_nodes;
 #             while ($ok_nodes < 0 && $wakeable_nodes > 0) {
 #               foreach my $node (@{$keepalive{$properties}{"nodes"}}) {
 #                 unless (grep(/^$node$/,@idle_nodes) || grep(/^$node$/,@occupied_nodes)) {
 #                   # we have a good candidate to wake up
 #                   # now, check if the node has a good status
 #                   $wakeable_nodes--;
 #                   if (grep(/^$node$/,@nodes_that_can_be_waked_up)) {
 #                     $ok_nodes++;
 #                     # add WAKEUP:$node to list of commands if not already
 #                     # into the current command list
 #                     if (not defined($nodes_list_running{$node})) {
 #                       $nodes_list_to_process{$node} =
 #                         { 'command' => "WAKEUP", 'timeout' => -1 };
 #                       oar_debug("[Hulot] Waking up $node to satisfy '$properties' keepalive (ok_nodes=$ok_nodes, wakeable_nodes=$wakeable_nodes)\n");
 #                     }else{
 #                        if ($nodes_list_running{$node}->{'command'} ne "WAKEUP") {
 #                        oar_debug("[Hulot] Wanted to wake up $node to satisfy '$properties' keepalive, but a command is already running on this node. So doing nothing and waiting for the next cycles to converge.\n");
 #                        }
 #                     }
 #                   }
 #                   last if ($ok_nodes >=0 || $wakeable_nodes <= 0);
 #                 }
 #               }
 #             }
 #           }
 #


######################



 
            
            logger.debug('Launching commands to nodes')
            # Launching commands
            while(executors_tolaunch):
                if (max_executors == -1) or (len(active_executors) < max_executors):
                    executor = Process(target=action_executor, args=(), kwargs=command)
                    executor.start()
                    executors.append(executor)
                else:
                    # max executor is reach 
                    for executor in executors:
                        if not executor.is_alive():
                            executors.remove(executor)
                    # sleep a little
                    
                        
                


            # From Hulot.pm
            # Suicide to workaround memory leaks. Almighty will restart hulot.
            # TODO ?
            
        if not loop:
            break
        
if __name__ == '__main__':  # pragma: no cover
    hulot = Hulot()
    hulot.run()
