#!/usr/bin/env python
# coding: utf-8
"""

 This module is responsible of waking up / shutting down nodes
 when the scheduler decides it (writes it on a named pipe)

 CHECK command is sent on the zmq PULL socket oh Hulot:

  - by MetaScheduler if there is no node to wake up / shut down in order:
      - to check timeout and check memorized nodes list <TODO>
      - to check booting nodes status

  TOFINISH: Hulot will integrate window guarded launching processes
  - by windowForker module:
      - to avoid zombie process
      - to messages received in queue (IPC)


Example of received message:
{
 "cmd": "WAKEUP",
 "nodes": ["node1", "node2" ] 
}


"""


from multiprocessing import Process
from oar.lib.compat import iterkeys
from oar.lib import (config, get_logger)
from oar.lib.node import (get_alive_nodes_with_jobs, get_nodes_with_given_sql,
                          change_node_state)
from oar.lib.event import (add_new_event, add_new_event_with_host) 
import oar.lib.tools as tools
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
        pass
    

def  check_reminded_list(nodes_list_running, nodes_list_to_remind, nodes_list_to_process)):
    # Checks if some nodes in list_to_remind can be processed 
    for node, cmd_info in iteritems(nodes_list_to_remind):
        if node not in nodes_list_running:
            # move this node from reminded list to list to process
            logger.debug("Adding '" + node + '=>' + cmd_info  + "' to list to process.")
            nodes_list_to_process[node] = {'command': cmd_info['command'], 'timeout': -1}}
            del nodes_list_to_remind[node]


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

    # Sends 'check' signal on the named pipe to Hulot
    def check(self):
        #my @tab = ();
        #return send_cmd_to_fifo( \@tab, "CHECK" );
        pass

    def halt_nodes(self, nodes):
        # return send_cmd_to_fifo( $nodes, "HALT" );
        pass

    def wake_up_nodes(self, nodes):
        pass

            
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

        # TODO
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

        keepalive = {}
        str_keepalive = config('ENERGY_SAVING_NODES_KEEPALIVE')

        if not re.match(r'.+:\d+,*', str_keepalive):
            logger.error('Syntax error into ENERGY_SAVING_NODES_KEEPALIVE !')
            exit(3)
        else:
            for keepalive_item in re.split('\s*\&\s*', str_keepalive):
                prop_nb = keepalive_item.strip(':')
                properties = prop_nb[0]
                nb_nodes = prop_nb[1]
                if not re.match(r'^(\d+)$'):
                    logger.error('Syntax error into ENERGY_SAVING_NODES_KEEPALIVE ! (not an integer)')
                    exit(2)
                keepalive[properties] = {'nodes': [], 'min': nb_nodes}
                logger.debug('Keepalive(' + properties + ') => ' + nb_nodes)
                
        # TODO
        #my $count_cycles;
        #

        
    def run(self, loop=True):
        logger.info("Starting Hulot's main loop")

        nodes_list_to_process = {}
        nodes_list_to_remind = {}
        nodes_list_running = {}

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
            nodes_that_can_be_waked_up = get_nodes_that_can_be_waked_up(tools.get_date())

            for properties in iterkeys(keepalive):
                occupied_nodes = []
                idle_nodes = []
                keepalive[properties]['nodes'] = [p for in get_nodes_with_given_sql(properties)]
                keepalive[properties]['current_idle'] = 0
                alive_nodes = get_nodes_with_given_sql(properties +
                                                       "and (state='Alive' or next_state='Alive')")
                for alive_node in alive_nodes:
                    if alive_node in all_occupied_nodes:
                        occupied_nodes.append(alive_node)
                    else:
                        keepalive[properties]['current_idle'] += 1
                        idle_nodes.append(alive_node)
                        
                logger.debug('current_idle('+ properties + ') => ' + keepalive[properties]['current_idle'])


                # Wake up some nodes corresponding to properties if needed
                ok_nodes = keepalive[properties]['current_idle'] -  keepalive[properties]['min']
                nodes = keepalive[properties]['nodes']
                wakeable_nodes = [n for n in nodes if (n not in occupied_nodes) and (n not in idle_nodes)]

                for node in wakeable_nodes:
                    if ok_nodes >= 0:
                        break
                    # we have a good candidate to wake up
                    # now, check if the node has a good status
                    if node in nodes_that_can_be_waked_up:
                        ok_nodes += 1
                        # add WAKEUP: node to list of commands if not already
                        # into the current command list
                        if node not in nodes_list_running:
                            nodes_list_running[node] =  { 'command': 'WAKEUP', 'timeout': -1 }
                            logger.debug('Waking up ' + node + " to satisfy '" + properties + "' keepalive (ok_nodes=" +\
                                         ok_nodes + ', wakeable_nodes=' + len(wakeable_nodes))
                        else:
                            if nodes_list_running[node]['command'] != 'WAKEUP':
                                logger.debug('Wanted to wake up ' + node + " to satisfy '" + properties +\
                                             "' keepalive, but a command is already running on this node. " +\
                                             'So doing nothing and waiting for the next cycles to converge.')
 
            # Retrieve list of nodes having at least one resource Alive
            nodes_alive = get_nodes_with_given_sql("state='Alive'")
            
            # Checks if some booting nodes need to be suspected
            for node, cmd_info in iteritems(nodes_list_running):
                if cmd_info['command'] == 'WAKEUP':
                    if node in nodes_alive:
                        logger.debug("Booting node '" + node + "' seems now up, so removing it from running list.")
                        # Remove node from the list running nodes
                        del nodes_list_running[node]
                    elif tools.get_date > cmd_info['timeout']:
                         change_node_state(node, 'Suspected', config)
                         str = '[Hulot] Node ' + node + 'was suspected because it did not wake up before the end of the timeout'
                         add_new_event_with_host('LOG_SUSPECTED', 0, str, [node])
                         # Remove suspected node from the list running nodes
                         del nodes_list_running[node]
                         # Remove this node from received list (if node is present) because it was suspected
                         del nodes[node]
                         
            # Check if some nodes in list_to_remind can be processed
            check_reminded_list(nodes_list_running, nodes_list_to_remind, nodes_list_to_process)

            # Checking if each couple node/command was already received or not
            for node in nodes:
                node_finded = False
                node_toAdd = False
                node_toRemind = False
                if nodes_list_running:
                    # Checking
                    for node_running, cmd_info in iteritems(nodes_list_running):
                        if node == node_running:
                            node_finded = True
                            if command != cmd_info['command']:
                                # This node is already planned for an other action
                                # We have to keep in memory this new couple node/command
                                node_toRemind = True
                            else:
                                logger.debug("Command '" + cmd_info['command'] + "' is already running on node '" + node + "'")
                                
                    if not node_finded:
                        node_toAdd = True
                                
                else:
                    node_toAdd = True

                if node_toAdd:
                    # Adding couple node/command to the list to process
                    logger.debug("Adding '" + node + '=>' + command +"' to list to process")
                    nodes_list_to_process[node] = {'command':command, 'timeout': -1}
                    
                if node_toRemind:
                    # Adding couple node/command to the list to remind
                    logger.debug("Adding '" + node + '=>' + command +"' to list to remember")
                    nodes_list_to_remind[node] = {'command': command, 'timeout': -1}

                    
            # Creating command list
            command_toLaunch = []
            match = False
            # Get the timeout taking into account the number of nodes
            # already waking up + the number of nodes to wake up
            timeout = get_timeout(timeouts, len(nodes_list_running) + len(nodes_list_to_process))

            for node, cmd_info in iteritems(nodes_list_to_process):
                cmd = cmd_info['command']
                if cmd == 'WAKEUP':
                    #Save the timeout for the nodes to be processed.
                    cmd_info['timeout'] = tools.get_date() + timeout
                    command_toLaunch.append('WAKEUP:' + node)
                elif cmd =='HALT':
                    # Don't halt nodes that needs to be kept alive
                    match = False
                    for properties, prop_info in iteritems(keepalive):
                        nodes = prop_info['nodes']
                        if node in nodes:
                          if prop_info['current_idle'] <= prop_info['min']:
                              logger.debug("Not halting '" + node + "' because I need to keep alive " +\
                                            prop_info['min'] + " nodes having '" + properties + "'")
                              match = True
                              del nodes_list_running[node]
                              del nodes_list_to_process[node]
                    # If the node is ok to be halted
                    if not match:
                        # Update the keepalive counts
                        for properties, prop_info in iteritems(keepalive):
                            nodes = prop_info['nodes']
                            if node in nodes:
                                prop_info['current_idle'] -= 1
                                
                        # Change state node to "Absent" and halt it
                        change_node_state(node, 'Absent')
                        logger.debug("Hulot module puts node '" + node + "' in energy saving mode (state~Absent)")
                        command_toLaunch.append('HALT:' + node)
                         
                else:
                   logger.error("Unknown command: '" + cmd + "' for node '" + node + "'")     
                   exit(1)            
            
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