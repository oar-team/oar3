#!/usr/bin/env python
# coding: utf-8

from oar.lib import (config, get_logger)
from oar.lib.tools import DEFAULT_CONFIG

from oar.lib.event import add_new_event

from oar.lib.tools import (Popen, TimeoutExpired)
                           
logger = get_logger("oar.modules.bipbip", forward_stderr=True)

class BipBip(object):

    def __init__(self):
        config.setdefault_config(DEFAULT_CONFIG)
        self.server_prologue = config['SERVER_PROLOGUE_EXEC_FILE']
        self.server_epilogue = config['SERVER_EPILOGUE_EXEC_FILE']

        self.exit_code = 0
        
    def run(self):
        #Check if we must treate the end of a oarexec
        pass

    def call_server_prologue(job):
        # PROLOGUE EXECUTED ON OAR SERVER #
        # Script is executing with job id in arguments
        if self.server_prologue:
            timeout = config['SERVER_PROLOGUE_EPILOGUE_TIMEOUT']
            cmd = [self.server_prologue, str(job.id)]

            try:
                child = Popen(cmd)
                return_code = child.wait(timeout)

                if return_code:
                    msg = '[' + str(job.id) + '] Server prologue exit code: ' + str(return_code)\
                          + ' (!=0) (cmd: ' + str(cmd) + ')'
                    logger.error(msg)
                    add_new_event('SERVER_PROLOGUE_EXIT_CODE_ERROR', job.id, '[bipbip] ' + msg)
                    tools.notify_almighty('ChState')
                    if (job.type == 'INTERACTIVE') and (job.reservation == 'None'):
                        tools.notify_interactif_user(job, 'ERROR: SERVER PROLOGUE returned a bad value')
                    self.exit_code = 2
                    return 1
                
            except OSError as e:   
                logger.error('Cannot run: ' + str(cmd))
                
            except TimeoutExpired as e:
                tools.kill_child_processes(child.pid)
                msg = '[' + str(job.id) + '] Server prologue timeouted (cmd: ' + str(cmd)
                logger.error(msg)
                add_new_event('SERVER_PROLOGUE_TIMEOUT', job.id, '[bipbip] ' + msg)
                tools.notify_almighty('ChState')
                if (job.type == 'INTERACTIVE') and (job.reservation == 'None'):
                    tools.notify_interactif_user(job, 'ERROR: SERVER PROLOGUE timeouted')
                self.exit_code = 2
                return 1
            
            return 0
            
            
def main():
    bipbip = BipBip()
    bipbip.run()
    return bipbip.exit_code
    
if __name__ == '__main__':  # pragma: no cover
    exit_code = main()
    sys.exit(exit_code)
