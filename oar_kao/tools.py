import os
import threading
import subprocess
 
from oar.lib import config, get_logger

log = get_logger("oar.kao.tools")

def fork_and_feed_stdin(cmd, timeout_cmd, nodes):
    log.error("OAR::Tools::fork_and_feed_stdin NOT YET IMPLEMENTED")
    return False

def send_to_hulot(cmd, data):
    config.setdefault_config({"FIFO_HULOT": "/tmp/oar_hulot_pipe"})
    fifoname = config["FIFO_HULOT"]
    try:
        with open(fifoname) as fifo:
            fifo.write('HALT' + ':' + data + '\n')
            fifo.flush()
    except IOError as e:
        e.strerror = 'Unable to communication with Hulot: %s (%s)' %  fifoname % e.strerror
        log.error(e.strerror)
        return 1
    return 0

class Command(object):
    """
    Run subprocess commands in a different thread with TIMEOUT option.
    Based on jcollado's solution:
    http://stackoverflow.com/questions/1191374/subprocess-with-timeout/4825933#4825933
    """
    def __init__(self, cmd):
        self.cmd = cmd
        self.process = None

    def run(self, timeout):
        def target():
            self.process = subprocess.Popen(self.cmd, shell=True)
            self.process.communicate()

        thread = threading.Thread(target=target)
        thread.start()

        error = None
        thread.join(timeout)
        if thread.is_alive():
            error ('Timeout: Terminating process')
            oar.error(error)
            self.process.terminate()
            thread.join()

        return (error, self.process.returncode)
