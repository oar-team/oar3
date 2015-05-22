import os
from oar.lib import config, get_logger

log = get_logger("oar.kao.tools")

def fork_and_feed_stdin(cmd, timeout_cmd, nodes):
    log.error("OAR::Tools::fork_and_feed_stdin NOT YET IMPLEMENTED")
    return False


def resume_job(job):
    log.error("RESUME PART is not YET IMPLEMENTED")
    #See oar_meta_sched.pl RESUME PART


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
