# coding: utf-8
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




def get_oar_pid_file_name(job_id):
    oar.error("get_oar_pid_file_name id not YET IMPLEMENTED")

def get_default_suspend_resume_file():
    oar.error("get_default_suspend_resume_file id not YET IMPLEMENTED")

def manage_remote_commands():
    oar.error("manage_remote_commands id not YET IMPLEMENTED")

