# coding: utf-8
from __future__ import unicode_literals, print_function

from oar.lib import config, get_logger

logger = get_logger("oar.kao.tools")


def fork_and_feed_stdin(cmd, timeout_cmd, nodes):
    logger.error("OAR::Tools::fork_and_feed_stdin NOT YET IMPLEMENTED")
    return True


def send_to_hulot(cmd, data):
    config.setdefault_config({"FIFO_HULOT": "/tmp/oar_hulot_pipe"})
    fifoname = config["FIFO_HULOT"]
    try:
        with open(fifoname, 'w') as fifo:
            fifo.write('HALT' + ':' + data + '\n')
            fifo.flush()
    except IOError as e:
        e.strerror = 'Unable to communication with Hulot: %s (%s)' % fifoname % e.strerror
        logger.error(e.strerror)
        return 1
    return 0


def get_oar_pid_file_name(job_id):
    logger.error("get_oar_pid_file_name id not YET IMPLEMENTED")


def get_default_suspend_resume_file():
    logger.error("get_default_suspend_resume_file id not YET IMPLEMENTED")


def manage_remote_commands():
    logger.error("manage_remote_commands id not YET IMPLEMENTED")
