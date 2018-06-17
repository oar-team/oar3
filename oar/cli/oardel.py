# -*- coding: utf-8 -*-
"""oardel - delete or checkpoint job(s)."""

import click
from oar import (VERSION)
from .utils import CommandReturns

from oar.lib import config

from oar.lib.job_handling import (get_array_job_ids, ask_checkpoint_signal_job,
                                  get_job_ids_with_given_properties,
                                  get_job_types, get_job_current_hostnames, frag_job,
                                  add_new_event)

import oar.lib.tools as tools

DEFAULT_CONFIG = {
    'COSYSTEM_HOSTNAME': '127.0.0.1',
    'DEPLOY_HOSTNAME': '127.0.0.1',
    'OPENSSH_CMD': '/usr/bin/ssh -p 6667',
    'OAR_SSH_CONNECTION_TIMEOUT': 200,
    'OAR_RUNTIME_DIRECTORY': '/var/lib/oar',
}


click.disable_unicode_literals_warning = True


def oardel(job_ids, checkpoint, signal, besteffort, array, sql, force_terminate_finishing_job, version, user=None, cli=True):
    
    config.setdefault_config(DEFAULT_CONFIG)

    cmd_ret = CommandReturns(cli)

    if version:
        cmd_ret.print_('OAR version : ' + VERSION)
        return cmd_ret

    if not job_ids and not sql and not array:
        cmd_ret.usage(1)
        return cmd_ret

    if array:
        job_ids = get_array_job_ids(array)

        if not job_ids:
            cmd_ret.warning("There are no job for this array job ({})".format(array), 4)
            return cmd_ret

    if sql:
        job_ids = get_job_ids_with_given_properties(sql)
        if not job_ids:
            cmd_ret.warning("There are no job for this SQL WHERE clause ({})".format(array), 4)
            return cmd_ret

    if checkpoint or signal:
        for job_id in job_ids:
            if checkpoint:
                tag = 'CHECKPOINT'
                cmd_ret.print_('Checkpointing the job {} ...'.format(job_id))
            else:
                tag = 'SIG'
                cmd_ret.print_('Signaling the job {} with {} signal {}.'\
                               .format(job_id, signal, user))

            error, error_msg = ask_checkpoint_signal_job(job_id, signal, user)

            if error > 0:
                cmd_ret.print_('ERROR')
                if error == 1:
                    cmd_ret.error(error_msg, error, 1)
                elif error == 3:
                    cmd_ret.error(error_msg, error, 7)
                else:
                    cmd_ret.error(error_msg, error, 5)
            else:
                # Retrieve hostnames used by the job
                hosts = get_job_current_hostnames(job_id)
                types = get_job_types(job_id)
                host_to_connect = hosts[0]
                if 'cosystem' in types:
                    host_to_connect = config['COSYSTEM_HOSTNAME']
                elif 'deploy' in types:
                    host_to_connect = config['DEPLOY_HOSTNAME']

                timeout_ssh = config['OAR_SSH_CONNECTION_TIMEOUT']
                # TODO 
                error = tools.signal_oarexec(host_to_connect, job_id, 'SIGUSR2',
                                             timeout_ssh, config['OPENSSH_CMD'], '')
                if error != 0:
                    cmd_ret.print_('ERROR')
                    if error == 3:
                        comment = 'Cannot contact {}, operation timouted ({} s).'.\
                                  format(host_to_connect, timeout_ssh)
                        cmd_ret.error(comment, 3, 3)
                    else:
                        comment = 'An unknown error occured.'
                        cmd_ret.error(comment, error, 1)
                    add_new_event('{}_ERROR'.format(tag), job_id, comment)
                else:
                    cmd_ret.print_('DONE')
                    comment = 'The job {} was notified to checkpoint itself on {}.'.\
                              format(job_id, host_to_connect)
                    add_new_event('{}_SUCCESS'.format(tag), job_id, comment)
            
    elif force_terminate_finishing_job:
        # TODO
        pass
    elif besteffort:
        # TODO
        pass
    else:
        # oardel is used to delete some jobs
        notify_almighty = False
        jobs_registred = []
        for job_id in job_ids:
            # TODO array of errors and error messages
            cmd_ret.info("Deleting the job = {} ...".format(job_id))
            error = frag_job(job_id)
            error_msg = ''
            if error == -1:
                error_msg = 'Cannot frag {} ; You are not the right user.'.format(job_id)
                cmd_ret.error(error_msg, -1, 1)
            elif error == -2:
                error_msg = 'Cannot frag {} ; This job was already killed.'.format(job_id)
                notify_almighty = True
                cmd_ret.warning(error_msg, -2, 6)
            elif error == -3:
                error_msg = 'Cannot frag {} ; Job does not exist.'.format(job_id)
                cmd_ret.warning(error_msg, -3, 7)
            else:
                cmd_ret.info(error_msg)
                notify_almighty = True
                jobs_registred.append(job_id)


        if notify_almighty:
            #Signal Almigthy
            # TODO: Send only Qdel ???? oar ChState and Qdel in one message
            completed = tools.notify_almighty('ChState')
            if completed:
                tools.notify_almighty('Qdel')
                cmd_ret.info('The job(s) {}  will be deleted in the near future.'\
                             .format(jobs_registred))
            else:
                cmd_ret.error('Unablde to notify Almighty', -1, 2)

    return cmd_ret


@click.command()
@click.argument('job_id', nargs=-1)
@click.option('-c', '--checkpoint', is_flag=True,
              help='Send the checkpoint signal designed from the "--signal"\
              oarsub command option (default is SIGUSR2) to the process launched by the job "job_id".')
@click.option('-s', '--signal', type=click.STRING,
              help='Send signal  to the process launched by the selected jobs.')
@click.option('-b', '--besteffort', is_flag=True, help='Change jobs to besteffort (or remove them if they are already besteffort)')
@click.option('--array', type=int, help='Handle array job ids, and their sub-jobs')
@click.option('--sql', type=click.STRING, help='Select jobs using a SQL WHERE clause on table jobs (e.g. "project = \'p1\'")')
@click.option('--force-terminate-finishing-job',
              help='Force jobs stuck in the Finishing state to switch to Terminated \
              (Warning: only use as a last resort). This using this option indicates \
              that something nasty happened, nodes where the jobs were executing will \
              subsequently be turned into Suspected.')
@click.option('-V', '--version', is_flag=True,  help='Print OAR version.')
def cli(job_id, checkpoint, signal, besteffort, array, sql, force_terminate_finishing_job, version):

    cmd_ret = oardel(job_id, checkpoint, signal, besteffort, array, sql, force_terminate_finishing_job, version, None)

    cmd_ret.exit()
