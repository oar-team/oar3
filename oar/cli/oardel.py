# -*- coding: utf-8 -*-
"""oardel - delete or checkpoint job(s)."""

from __future__ import print_function
import click
from oar import (VERSION)
from .utils import (print_warning, print_error, print_info, print_error_exit, usage)

from oar.lib.job_handling import (get_array_job_ids, ask_checkpoint_signal_job,
                                  get_job_ids_with_given_properties,
                                  get_job_types, get_job_current_hostnames, frag_job)

import oar.lib.tools as tools

click.disable_unicode_literals_warning = True

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
@click.option('-V', '--version',  help='Print OAR version.')
def cli(job_id, checkpoint, signal, besteffort, array, sql, force_terminate_finishing_job, version):

    job_ids = job_id

    exit_value = 0

    # import pdb; pdb.set_trace()
    
    if not job_ids and not sql:
        usage()
        exit(1)
    
    if version:
        print('OAR version : ' + VERSION)

    if array:
        job_ids = get_array_job_ids(job_ids)
        if not job_ids:
            print_warning("There are no job for this array job ({})".format(array))
            exit_value = 4

    if sql:
        job_ids = get_job_ids_with_given_properties(sql)
        if not job_ids:
            print_warning("There are no job for this SQL WHERE clause ({})".format(array))
            exit_value = 4

    if checkpoint:
        for job_id in job_ids:
            print_info("Checkpointing the job {} ...".format(job_id))
            error, error_msg = ask_checkpoint_signal_job(job_id)
            if error > 0:
                if error == 1:
                    print_error(error_msg, 1)
                    exit_value = 1
                elif error == 3:
                    print_error(error_msg, 7)
                    exit_value = 7
                else:
                    print_error(error_msg, 5)
                    exit_value = 5
            else:
                # Retrieve node names used by the job
                nodes = get_job_current_hostnames(job_id)
                types = get_job_types(job_id)
                # TODO


    elif signal:
        # TODO
        pass
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
            print_info("Deleting the job = {} ...".format(job_id))
            error = frag_job(job_id)
            error_msg = ''
            if error == -1:
                error_msg = 'Cannot frag {} ; You are not the right user.'.format(job_id)
                exit_value = 1
            if error == -2:
                error_msg = 'Cannot frag {} ; This job was already killed.'.format(job_id)
                notify_almighty = True
                exit_value = 6
            if error != 0:
                print_warning(error_msg)
            else:
                print_info(error_msg)
                notify_almighty = True
                jobs_registred.append(job_id)


        if notify_almighty:
            #Signal Almigthy
            nb_sent = tools.notify_almighty('ChState')
            if nb_sent > 0:
                tools.notify_almighty('Qdel')

            if nb_sent <= 0:
                print_error('Unablde to notify Almighty')
                exit_value = 2
            else:
                print_info('The job(s) {}  will be deleted in the near future.'\
                           .format(jobs_registred))

    exit(exit_value)

