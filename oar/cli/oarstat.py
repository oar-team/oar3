# -*- coding: utf-8 -*-
import os
import re
import datetime

from oar import VERSION
from .utils import CommandReturns

from oar.lib import (db, config)
from oar.lib.accounting import (get_accounting_summary, get_accounting_summary_byproject,
                                get_last_project_karma)
from oar.lib.tools import (get_username, sql_to_local, local_to_sql, get_duration)
import oar.lib.tools as tools

import click
click.disable_unicode_literals_warning = True

STATE2CHAR = {
    'Waiting': 'W',
    'toLaunch': 'L',
    'Launching': 'L',
    'Hold': 'H',
    'Running': 'R',
    'Terminated': 'T',
    'Error': 'E',
    'toError': 'E',
    'Finishing': 'F',
    'Suspended': 'S',
    'Resuming': 'S',
    'toAckReservation': 'W',
    'NA': '-'
}

def print_jobs(legacy, jobs):

    now = tools.get_date()

    if legacy:
        print('Job id    S User     Duration   System message\n' +
              '--------- - -------- ---------- ------------------------------------------------')
        now = tools.get_date()
        for job in jobs:
            duration = 0
            if job.start_time:
                if now > job.start_time:
                    if job.state in ['Running', 'Launching', 'Finishing']:
                        duration = now - job.start_time
                    elif job.stop_time != 0:
                        duration = job.stop_time - job.start_time
                    else:
                        duration = -1

            print('{:9}'.format(str(job.id)) + ' ' + STATE2CHAR[job.state] + ' ' +
                  '{:8}'.format(str(job.user)) + ' ' +
                  '{:>10}'.format(str(datetime.timedelta(seconds=duration))) + ' ' +
                  '{:48}'.format(job.message))
    else:
        # TODO
        print(jobs.text)

def print_accounting(accounting, user, sql_property):
    m = re.match(r'\s*(\d{4}\-\d{1,2}\-\d{1,2})\s*,\s*(\d{4}\-\d{1,2}\-\d{1,2})\s*', accounting)
    if m:
        date1 = m.group(1) + ' 00:00:00'
        date2 = m.group(2) + ' 00:00:00'
        d1_local = sql_to_local(date1)
        d2_local = sql_to_local(date2)

        consumptions = get_accounting_summary(d1_local, d2_local, user, sql_property)
        #import pdb; pdb.set_trace()
        # One user output
        if user:
            asked = 0
            if user in consumptions and 'ASKED' in consumptions[user]:
                asked = consumptions[user]['ASKED']
            used = 0
            if user in consumptions and 'USED' in consumptions[user]:
                used = consumptions[user]['USED']

            print('Usage summary for user {} from {} to {}:'.format(user, date1, date2))
            print('-------------------------------------------------------------')

            start_first_window = 'No window found'
            if consumptions[user]['begin']:
                start_first_window = local_to_sql(consumptions[user]['begin'])
            print('{:>28}: {}'.format('Start of the first window', start_first_window)) 

            end_last_window = 'No window found'
            if consumptions[user]['end']:
                end_last_window = local_to_sql(consumptions[user]['end'])
            print('{:>28}: {}'.format('End of the last window', end_last_window))

            print('{:>28}: {} ( {})'.format('Asked consumption', asked, get_duration(asked)))
            print('{:>28}: {} ( {})'.format('Used consumption', used, get_duration(used)))

            print('By project consumption:')

            consumptions_by_project = get_accounting_summary_byproject(d1_local, d2_local, user)
            for project, consumptions_proj in consumptions_by_project.items:
                print('  ' + project + ':')
                asked = 0
                if 'ASKED' in consumptions_proj and user in consumptions_proj['ASKED']:
                    asked = consumptions_proj['ASKED'][user]
                used = 0
                if 'USED' in consumptions_proj and user in consumptions_proj['USED']:
                    used = consumptions_proj['USED'][user]

                print('{:>28}: {} ( {})'.format('Asked consumption', asked, get_duration(asked)))
                print('{:>28}: {} ( {})'.format('Used consumption', used, get_duration(used)))

                last_karma = get_last_project_karma(user, project, d2_local)
                if last_karma:
                    m = re.match(r'.*Karma\s*\=\s*(\d+\.\d+)', last_karma[0])
                    if m:
                      print('{:>28}: {}'.format('Last Karma', m.group(1)))
        # All users array output
        else:
            print('User       First window starts  Last window ends     Asked (seconds)  Used (seconds)')
            print('---------- -------------------- -------------------- ---------------- ----------------')
            for user, consumption_user in consumptions.items():
                asked = 0
                if 'ASKED' in consumption_user:
                    asked = consumption_user['ASKED']
                used = 0
                if 'USED' in consumption_user:
                    used = consumption_user['USED']    

                begin = local_to_sql(consumption_user['begin'])
                end = local_to_sql(consumption_user['end'])

                print('{:>10} {:>20} {:>20} {:>16} {:>16}'.format(user, begin, end, asked, used))
    else:
        print('Bad syntax for --accounting')
    
    
        

@click.command()
@click.option('-j', '--job', type=click.INT, multiple=True,
              help='show informations only for the specified job(s)')
@click.option('-f', '--full', is_flag=True, help='show full informations')
@click.option('-s', '--state', type=click.STRING, multiple=True, help='show only the state(s) of a job (optimized query)')
@click.option('-u', '--user', is_flag=True, help='show informations for this user only')
@click.option('-a', '--array', type=int, help='show informations for the specified array_job(s) and toggle array view in')
@click.option('-c', '--compact', is_flag=True, help='prints a single line for array jobs')
@click.option('-g', '--gantt', type=click.STRING, help='show job informations between two date-times')
@click.option('-e', '--events', type=click.STRING, help='show job events')
@click.option('-p', '--properties', type=click.STRING, help='show job properties')
@click.option('-A', '--accounting', type=click.STRING, help='show accounting informations between two dates')
@click.option('-S', '--sql', type=click.STRING,
              help='restricts display by applying the SQL where clause on the table jobs (ex: "project = \'p1\'")')
@click.option('-F', '--format', type=int, help='select the text output format. Available values 1 an 2')
@click.option('-J', '--json', is_flag=True, help='print result in JSON format')
@click.option('-Y', '--yaml', is_flag=True, help='print result in YAML format')
@click.option('-V', '--version', is_flag=True, help='print OAR version number')
def cli(job, full, state, user, array, compact, gantt, events, properties, accounting, sql, format, json, yaml, version):
    
    job_ids = job
    array_id = array
    states = state
    # TODO: extract gantt string
    start_time = None
    stop_time = None

    cmd_ret = CommandReturns(cli)
    # Print OAR version and exit
    if version:
        cmd_ret.print_('OAR version : ' + VERSION)
        cmd_ret.exit()
    
    username = get_username() if user else None

    if job_ids and array_id:
       cmd_ret.error('Conflicting Job IDs and Array IDs (--array and -j cannot be used together)',
                     error, 1) 
       cmd_ret.exit()

    jobs = None
    if not accounting:
        jobs = db.queries.get_jobs_for_user(username, start_time, stop_time,
                                            states, job_ids, array_id, sql,
                                            detailed=full).all()
    if gantt:
        print_gantt()
    elif accounting:
        print_accounting(accounting, username, sql)
    elif events:
        print_events()
    elif properties:
        print_properties()
    elif states:
        print_states()
    else:
        if jobs:
            if not json or not yaml:
                print_jobs(True, jobs)

        

