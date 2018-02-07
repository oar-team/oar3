# -*- coding: utf-8 -*-
import os
import datetime

from oar.lib import (db, config)

from oar.lib.tools import get_username
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

# TODO: document as example use of rest api
# DEFAULT_CONFIG = {
#    'OARAPI_URL': 'http://localhost:46668/oarapi/'
#    }
# class OarApi(object):
#     def __init__(self):
#         self.oarapi_url = config['OARAPI_URL']

#     def http_error(self, r):
#         pass

#     def get(self, params):
#         r = requests.get(self.oarapi_url + params)
#         if r.status_code != 200:
#             self.http_error(r)
#         else:
#             return(r)

#oarapi = OarApi()

#    if not job:
#        answer = oarapi.get('jobs/details.json')
#        print_jobs(True, answer)

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
                  '{:48}'.format(job.message)
            )
    else:
        # TODO
        print(jobs.text)

def print_oar_version():
    # TODO
    pass


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

    username = get_username() if user else None

    #db.query() # TODO:it is work around
    #BUG when detailed=False
    # sqlalchemy.exc.NoInspectionAvailable: No inspection system is available for object of type
    # <class 'oar.lib.database._BoundDeclarativeMeta'>
    jobs = db.queries.get_jobs_for_user(username, start_time, stop_time,
                                        states, job_ids, array_id, detailed=full).all()

    if jobs:
        if not json or not yaml:
            print_jobs(True, jobs)

        

