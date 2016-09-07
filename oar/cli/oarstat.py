# -*- coding: utf-8 -*-
from __future__ import print_function
import oar.lib.tools as tools

import requests
from oar.lib import config
import click
click.disable_unicode_literals_warning = True

DEFAULT_CONFIG = {
    'OARAPI_URL': 'http://localhost:46668/oarapi/'
    }

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
    if legacy:
        print('Job id    S User     Duration   System message\n' +
              '--------- - -------- ---------- ------------------------------------------------')

        job_items = jobs.json()['items']
        for job in job_items:
            print('{:9}'.format(str(job['id'])) + ' ' + STATE2CHAR[job['state']] + ' ' +
                  '{:8}'.format(str(job['owner'])) + ' ' +
                  '{:8}'.format(str(job['owner']))
            )
    else:
        print(jobs.text)

def http_error(r):
    pass


@click.command()
@click.option('-j', '--job', type=click.INT, multiple=True,
              help='show informations only for the specified job')
@click.option('-f', '--full', type=click., help='show full informations')

@click.option('-s', '--state', type=click., help='show only the state of a job (optimized query)')
@click.option('-u', '--user', type=click., help='show informations for this user only')
@click.option('-a', '--array', type=click., help='show informations for the specified array_job(s) and toggle array view in')
@click.option('-c', '--compact', type=click., help='prints a single line for array jobs')
@click.option('-g', '--gantt', type=click., help='show job informations between two date-times')
@click.option('-e', '---events', type=click., help='show job events')
@click.option('-p', '--properties', type=click., help='show job properties')
@click.option('-A', '--accounting', type=click., help='show accounting informations between two dates')
@click.option('-S', '--sql', type=click.,
              help='restricts display by applying the SQL where clause on the table jobs (ex: "project = \'p1\'")')
@click.option('-F', '--format', type=click., help='select the text output format. Available values 1 an 2')
@click.option('-J', '--json,', type=click. help='print result in JSON format')
@click.option('-Y', '--yaml', type=click. help='print result in YAML format')
def cli(job, full, state, user, array, compact, gantt, events, properties, accounting, sql, format, json, yaml):
    config.setdefault_config(DEFAULT_CONFIG)

    oarapi_url = config['OARAPI_URL']

    if not job:
        r = requests.get(oarapi_url + 'jobs.json')
        if r.status_code != 200:
            http_error(r)
        else:
            print_jobs(True, r)


