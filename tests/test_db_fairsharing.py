# coding: utf-8
from oar.lib import (db, Resource, GanttJobsPrediction, Accounting)

from oar.kao.job import insert_job

from oar.kao.platform import Platform

from oar.kao.kamelot import schedule_cycle

import time
from random import sample

from oar.lib import config
from __init__ import DEFAULT_CONFIG

#DEFAULT_CONFIG['DB_BASE_FILE'] = "/tmp/oar1.sqlite"
DEFAULT_CONFIG['FAIRSHARING_ENABLED'] = 'yes'
#db.create_all()
 
def setup_db1(fs=False):
    print "setup db for fair sharing"
    config.clear()
    DEFAULT_CONFIG['DB_BASE_FILE'] = "/tmp/oar1.sqlite"
    if fs:
        DEFAULT_CONFIG['FAIRSHARING_ENABLED'] = 'yes'

    config.update(DEFAULT_CONFIG.copy())
    config["LOG_FILE"] = '/tmp/oar.log'

    print ("db.create_all()")
    
    db.create_all()
    


def db_flush():
    db.session.flush()
    db.session.expunge_all()
    db.session.commit()

def del_accounting():
    db.engine.execute(Accounting.__table__.delete())
    db.commit()


def set_accounting(accountings, consumption_type):
    ins_accountings = []
    for a in accountings:
        w_start, w_stop, proj, user, queue, consumption = a
        ins_accountings.append({'window_start': w_start,
                                'window_stop': w_stop,
                                'accounting_project': proj,
                                'accounting_user': user,
                                'queue_name': queue,
                                'consumption_type': consumption_type,
                                'consumption': consumption})

    db.engine.execute(Accounting.__table__.insert(), ins_accountings)
    db.commit()


def generate_accountings(nb_users=5, t_window=24 * 36000, queue="default",
                         project="default"):
    del_accounting()

    nb_accounts = 5

    now = time.time()
    offset = now - t_window * nb_accounts

    accountings_a = []
    accountings_u = []
    for u in range(nb_users):
        user = "zozo" + str(u)

        for i in range(nb_accounts):
            w_start = t_window * (i + 1) + offset
            w_stop = t_window * (i + 1) + t_window / 10 + offset
            consumption = 1000 * (1 + u)

            accountings_a.append(
                (w_start, w_stop, project, user, queue, consumption + 1000))
            accountings_u.append(
                (w_start, w_stop, project, user, queue, consumption))

    set_accounting(accountings_a, "ASKED")
    set_accounting(accountings_u, "USED")


##########


def test_db_fairsharing():

    print "Test_db_fairsharing"

    print "DB_BASE_FILE: ", config["DB_BASE_FILE"]
    #setup_db1(False)
    generate_accountings()

    # add some resources
    for i in range(5):
        db.add(Resource(network_address="localhost"))
    # db_flush()

    nb_users = 5

    users = [str(x) for x in sample(range(nb_users), nb_users)]

    print "users:", users
    jid_2_u = {}
    for i, user in enumerate(users):
        insert_job(
            job_user="zozo" + user,
            res=[(60, [('resource_id=4', "")])],
            properties=""
        )
        jid_2_u[i + 1] = int(user)

    db_flush()

    plt = Platform()
    r = plt.resource_set()

    print "r.roid_itvs: ", r.roid_itvs

    schedule_cycle(plt, plt.get_time())

    req = db.query(GanttJobsPrediction).order_by(
        GanttJobsPrediction.start_time).all()
    flag = True

    print jid_2_u

    for i, r in enumerate(req):
        print "req:", r.moldable_id, jid_2_u[r.moldable_id], i
        if jid_2_u[r.moldable_id] != i:
            flag = False
            break

    assert flag

