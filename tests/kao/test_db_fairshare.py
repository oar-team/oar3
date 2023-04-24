# coding: utf-8
import re
import time
from random import sample

import pytest

from oar.kao.kamelot import schedule_cycle
from oar.kao.platform import Platform
from oar.lib.job_handling import insert_job
from oar.lib.models import Job


@pytest.fixture(scope="function", autouse=True)
def minimal_db_initialization(request):
    with db.session(ephemeral=True):
        yield


@pytest.fixture(scope="module", autouse=True)
def oar_conf(request):
    config["JOB_PRIORITY"] = "FAIRSHARE"

    @request.addfinalizer
    def remove_fairsharing():
        config["JOB_PRIORITY"] = "FIFO"


def del_accounting():
    db.session.execute(db["accounting"].delete())
    db.commit()


def set_accounting(accountings, consumption_type):
    ins_accountings = []
    for a in accountings:
        w_start, w_stop, proj, user, queue, consumption = a
        ins_accountings.append(
            {
                "window_start": w_start,
                "window_stop": w_stop,
                "accounting_project": proj,
                "accounting_user": user,
                "queue_name": queue,
                "consumption_type": consumption_type,
                "consumption": consumption,
            }
        )

    db.session.execute(db["accounting"].insert(), ins_accountings)
    db.commit()


def generate_accountings(
    nb_users=5, t_window=24 * 36000, queue="default", project="default"
):
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
                (w_start, w_stop, project, user, queue, consumption + 1000)
            )
            accountings_u.append((w_start, w_stop, project, user, queue, consumption))

    set_accounting(accountings_a, "ASKED")
    set_accounting(accountings_u, "USED")


def test_db_fairsharing():
    print("Test_db_fairsharing")

    print("DB_BASE_FILE: ", config["DB_BASE_FILE"])

    generate_accountings()

    # add some resources
    for i in range(5):
        db["Resource"].create(network_address="localhost")

    nb_users = 5

    users = [str(x) for x in sample(range(nb_users), nb_users)]

    print("users:", users)
    jid_2_u = {}
    for i, user in enumerate(users):
        insert_job(
            job_user="zozo" + user, res=[(60, [("resource_id=4", "")])], properties=""
        )
        jid_2_u[i + 1] = int(user)

    plt = Platform()
    r = plt.resource_set()

    print("r.roid_itvs: ", r.roid_itvs)

    schedule_cycle(plt, plt.get_time())

    req = (
        db["GanttJobsPrediction"]
        .query.order_by(db["GanttJobsPrediction"].start_time)
        .all()
    )
    flag = True

    print(jid_2_u)

    min_jid = min(r.moldable_id for r in req)
    min_jid -= 1

    for i, r in enumerate(req):
        print("req:", r.moldable_id, jid_2_u[r.moldable_id - min_jid], i)
        if jid_2_u[r.moldable_id - min_jid] != i:
            flag = False
            break

    assert flag

    # Check if messages are updated and valid
    r = re.compile("R=\d+,W=\d+,J=(P|I),Q=\w+ \(Karma=\d+.\d+\)$")
    req = db.query(Job).all()

    for j in req:
        assert r.match(j.message) is not None
