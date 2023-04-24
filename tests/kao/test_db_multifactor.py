from random import sample
from tempfile import mkstemp

import pytest

from oar.kao.kamelot import schedule_cycle
from oar.kao.platform import Platform
from oar.lib.job_handling import insert_job

from .test_db_fairshare import generate_accountings


@pytest.fixture(scope="function", autouse=True)
def minimal_db_initialization(request):
    with db.session(ephemeral=True):
        for i in range(5):
            db["Resource"].create(network_address="localhost")
        yield


@pytest.fixture(scope="module", autouse=True)
def oar_conf(request, setup_config):
    config, _, _ = setup_config
    config["JOB_PRIORITY"] = "MULTIFACTOR"

    @request.addfinalizer
    def remove_fairsharing():
        config["JOB_PRIORITY"] = "FIFO"


# def test_db_multifactor_void():
#    plt = Platform()


def test_db_multifactor_fairshare(minimal_db_initialization, oar_conf):
    _, priority_file_name = mkstemp()
    config = oar_conf

    config["PRIORITY_CONF_FILE"] = priority_file_name

    with open(config["PRIORITY_CONF_FILE"], "w", encoding="utf-8") as priority_fd:
        priority_fd.write('{"karma_weight": 1.0}')

    generate_accountings()

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
