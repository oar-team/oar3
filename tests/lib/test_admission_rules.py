# coding: utf-8
import os
import re

import pytest

from oar.lib import db
from oar.lib.job_handling import insert_job
from oar.lib.submission import estimate_job_nb_resources  # noqa: F401
from oar.lib.submission import JobParameters, check_reservation
from oar.lib.tools import sql_to_duration  # noqa: F401


@pytest.fixture(scope="function", autouse=True)
def minimal_db_initialization(request):
    with db.session(ephemeral=True):
        # add some resources
        for i in range(15):
            db["Resource"].create(network_address="localhost")

        db["Queue"].create(name="default")
        yield


def default_job_parameters(**kwargs):
    default_job_params = {
        "job_type": "PASSIVE",
        "resource": None,
        "name": "yop",
        "project": "yop",
        "command": "sleep",
        "info_type": "",
        "queue": None,
        "properties": "",
        "checkpoint": 0,
        "signal": 12,
        "notify": "",
        "types": None,
        "directory": "/tmp",
        "dependencies": None,
        "stdout": None,
        "stderr": None,
        "hold": None,
        "initial_request": "foo",
        "user": "alice",
        "array_id": 0,
        "start_time": 0,
    }

    if kwargs:
        for key, value in kwargs.items():
            default_job_params[key] = value

    return JobParameters(**default_job_params)


def apply_admission_rules(job_parameters, rule=None):
    if rule:
        regex = rule
    else:
        regex = r"(^|^OFF_)\d+_.*"
    # Read admission_rules
    rules_dir = os.path.dirname(__file__) + "/etc/oar/admission_rules.d/"
    file_names = os.listdir(rules_dir)

    file_names.sort()
    rules = ""
    for file_name in file_names:
        if re.match(regex, file_name):
            with open(rules_dir + file_name, "r") as rule_file:
                for line in rule_file:
                    rules += line

    # Apply rules
    print("------{}-------\n".format(file_name), rules, "---------------\n")
    code = compile(rules, "<string>", "exec")

    # exec(code, job_parameters.__dict__)
    # exec(code, globals(), job_parameters.__dict__)
    exec(
        code,
        globals(),
        job_parameters.__dict__,
    )


def test_01_default_queue():
    job_parameters = default_job_parameters()
    apply_admission_rules(job_parameters)
    assert job_parameters.queue == "default"


def test_02_prevent_root_oar_toSubmit_ok():
    job_parameters = default_job_parameters(user="alice")
    apply_admission_rules(job_parameters)


def test_02_prevent_root_oar_toSubmit_bad():
    job_parameters = default_job_parameters(user="oar")
    with pytest.raises(Exception):
        apply_admission_rules(job_parameters)


def test_03_avoid_jobs_on_resources_in_drain_mode():
    job_parameters = default_job_parameters()
    apply_admission_rules(job_parameters)
    assert job_parameters.properties_applied_after_validation == "drain='NO'"


def test_04_submit_in_admin_queue():
    job_parameters = default_job_parameters(user="yop", queue="admin")
    with pytest.raises(Exception):
        apply_admission_rules(job_parameters)


def test_05_filter_bad_resources():
    # job_parameters.resource_request
    # [([{'property': '', 'resources': [{'resource': 'switch', 'value': '2'}, {'resource': 'resource_id', 'value': '10'}]}, {'property': "lic_type = 'mathlab'", 'resources': [{'resource': 'state', 'value': '2'}]}], 216000)]
    job_parameters = default_job_parameters(
        resource=["/nodes=2/cpu=10+{lic_type = 'mathlab'}/state=2, walltime = 60"]
    )
    with pytest.raises(Exception):
        apply_admission_rules(job_parameters)


def test_06_formatting_besteffort():
    job_parameters = default_job_parameters(queue="besteffort")
    apply_admission_rules(job_parameters, r"06.*")
    assert job_parameters.types == ["besteffort"]
    job_parameters = default_job_parameters(types=["besteffort"])
    apply_admission_rules(job_parameters, r"06.*")
    assert job_parameters.queue == "besteffort"
    assert job_parameters.properties == "besteffort = 'YES'"
    job_parameters = default_job_parameters(properties="yop=yop", queue="besteffort")
    apply_admission_rules(job_parameters, r"06.*")
    assert job_parameters.properties == "(yop=yop) AND besteffort = 'YES'"


def test_07_besteffort_advance_reservation():
    job_parameters = default_job_parameters(
        queue="besteffort", reservation_date=check_reservation("2018-09-19 09:59:00")
    )
    with pytest.raises(Exception):
        apply_admission_rules(job_parameters)


def test_08_formatting_deploy():
    job_parameters = default_job_parameters(
        properties="yop=yop", types=["deploy"], resource=["network_address=1"]
    )
    apply_admission_rules(job_parameters, r"08.*")
    assert job_parameters.properties == "(yop=yop) AND deploy = 'YES'"


def test_09_prevent_deploy_on_non_entire_nodes():
    job_parameters = default_job_parameters(
        types=["deploy"], resource=["/cpu=2, walltime = 60"]
    )
    with pytest.raises(Exception):
        apply_admission_rules(job_parameters)


# def test_10_desktop_computingèforamttingr():
# desktop_computing jobs in OAR3 is not entirely supported
# Force desktop_computing jobs to go on nodes with the desktop_computing property


def test_11_advance_reservation_limitation():
    insert_job(
        res=[(60, [("resource_id=2", "")])], reservation="toSchedule", user="yop"
    )
    insert_job(
        res=[(60, [("resource_id=2", "")])], reservation="toSchedule", user="yop"
    )
    job_parameters = default_job_parameters(
        user="yop", reservation_date=check_reservation("2018-09-19 09:59:00")
    )
    with pytest.raises(Exception):
        apply_admission_rules(job_parameters)


def test_13_default_walltime():
    job_parameters = default_job_parameters(resource=["/nodes=2/cpu=10"])

    apply_admission_rules(job_parameters)
    print(job_parameters.resource_request)
    assert job_parameters.resource_request[0][1] == 7200


def test_14_interactive_max_walltime():
    job_parameters = default_job_parameters(
        job_type="INTERACTIVE", resource=["/nodes=2/core=10, walltime=14:00:00"]
    )
    apply_admission_rules(job_parameters)
    print(job_parameters.resource_request)
    assert job_parameters.resource_request[0][1] == 43200


def test_15_check_types():
    job_parameters = default_job_parameters(types=["idempotent", "cosystem=bug"])
    with pytest.raises(Exception):
        apply_admission_rules(job_parameters)


def test_16_default_resource_property():
    job_parameters = default_job_parameters(
        resource=["/nodes=2/core=10+{lic='yop'}/n=1, walltime=14:00:00"]
    )
    apply_admission_rules(job_parameters)
    print(job_parameters.resource_request[0][0][1]["property"])
    assert job_parameters.resource_request[0][0][0]["property"] == "type='default'"
    assert (
        job_parameters.resource_request[0][0][1]["property"]
        == "(lic='yop') AND type='default'"
    )


def test_20_job_properties_cputype():
    job_parameters = default_job_parameters()

    apply_admission_rules(job_parameters, r"^OFF_20.*")
    print(job_parameters.properties)
    assert job_parameters.properties == "cputype = 'westmere'"

    job_parameters = default_job_parameters(properties="t='e'")
    apply_admission_rules(job_parameters, r"^OFF_20.*")
    print(job_parameters.properties)
    assert job_parameters.properties == "(t='e') AND cputype = 'westmere'"


def test_21_add_sequential_constraint():
    job_parameters = default_job_parameters(
        resource=["resource_id=2,walltime=50:00:00", "resource_id=12,walltime=1:00:00"]
    )
    apply_admission_rules(job_parameters, r"^OFF_21.*")
    print(job_parameters.properties)
    assert job_parameters.properties == "sequentiel = 'YES'"
