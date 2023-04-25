# coding: utf-8
import pytest
from sqlalchemy.orm import scoped_session, sessionmaker

import oar.lib.tools  # for monkeypatching
from oar.kao.quotas import Quotas
from oar.lib.database import ephemeral_session
from oar.lib.job_handling import get_job_types
from oar.lib.models import AdmissionRule, JobResourceDescription, Queue, Resource
from oar.lib.submission import JobParameters, add_micheline_jobs, scan_script

from ..kao.test_db_all_in_one import active_quotas

fake_popen_process_stdout = ""


class FakeProcessStdout(object):
    def __init__(self):
        pass

    def decode(self):
        return fake_popen_process_stdout


class FakePopen(object):
    def __init__(self, cmd, stdout):
        pass

    def communicate(self):
        process_sdtout = FakeProcessStdout()
        return [process_sdtout]


@pytest.fixture(scope="function")
def monkeypatch_tools(request, monkeypatch):
    monkeypatch.setattr(oar.lib.tools, "Popen", FakePopen)


@pytest.fixture(scope="function", autouse=True)
def minimal_db_initialization(request, setup_config):
    _, _, engine = setup_config
    session_factory = sessionmaker(bind=engine)
    scoped = scoped_session(session_factory)

    with ephemeral_session(scoped, engine, bind=engine) as session:
        Queue.create(
            session,
            name="default",
            priority=3,
            scheduler_policy="kamelot",
            state="Active",
        )
        Queue.create(
            session,
            name="admin",
            priority=1,
            scheduler_policy="kamelot",
            state="Active",
        )
        # add some resources
        for i in range(5):
            Resource.create(session, network_address="localhost" + str(int(i / 2)))

        session.execute(AdmissionRule.__table__.delete())
        AdmissionRule.create(session, rule="name='yop'")
        yield session


@pytest.fixture(scope="function", autouse=False)
def create_hierarchy(request, setup_config, minimal_db_initialization):
    session = minimal_db_initialization
    for i in range(4):
        Resource.create(
            session,
            network_address="localhost-1",
            host="localhost-1",
            cpu=str(i // 2 + 1),
            core=i + 1,
        )

    for i in range(i + 1, 8):
        Resource.create(
            session,
            network_address="localhost-2",
            host="localhost-2",
            cpu=str(i // 2 + 1),
            core=i + 1,
        )

    for i in range(i + 1, 12):
        Resource.create(
            session,
            network_address="localhost-3",
            host="localhost-3",
            cpu=str(i // 2 + 1),
            core=i + 1,
        )

    yield


@pytest.fixture(scope="function")
def active_quotas(request, setup_config):
    config, _, _ = setup_config
    print("active_quotas")
    config["QUOTAS"] = "yes"

    def teardown():
        Quotas.enabled = False
        Quotas.calendar = None
        Quotas.default_rules = {}
        Quotas.job_types = ["*"]
        config["QUOTAS"] = "no"

    request.addfinalizer(teardown)

    yield config


def default_job_parameters(config, resource_request):
    return JobParameters(
        config,
        job_type="PASSIVE",
        resource=resource_request,
        name="yop",
        project="yop",
        command="sleep",
        info_type="",
        queue="default",
        properties="",
        checkpoint=0,
        signal=12,
        notify="",
        types=[],
        directory="/tmp",
        dependencies=None,
        stdout=None,
        stderr=None,
        hold=None,
        initial_request="foo",
        user="bob",
        array_id=0,
        start_time=0,
        reservation_field=None,
    )


def test_add_micheline_jobs_1(setup_config, minimal_db_initialization):
    config, _, _ = setup_config

    job_parameters = default_job_parameters(config, None)
    import_job_key_inline = ""
    import_job_key_file = ""
    export_job_key_file = ""
    (error, job_id_lst) = add_micheline_jobs(
        minimal_db_initialization,
        config,
        job_parameters,
        import_job_key_inline,
        import_job_key_file,
        export_job_key_file,
    )

    print("job id:", job_id_lst)
    print("error:", error)
    assert error == (0, "")
    assert len(job_id_lst) == 1


def test_add_micheline_jobs_2(setup_config, minimal_db_initialization):
    config, _, _ = setup_config
    job_parameters = default_job_parameters(config, None)
    import_job_key_inline = ""
    import_job_key_file = ""
    export_job_key_file = ""
    job_parameters.stdout = "yop"
    job_parameters.stderr = "poy"
    job_parameters.types = ["foo"]

    (error, job_id_lst) = add_micheline_jobs(
        minimal_db_initialization,
        config,
        job_parameters,
        import_job_key_inline,
        import_job_key_file,
        export_job_key_file,
    )

    print("job id:", job_id_lst)
    print("error:", error)
    assert error == (0, "")
    assert len(job_id_lst) == 1


def test_add_micheline_jobs_no_quotas_1(
    setup_config, minimal_db_initialization, active_quotas
):
    config = active_quotas
    job_parameters = default_job_parameters(config, None)
    import_job_key_inline = ""
    import_job_key_file = ""
    export_job_key_file = ""
    job_parameters.stdout = "yop"
    job_parameters.stderr = "poy"
    job_parameters.types = ["foo", "no_quotas"]

    (error, job_id_lst) = add_micheline_jobs(
        minimal_db_initialization,
        config,
        job_parameters,
        import_job_key_inline,
        import_job_key_file,
        export_job_key_file,
    )

    print("job id:", job_id_lst)
    print("error:", error)
    assert error == (0, "")
    assert len(job_id_lst) == 1
    job_types = get_job_types(minimal_db_initialization, job_id_lst[0])
    assert job_types == {"foo": True}


# @pytest.mark.usefixtures("active_quotas")
def test_add_micheline_jobs_quotas_admin(setup_config, minimal_db_initialization, active_quotas):
    config, _, _ = setup_config
    job_parameters = default_job_parameters(config, None)
    import_job_key_inline = ""
    import_job_key_file = ""
    export_job_key_file = ""
    job_parameters.stdout = "yop"
    job_parameters.stderr = "poy"
    job_parameters.types = ["foo"]
    job_parameters.queue = "admin"

    (error, job_id_lst) = add_micheline_jobs(
        minimal_db_initialization,
        config,
        job_parameters,
        import_job_key_inline,
        import_job_key_file,
        export_job_key_file,
    )

    print("job id:", job_id_lst)
    print("error:", error)
    assert error == (0, "")
    assert len(job_id_lst) == 1
    job_types = get_job_types(minimal_db_initialization, job_id_lst[0])
    print(job_types)
    assert "no_quotas" in job_types


def test_add_micheline_simple_array_job(setup_config, minimal_db_initialization):
    config, _, _ = setup_config
    prev_conf0 = config["OARSUB_DEFAULT_RESOURCES"]
    prev_conf1 = config["OARSUB_NODES_RESOURCES"]

    config[
        "OARSUB_DEFAULT_RESOURCES"
    ] = "network_address=2/resource_id=1+/resource_id=2"
    config["OARSUB_NODES_RESOURCES"] = "resource_id"

    job_parameters = default_job_parameters(config, None)
    import_job_key_inline = ""
    import_job_key_file = ""
    export_job_key_file = ""
    job_parameters.types = ["foo"]

    job_parameters.array_nb = 5
    job_parameters.array_params = [str(i) for i in range(job_parameters.array_nb)]

    # print(job_vars)

    (error, job_id_lst) = add_micheline_jobs(
        minimal_db_initialization,
        config,
        job_parameters,
        import_job_key_inline,
        import_job_key_file,
        export_job_key_file,
    )

    res = minimal_db_initialization.query(JobResourceDescription).all()
    for item in res:
        print(item.to_dict())
    res = minimal_db_initialization.query(JobResourceDescription).all()
    for item in res:
        print(item.to_dict())

    print("job id:", job_id_lst)
    print("error:", error)
    assert error == (0, "")
    assert len(job_id_lst) == 5

    config["OARSUB_DEFAULT_RESOURCES"] = prev_conf0
    config["OARSUB_NODES_RESOURCES"] = prev_conf1


def test_scan_script(monkeypatch_tools, setup_config, minimal_db_initialization):
    config, _, _ = setup_config
    global fake_popen_process_stdout
    fake_popen_process_stdout = (
        "#Funky job\n"
        "#OAR -l nodes=10,walltime=3600\n"
        "#OAR -l gpu=10\n"
        "#OAR -q yop\n"
        "#OAR -p pa=b\n"
        "#OAR --checkpoint 12\n"
        "#OAR --notify noti-exec\n"
        "#OAR -d /tmp/\n"
        "#OAR -n funky\n"
        "#OAR --project batcave\n"
        "#OAR --hold\n"
        "#OAR -a 12\n"
        "#OAR -a 32\n"
        "#OAR --signal 12\n"
        "#OAR -O sto\n"
        "#OAR -E ste\n"
        "#OAR -k\n"
        "#OAR --import-job-key-inline-priv key\n"
        "#OAR -i key_file\n"
        "#OAR -e key_file\n"
        "#OAR -s stage_filein\n"
        "#OAR --stagein-md5sum file_md5sum\n"
        "#OAR --array 10\n"
        "#OAR --array-param-file p_file\n"
        "beast_application"
    )

    result = {
        "initial_request": "command -l nodes=10,walltime=3600 -l gpu=10 -q yop -p pa=b --checkpoint 12 --notify noti-exec -d /tmp/ -n funky --project batcave --hold -a 12 -a 32 --signal 12 -O sto -E ste -k --import-job-key-inline-priv key -i key_file -e key_file -s stage_filein --stagein -md5sum file_md5sum --array 10 --array-param-file p_file",
        "resource": ["nodes=10,walltime=3600", "gpu=10"],
        "queue": "yop",
        "property": "pa=b",
        "checkpoint": 12,
        "notify": "noti-exec",
        "directory": "/tmp/",
        "name": "funky",
        "project": "batcave",
        "hold": True,
        "dependencies": [12, 32],
        "signal": 12,
        "stdout": "sto",
        "stderr": "ste",
        "use_job_key": True,
        "import_job_key_inline": "key",
        "import_job_key_file": "key_file",
        "export_job_key_file": "key_file",
        "stagein": "-md5sum file_md5sum",
        "array": 10,
        "array_param_file": "p_file",
    }

    (error, res) = scan_script("yop", "command", "zorglub")
    print(error, fake_popen_process_stdout, result)
    assert error == (0, "")
    assert res == result


def test_job_parameter_notify(setup_config, minimal_db_initialization):
    config, _, _ = setup_config
    job_parameters = default_job_parameters(config, None)
    job_parameters.notify = "mail:name@domain.com"
    error = job_parameters.check_parameters()
    assert error[0] == 0


def test_job_parameter_notify_badexec(setup_config, minimal_db_initialization):
    config, _, _ = setup_config
    job_parameters = default_job_parameters(config, None)
    job_parameters.notify = "exec:/path/to/script args rogue$*"
    error = job_parameters.check_parameters()
    assert error == (
        16,
        "insecure characters found in the notification method (the allowed regexp is: "
        "[a-zA-Z0-9_.\\/ -]+).",
    )


@pytest.mark.parametrize(
    "res_request,expected",
    [
        (
            [
                {
                    "property": "cpu = '1'",
                    "resources": [{"resource": "core", "value": "1"}],
                },
                {
                    "property": "cpu = '2'",
                    "resources": [{"resource": "core", "value": "1"}],
                },
            ],
            2,
        ),
        (
            [
                {
                    "property": "host = 'localhost-2'",
                    "resources": [{"resource": "cpu", "value": "1"}],
                }
            ],
            2,
        ),
        (
            [
                {
                    "property": "host = 'localhost-1'",
                    "resources": [{"resource": "core", "value": "4"}],
                },
                {
                    "property": "host = 'localhost-2'",
                    "resources": [{"resource": "core", "value": "2"}],
                },
            ],
            6,
        ),
        (
            [
                {
                    "property": "host = 'localhost-1'",
                    "resources": [{"resource": "cpu", "value": "1"}],
                },
                {
                    "property": "host = 'localhost-2'",
                    "resources": [{"resource": "core", "value": "2"}],
                },
            ],
            4,
        ),
    ],
)
# This doesn't test moldable jobs
def test_estimate_job_nb_resources(
    monkeypatch,
    create_hierarchy,
    res_request,
    expected,
    setup_config,
    minimal_db_initialization,
):
    config, _, _ = setup_config
    from oar.lib.submission import estimate_job_nb_resources

    request = [
        (
            res_request,
            None,
        )
    ]

    error, resource_available, estimated_nb_resources = estimate_job_nb_resources(
        minimal_db_initialization, config, request, None
    )

    assert estimated_nb_resources[0][0] == expected
