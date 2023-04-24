# coding: utf-8
import os

from procset import ProcSet

from oar.kao.simsim import JobSimu, ResourceSetSimu, SimSched, SWFWorkload


def _test_simsim_1():
    # Set undefined config value to default one
    DEFAULT_CONFIG = {
        "HIERARCHY_LABELS": "resource_id,network_address",
        "SCHEDULER_RESOURCE_ORDER": "resource_id ASC",
        "SCHEDULER_JOB_SECURITY_TIME": "60",
        "SCHEDULER_AVAILABLE_SUSPENDED_RESOURCE_TYPE": "default",
        "FAIRSHARING_ENABLED": "no",
        "SCHEDULER_FAIRSHARING_MAX_JOB_PER_USER": "30",
        "QUOTAS": "no",
    }

    config.setdefault_config(DEFAULT_CONFIG)

    nb_res = 32

    #
    # generate ResourceSet
    #
    hy_resource_id = [ProcSet(i) for i in range(1, nb_res + 1)]
    res_set = ResourceSetSimu(
        rid_i2o=range(nb_res + 1),
        rid_o2i=range(nb_res + 1),
        roid_itvs=ProcSet(*[(1, nb_res)]),
        hierarchy={"resource_id": hy_resource_id},
        available_upto={2147483600: ProcSet(*[(1, nb_res)])},
    )

    #
    # generate jobs
    #

    nb_jobs = 4
    jobs = {}
    submission_time_jids = []

    for i in range(1, nb_jobs + 1):
        jobs[i] = JobSimu(
            id=i,
            state="Waiting",
            queue="test",
            start_time=0,
            walltime=0,
            types={},
            res_set=ProcSet(),
            moldable_id=0,
            mld_res_rqts=[
                (i, 60, [([("resource_id", 15)], ProcSet(*[(0, nb_res - 1)]))])
            ],
            run_time=20 * i,
            deps=[],
            key_cache={},
            ts=False,
            ph=0,
            assign=False,
            find=False,
            no_quotas=False,
        )

        submission_time_jids.append((10, [i]))

        # submission_time_jids= [(10, [1,2,3,4])]
        # submission_time_jids= [(10, [1,2]), (10, [3])]

    print(submission_time_jids)
    simsched = SimSched(res_set, jobs, submission_time_jids)
    simsched.run()

    plt = simsched.platform

    print("Number completed jobs:", len(plt.completed_jids))
    print("Completed job ids:", plt.completed_jids)

    print(jobs)

    # assert True
    assert len(plt.completed_jids) == nb_jobs


def test_SWFWorkload():
    swf_wkld = SWFWorkload(os.path.dirname(__file__) + "/gofree_sample_1.swf")
    simu_jobs, sub_time_jids = swf_wkld.gene_jobsim_sub_time(100, 300, 1000)
    print(len(simu_jobs), len(sub_time_jids))
    assert len(simu_jobs) == 247
    assert len(sub_time_jids) == 6
