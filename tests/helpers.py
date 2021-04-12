import oar.lib.tools as tools
from oar.lib import (
    Accounting,
    AssignedResource,
    Job,
    MoldableJobDescription,
    Resource,
    config,
    db,
)
from oar.lib.accounting import check_accounting_update
from oar.lib.job_handling import insert_job


def insert_terminated_jobs(update_accounting=True, nb_jobs=5, window_size=86400):
    j_duration = window_size * 10
    j_walltime = j_duration + 2 * window_size
    job_ids = []

    user = "zozo"
    project = "yopa"
    resources = db.query(Resource).all()
    for i in range(nb_jobs):
        start_time = 30000 + (window_size / 4) * i
        stop_time = start_time + j_duration
        job_id = insert_job(
            res=[(j_walltime, [("resource_id=2", "")])],
            properties="",
            command="yop",
            user=user,
            project=project,
            start_time=start_time,
            stop_time=stop_time,
            state="Terminated",
        )
        job_ids.append(job_id)
        mld_id = (
            db.query(MoldableJobDescription.id)
            .filter(MoldableJobDescription.job_id == job_id)
            .one()[0]
        )
        db.query(Job).filter(Job.id == job_id).update(
            {Job.assigned_moldable_job: mld_id}, synchronize_session=False
        )

        for r in resources[i : i + 2]:
            AssignedResource.create(moldable_id=mld_id, resource_id=r.id)
            print(r.id, r.network_address)
        db.commit()
    if update_accounting:
        check_accounting_update(window_size)
    return job_ids


def insert_running_jobs(nb_jobs=5, user="zozo", project="yopa", types=[]):
    j_walltime = 60
    job_ids = []
    resources = db.query(Resource).all()
    for i in range(nb_jobs):
        start_time = tools.get_date()
        job_id = insert_job(
            res=[(j_walltime, [("resource_id=2", "")])],
            properties="",
            command="yop",
            user=user,
            project=project,
            start_time=start_time,
            types=types,
            state="Running",
        )
        job_ids.append(job_id)

        mld_id = (
            db.query(MoldableJobDescription.id)
            .filter(MoldableJobDescription.job_id == job_id)
            .one()[0]
        )
        db.query(Job).filter(Job.id == job_id).update(
            {Job.assigned_moldable_job: mld_id}, synchronize_session=False
        )

        for r in resources[i : i + 2]:
            AssignedResource.create(moldable_id=mld_id, resource_id=r.id)
            print(job_id, mld_id, r.id, r.network_address)
        db.commit()
    return job_ids
