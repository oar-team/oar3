from oar.lib import (db, config, Job, Accounting, Resource, AssignedResource)
from oar.lib.job_handling import (insert_job)
from oar.lib.accounting import(check_accounting_update)

def insert_terminated_jobs(window_size=86400):
    j_duration = window_size * 10
    j_walltime = j_duration + 2 * window_size

    user = 'zozo'
    project = 'yopa'
    resources = db.query(Resource).all()
    for i in range(5):
        start_time = 30000 + (window_size / 4) * i
        stop_time = start_time + j_duration
        job_id = insert_job(res=[(j_walltime, [('resource_id=2', '')])],
                            properties='', command='yop',
                            user = user, project = project,
                            start_time = start_time,
                            stop_time = stop_time,
                            state='Terminated')
        db.query(Job).update({Job.assigned_moldable_job: job_id}, synchronize_session=False)

        for r in resources[i:i+2]:
            AssignedResource.create(moldable_id=job_id, resource_id=r.id)
            print(r.id, r.network_address)
        db.commit()
    check_accounting_update(window_size)
