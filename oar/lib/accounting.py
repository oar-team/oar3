# -*- coding: utf-8 -*-
from sqlalchemy import (func, or_)
from oar.lib import (db, Accounting, Job, MoldableJobDescription, AssignedResource,
                     Resource)

# # ACCOUNTING

# sub get_accounting_summary($$$$$);
# sub get_accounting_summary_byproject($$$$$$);
# sub get_last_project_karma($$$$);
# sub get_sum_accounting_for_param($$$$$);
# sub get_sum_accounting_window($$$$); -> see Karma.py


def update_accounting(start_time, stop_time, window_size, user, project, queue_name,
                      c_type, nb_resources):
    """Insert accounting data in table accounting
    # params : start date in second, stop date in second, window size, user, queue, type(ASKED or USED)
    """
    nb_windows = int(start_time / window_size)
    window_start = nb_windows * window_size
    window_stop = window_start + window_size - 1

    consumption = 0
    # Accounting algo
    while (stop_time > start_time):
        if (stop_time <= window_stop):
            consumption = stop_time - start_time
        else:
            consumption = window_stop - start_time + 1

        consumption = consumption * nb_resources
        add_accounting_row(window_start, window_stop, user, project, queue_name, c_type, consumption)
        window_start = window_stop + 1
        start_time = window_start
        window_stop += window_size


def add_accounting_row(window_start, window_stop, user, project, queue_name, c_type, consumption):
    # Insert or update one row according to consumption

    # Test if the window exists
    # TODO: Need to be cached (in python process or externaly through Redis by example
    result = db.query(Accounting.consumption)\
               .filter(Accounting.user == user)\
               .filter(Accounting.project == project)\
               .filter(Accounting.consumption_type == c_type)\
               .filter(Accounting.queue_name == queue_name)\
               .filter(Accounting.window_start == window_start)\
               .filter(Accounting.window_stop == window_stop)\
               .one_or_none()

    if result:
        consumption = consumption + result[0]
        print('[ACCOUNTING] Update the existing window ' + str(window_start) + ' --> ' +
              str(window_stop) + ', project ' + project + ', user ' + user + ', queue' +
              queue_name + ', type ' + 'type with conso = ' + str(consumption) + ' s')

        db.query(Accounting).filter(Accounting.user == user)\
                            .filter(Accounting.project == project)\
                            .filter(Accounting.consumption_type == c_type)\
                            .filter(Accounting.queue_name == queue_name)\
                            .filter(Accounting.window_start == window_start)\
                            .filter(Accounting.window_stop == window_stop)\
                            .update({Accounting.consumption: consumption}, synchronize_session=False)
    else:

        # Create the window
        print('[ACCOUNTING] Create new window ' + str(window_start) + ' --> ' +
              str(window_stop) + ', project ' + project + ', user ' + user + ', queue' +
              queue_name + ', type ' + 'type with conso = ' + str(consumption) + ' s')

        Accounting.create(user=user, consumption_type=c_type, queue_name=queue_name,
                          window_start=window_start, window_stop=window_stop,
                          consumption=consumption, project=project)

def check_accounting_update(window_size):
    """Check jobs that are not treated in accounting table
    params : base, window size"""

    result = db.query(Job.start_time, Job.stop_time, MoldableJobDescription.walltime, Job.id,
                      Job.user, Job.queue_name, func.count(AssignedResource.resource_id),
                      Job.project)\
               .filter(Job.accounted == 'NO')\
               .filter(or_(Job.state == 'Terminated', Job.state == 'Error'))\
               .filter(Job.stop_time >= Job.start_time)\
               .filter(Job.start_time > 1)\
               .filter(Job.assigned_moldable_id == MoldableJobDescription.id)\
               .filter(AssignedResource.moldable_id == MoldableJobDescription.id)\
               .filter(AssignedResource.resource_id == Resource.id)\
               .filter(Resource.type == 'default')\
               .group_by(Job.start_time, Job.stop_time, MoldableJobDescription.walltime,
                         Job.id, Job.project, Job.user, Job.queue_name)\
               .all()

    for job_accounting_info in result:
        (start_time, stop_time, walltime, job_id, user,
         queue_name, nb_resources, project) = job_accounting_info
        max_stop_time = stop_time + walltime
        print('[ACCOUNTING] Treate job ' + job_id)
        update_accounting(start_time, stop_time, window_size, user,
                          project, queue_name, 'USED', nb_resources)
        update_accounting(start_time, max_stop_time, window_size, user,
                          project, queue_name, 'ASKED', nb_resources)

        db.query(Job).update({Job.accounted: 'YES'}, synchronize_session=False)

    db.commit()


def delete_all_from_accounting():
    """Empty the table accounting and update the jobs table."""
    db.query(Accounting).delete(synchronize_session=False)
    db.query(Job).update({Job.accounted: 'NO'}, synchronize_session=False)
    db.commit()

def delete_accounting_windows_before(duration):
    """Remove windows from accounting."""
    db.query(Accounting).filter(Accounting.window_stop <= duration).delete(synchronize_session=False)
    db.commit()
