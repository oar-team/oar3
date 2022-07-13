# coding: utf-8
""" Functions to handle resource"""
import os

from sqlalchemy import distinct, func, or_, text

import oar.lib.tools as tools
from oar.lib import (
    AssignedResource,
    EventLog,
    FragJob,
    Job,
    JobType,
    MoldableJobDescription,
    Resource,
    ResourceLog,
    config,
    db,
    get_logger,
)
from oar.lib.event import add_new_event, is_an_event_exists
from oar.lib.psycopg2 import pg_bulk_insert

State_to_num = {"Alive": 1, "Absent": 2, "Suspected": 3, "Dead": 4}

logger = get_logger("oar.lib.resource_handling")


def add_resource(name, state):
    """Adds a new resource in the table resources and resource_properties
    # parameters : base, name, state
    # return value : new resource id"""
    ins = Resource.__table__.insert().values(
        {
            Resource.network_address: name,
            Resource.state: state,
            Resource.state_num: State_to_num[state],
        }
    )
    result = db.session.execute(ins)
    r_id = result.inserted_primary_key[0]

    date = tools.get_date()

    ins = ResourceLog.__table__.insert().values(
        {
            ResourceLog.resource_id: r_id,
            ResourceLog.attribute: "state",
            ResourceLog.value: state,
            ResourceLog.date_start: date,
        }
    )
    db.session.execute(ins)
    db.session.commit()

    return r_id


def get_resource(resource_id):
    return db.query(Resource).filter(Resource.id == resource_id).one()


def get_resources_from_ids(resource_ids):
    return (
        db.query(Resource)
        .filter(Resource.id.in_(resource_ids))
        .order_by(Resource.id)
        .all()
    )


def set_resource_state(resource_id, state, finaud_decision):
    """set the state field of a resource"""
    db.query(Resource).filter(Resource.id == resource_id).update(
        {
            Resource.state: state,
            Resource.finaud_decision: finaud_decision,
            Resource.state_num: State_to_num[state],
        },
        synchronize_session=False,
    )

    date = tools.get_date()

    db.query(ResourceLog).filter(ResourceLog.date_stop == 0).filter(
        ResourceLog.attribute == "state"
    ).filter(ResourceLog.resource_id == resource_id).update(
        {ResourceLog.date_stop: date}, synchronize_session=False
    )

    ins = ResourceLog.__table__.insert().values(
        {
            ResourceLog.resource_id: resource_id,
            ResourceLog.attribute: "state",
            ResourceLog.value: state,
            ResourceLog.date_start: date,
            ResourceLog.finaud_decision: finaud_decision,
        }
    )
    db.session.execute(ins)


def set_resource_nextState(resource_id, next_state):
    """Set the nextState field of a resource identified by its resource_id"""
    db.query(Resource).filter(Resource.id == resource_id).update(
        {Resource.next_state: next_state, Resource.next_finaud_decision: "NO"},
        synchronize_session=False,
    )
    db.commit()


def set_resources_nextState(resource_ids, next_state):
    """Set the nextState field of a resources identified by their resource_id and return the number
    of matched rows (becarefull is not necessarily egal to the number of updated rows"""
    nb_matched_row = (
        db.query(Resource)
        .filter(Resource.id.in_(tuple(resource_ids)))
        .update(
            {Resource.next_state: next_state, Resource.next_finaud_decision: "NO"},
            synchronize_session=False,
        )
    )
    db.commit()
    return nb_matched_row


def set_resources_property(resources, hostnames, prop_name, prop_value):
    """Change a property value in the resource table
    parameters: resources or hostname to change, property name, value
    return : number of changed rows"""

    query = db.query(Resource.id)
    if hostnames:
        query = query.filter(Resource.network_address.in_(tuple(hostnames)))
    else:
        query = query.filter(Resource.id.in_(tuple(resources)))
    query = query.filter(
        or_(
            getattr(Resource, prop_name) != prop_value,
            # Because sqlalchemy relies on operator overloading which is not possible with is.
            getattr(Resource, prop_name) == None,  # noqa: E711
        )
    )
    # query = query.filter(text("( {} != '{}' OR {} IS NULL )".format(prop_name, prop_value, prop_name)))
    res = query.all()

    nb_resources = len(res)
    nb_affected_rows = len(res)

    rids = tuple(r[0] for r in res)
    if nb_resources > 0:
        # TODO TOVERIFY nb_affected_row -> NO, nb of MATCHED ROW
        nb_affected_row = (  # noqa: F841
            db.query(Resource)
            .filter(Resource.id.in_(rids))
            .update(
                {getattr(Resource, prop_name): prop_value}, synchronize_session=False
            )
        )
        if nb_affected_rows > 0:
            # Update LOG table
            date = tools.get_date()
            db.query(ResourceLog).filter(ResourceLog.date_stop == 0).filter(
                ResourceLog.attribute == prop_name
            ).filter(ResourceLog.resource_id.in_(rids)).update(
                {ResourceLog.date_stop: date}, synchronize_session=False
            )
            db.commit()
            # Insert Logs
            resource_logs = []
            for rid in rids:
                resource_logs.append(
                    {
                        ResourceLog.resource_id.name: rid,
                        ResourceLog.attribute.name: prop_name,
                        ResourceLog.value.name: prop_value,
                        ResourceLog.date_start.name: date,
                    }
                )
            db.session.execute(ResourceLog.__table__.insert(), resource_logs)
            db.commit()
        else:
            logger.warning("Failed to update resources")

    return nb_affected_rows


def remove_resource(resource_id, user=None):
    """Remove resource"""

    if not user:
        if "OARDO_USER" in os.environ:
            user = os.environ["OARDO_USER"]
        else:
            user = os.environ["USER"]

    if (user != "oar") and (user != "root"):
        return (4, "Only the oar or root users can delete resources")

    # get resources
    res = db.query(Resource.state).filter(Resource.id == resource_id).one()
    state = res[0]
    if state == "Dead":
        results = (
            db.query(Job.id, Job.assigned_moldable_job)
            .filter(AssignedResource.resource_id == resource_id)
            .filter(AssignedResource.moldable_id == Job.assigned_moldable_job)
            .all()
        )
        for job_mod_id in results:
            job_id, moldable_id = job_mod_id
            db.query(EventLog).where(EventLog.job_id == job_id).delete(
                synchronize_session=False
            )
            db.query(FragJob).where(FragJob.job_id == job_id).delete(
                synchronize_session=False
            )
            db.query(Job).where(Job.id == job_id).delete(synchronize_session=False)
            db.query(AssignedResource).filter(
                AssignedResource.moldable_id == moldable_id
            ).delete(synchronize_session=False)

        db.query(AssignedResource).filter(
            AssignedResource.resource_id == resource_id
        ).delete(synchronize_session=False)
        db.query(ResourceLog).filter(ResourceLog.resource_id == resource_id).delete(
            synchronize_session=False
        )
        db.query(Resource).filter(Resource.id == resource_id).delete(
            synchronize_session=False
        )

        db.commit()
        # db.session.expire_all()  # TODO / TOFIX / TOCOMMENT???
        return (0, None)
    else:
        return (3, "Resource must be in DEAD state.")


def get_current_resources_with_suspended_job():
    """Return the list of resources where there are Suspended jobs"""
    res = (
        db.query(AssignedResource.resource_id)
        .filter(AssignedResource.index == "CURRENT")
        .filter(Job.state == "Suspended")
        .filter(Job.assigned_moldable_job == AssignedResource.moldable_id)
        .all()
    )
    return tuple(r[0] for r in res)


def get_current_assigned_job_resources(moldable_id):
    """Returns the current resources ref for a job"""
    res = (
        db.query(Resource)
        .filter(AssignedResource.index == "CURRENT")
        .filter(AssignedResource.moldable_id == moldable_id)
        .filter(Resource.id == AssignedResource.resource_id)
        .all()
    )
    return res


def get_resources_change_state():
    """Get resource ids that will change their state"""
    res = (
        db.query(Resource.id, Resource.next_state)
        .filter(Resource.next_state != "UnChanged")
        .all()
    )
    return {r.id: r.next_state for r in res}


def get_expired_resources():
    """Get the list of resources whose expiry_date is in the past and which are not dead yet.
    0000-00-00 00:00:00 is always considered as in the future. Used for desktop computing schema."""
    # TODO: UNUSED (Desktop computing)
    date = tools.get_date()

    res = (
        db.query(Resource.id)
        .filter(Resource.state == "Alive")
        .filter(Resource.expiry_date > 0)
        .filter(Resource.desktop_computing == "YES")
        .filter(Resource.expiry_date < date)
        .all()
    )
    return [r[0] for r in res]


def get_absent_suspected_resources_for_a_timeout(timeout):
    date = tools.get_date()
    res = (
        db.query(ResourceLog.resource_id)
        .filter(ResourceLog.attribute == "state")
        .filter(ResourceLog.date_stop == 0)
        .filter((ResourceLog.date_start + timeout) < date)
        .all()
    )
    return [r[0] for r in res]


def update_resource_nextFinaudDecision(resource_id, finaud_decision):
    """Update nextFinaudDecision field"""
    db.query(Resource).filter(Resource.id == resource_id).update(
        {Resource.next_finaud_decision: finaud_decision}, synchronize_session=False
    )
    db.commit()


def update_scheduler_last_job_date(date, moldable_id):
    db.query(Resource).filter(AssignedResource.moldable_id == moldable_id).filter(
        AssignedResource.resource_id == Resource.resource_id
    ).update({Resource.last_job_date: date}, synchronize_session=False)


def update_current_scheduler_priority(job, value, state):
    """Update the scheduler_priority field of the table resources"""
    # TODO: need to adress this s
    from oar.lib.job_handling import get_job_types

    # TO FINISH
    # TODO: MOVE TO resource.py ???

    logger.info(
        "update_current_scheduler_priority "
        + " job.id: "
        + str(job.id)
        + ", state: "
        + state
        + ", value: "
        + str(value)
    )

    if "SCHEDULER_PRIORITY_HIERARCHY_ORDER" in config:
        sched_priority = config["SCHEDULER_PRIORITY_HIERARCHY_ORDER"]

        try:
            job_types = job.types
        except AttributeError:
            job_types = get_job_types(job.id)

        if (
            ("besteffort" in job_types.keys()) or ("timesharing" in job_types.keys())
        ) and (
            (
                (state == "START")
                and is_an_event_exists(job.id, "SCHEDULER_PRIORITY_UPDATED_START") <= 0
            )
            or (
                (state == "STOP")
                and is_an_event_exists(job.id, "SCHEDULER_PRIORITY_UPDATED_START") > 0
            )
        ):

            coeff = 1
            if ("besteffort" in job_types.keys()) and (
                "timesharing" not in job_types.keys()
            ):
                coeff = 10

            index = 0
            for f in sched_priority.split("/"):
                if f == "":
                    continue
                elif f == "resource_id":
                    f = "id"

                index += 1

                res = (
                    db.query(distinct(getattr(Resource, f)))
                    .filter(AssignedResource.index == "CURRENT")
                    .filter(AssignedResource.moldable_id == job.assigned_moldable_job)
                    .filter(AssignedResource.resource_id == Resource.id)
                    .all()
                )

                resources = tuple(r[0] for r in res)

                if resources == ():
                    return

                incr_priority = int(value) * index * coeff
                db.query(Resource).filter((getattr(Resource, f)).in_(resources)).update(
                    {Resource.scheduler_priority: incr_priority},
                    synchronize_session=False,
                )

            add_new_event(
                "SCHEDULER_PRIORITY_UPDATED_" + state,
                job.id,
                "Scheduler priority for job "
                + str(job.id)
                + "updated ("
                + sched_priority
                + ")",
            )


def get_resources_jobs(r_id):
    # returns the list of jobs associated to all resources
    # Provide by basequerie
    raise NotImplementedError("TODO")


def get_resource_job_to_frag(r_id):
    # same as get_resource_job but excepts the cosystem jobs
    subq = (
        db.query(JobType.job_id)
        .filter(or_(JobType.type == "cosystem", JobType.type == "noop"))
        .filter(JobType.types_index == "CURRENT")
        .subquery()
    )

    res = (
        db.query(Job.id)
        .filter(AssignedResource.index == "CURRENT")
        .filter(MoldableJobDescription.index == "CURRENT")
        .filter(AssignedResource.resource_id == r_id)
        .filter(AssignedResource.moldable_id == MoldableJobDescription.id)
        .filter(MoldableJobDescription.job_id == Job.id)
        .filter(Job.state != "Terminated")
        .filter(Job.state != "Error")
        .filter(~Job.id.in_(subq))
        .order_by(Job.id)
        .all()
    )

    return [r[0] for r in res]


def get_resources_with_given_sql(sql):
    """Returns the resource ids with specified properties parameters : where SQL constraints."""
    results = db.query(Resource.id).filter(text(sql)).order_by(Resource.id).all()
    return [r[0] for r in results]


def log_resource_maintenance_event(resource_id, maintenance, date):
    """log maintenance event, two cases according to maintenance value (on|off)
    maintenance on:
    add an event in the table resource_logs indicating that this
    resource is in maintenance (state = Absent, available_upto = 0)
    maintenance off:
    update the event in the table resource_logs indicating that this
    resource is in maintenance (state = Absent, available_upto = 0)
    set the date_stop"""
    if maintenance == "on":
        ins = ResourceLog.__table__.insert().values(
            {
                ResourceLog.resource_id: resource_id,
                ResourceLog.attribute: "maintenance",
                ResourceLog.value: maintenance,
                ResourceLog.date_start: date,
            }
        )
        db.session.execute(ins)
    else:
        db.query(ResourceLog).filter(ResourceLog.date_stop == 0).filter(
            ResourceLog.attribute == "maintenance"
        ).filter(ResourceLog.resource_id == resource_id).update(
            {ResourceLog.date_stop: date}, synchronize_session=False
        )
        db.commit()


def get_resource_max_value_of_property(property_name):
    # returns the max numerical value for a property amongst resources
    propery_field = None
    try:
        propery_field = getattr(Resource, property_name)
    except AttributeError:
        # Property doesn't exist
        return None
    return db.query(func.max(propery_field)).scalar()


def get_resources_state(resource_ids):
    date = tools.get_date()
    result = (
        db.query(Resource.id, Resource.state, Resource.available_upto)
        .filter(Resource.id.in_(tuple(resource_ids)))
        .order_by(Resource.id)
        .all()
    )
    res = [
        {
            r.id: "Standby"
            if (r.state == "Absent") and (r.available_upto >= date)
            else r.state
        }
        for r in result
    ]
    return res


def get_count_busy_resources():
    active_moldable_job_ids = db.query(Job.assigned_moldable_job).filter(
        Job.state.in_(("toLaunch", "Running", "Resuming"))
    )
    count_busy_resources = (
        db.query(func.count(distinct(AssignedResource.resource_id)))
        .filter(AssignedResource.moldable_id.in_(active_moldable_job_ids))
        .scalar()
    )
    return count_busy_resources


def resources_creation(node_name, nb_nodes, nb_core=1, vfactor=1):
    for i in range(nb_nodes * nb_core * vfactor):
        Resource.create(
            network_address=f"{node_name}{int(i/(nb_core * vfactor)+1)}",
            cpuset=i % nb_core,
            state="Alive",
        )
    db.commit()


def resources_creation_bulk(node_name, nb_nodes, nb_cpusets=1):
    # TODO need all provide all fields
    resources = [
        (f"{node_name}{int(i/nb_cpusets)+1}", i % nb_cpusets, "Alive")
        for i in range(nb_nodes * nb_cpusets)
    ]

    with db.engine.connect() as to_conn:
        cursor = to_conn.connection.cursor()
        pg_bulk_insert(
            cursor,
            db["resources"],
            resources,
            ("network_address", "cpuset", "state"),
            binary=True,
        )
