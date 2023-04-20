# coding: utf-8
""" Functions to handle resource"""
import os

from sqlalchemy import distinct, func, or_, text

import oar.lib.tools as tools
from oar.lib.event import add_new_event, is_an_event_exists
from oar.lib.globals import init_oar
from oar.lib.logging import get_logger
from oar.lib.models import (
    AssignedResource,
    EventLog,
    FragJob,
    Job,
    JobType,
    MoldableJobDescription,
    Resource,
    ResourceLog,
)
from oar.lib.psycopg2 import pg_bulk_insert

State_to_num = {"Alive": 1, "Absent": 2, "Suspected": 3, "Dead": 4}

config, db, logger = init_oar()

logger = get_logger("oar.lib.resource_handling")


def add_resource(session, name, state):
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
    result = session.session.execute(ins)
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
    session.session.execute(ins)
    session.session.commit()

    return r_id


def get_resource(session, resource_id):
    return session.query(Resource).filter(Resource.id == resource_id).one()


def get_resources_from_ids(session, resource_ids):
    return (
        session.query(Resource)
        .filter(Resource.id.in_(resource_ids))
        .order_by(Resource.id)
        .all()
    )


def set_resource_state(session, resource_id, state, finaud_decision):
    """set the state field of a resource"""
    session.query(Resource).filter(Resource.id == resource_id).update(
        {
            Resource.state: state,
            Resource.finaud_decision: finaud_decision,
            Resource.state_num: State_to_num[state],
        },
        synchronize_session=False,
    )

    date = tools.get_date(session)

    session.query(ResourceLog).filter(ResourceLog.date_stop == 0).filter(
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
    session.execute(ins)


def set_resource_nextState(session, resource_id, next_state):
    """Set the nextState field of a resource identified by its resource_id"""
    session.query(Resource).filter(Resource.id == resource_id).update(
        {Resource.next_state: next_state, Resource.next_finaud_decision: "NO"},
        synchronize_session=False,
    )
    session.commit()


def set_resources_nextState(session, resource_ids, next_state):
    """Set the nextState field of a resources identified by their resource_id and return the number
    of matched rows (becarefull is not necessarily egal to the number of updated rows"""
    nb_matched_row = (
        session.query(Resource)
        .filter(Resource.id.in_(tuple(resource_ids)))
        .update(
            {Resource.next_state: next_state, Resource.next_finaud_decision: "NO"},
            synchronize_session=False,
        )
    )
    session.commit()
    return nb_matched_row


def set_resources_property(session, resources, hostnames, prop_name, prop_value):
    """Change a property value in the resource table
    parameters: resources or hostname to change, property name, value
    return : number of changed rows"""

    query = session.query(Resource.id)
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
            session.query(Resource)
            .filter(Resource.id.in_(rids))
            .update(
                {getattr(Resource, prop_name): prop_value}, synchronize_session=False
            )
        )
        if nb_affected_rows > 0:
            # Update LOG table
            date = tools.get_date()
            session.query(ResourceLog).filter(ResourceLog.date_stop == 0).filter(
                ResourceLog.attribute == prop_name
            ).filter(ResourceLog.resource_id.in_(rids)).update(
                {ResourceLog.date_stop: date}, synchronize_session=False
            )
            session.commit()
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
            session.session.execute(ResourceLog.__table__.insert(), resource_logs)
            session.commit()
        else:
            logger.warning("Failed to update resources")

    return nb_affected_rows


def remove_resource(session, resource_id, user=None):
    """Remove resource"""

    if not user:
        if "OARDO_USER" in os.environ:
            user = os.environ["OARDO_USER"]
        else:
            user = os.environ["USER"]

    if (user != "oar") and (user != "root"):
        return (4, "Only the oar or root users can delete resources")

    # get resources
    res = session.query(Resource.state).filter(Resource.id == resource_id).one()
    state = res[0]
    if state == "Dead":
        results = (
            session.query(Job.id, Job.assigned_moldable_job)
            .filter(AssignedResource.resource_id == resource_id)
            .filter(AssignedResource.moldable_id == Job.assigned_moldable_job)
            .all()
        )
        for job_mod_id in results:
            job_id, moldable_id = job_mod_id
            session.query(EventLog).where(EventLog.job_id == job_id).delete(
                synchronize_session=False
            )
            session.query(FragJob).where(FragJob.job_id == job_id).delete(
                synchronize_session=False
            )
            session.query(Job).where(Job.id == job_id).delete(synchronize_session=False)
            session.query(AssignedResource).filter(
                AssignedResource.moldable_id == moldable_id
            ).delete(synchronize_session=False)

        session.query(AssignedResource).filter(
            AssignedResource.resource_id == resource_id
        ).delete(synchronize_session=False)
        session.query(ResourceLog).filter(
            ResourceLog.resource_id == resource_id
        ).delete(synchronize_session=False)
        session.query(Resource).filter(Resource.id == resource_id).delete(
            synchronize_session=False
        )

        session.commit()
        # session.session.expire_all()  # TODO / TOFIX / TOCOMMENT???
        return (0, None)
    else:
        return (3, "Resource must be in DEAD state.")


def get_current_resources_with_suspended_job(
    session,
):
    """Return the list of resources where there are Suspended jobs"""
    res = (
        session.query(AssignedResource.resource_id)
        .filter(AssignedResource.index == "CURRENT")
        .filter(Job.state == "Suspended")
        .filter(Job.assigned_moldable_job == AssignedResource.moldable_id)
        .all()
    )
    return tuple(r[0] for r in res)


def get_current_assigned_job_resources(session, moldable_id):
    """Returns the current resources ref for a job"""
    res = (
        session.query(Resource)
        .filter(AssignedResource.index == "CURRENT")
        .filter(AssignedResource.moldable_id == moldable_id)
        .filter(Resource.id == AssignedResource.resource_id)
        .all()
    )
    return res


def get_resources_change_state(session):
    """Get resource ids that will change their state"""
    res = (
        session.query(Resource.id, Resource.next_state)
        .filter(Resource.next_state != "UnChanged")
        .all()
    )
    return {r.id: r.next_state for r in res}


def get_expired_resources(
    session,
):
    """Get the list of resources whose expiry_date is in the past and which are not dead yet.
    0000-00-00 00:00:00 is always considered as in the future. Used for desktop computing schema.
    """
    # TODO: UNUSED (Desktop computing)
    date = tools.get_date()

    res = (
        session.query(Resource.id)
        .filter(Resource.state == "Alive")
        .filter(Resource.expiry_date > 0)
        .filter(Resource.desktop_computing == "YES")
        .filter(Resource.expiry_date < date)
        .all()
    )
    return [r[0] for r in res]


def get_absent_suspected_resources_for_a_timeout(session, timeout):
    date = tools.get_date()
    res = (
        session.query(ResourceLog.resource_id)
        .filter(ResourceLog.attribute == "state")
        .filter(ResourceLog.date_stop == 0)
        .filter((ResourceLog.date_start + timeout) < date)
        .all()
    )
    return [r[0] for r in res]


def update_resource_nextFinaudDecision(session, resource_id, finaud_decision):
    """Update nextFinaudDecision field"""
    session.query(Resource).filter(Resource.id == resource_id).update(
        {Resource.next_finaud_decision: finaud_decision}, synchronize_session=False
    )
    session.commit()


def update_scheduler_last_job_date(session, date, moldable_id):
    session.query(Resource).filter(AssignedResource.moldable_id == moldable_id).filter(
        AssignedResource.resource_id == Resource.resource_id
    ).update({Resource.last_job_date: date}, synchronize_session=False)


def update_current_scheduler_priority(session, job, value, state):
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
                    session.query(distinct(getattr(Resource, f)))
                    .filter(AssignedResource.index == "CURRENT")
                    .filter(AssignedResource.moldable_id == job.assigned_moldable_job)
                    .filter(AssignedResource.resource_id == Resource.id)
                    .all()
                )

                resources = tuple(r[0] for r in res)

                if resources == ():
                    return

                incr_priority = int(value) * index * coeff
                session.query(Resource).filter(
                    (getattr(Resource, f)).in_(resources)
                ).update(
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


def get_resources_jobs(session, r_id):
    # returns the list of jobs associated to all resources
    # Provide by basequerie
    raise NotImplementedError("TODO")


def get_resource_job_to_frag(session, r_id):
    # same as get_resource_job but excepts the cosystem jobs
    subq = (
        session.query(JobType.job_id)
        .filter(or_(JobType.type == "cosystem", JobType.type == "noop"))
        .filter(JobType.types_index == "CURRENT")
        .subquery()
    )

    res = (
        session.query(Job.id)
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


def get_resources_with_given_sql(session, sql):
    """Returns the resource ids with specified properties parameters : where SQL constraints."""
    results = session.query(Resource.id).filter(text(sql)).order_by(Resource.id).all()
    return [r[0] for r in results]


def log_resource_maintenance_event(session, resource_id, maintenance, date):
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
        session.session.execute(ins)
    else:
        session.query(ResourceLog).filter(ResourceLog.date_stop == 0).filter(
            ResourceLog.attribute == "maintenance"
        ).filter(ResourceLog.resource_id == resource_id).update(
            {ResourceLog.date_stop: date}, synchronize_session=False
        )
        session.commit()


def get_resource_max_value_of_property(session, property_name):
    # returns the max numerical value for a property amongst resources
    propery_field = None
    try:
        propery_field = getattr(Resource, property_name)
    except AttributeError:
        # Property doesn't exist
        return None
    return session.query(func.max(propery_field)).scalar()


def get_resources_state(session, resource_ids):
    date = tools.get_date()
    result = (
        session.query(Resource.id, Resource.state, Resource.available_upto)
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


def get_count_busy_resources(
    session,
):
    active_moldable_job_ids = session.query(Job.assigned_moldable_job).filter(
        Job.state.in_(("toLaunch", "Running", "Resuming"))
    )
    count_busy_resources = (
        session.query(func.count(distinct(AssignedResource.resource_id)))
        .filter(AssignedResource.moldable_id.in_(active_moldable_job_ids))
        .scalar()
    )
    return count_busy_resources


def resources_creation(session, node_name, nb_nodes, nb_core=1, vfactor=1):
    for i in range(nb_nodes * nb_core * vfactor):
        Resource.create(
            session,
            network_address=f"{node_name}{int(i/(nb_core * vfactor)+1)}",
            cpuset=i % nb_core,
            state="Alive",
        )
    session.commit()
