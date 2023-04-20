# coding: utf-8

from sqlalchemy import and_, distinct, func, or_, text

import oar.lib.tools as tools
from oar.lib.globals import init_oar
from oar.lib.logging import get_logger
from oar.lib.models import (  # config,; db,; get_logger,
    AssignedResource,
    EventLog,
    EventLogHostname,
    GanttJobsPrediction,
    GanttJobsResource,
    Job,
    JobType,
    MoldableJobDescription,
    Resource,
    ResourceLog,
)
from oar.lib.resource_handling import get_resources_state

_, _, logger = init_oar()

STATE2NUM = {"Alive": 1, "Absent": 2, "Suspected": 3, "Dead": 4}

logger = get_logger(logger, "oar.lib.node")


# TODO change name
def get_all_resources_on_node(hostname):
    """Return the current resources on node whose hostname is passed in parameter"""
    result = db.query(Resource.id).filter(Resource.network_address == hostname).all()
    return [r[0] for r in result]


def get_resources_of_nodes(hostnames):
    """Return the current resources on node whose hostname is passed in parameter"""
    result = (
        db.query(Resource)
        .filter(Resource.network_address.in_(tuple(hostnames)))
        .order_by(Resource.id)
        .all()
    )
    return result


def get_nodes_with_state(nodes):
    result = (
        db.query(Resource.network_address, Resource.state)
        .filter(Resource.network_address.in_(tuple(nodes)))
        .all()
    )
    return result


def search_idle_nodes(date):
    result = (
        db.query(distinct(Resource.network_address))
        .filter(Resource.id == GanttJobsResource.resource_id)
        .filter(GanttJobsPrediction.start_time <= date)
        .filter(Resource.network_address != "")
        .filter(Resource.type == "default")
        .filter(GanttJobsPrediction.moldable_id == GanttJobsResource.moldable_id)
        .all()
    )

    busy_nodes = {}  # TODO can be remove ? to replace by busy_nodes = result
    for network_address in result:
        busy_nodes[network_address[0]] = True

    result = (
        db.query(Resource.network_address, func.max(Resource.last_job_date))
        .filter(Resource.state == "Alive")
        .filter(Resource.network_address != "")
        .filter(Resource.type == "default")
        .filter(Resource.available_upto < 2147483647)
        .filter(Resource.available_upto > 0)
        .group_by(Resource.network_address)
        .all()
    )

    idle_nodes = {}
    for x in result:
        network_address, last_job_date = x
        if network_address not in busy_nodes:
            idle_nodes[network_address] = last_job_date

    return idle_nodes


# TODO MOVE TO GANTT
def get_gantt_hostname_to_wake_up(date, wakeup_time):
    """Get hostname that we must wake up to launch jobs"""
    hostnames = (
        db.query(Resource.network_address)
        .filter(GanttJobsResource.moldable_id == GanttJobsPrediction.moldable_id)
        .filter(MoldableJobDescription.id == GanttJobsPrediction.moldable_id)
        .filter(Job.id == MoldableJobDescription.job_id)
        .filter(GanttJobsPrediction.start_time <= date + wakeup_time)
        .filter(Job.state == "Waiting")
        .filter(Resource.id == GanttJobsResource.resource_id)
        .filter(Resource.state == "Absent")
        .filter(Resource.network_address != "")
        .filter(Resource.type == "default")
        .filter(
            (GanttJobsPrediction.start_time + MoldableJobDescription.walltime)
            <= Resource.available_upto
        )
        .group_by(Resource.network_address)
        .all()
    )
    hosts = [h_tpl[0] for h_tpl in hostnames]
    return hosts


def get_next_job_date_on_node(hostname):
    result = (
        db.query(func.min(GanttJobsPrediction.start_time))
        .filter(Resource.network_address == hostname)
        .filter(GanttJobsResource.resource_id == Resource.id)
        .filter(GanttJobsPrediction.moldable_id == GanttJobsResource.moldable_id)
        .scalar()
    )
    return result


def get_last_wake_up_date_of_node(hostname):
    result = (
        db.query(EventLog.date)
        .filter(EventLogHostname.event_id == EventLog.id)
        .filter(EventLogHostname.hostname == hostname)
        .filter(EventLog.type == "WAKEUP_NODE")
        .order_by(EventLog.date.desc())
        .limit(1)
        .scalar()
    )
    return result


def get_alive_nodes_with_jobs():
    """Returns the list of occupied nodes"""
    result = (
        db.query(distinct(Resource.network_address))
        .filter(Resource.id == AssignedResource.resource_id)
        .filter(AssignedResource.moldable_id == MoldableJobDescription.id)
        .filter(MoldableJobDescription.job_id == Job.id)
        .filter(
            Job.state.in_(
                (
                    "Waiting",
                    "Hold",
                    "toLaunch",
                    "toError",
                    "toAckReservation",
                    "Launching",
                    "Running",
                    "Suspended ",
                    "Resuming",
                )
            )
        )
        .filter(or_(Resource.state == "Alive", Resource.next_state == "Alive"))
        .all()
    )
    return [r[0] for r in result]


def get_nodes_that_can_be_waked_up(date):
    """Returns the list nodes that can be waked up from to the given date"""
    result = (
        db.query(distinct(Resource.network_address))
        .filter(Resource.state == "Absent")
        .filter(Resource.available_upto > date)
        .all()
    )
    return [r[0] for r in result]


def get_nodes_with_given_sql(properties):
    """Gets the nodes list with the given sql properties"""
    result = (
        db.query(Resource.network_address).distinct().filter(text(properties)).all()
    )
    return [r[0] for r in result]


def set_node_state(hostname, state, finaud_tag):
    """Sets the state field of some node identified by its hostname in the DB.
    - parameters : base, hostname, state, finaudDecision
    - side effects : changes the state value in some field of the nodes table"""
    if state == "Suspect":
        query = db.query(Resource).filter(Resource.network_address == hostname)
        if finaud_tag == "YES":
            query = query.filter(Resource.state == "Alive")
        else:
            query = query.filter(
                or_(
                    Resource.state == "Alive",
                    and_(
                        Resource.state == "Suspected", Resource.finaud_decision == "YES"
                    ),
                )
            )
        nb_rows = query.update(
            {
                Resource.state: state,
                Resource.finaud_decision: finaud_tag,
                Resource.state_num: STATE2NUM[state],
            },
            synchronize_session=False,
        )

        if nb_rows == 0:
            # OAR wants to turn the node into Suspected state but it is not in
            # the Alive state --> so we do nothing
            logger.debug(
                "Try to turn the node: + "
                + hostname
                + " into Suspected but it is not into the Alive state SO we do nothing"
            )
            return

    else:
        db.query(Resource).filter(Resource.network_address == hostname).update(
            {
                Resource.state: state,
                Resource.finaud_decision: finaud_tag,
                Resource.state_num: STATE2NUM[state],
            },
            synchronize_session=False,
        )
        db.commit()
    date = tools.get_date()
    if config["DB_TYPE"] == "Pg":
        db.query(ResourceLog).filter(ResourceLog.date_stop == 0).filter(
            ResourceLog.attribute == "state"
        ).filter(Resource.network_address == hostname).filter(
            ResourceLog.resource_id == Resource.id
        ).update(
            {ResourceLog.date_stop: date}, synchronize_session=False
        )
    else:
        logger.debug("Warnning: Sqlite must not be used in production")
        cur = db.session
        cur.execute(
            """UPDATE resource_logs SET date_stop = %s
        WHERE EXISTS (SELECT 1 FROM resources WHERE resources.network_address = '%s'
        AND resource_logs.resource_id = resources.resource_id)
        AND resource_logs.date_stop = 0
        AND resource_logs.attribute = '%s'"""
            % (str(date), hostname, state)
        )
    db.commit()

    # sel = select([Resource.id, text('state'), text(state), text(str(date)), text(finaud_tag)])\
    #     .where(Resource.network_address == hostname)

    # ins = ResourceLog.__table__.insert()\
    #                           .from_select((ResourceLog.resource_id, ResourceLog.attribute,
    #                                         ResourceLog.value, ResourceLog.date_start,
    #                                         ResourceLog.finaud_decision), sel)
    # db.session.execute(ins)

    # sqlalchemy.exc.ProgrammingError: (psycopg2.ProgrammingError) column "suspected" does not exist
    # LINE 1: ...ud_decision) SELECT resources.resource_id, state, Suspected,...

    # [SQL: 'INSERT INTO resource_logs (resource_id, attribute, value, date_start, finaud_decision) SELECT resources.resource_id, state, Suspected, 1512296767, NO \nFROM resources \nWHERE resources.network_address = %(network_address_1)s'] [parameters: {'network_address_1': ('node1',)}]
    # [   DEBUG] [2017-12-03 10:26:07,603] [oar.modules.almighty]: /usr/local/lib/oar/oar3-node-change-state terminated

    cur = db.session
    cur.execute(
        """INSERT INTO resource_logs (resource_id,attribute,value,date_start,finaud_decision)
                SELECT resources.resource_id, 'state', '%s', '%s' , '%s'
                FROM resources
                WHERE
                    resources.network_address = '%s'"""
        % (state, str(date), finaud_tag, hostname)
    )
    db.commit()


def set_node_nextState(hostname, next_state):
    """Sets the nextState field of a node identified by its network_address"""
    nb_matched = (
        db.query(Resource)
        .filter(Resource.network_address == hostname)
        .update(
            {Resource.next_state: next_state, Resource.next_finaud_decision: "NO"},
            synchronize_session=False,
        )
    )
    db.commit()
    return nb_matched


def change_node_state(node, state, config):
    """Changes node state and notify central automaton"""
    set_node_nextState(node, state)
    tools.notify_almighty("ChState")


def get_finaud_nodes():
    """Return the list of network address nodes for Finaud"""
    # TODO: db.query(Resource).distinct(Resource.network_address) should not properly work with SQLITE
    # https://stackoverflow.com/questions/17223174/returning-distinct-rows-in-sqlalchemy-with-sqlite

    return (
        db.query(Resource)
        .distinct(Resource.network_address)
        .filter(
            or_(
                Resource.state == "Alive",
                and_(Resource.state == "Suspected", Resource.finaud_decision == "YES"),
            )
        )
        .filter(Resource.type == "default")
        .filter(Resource.desktop_computing == "NO")
        .filter(Resource.next_state == "UnChanged")
        .all()
    )


def get_current_assigned_nodes():
    """Returns the current nodes"""
    results = (
        db.query(distinct(Resource.network_address))
        .filter(AssignedResource.index == "CURRENT")
        .filter(Resource.id == AssignedResource.resource_id)
        .filter(Resource.type == "default")
        .all()
    )
    return [r[0] for r in results]


def update_node_nextFinaudDecision(network_address, finaud_decision):
    # Update nextFinaudDecision field
    db.query(Resource).filter(Resource.network_address == network_address).update(
        {Resource.next_finaud_decision: finaud_decision}, synchronize_session=False
    )
    db.commit()


def get_node_job_to_frag(hostname):
    # same as get_node_job but excepts cosystem jobs
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
        .filter(Resource.network_address == hostname)
        .filter(AssignedResource.resource_id == Resource.id)
        .filter(AssignedResource.moldable_id == MoldableJobDescription.id)
        .filter(MoldableJobDescription.job_id == Job.id)
        .filter(Job.state != "Terminated")
        .filter(Job.state != "Error")
        .filter(~Job.id.in_(subq))
        .order_by(Job.id)
        .all()
    )

    return [r[0] for r in res]


def get_all_network_address():
    res = db.query(distinct(Resource.network_address)).all()
    return [r[0] for r in res]


def get_resources_state_for_host(host):
    resource_ids = [
        r[0]
        for r in db.query(Resource.id)
        .filter(Resource.network_address == host)
        .order_by(Resource.id)
        .all()
    ]
    return get_resources_state(resource_ids)
