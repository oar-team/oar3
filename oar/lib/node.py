# coding: utf-8

from typing import List

from sqlalchemy import and_, distinct, func, or_, text
from sqlalchemy.orm import Session

import oar.lib.tools as tools
from oar.lib.configuration import Configuration
from oar.lib.globals import get_logger
from oar.lib.models import (
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

STATE2NUM = {"Alive": 1, "Absent": 2, "Suspected": 3, "Dead": 4}

logger = get_logger("oar.lib.node")


# TODO change name
def get_all_resources_on_node(session: Session, hostname: str):
    """Return the current resources on node whose hostname is passed in parameter"""
    result = (
        session.query(Resource.id).filter(Resource.network_address == hostname).all()
    )
    return [r[0] for r in result]


def get_resources_of_nodes(session: Session, hostnames: str) -> List[Resource]:
    """Return the current resources on node whose hostname is passed in parameter"""
    result = (
        session.query(Resource)
        .filter(Resource.network_address.in_(tuple(hostnames)))
        .order_by(Resource.id)
        .all()
    )
    return result


def get_nodes_with_state(session: Session, nodes: List[str]) -> List[Resource]:
    result = (
        session.query(Resource.network_address, Resource.state)
        .filter(Resource.network_address.in_(tuple(nodes)))
        .all()
    )
    return result


def search_idle_nodes(session: Session, date: int) -> dict[str, int]:
    result = (
        session.query(distinct(Resource.network_address))
        .filter(Resource.id == GanttJobsResource.resource_id)
        .filter(GanttJobsPrediction.start_time <= date)
        .filter(Resource.network_address != "")
        .filter(Resource.type == "default")
        .filter(GanttJobsPrediction.moldable_id == GanttJobsResource.moldable_id)
        .all()
    )

    busy_nodes = {}  # TODO can be remove ? to replace by busy_nodes = result
    for network_address in result:
        logger.debug(f"{network_address}")
        busy_nodes[network_address[0]] = True

    query = (
        session.query(Resource.network_address, func.max(Resource.last_job_date))
        .filter(Resource.state == "Alive")
        .filter(Resource.network_address != "")
        .filter(Resource.type == "default")
        .filter(Resource.available_upto < 2147483647)
        .filter(Resource.available_upto > 0)
        .group_by(Resource.network_address)
    )

    logger.debug(f"idle nodes query: {query}")
    result = query.all()
    logger.debug(f"idle nodes query: {result}")
    idle_nodes = {}

    for x in result:
        network_address, last_job_date = x
        if network_address not in busy_nodes:
            idle_nodes[network_address] = last_job_date

    return idle_nodes


# TODO MOVE TO GANTT
def get_gantt_hostname_to_wake_up(session: Session, date: int, wakeup_time: int):
    """Get hostname that we must wake up to launch jobs"""
    hostnames = (
        session.query(Resource.network_address)
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


def get_next_job_date_on_node(session: Session, hostname: str):
    result = (
        session.query(func.min(GanttJobsPrediction.start_time))
        .filter(Resource.network_address == hostname)
        .filter(GanttJobsResource.resource_id == Resource.id)
        .filter(GanttJobsPrediction.moldable_id == GanttJobsResource.moldable_id)
        .scalar()
    )
    return result


def get_last_wake_up_date_of_node(session: Session, hostname: str):
    result = (
        session.query(EventLog.date)
        .filter(EventLogHostname.event_id == EventLog.id)
        .filter(EventLogHostname.hostname == hostname)
        .filter(EventLog.type == "WAKEUP_NODE")
        .order_by(EventLog.date.desc())
        .limit(1)
        .scalar()
    )
    return result


def get_alive_nodes_with_jobs(
    session: Session,
):
    """Returns the list of occupied nodes"""
    result = (
        session.query(distinct(Resource.network_address))
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


def get_nodes_that_can_be_waked_up(session: Session, date: int):
    """Returns the list nodes that can be waked up from to the given date"""
    result = (
        session.query(distinct(Resource.network_address))
        .filter(Resource.state == "Absent")
        .filter(Resource.available_upto > date)
        .all()
    )
    return [r[0] for r in result]


def get_nodes_with_given_sql(session: Session, properties: List[str]) -> List[str]:
    """Gets the nodes list with the given sql properties"""
    result = (
        session.query(Resource.network_address, Resource.state, Resource.next_state)
        .distinct()
        .filter(text(properties))
        .all()
    )
    return [r for r in result]


def set_node_state(
    session: Session, hostname: str, state: str, finaud_tag: str, config: Configuration
):
    """Sets the state field of some node identified by its hostname in the session.
    - parameters : base, hostname, state, finaudDecision
    - side effects : changes the state value in some field of the nodes table"""
    if state == "Suspect":
        query = session.query(Resource).filter(Resource.network_address == hostname)
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
        session.query(Resource).filter(Resource.network_address == hostname).update(
            {
                Resource.state: state,
                Resource.finaud_decision: finaud_tag,
                Resource.state_num: STATE2NUM[state],
            },
            synchronize_session=False,
        )
        session.commit()
    date = tools.get_date(session)
    if config["DB_TYPE"] == "Pg":
        session.query(ResourceLog).filter(ResourceLog.date_stop == 0).filter(
            ResourceLog.attribute == "state"
        ).filter(Resource.network_address == hostname).filter(
            ResourceLog.resource_id == Resource.id
        ).update(
            {ResourceLog.date_stop: date}, synchronize_session=False
        )
    else:
        logger.debug("Warnning: Sqlite must not be used in production")
        cur = session
        cur.execute(
            text(
                f"""UPDATE resource_logs SET date_stop = {str(date)}
        WHERE EXISTS (SELECT 1 FROM resources WHERE resources.network_address = '{hostname}'
        AND resource_logs.resource_id = resources.resource_id)
        AND resource_logs.date_stop = 0
        AND resource_logs.attribute = '{state}'"""
            )
        )
    session.commit()

    # sel = select([Resource.id, text('state'), text(state), text(str(date)), text(finaud_tag)])\
    #     .where(Resource.network_address == hostname)

    # ins = ResourceLog.__table__.insert()\
    #                           .from_select((ResourceLog.resource_id, ResourceLog.attribute,
    #                                         ResourceLog.value, ResourceLog.date_start,
    #                                         ResourceLog.finaud_decision), sel)
    # session.execute(ins)

    # sqlalchemy.exc.ProgrammingError: (psycopg2.ProgrammingError) column "suspected" does not exist
    # LINE 1: ...ud_decision) SELECT resources.resource_id, state, Suspected,...

    # [SQL: 'INSERT INTO resource_logs (resource_id, attribute, value, date_start, finaud_decision) SELECT resources.resource_id, state, Suspected, 1512296767, NO \nFROM resources \nWHERE resources.network_address = %(network_address_1)s'] [parameters: {'network_address_1': ('node1',)}]
    # [   DEBUG] [2017-12-03 10:26:07,603] [oar.modules.almighty]: /usr/local/lib/oar/oar3-node-change-state terminated

    cur = session
    cur.execute(
        text(
            f"""INSERT INTO resource_logs (resource_id,attribute,value,date_start,finaud_decision)
                SELECT resources.resource_id, 'state', '{state}', '{str(date)}' , '{finaud_tag}'
                FROM resources
                WHERE
                    resources.network_address = '{hostname}'"""
        )
    )
    session.commit()


def set_node_nextState(session: Session, hostname: str, next_state: str) -> int:
    """Sets the nextState field of a node identified by its network_address"""
    nb_matched = (
        session.query(Resource)
        .filter(Resource.network_address == hostname)
        .update(
            {Resource.next_state: next_state, Resource.next_finaud_decision: "NO"},
            synchronize_session=False,
        )
    )
    session.commit()
    return nb_matched


def change_node_state(session: Session, node: str, state, config: Configuration):
    """Changes node state and notify central automaton"""
    set_node_nextState(session, node, state)
    tools.notify_almighty("ChState")


def get_finaud_nodes(
    session: Session,
):
    """Return the list of network address nodes for Finaud"""
    # TODO: session.query(Resource).distinct(Resource.network_address) should not properly work with SQLITE
    # https://stackoverflow.com/questions/17223174/returning-distinct-rows-in-sqlalchemy-with-sqlite

    return (
        session.query(Resource)
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


def get_current_assigned_nodes(
    session: Session,
) -> List[str]:
    """Returns the current nodes"""
    results = (
        session.query(distinct(Resource.network_address))
        .filter(AssignedResource.index == "CURRENT")
        .filter(Resource.id == AssignedResource.resource_id)
        .filter(Resource.type == "default")
        .all()
    )
    return [r[0] for r in results]


def update_node_nextFinaudDecision(
    session: Session, network_address: str, finaud_decision: str
):
    # Update nextFinaudDecision field
    session.query(Resource).filter(Resource.network_address == network_address).update(
        {Resource.next_finaud_decision: finaud_decision}, synchronize_session=False
    )
    session.commit()


def get_node_job_to_frag(session: Session, hostname: str) -> List[int]:
    # same as get_node_job but excepts cosystem jobs
    subq = (
        session.query(JobType.job_id)
        .filter(or_(JobType.type == "cosystem", JobType.type == "noop"))
        .filter(JobType.types_index == "CURRENT")
    )

    res = (
        session.query(Job.id)
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


def get_all_network_address(
    session: Session,
) -> List[str]:
    res = session.query(distinct(Resource.network_address)).all()
    return [r[0] for r in res]


def get_resources_state_for_host(session: Session, host: str):
    resource_ids = [
        r[0]
        for r in session.query(Resource.id)
        .filter(Resource.network_address == host)
        .order_by(Resource.id)
        .all()
    ]
    return get_resources_state(session, resource_ids)
