# coding: utf-8

from typing import List, Optional

from sqlalchemy import desc, func
from sqlalchemy.orm import Session

from oar.lib import tools
from oar.lib.models import EventLog, EventLogHostname


def add_new_event(
    session: Session, ev_type: str, job_id: int, description: str, to_check: str = "YES"
):
    """Add a new entry in event_log table"""
    event_data = EventLog(
        type=ev_type,
        job_id=job_id,
        date=tools.get_date(session),
        description=description[:255],
        to_check="YES",
    )
    session.add(event_data)
    session.commit()


def add_new_event_with_host(
    session: Session, ev_type: str, job_id: int, description: str, hostnames: List[str]
):
    ins = EventLog.__table__.insert().values(
        {
            "type": ev_type,
            "job_id": job_id,
            "date": tools.get_date(session),
            "description": description[:255],
        }
    )
    result = session.execute(ins)
    event_id = result.inserted_primary_key[0]

    # Forces unique values in hostnames by using set and
    # fills the EventLogHostname
    if not isinstance(hostnames, list):
        raise TypeError("hostnames must be a list")
    for hostname in set(hostnames):
        session.add(EventLogHostname(event_id=event_id, hostname=hostname))
    session.commit()


def is_an_event_exists(session: Session, job_id: int, event: str):
    res = (
        session.query(func.count(EventLog.id))
        .filter(EventLog.job_id == job_id)
        .filter(EventLog.type == event)
        .scalar()
    )
    return res


def get_job_events(session: Session, job_id: int):
    """Get events for the specified job"""
    result = (
        session.query(EventLog)
        .filter(EventLog.job_id == job_id)
        .order_by(EventLog.date)
        .all()
    )
    return result


def get_jobs_events(session: Session, job_ids: List[int]):
    """Get events for the specified jobs"""
    result = (
        session.query(EventLog)
        .filter(EventLog.job_id.in_(tuple(job_ids)))
        .order_by(EventLog.job_id, EventLog.date)
        .all()
    )
    return result


def get_to_check_events(session: Session):
    """ "Get all events with toCheck field on YES"""
    result = (
        session.query(EventLog)
        .filter(EventLog.to_check == "YES")
        .order_by(EventLog.id)
        .all()
    )
    return result


def check_event(session: Session, event_type: str, job_id: int):
    """Turn the field toCheck into NO"""
    session.query(EventLog).filter(EventLog.job_id == job_id).filter(
        EventLog.type == event_type
    ).filter(EventLog.to_check == "YES").update(
        {"to_check": "NO"}, synchronize_session=False
    )
    session.commit()


def get_hostname_event(session: Session, event_id: int):
    """Get hostnames corresponding to an event Id"""
    res = (
        session.query(EventLogHostname.hostname)
        .filter(EventLogHostname.event_id == event_id)
        .all()
    )
    return [h[0] for h in res]


def get_events_for_hostname_from(
    session: Session, host: str, date: Optional[int] = None
) -> List[EventLog]:
    """Get events for the hostname given as parameter
    If date is given, returns events since that date, else return the 30 last events.
    """
    query = (
        session.query(EventLog)
        .filter(EventLogHostname.event_id == EventLog.id)
        .filter(EventLogHostname.hostname == host)
    )
    if date:
        query = query.filter(EventLog.date >= tools.sql_to_local(date)).order_by(
            desc(EventLog.date)
        )
    else:
        query = query.order_by(desc(EventLog.date)).limit(30)

    return query.all()
