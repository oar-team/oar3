# coding: utf-8

from sqlalchemy import desc, func

from oar.lib import tools

# logger = get_logger("oar.lib.event")
from oar.lib.globals import init_oar
from oar.lib.models import EventLog, EventLogHostname  # , db, get_logger, tools

config, db, logger = init_oar()


def add_new_event(session, ev_type, job_id, description, to_check="YES"):
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


def add_new_event_with_host(session, ev_type, job_id, description, hostnames):
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


def is_an_event_exists(session, job_id, event):
    res = (
        session.query(func.count(EventLog.id))
        .filter(EventLog.job_id == job_id)
        .filter(EventLog.type == event)
        .scalar()
    )
    return res


def get_job_events(session, job_id):
    """Get events for the specified job"""
    result = (
        session.query(EventLog)
        .filter(EventLog.job_id == job_id)
        .order_by(EventLog.date)
        .all()
    )
    return result


def get_jobs_events(session, job_ids):
    """Get events for the specified jobs"""
    result = (
        session.query(EventLog)
        .filter(EventLog.job_id.in_(tuple(job_ids)))
        .order_by(EventLog.job_id, EventLog.date)
        .all()
    )
    return result


def get_to_check_events(session):
    """ "Get all events with toCheck field on YES"""
    result = (
        session.query(EventLog)
        .filter(EventLog.to_check == "YES")
        .order_by(EventLog.id)
        .all()
    )
    return result


def check_event(session, event_type, job_id):
    """Turn the field toCheck into NO"""
    session.query(EventLog).filter(EventLog.job_id == job_id).filter(
        EventLog.type == event_type
    ).filter(EventLog.to_check == "YES").update(
        {"to_check": "NO"}, synchronize_session=False
    )
    session.commit()


def get_hostname_event(session, event_id):
    """Get hostnames corresponding to an event Id"""
    res = (
        session.query(EventLogHostname.hostname)
        .filter(EventLogHostname.event_id == event_id)
        .all()
    )
    return [h[0] for h in res]


def get_events_for_hostname_from(session, host, date=None):
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
