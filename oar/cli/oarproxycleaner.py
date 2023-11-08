import time

import click
import escapism
from sqlalchemy.orm import scoped_session, sessionmaker

from oar.lib.globals import init_oar
from oar.lib.job_handling import get_jobs_state
from oar.rest_api.proxy_utils import (
    acquire_lock,
    del_traefik_rule,
    load_traefik_rules,
    release_lock,
    save_treafik_rules,
)

from .utils import CommandReturns


@click.command()
@click.option(
    "--period",
    default=120,
    type=int,
    help="Period between each proxy's rules cleaning (delete rule is associated Job is finished",
)
def cli(period):
    """Proxy's rules cleaner which checks if jobs associated to Traefik proxy rule is running if not
    rule is deleted from rules file"""
    config, engine, log = init_oar()
    session_factory = sessionmaker(bind=engine)
    scoped = scoped_session(session_factory)
    session = scoped()

    cmd_ret = CommandReturns()
    proxy_rules_filename = config["PROXY_TRAEFIK_RULES_FILE"]

    while True:
        lock_fd = None
        rules = None
        try:
            lock_fd = acquire_lock()
            rules = load_traefik_rules(proxy_rules_filename)
        except FileNotFoundError:
            cmd_ret.warning(f"Waiting for rules file: {proxy_rules_filename}")
        except Exception as err:
            cmd_ret.error(
                f"Rest API: proxy_cleaning failed to read proxy rules files {err}"
            )
        finally:
            if rules and "frontends" in rules:
                # retrieve job_ids from rules
                job_ids = [
                    escapism.unescape(k).split("/")[-1]
                    for k in rules["frontends"].keys()
                ]
                print("job_ids: {}".format(job_ids))
                if job_ids:
                    flag_to_save = False
                    for job_id_state in get_jobs_state(session, job_ids):
                        job_id, state = job_id_state
                        if state == "Error" or state == "Terminated":
                            flag_to_save = True
                            proxy_path = "{}/{}".format(
                                config["OAR_PROXY_BASE_URL"], job_id
                            )
                            del_traefik_rule(rules, proxy_path)
                    if flag_to_save:
                        save_treafik_rules(proxy_rules_filename, rules)
            release_lock(lock_fd)
            time.sleep(period)
