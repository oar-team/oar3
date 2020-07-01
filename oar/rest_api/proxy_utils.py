import toml
import fcntl
import escapism
import os
import time

from tempfile import NamedTemporaryFile
from contextlib import contextmanager

from oar.lib import config
from oar.lib.job_handling import get_jobs_state


def frontend_backend_traefik(proxy_path):
    path_prefix = escapism.escape(proxy_path)
    frontend = 'frontend_' + path_prefix
    backend = 'backend_' + path_prefix
    return (frontend, backend)


def add_traefik_rule(rules, proxy_path, target_url):
    frontend, backend = frontend_backend_traefik(proxy_path)

    if 'frontends' not in rules:
        rules['frontends'] = {}

    rules['frontends'][frontend] = {
        'backend': backend,
        'passHostHeader': True,
        'routes': {'test': {'rule': 'PathPrefix:' + proxy_path}}
    }

    if 'backends' not in rules:
        rules['backends'] = {}

    rules['backends'][backend] = {
        'servers': {'server1': {'url': target_url, 'weight': 1}}
    }


def del_traefik_rule(rules, proxy_path):
    frontend, backend = frontend_backend_traefik(proxy_path)
    if 'frontends' in rules and frontend in rules['frontends']:
        del rules['frontends'][frontend]

    if 'backends' in rules and backend in rules['backends']:
        del rules['backends'][backend]


def acquire_lock():
    ''' acquire exclusive lock file access '''
    locked_file_descriptor = open('/tmp/rules_oar_proxy.lock', 'w+')
    fcntl.lockf(locked_file_descriptor, fcntl.LOCK_EX)
    return locked_file_descriptor


def release_lock(locked_file_descriptor):
    ''' release exclusive lock file access '''
    locked_file_descriptor.close()


def load_traefik_rules(filename):
    try:
        with open(filename, "r") as rules_fd:
            return toml.load(rules_fd)
    except:
        raise  


def save_treafik_rules(filename, rules):
    with atomic_writing(filename) as rules_fd:
        toml.dump(rules, rules_fd)


# Below code borrowed from traefik-proxy

# atomic writing adapted from jupyter/notebook 5.7
# unlike atomic writing there, which writes the canonical path
# and only use the temp file for recovery,
# we write the temp file and then replace the canonical path
# to ensure that traefik never reads a partial file


@contextmanager
def atomic_writing(path):
    """Write temp file before copying it into place

    Avoids a partial file ever being present in `path`,
    which could cause traefik to load a partial routing table.
    """
    fileobj = NamedTemporaryFile(
        prefix=os.path.abspath(path) + "-tmp-", mode="w", delete=False
    )
    try:
        with fileobj as f:
            yield f
        os.replace(fileobj.name, path)
    finally:
        try:
            os.unlink(fileobj.name)
        except FileNotFoundError:
            # already deleted by os.replace above
            pass


def proxy_cleaning(proxy_rules_filename):
    """ Loop of 120 sec period which checks if jobs associated to Traefik rule is running if not
        rule is deleted from rules file"""

    while True:
        print("UUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUU")
        lock_fd = None
        rules = None
        try:
            lock_fd = acquire_lock()
            rules = load_traefik_rules(proxy_rules_filename)

        except Exception as err:
            print(f'Rest API: proxy_cleaning failed to read proxy rules files {err}')
        finally:
            if rules and 'frontends' in rules:
                # retrieve job_ids from rules
                job_ids = [escapism.unescape(k).split('/')[-1] for k in rules['frontends'].keys()]

                if job_ids:
                    flag_to_save = False
                    for job_id_state in get_jobs_state(job_ids):
                        job_id, state = job_id_state
                        if state == 'Error' or state == 'Terminated':
                            flag_to_save = True
                            proxy_path = '{}/{}'.format(config['OAR_PROXY_BASE_URL'], job_id)
                            del_traefik_rule(rules, proxy_path)
                    if flag_to_save:
                        save_treafik_rules(proxy_rules_filename, rules)
            if lock_fd:
                release_lock(lock_fd)
            time.sleep(20)
