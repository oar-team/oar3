# -*- coding: utf-8 -*-
from __future__ import with_statement, absolute_import, unicode_literals
import pickle

import requests
import zerorpc

from oar.lib.utils import SimpleNamespace


class CoormApplication(object):
    """
    The CoormApplication implement a COORM RPC server to interact with the OAR
    scheduler.

    :param command: The command or the script to run when the job starts.
    :param api_host: The OAR Rest API URL.
    :param api_credentials: A optional username/password tuple.
    :param zeromq_bind_uri:  ZeroMQ bind URI.
    :param nodes: The initial needed number of nodes (default: 0)
    :param walltime:  The default walltime (default: 1:00:00).
    """
    def __init__(self, command, api_host, zeromq_bind_uri,
                 api_credentials=None, nodes=1, walltime="1:00:00", **kwargs):
        self.nodes = 1
        self.command = command
        self.walltime = walltime
        self.zeromq_bind_uri = zeromq_bind_uri
        self.api_host = api_host
        self.api_credentials = api_credentials
        for key in kwargs:
            setattr(self, key, kwargs[key])
        from oar.lib import get_logger
        self.logger = get_logger("oar.coorm", forward_stderr=True)

    def find_resource_hierarchies(self, itvs_avail, hy_res_rqts, hy):
        '''Find resources in interval for all resource subrequests of a
        moldable instance of a job'''
        from oar.kao.scheduling import find_resource_hierarchies_job
        return find_resource_hierarchies_job(itvs_avail, hy_res_rqts, hy)

    def assign_resources(self, slots_set, job, hy, min_start_time):
        '''Assign resources to a job and update by spliting the concerned
        slots'''
        from oar.kao.scheduling import assign_resources_mld_job_split_slots
        return assign_resources_mld_job_split_slots(slots_set, job, hy,
                                                    min_start_time)

    def submit(self):
        """ Submit a new OAR job from the Rest API"""
        self.logger.info("Submit a job to the OAR API")
        auth = self.api_credentials
        try:
            r = requests.get('%s/jobs' % self.api_host, auth=auth)
            assert r.status_code == 200
        except:
            self.logger.error("Failed to establish connection to the API "
                              "Please check the http server address or your "
                              "credentials")
            return
        zmq_protocol = self.zeromq_bind_uri.split('://')[0]
        zmq_ip = self.zeromq_bind_uri.split('://')[1].split(':')[0]
        zmq_port = self.zeromq_bind_uri.split('://')[1].split(':')[1]
        job_type = "assign=coorm:%s:%s:%s" % (zmq_protocol, zmq_ip, zmq_port)
        data = {
            'resource': '/nodes=%s,walltime=%s' % (self.nodes, self.walltime),
            'command': self.command,
            'type': job_type,
        }
        req = requests.post('%s/jobs' % self.api_host, auth=auth, json=data)

        if req.status_code in (200, 201, 202):
            for line in req.json()["cmd_output"].split('\n'):
                self.logger.info(line)
            return req.json()['id']
        else:
            error_msg = req.json()["message"]
            for line in error_msg.split('\n'):
                self.logger.error(line)
            return

    def run(self):
        job_id = self.submit()
        if job_id is not None:
            s = zerorpc.Server(_CoormApplicationProxy(self))
            self.logger.info("Running ZeroMQ RPC server : %s" %
                             self.zeromq_bind_uri)
            s.bind(self.zeromq_bind_uri)
            s.run()
            return 0
        else:
            return 1


class _CoormApplicationProxy(object):

    def __init__(self, app):
        self.app = app

    def find_resource_hierarchies(self, *args, **kwargs):
        self.app.logger.info("┳ OAR ask to find find resource hierarchies")
        result = self.app.find_resource_hierarchies(*args)
        self.app.logger.info("┻ Returns : %s" % result)
        return result

    def assign_resources(self, *proxy_args, **proxy_kwargs):
        self.app.logger.info("┳ OAR ask to assign resources")
        slots_set = pickle.loads(proxy_args[0])
        job_dict = proxy_args[1]
        job = SimpleNamespace(job_dict)

        self.app.logger.debug("┃ Before COORM scheduling")
        for line in ("%s" % slots_set).split('\n'):
            self.app.logger.debug("┃ %s" % line)

        try:
            prev_sid_left, prev_sid_right, job = \
                self.app.assign_resources(slots_set, job, *proxy_args[2:])
        except Exception:
            import pdb; pdb.set_trace()  # noqa
        self.app.logger.debug("┃ After COORM scheduling")
        for line in ("%s" % slots_set).split('\n'):
            self.app.logger.debug("┃ %s" % line)

        self.app.logger.info("┻ Returns : [%s, %s]" %
                             (prev_sid_left, prev_sid_right))
        return prev_sid_left, prev_sid_right, dict(job)
