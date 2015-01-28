# -*- coding: utf-8 -*-
from __future__ import division


class WSGIApplication(object):
    def __init__(self, app):
        self.app = app

    def __call__(self, environ, start_response):
        user = environ.pop('X_REMOTE_IDENT', None)
        environ['USER'] = user
        return self.app(environ, start_response)
