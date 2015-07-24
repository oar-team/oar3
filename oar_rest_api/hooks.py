# -*- coding: utf-8 -*-
import os
from flask import g, request
from oar.lib import db


def register_hooks(app):
    """Declare all flask application hooks."""
    # 0.9 and later
    if hasattr(app, 'teardown_appcontext'):
        teardown = app.teardown_appcontext
    # 0.7 to 0.8
    elif hasattr(app, 'teardown_request'):
        teardown = app.teardown_request
    # Older Flask versions
    else:
        teardown = app.after_request

    def shutdown_db_session(response_or_exc):
        db.session.remove()

    def reflect_database():
        db.reflect()

    def authenticate():
        if app.debug:
            g.current_user = 'docker'
        else:
            g.current_user = request.environ.get('USER', None)
        if g.current_user is not None:
            os.environ['OARDO_USER'] = g.current_user
        else:
            if 'OARDO_USER' in os.environ:
                del os.environ['OARDO_USER']

    app.before_request(reflect_database)
    app.before_request(authenticate)
    teardown(shutdown_db_session)
