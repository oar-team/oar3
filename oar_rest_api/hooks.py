# -*- coding: utf-8 -*-
from oar.lib import db


def shutdown_db_session(response_or_exc):
    db.session.remove()


def register_hooks(app):
    '''Declares all flask application hooks'''
     # 0.9 and later
    if hasattr(app, 'teardown_appcontext'):
        teardown = app.teardown_appcontext
    # 0.7 to 0.8
    elif hasattr(app, 'teardown_request'):
        teardown = app.teardown_request
    # Older Flask versions
    else:
        teardown = app.after_request

    teardown(shutdown_db_session)
