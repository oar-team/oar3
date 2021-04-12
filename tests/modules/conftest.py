# -*- coding: utf-8 -*-

import pytest

import oar.lib.tools  # for monkeypatching
from oar.lib import Queue, Resource, db

# @pytest.yield_fixture(scope='function', autouse=True)
# def minimal_db_initialization(request):
#     with db.session(ephemeral=True):
#         # add some resources
#         for i in range(5):
#             Resource.create(network_address='localhost'+str(i))
#         Queue.create(name='default')
#         db.commit()
#         yield
