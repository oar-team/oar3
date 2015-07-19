# coding: utf-8
from __future__ import unicode_literals, print_function
import pytest
import re
from tempfile import mkstemp
from oar.lib import config

from . import DEFAULT_CONFIG


@pytest.fixture(scope="module", autouse=True)
def setup_config(request):
    if not re.search(r'test_db_metasched', request.node.name):
        print("setup_config")
        config.clear()
        config.update(DEFAULT_CONFIG.copy())
        _, config["LOG_FILE"] = mkstemp()

    # if re.search(r'test_db_metasched', request.node.name):
    #    _, config["LOG_FILE"] = mkstemp()
