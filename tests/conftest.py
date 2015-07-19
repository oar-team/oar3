# coding: utf-8
from __future__ import unicode_literals, print_function
import pytest
from tempfile import mkstemp
from oar.lib import config

from . import DEFAULT_CONFIG


@pytest.fixture(scope="module", autouse=True)
def setup_config(request):
    config.clear()
    config.update(DEFAULT_CONFIG.copy())
    _, config["LOG_FILE"] = mkstemp()
