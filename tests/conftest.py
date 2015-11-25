# -*- coding: utf-8 -*-
import pytest

from oar.rest_api.app import create_app


@pytest.fixture
def app():
    app = create_app()
    return app
