# -*- coding: utf-8 -*-
import pytest
from flask import url_for
from oar.rest_api.app import create_app


@pytest.fixture
def app():
    app = create_app()
    return app


def test_app(client):
    assert client.get(url_for('frontend.index')).status_code == 200
