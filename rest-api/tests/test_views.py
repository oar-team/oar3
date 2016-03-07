# -*- coding: utf-8 -*-
from flask import url_for


def test_app(client):
    assert client.get(url_for('frontend.index')).status_code == 200
