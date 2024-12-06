# -*- coding: utf-8 -*-
import json

import pytest


def test_whoami_not_auth(client):
    res = client.get("/whoami")
    assert res.status_code == 403


def test_invalid_token(client):
    res = client.get(
        "/check_token",
        headers={"Authorization": "Bearer bad-token-value"},
    )

    assert res.status_code == 401

    data = json.loads(res.content)
    assert data["detail"] == "Could not validate credentials !!! "


@pytest.mark.parametrize(
    "user, status_code",
    [("bob", 200), ("globally_revoked_token", 401), ("old_token", 401)],
)
def test_token_revocation(client, user_tokens, user, status_code):
    res = client.get(
        "/check_token",
        headers={"Authorization": f"Bearer {user_tokens[user]}"},
    )

    assert res.status_code == status_code

    if res.status_code != 200:
        data = json.loads(res.content)
        assert data["detail"] == "Token not valid anymore (revoked by an admin)"
