# -*- coding: utf-8 -*-
import copy
import json

import pytest

from oar.api.app import create_app


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


def test_token_renew(client, user_tokens):
    res = client.get(
        "/get_new_token",
        headers={"Authorization": f"Bearer {user_tokens['bob']}"},
    )

    assert res.status_code == 200

    data = json.loads(res.content)
    assert data["OAR_API_TOKEN"] is not None


@pytest.mark.parametrize("secret", ["TO_CHANGE", "", None])
def test_api_does_not_start_with_invalid_secret(setup_config, secret):
    """The API must refuse to start while API_SECRET_KEY is the public
    placeholder (TO_CHANGE), empty or missing. Otherwise anybody could forge
    valid tokens, see oar.lib.access_token.check_api_secret_key.
    """
    config, engine = setup_config
    # Deep copy: setup_config is session scoped, we must not pollute it.
    config = copy.deepcopy(config)
    config["API_SECRET_KEY"] = secret

    with pytest.raises(ValueError, match="API_SECRET_KEY"):
        create_app(config=config, engine=engine)


def test_api_starts_with_valid_secret(setup_config):
    """A real (non placeholder) secret must allow the API to start."""
    config, engine = setup_config
    config = copy.deepcopy(config)
    config["API_SECRET_KEY"] = (
        "2d610d90251c884d572057ac19335f21467a82bce3084a4e5ea94e72bb61663c"
    )

    app = create_app(config=config, engine=engine)
    assert app is not None
