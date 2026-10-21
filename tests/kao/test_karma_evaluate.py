# coding: utf-8
"""Unit tests (no DB) for the merge of running-job consumption into karma.

``evaluate_jobs_karma`` must optionally add the consumption of currently
running jobs to the accounting values, gated by the config flag
``SCHEDULER_FAIRSHARING_INCLUDE_RUNNING_JOBS`` (default ``yes``).

These tests use a fake ``plt`` (Platform) so that no database is required.
"""

import types

import pytest

from oar.kao.karma import evaluate_jobs_karma


class FakePlt:
    """Fake Platform recording how many times each accessor is called."""

    def __init__(self):
        self.window_calls = 0
        self.project_calls = 0
        self.user_calls = 0
        self.running_calls = 0

    def get_sum_accounting_window(self, session, queues, window_start, window_stop):
        self.window_calls += 1
        return (100.0, 100.0)

    def get_sum_accounting_by_project(self, session, queues, window_start, window_stop):
        self.project_calls += 1
        return ({}, {"default": 100.0})

    def get_sum_accounting_by_user(self, session, queues, window_start, window_stop):
        self.user_calls += 1
        return ({"alice": 100.0}, {"alice": 100.0})

    def get_running_consumption(self, session, queues, window_start, window_stop):
        self.running_calls += 1
        return (50.0, 50.0, {}, {}, {"alice": 50.0}, {"alice": 50.0})


def _config(include_running):
    return {
        "SCHEDULER_FAIRSHARING_WINDOW_SIZE": 1000,
        "SCHEDULER_FAIRSHARING_PROJECT_TARGETS": "{default => 0}",
        "SCHEDULER_FAIRSHARING_USER_TARGETS": "{alice => 0}",
        "SCHEDULER_FAIRSHARING_COEF_PROJECT": "1",
        "SCHEDULER_FAIRSHARING_COEF_USER": "1",
        "SCHEDULER_FAIRSHARING_COEF_USER_ASK": "1",
        "SCHEDULER_FAIRSHARING_INCLUDE_RUNNING_JOBS": include_running,
    }


def test_evaluate_jobs_karma_includes_running_consumption():
    fake = FakePlt()
    job = types.SimpleNamespace(project="default", user="alice")
    jobs = {1: job}

    evaluate_jobs_karma(
        session=None,
        config=_config("yes"),
        queues=["default"],
        now=100000,
        jids=[1],
        jobs=jobs,
        plt=fake,
    )

    # asked: 100 (accounting) + 50 (running) = 150
    # used : 100 (accounting) + 50 (running) = 150
    # project used = 100, user used = 150, user asked = 150
    assert job.karma == pytest.approx(100 / 150 + 150 / 150 + 150 / 150)
    assert fake.running_calls == 1
    assert fake.window_calls == 1
    assert fake.project_calls == 1
    assert fake.user_calls == 1


def test_evaluate_jobs_karma_excludes_running_consumption():
    fake = FakePlt()
    job = types.SimpleNamespace(project="default", user="alice")
    jobs = {1: job}

    evaluate_jobs_karma(
        session=None,
        config=_config("no"),
        queues=["default"],
        now=100000,
        jids=[1],
        jobs=jobs,
        plt=fake,
    )

    # project used = 100, user used = 100, user asked = 100
    assert job.karma == pytest.approx(3.0)
    assert fake.running_calls == 0
    assert fake.window_calls == 1
    assert fake.project_calls == 1
    assert fake.user_calls == 1
