# coding: utf-8
import pytest
import os

live_testing = pytest.mark.skipif(
    "OAR_LIVE_TEST" not in os.environ, reason="Live testing is disabled"
)
pytestmark = pytest.mark.script_launch_mode("subprocess")
