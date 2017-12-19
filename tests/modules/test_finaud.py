# coding: utf-8

from oar.modules.finaud import Finaud
from oar.lib import config
from .fakezmq import FakeZmq
import oar.lib.tools

import pytest

def test_finaud_void():
    finaud = Finaud()
    finaud.run()
    print(finaud.return_value)
    assert finaud.return_value == 0
