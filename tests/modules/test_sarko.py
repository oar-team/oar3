# coding: utf-8
from oar.modules.sarko import Sarko

import pytest

def test_sarko_void():
    sarko = Sarko()
    sarko.run()
    print(sarko.guilty_found)
    assert sarko.guilty_found == 0
