# coding: utf-8
from oar.modules.bipbip import BipBip

import pytest

def test_bipbip_void():
    # Leon needs of job id
    bipbip = BipBip(None)
    bipbip.run()
    print(bipbip.exit_code)
    assert bipbip.exit_code == 1
