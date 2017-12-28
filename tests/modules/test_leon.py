# coding: utf-8
from oar.modules.leon import Leon

import pytest

def test_leon_void():
    # Leon needs of job id
    leon = Leon()
    leon.run()
    print(leon.exit_code)
    assert leon.exit_code == 0
