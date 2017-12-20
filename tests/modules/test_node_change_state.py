# coding: utf-8
from oar.modules.node_change_state import NodeChangeState

import pytest

def test_node_change_state_void():
    # Leon needs of job id
    node_change_state = NodeChangeState()
    node_change_state.run()
    print(node_change_state.exit_code)
    assert node_change_state.exit_code == 0
