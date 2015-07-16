import pytest

from . import DEFAULT_CONFIG


@pytest.fixture(scope="module", autouse=True)
def config(request):
    # Create the tables based on the current model
    from oar.lib import config
    config.clear()
    config.update(DEFAULT_CONFIG.copy())
    return config
