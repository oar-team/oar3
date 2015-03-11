# -*- coding: utf-8 -*-
from __future__ import with_statement
import pytest

from contextlib import contextmanager


@contextmanager
def assert_raises(exception_class, message_part):
    """
    Check that an exception is raised and its message contains some string.
    """
    with pytest.raises(exception_class) as exception:
        yield
    message = '%s' % exception
    assert message_part.lower() in message.lower()
