# coding: utf-8
import pytest

from click.testing import CliRunner

from oar.lib import db
from oar.cli.oarproperty import cli

def test_oarpropery_simple():
    runner = CliRunner()
    result = runner.invoke(cli, ['-a core', '-c'])
    print(result)
    assert result.exit_code == 2
