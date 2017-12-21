# coding: utf-8
import pytest

from click.testing import CliRunner

from oar.lib import db
from oar.cli.oarstat import cli
from oar.lib.job_handling import insert_job

NB_JOBS=5

def test_oarstat_simple():
    for _ in range(NB_JOBS):
        insert_job(res=[(60, [('resource_id=4', "")])], properties="")
    runner = CliRunner()
    result = runner.invoke(cli)
    nb_lines = len(result.output_bytes.decode().split('\n'))
    print(result.output_bytes.decode())
    assert nb_lines == NB_JOBS + 3
    assert result.exit_code == 0
