# coding: utf-8
import pytest
import re

from ..helpers import insert_terminated_jobs

from click.testing import CliRunner

from oar.lib import (db, Resource)
from oar.cli.oarnodesetting import cli

import oar.lib.tools  # for monkeypatching

fake_notifications = []

def fake_notify_almighty(notification):
    global fake_notifications
    fake_notifications.append(notification)

@pytest.yield_fixture(scope='function', autouse=True)
def minimal_db_initialization(request):
    global fake_notifications
    fake_notifications = []
    with db.session(ephemeral=True):
        yield

        
@pytest.fixture(scope='function', autouse=True)
def monkeypatch_tools(request, monkeypatch):
    monkeypatch.setattr(oar.lib.tools, 'notify_almighty', fake_notify_almighty)

    
def test_version():
    runner = CliRunner()
    result = runner.invoke(cli, ['-V'])
    print(result.output)
    assert re.match(r'.*\d\.\d\.\d.*', result.output)

def test_oarnodesetting_simple():
    runner = CliRunner()
    result = runner.invoke(cli, ['-a'])
    resource = db['Resource'].query.one()
    print(resource)
    #nb_lines = len(result.output_bytes.decode().split('\n'))
    #assert nb_lines == NB_LINES_PER_NODE * NB_NODES + 1 # + 1 for last \n
    assert resource.state == 'Alive'
    assert fake_notifications[-2:] == ['ChState', 'Term']
    assert result.exit_code == 0

def test_oarnodesetting_core_cpu():
    runner = CliRunner()
    result = runner.invoke(cli, ['-a', '-p core=1', '-p cpu=2'])
    print(result)
    resource = db['Resource'].query.one()
    print(resource)
    print(fake_notifications)
    #import pdb; pdb.set_trace()
    assert resource.core == 1
    assert resource.cpu == 2
    assert fake_notifications[-3:] == ['Term','ChState', 'Term']
    assert result.exit_code == 0

def test_oarnodesetting_error_1():
    runner = CliRunner()
    result = runner.invoke(cli, ['-r', '1'])
    print(result.output)
    assert result.exit_code == 1
 
def test_oarnodesetting_error_2():
    runner = CliRunner()
    result = runner.invoke(cli, ['-r', '1', '--state', 'Suspected'])
    print(result.output)
    assert result.exit_code == 1
    
def test_oarnodesetting_error_3():
    runner = CliRunner()
    result = runner.invoke(cli, ['-r', '1', '--maintenance', 'midoff'])
    print(result.output)
    assert result.exit_code == 1
    
def test_oarnodesetting_error_4():
    runner = CliRunner()
    result = runner.invoke(cli, ['-r', '1', '--drain', 'midoff'])
    print(result.output)
    assert result.exit_code == 1
    
def test_oarnodesetting_error_5():
    runner = CliRunner()
    result = runner.invoke(cli, ['-r', '1', '--add'])
    print(result.output)
    assert result.exit_code == 1
    
def test_oarnodesetting_sql_drain():
    db['Resource'].create(network_address="localhost")
    runner = CliRunner()
    result = runner.invoke(cli, ['--sql', "state=\'Alive\'", '--drain', 'on'])
    resource = db['Resource'].query.one()
    print(result.output)
    assert resource.drain == 'YES'

def test_oarnodesetting_drain_off():
    db['Resource'].create(network_address="localhost", drain='YES')
    resource = db['Resource'].query.one()
    print(resource.drain)
    runner = CliRunner()
    result = runner.invoke(cli, ['-h', 'localhost', '--drain', 'off'])
    resource = db['Resource'].query.one()
    print(result.output)
    assert resource.drain == 'NO'
    
def test_oarnodesetting_sql_void():
    db['Resource'].create(network_address="localhost")
    runner = CliRunner()
    result = runner.invoke(cli,  ['--sql', "state=\'NotExist\'", '--drain', 'on'])
    resource = db['Resource'].query.one()
    print(result.output)
    assert re.match('.*are no resource.*', result.output)
    assert result.exit_code == 0

def test_oarnodesetting_system_property_error():
    db['Resource'].create(network_address="localhost")
    runner = CliRunner()
    result = runner.invoke(cli,  ['-h', 'localhost', '-p', 'state=Alive', '--drain', 'on'])
    resource = db['Resource'].query.one()
    print(result.output)
    assert result.exit_code == 8
    
def test_oarnodesetting_malformed_property_error():
    db['Resource'].create(network_address="localhost")
    runner = CliRunner()
    result = runner.invoke(cli,  ['-h', 'localhost', '-p', 'state=Ali=ve', '--drain', 'on'])
    resource = db['Resource'].query.one()
    print(result.output)
    assert result.exit_code == 10

def test_oarnodesetting_sql_state():
    for _ in range(2):
        db['Resource'].create(network_address='localhost', state='Absent')
    db.commit()
    runner = CliRunner()
    result = runner.invoke(cli,  ['--sql', "network_address=\'localhost\'", '--state', 'Alive'])
    assert fake_notifications == ['ChState']
    assert result.exit_code == 0
    
def test_oarnodesetting_hosts_state():
    db['Resource'].create(network_address='localhost0', state='Absent')
    db['Resource'].create(network_address='localhost1', state='Absent')
    db.commit()
    runner = CliRunner()
    result = runner.invoke(cli,  ['-h', 'localhost0', '-h', 'localhost1', '--state', 'Alive'])
    print(result.output)
    resources = db['Resource'].query.all()
    assert resources[0].next_state == 'Alive'
    assert fake_notifications == ['ChState']
    assert result.exit_code == 0

def test_oarnodesetting_last_property_value():
    db['Resource'].create(network_address='localhost', core='1')
    db['Resource'].create(network_address='localhost', core='2')
    db.commit()
    runner = CliRunner()
    result = runner.invoke(cli, ['--last-property-value', 'core'])
    print(result.output)
    assert re.match(r'2', result.output)


def test_oarnodesetting_last_property_value_error0():
    db['Resource'].create(network_address='localhost')
    db['Resource'].create(network_address='localhost')
    db.commit()
    runner = CliRunner()
    result = runner.invoke(cli, ['--last-property-value', 'NotExist'])
    print(result.output)
    assert re.match(r'.*retrieve the last value.*', result.output)
    
def todo_test_oarnodesetting_last_property_value_error1():
    db['Resource'].create(network_address='localhost')
    db['Resource'].create(network_address='localhost')
    db.commit()
    runner = CliRunner()
    result = runner.invoke(cli, ['--last-property-value', 'drain'])
    print(result.output)
    assert re.match(r'.*retrieve the last value.*', result.output)

def test_oarnodesetting_maintenance_on_nowait():
    db['Resource'].create(network_address='localhost')
    db.commit()
    last_available_upto = db['Resource'].query.one().last_available_upto
    runner = CliRunner()
    result = runner.invoke(cli,  ['-h', 'localhost', '--maintenance', 'on', '--no-wait'])
    print(result.output)
    print(fake_notifications)
    assert fake_notifications == ['ChState']
    assert last_available_upto != db['Resource'].query.one().last_available_upto
    
def test_oarnodesetting_maintenance_off_nowait():
    db['Resource'].create(network_address='localhost', last_available_upto=12334567)
    db.commit()
    last_available_upto = db['Resource'].query.one().available_upto
    runner = CliRunner()
    result = runner.invoke(cli,  ['-h', 'localhost', '--maintenance', 'off', '--no-wait'])
    print(result.output)
    print(fake_notifications)
    assert fake_notifications == ['ChState']
    assert last_available_upto != db['Resource'].query.one().available_upto

def test_oarnodesetting_maintenance_on_wait():
    for _ in range(10):
        db['Resource'].create(network_address='localhost')
    db.commit()
    resources = db['Resource'].query.order_by(Resource.id).all()
    last_available_upto = resources[0].last_available_upto
    runner = CliRunner()
    result = runner.invoke(cli,  ['-h', 'localhost', '--maintenance', 'on'])
    print(result.output)
    print(fake_notifications)
    assert len(fake_notifications) == 10
    resources = db['Resource'].query.order_by(Resource.id).all()
    assert last_available_upto != resources[0].last_available_upto
