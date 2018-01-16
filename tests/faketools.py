# coding: utf-8
from oar.lib.tools import TimeoutExpired

fake_popen = {'wait_return_code': 0, 'exception': None}
class FakePopen(object):
    def __init__(self, cmd):
        self.cmd = cmd
        self.pid = 111

    def wait(self, timeout):
        print(timeout)
        #import pdb; pdb.set_trace()
        if fake_popen['exception']:
            if fake_popen['exception'] == 'OSError':
                raise OSError
            elif fake_popen['exception'] == 'TimeoutExpired':
                raise TimeoutExpired(cmd=self.cmd, timeout=timeout)
        return(fake_popen['wait_return_code'])

fake_process = {'is_alive': True}
class FakeProcess(object):
    def __init__(self, **kargs):
        self.target = kargs['target']
        self.kwargs = kargs['kwargs']
        
    def start(self):
        self.target(**self.kwargs)
        
    def join(self):
        pass
    def is_alive(self):
        return fake_process['is_alive']

fake_called_command = {'cmd': None, 'exit_value': 0}
def fake_call(cmd):
    fake_called_command['cmd'] = cmd
    if isinstance(fake_called_command['exit_value'], list):
        exit_value = fake_called_command['exit_value'].pop()
    else:
        exit_value = fake_called_command['exit_value']
    return exit_value
