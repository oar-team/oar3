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
