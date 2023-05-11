# coding: utf-8
from oar.lib.tools import TimeoutExpired

fake_popen = {"cmd": None, "wait_return_code": 0, "exception": None}


class FakePopen(object):
    def __init__(self, cmd, env={}, stdout=None, stderr=None, shell=True):
        print("Command: {}".format(cmd))
        print("Env: {}".format(env))

        fake_popen["cmd"] = cmd
        fake_popen["env"] = env

        self.cmd = cmd
        self.pid = 111

    def wait(self, timeout=None):
        print(timeout)
        # import pdb; pdb.set_trace()
        if fake_popen["exception"]:
            if fake_popen["exception"] == "OSError":
                raise OSError
            elif fake_popen["exception"] == "TimeoutExpired":
                raise TimeoutExpired(cmd=self.cmd, timeout=timeout)

        if isinstance(fake_popen["wait_return_code"], list):
            exit_value = fake_popen["wait_return_code"].pop()
        else:
            exit_value = fake_popen["wait_return_code"]

        return exit_value

    def communicate(self):
        return (b"", b"")


fake_process = {"is_alive": True}


class FakeProcess(object):
    def __init__(self, **kargs):
        self.target = kargs["target"]
        self.kwargs = kargs["kwargs"]
        self.args = kargs["args"]

    def start(self):
        print(self.args)
        print(self.kwargs)
        self.target(*self.args,**self.kwargs)
        # self.target(*self.args)

    def join(self):
        pass

    def is_alive(self):
        return fake_process["is_alive"]


fake_called_command = {"cmd": None, "exit_value": 0}


def fake_call(cmd):
    fake_called_command["cmd"] = cmd
    if isinstance(fake_called_command["exit_value"], list):
        exit_value = fake_called_command["exit_value"].pop()
    else:
        exit_value = fake_called_command["exit_value"]
    return exit_value


def fake_kill(pid, signal):
    pass


fake_date = 0


def set_fake_date(date):
    global fake_date
    fake_date = date


def fake_get_date():
    return fake_date
