#!/usr/bin/env python
# coding: utf-8

import sys
import json # (de)serialize json.dumps({'foo': 'bar'}) /  json.loads(_)
from oar.lib import (config, get_logger)

import oar.lib.tools as tools

from oar.lib.tools import DEFAULT_CONFIG

class BipBip(object):

    def __init__(self, args):
        config.setdefault_config(DEFAULT_CONFIG)
        self.exit_code = 0

        self.job_id = args[0]
        self.oarexec_reattach_exit_value = args[1]
        self.oarexec_reattach_script_exit_value = args[2]
        self.oarexec_challenge = args[3]

    def run(self):

        
        pass

def main(args):
    bipbip = BipBip(args)
    bipbip.run()
    return bipbip.exit_code

if __name__ == '__main__':  # pragma: no cover
    if len(sys.argv) != 5:
        # TODO
        sys.exit(1)
    exit_code = main(sys.argv[1:])
    sys.exit(exit_code)
