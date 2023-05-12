# -*- coding: utf-8 -*-
import os
import time

from oar.lib.fixture import load_fixtures

if __name__ == "__main__":
    # populate database
    here = os.path.abspath(os.path.dirname(__file__))
    filename = os.path.join(here, "dataset_1.json")
    # load_fixtures(db, filename, ref_time=int(time.time()), clear=True)
