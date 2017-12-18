# coding: utf-8
import os
#TODO toremove ?
_ROOT = os.path.abspath(os.path.dirname(__file__))
def get_absolute_script_path(path):
    return os.path.join(_ROOT, path)
