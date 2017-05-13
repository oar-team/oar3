# -*- coding: utf-8 -*-
from __future__ import division
from . import Blueprint
from ..utils import Arg

app = Blueprint('config', __name__, url_prefix='/config')
