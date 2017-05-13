# -*- coding: utf-8 -*-
from __future__ import division
from . import Blueprint
from ..utils import Arg

app = Blueprint('admission_rules', __name__, url_prefix='/admission_rules')
