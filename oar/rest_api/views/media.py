# -*- coding: utf-8 -*-
from __future__ import division
from . import Blueprint
from ..utils import Arg

app = Blueprint('media', __name__, url_prefix='/media')
