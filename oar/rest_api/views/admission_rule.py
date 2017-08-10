# -*- coding: utf-8 -*-
from . import Blueprint
from ..utils import Arg

app = Blueprint('admission_rules', __name__, url_prefix='/admission_rules')
