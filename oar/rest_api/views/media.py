# -*- coding: utf-8 -*-
from . import Blueprint
from ..utils import Arg

app = Blueprint('media', __name__, url_prefix='/media')
