# -*- coding: utf-8 -*-
from ..utils import Arg
from . import Blueprint

app = Blueprint("config", __name__, url_prefix="/config")
