# -*- coding: utf-8 -*-
from . import Blueprint
from ..utils import Arg

app = Blueprint("config", __name__, url_prefix="/config")
