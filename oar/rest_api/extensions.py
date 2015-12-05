# -*- coding: utf-8 -*-
from __future__ import division, unicode_literals

from werkzeug.routing import BaseConverter


class RegexConverter(BaseConverter):
    def __init__(self, url_map, *items):
        super(RegexConverter, self).__init__(url_map)
        self.regex = items[0]


def register_extensions(app):
        # Use the RegexConverter function as a converter
    # method for mapped urls
    app.url_map.converters['regex'] = RegexConverter
