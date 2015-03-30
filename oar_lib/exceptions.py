# -*- coding: utf-8 -*-


class OARException(Exception):
    pass


class InvalidConfiguration(OARException):
    pass


class DatabaseError(OARException):
    pass


class DoesNotExist(Exception):
    pass
