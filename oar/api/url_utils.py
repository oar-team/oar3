from urllib.parse import parse_qs, urlencode, urlsplit

from fastapi import HTTPException


def replace_query_params(url: str, params: dict) -> str:
    """
    TODO
    """
    _url = urlsplit(url)
    _query = parse_qs(_url.query)
    _query.update(params)
    querystr = urlencode(_query, doseq=True)
    return _url._replace(query=querystr).geturl()


def list_paginate(items, offset, limit, error_out=True):
    if error_out and (offset < 0 or offset > len(items)):
        raise HTTPException(status_code=404)

    # TODO
    # if limit is None:
    #     limit = current_app.config.get("API_DEFAULT_MAX_ITEMS_NUMBER")

    if limit is None:
        limit = 25

    items_paginated = items[offset : min(len(items), offset + limit)]

    return items_paginated
