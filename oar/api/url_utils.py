from urllib.parse import parse_qs, urlencode, urlsplit


def replace_query_params(url: str, params: dict) -> str:
    """
    TODO
    """
    _url = urlsplit(url)
    _query = parse_qs(_url.query)
    _query.update(params)
    querystr = urlencode(_query, doseq=True)
    return _url._replace(query=querystr).geturl()
