from typing import Optional

from fastapi import Header


def get_user(x_remote_ident: Optional[str] = Header(None)):
    return x_remote_ident
