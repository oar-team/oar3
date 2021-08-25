from typing import Optional

from fastapi import APIRouter, Depends  # ,Request, Header, Depends

from oar import VERSION

from .. import API_VERSION
from ..dependencies import get_user
from . import TimestampRoute

# from oar.lib import config


router = APIRouter(
    # prefix="/",
    route_class=TimestampRoute,
    tags=["frontend"],
    responses={404: {"description": "Not found"}},
)


@router.get("/")
async def root():
    return {"api_version": API_VERSION, "oar_version": VERSION}


@router.get(
    "/whoami",
)
async def whoami(user: Optional[str] = Depends(get_user)):
    return {"user": user}
