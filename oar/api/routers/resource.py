import json
from typing import Optional

from fastapi import APIRouter, Body, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.orm import Session

import oar.lib.tools as tools
from oar.api.query import APIQueryCollection, paginate
from oar.lib.models import Resource
from oar.lib.resource_handling import (
    get_count_busy_resources,
    remove_resource,
    set_resource_state,
)

from ..auth import need_authentication
from ..dependencies import get_db
from ..url_utils import replace_query_params
from . import TimestampRoute

router = APIRouter(
    route_class=TimestampRoute,
    prefix="/resources",
    tags=["resources"],
    responses={404: {"description": "Not found"}},
)


@router.get("")
@router.get("/")
def index(
    offset: int = 0,
    limit: int = 25,
    detailed: bool = Query(False),
    network_address: Optional[str] = Query(None),
    db: Session = Depends(get_db),
):
    """Replie a comment to the post.

    :param offset: post's unique id
    :type offset: int

    :form email: author email address
    :form body: comment body
    :reqheader Accept: the response content type depends on
                      :mailheader:`Accept` header
    :status 302: and then redirects to :http:get:`/resources/(int:resource_id)`
    :status 400: when form parameters are missing
    """
    queryCollection = APIQueryCollection(db)
    query = queryCollection.get_resources(network_address, detailed)
    page = paginate(query, offset, limit)

    data = {}
    data["total"] = page.total
    data["offset"] = offset
    data["items"] = []
    for item in page:
        data["items"].append(item)

    return data


# @router.get("/{resource_id}", response_model=schemas.DynamicResourceSchema)
# def get_resource(resource_id: int):
#     print("get resources: ", resource_id)
#     print(db.query(Resource).all())
#     resource = db.query(Resource).get(resource_id)
#     return resource


@router.get("/busy")
def busy(db: Session = Depends(get_db)):
    return {"busy": get_count_busy_resources(db)}


@router.get("/{resource_id}")
def show(resource_id, db: Session = Depends(get_db)):
    resource = db.query(Resource).get_or_404(resource_id)
    if resource is None:
        raise HTTPException(status_code=404, detail="Resource not found")
    data = {}
    data.update(resource.asdict())


@router.get("/{resource_id}/jobs")
def jobs(
    limit: int = 50,
    offset: int = 0,
    resource_id: int = None,
    db: Session = Depends(get_db),
):
    queryCollection = APIQueryCollection(db)

    query = queryCollection.get_jobs_resource(resource_id)
    page = paginate(query, offset, limit)
    data = {}
    data["total"] = page.total
    data["offset"] = offset
    data["items"] = []
    for item in page:
        data["items"].append(item)
    return data


class StateParameters(BaseModel):
    state: str = Body(...)


@router.post("/{resource_id}/state")
def state(
    resource_id: int,
    params: StateParameters,
    user: str = Depends(need_authentication),
    db: Session = Depends(get_db),
):
    """POST /resources/<id>/state
    Change the state
    """
    state = params.state
    data = {}
    if (user == "oar") or (user == "root"):
        set_resource_state(db, resource_id, state, "NO")

        tools.notify_almighty("ChState")
        tools.notify_almighty("Term")

        data["id"] = resource_id
        data["uri"] = router.url_path_for("show", resource_id=resource_id)
        data["status"] = "Change state request registered"
    else:
        data["status"] = "Bad user"

    return data


@router.post("")
@router.post("/")
def create(
    hostname: str,
    properties: str,
    user: str = Depends(need_authentication),
    db: Session = Depends(get_db),
):
    """POST /resources"""
    props = json.loads(properties)
    data = {}
    if (user == "oar") or (user == "root"):
        resource_fields = {"network_address": hostname}
        resource_fields.update(props)
        ins = Resource.__table__.insert().values(**resource_fields)
        result = db.execute(ins)
        resource_id = result.inserted_primary_key[0]
        data["id"] = resource_id
        data["uri"] = replace_query_params(
            router.url_path_for("index"), params={"resource_id": resource_id}
        )
        data["status"] = "ok"
    else:
        data["status"] = "Bad user"
    return data


@router.delete("/{resource_id}")
def delete(
    resource_id: int,
    user: str = Depends(need_authentication),
    db: Session = Depends(get_db),
):
    """DELETE /resources/<id>
    Delete the resource identified by *d)
    """
    # TODO: DELETE /resources/<node>/<cpuset_id>
    #
    # if ($id == 0) {
    #  $query="WHERE network_address = \"$node\" AND cpuset = $cpuset";
    # }

    data = {}
    if resource_id:
        error, error_msg = remove_resource(db, resource_id, user)
        data["id"] = resource_id
        if error == 0:
            data["status"] = "Deleted"
        else:
            data["status"] = error_msg
        data["exit_value"] = error

    else:
        data["status"] = "Can not determine resource id"
        data["exit_value"] = 1

    return data
