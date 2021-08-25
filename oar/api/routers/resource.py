from fastapi import APIRouter, HTTPException

from oar.lib import Resource, db

from .. import schemas
from . import TimestampRoute

router = APIRouter(
    route_class=TimestampRoute,
    prefix="/resources",
    tags=["resources"],
    responses={404: {"description": "Not found"}},
)


# Dependency
def get_db():
    try:
        # import pdb
        # pdb.set_trace()
        yield db.session
    finally:
        db.session.close()


def attach_links(resource):
    rel_map = (
        ("node", "member", "resource_index"),
        # ("show", "self", "show"),
        # ("jobs", "collection", "jobs"),
    )
    links = []
    for title, rel, endpoint in rel_map:
        if title == "node" and "network_address" in resource:
            url = router.url_path_for(
                endpoint,
                network_address=resource["network_address"],
            )
            links.append({"rel": rel, "href": url, "title": title})
        elif title != "node" and "id" in resource:
            router.url_path_for(endpoint, resource_id=resource["id"])
            links.append({"rel": rel, "href": url, "title": title})
    resource["links"] = links


@router.get("/")  # , response_model=List[schemas.DynamicResourceSchema])
async def resource_index(offset: int = 0, limit: int = 100):
    # detailed = "full"
    # resources = db.queries.get_resources(None, detailed)
    # import pdb; pdb.set_trace()
    resources = db.query(Resource).offset(offset).limit(limit).all()
    return {"items": resources}


@router.get("/{resource_id}", response_model=schemas.DynamicResourceSchema)
async def get_resource(resource_id: int):
    print("get resources: ", resource_id)
    print(db.query(Resource).all())
    resource = db.query(Resource).get(resource_id)
    if resource is None:
        raise HTTPException(status_code=404, detail="Resource not found")
    return resource
