from typing import List

from fastapi import APIRouter, HTTPException

from oar.lib import Resource, db

from .. import schemas

router = APIRouter(
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


@router.get("/", response_model=List[schemas.DynamicResourceSchema])
def resource_index(offset: int = 0, limit: int = 100):
    # detailed = "full"
    # resources = db.queries.get_resources(None, detailed)
    # import pdb; pdb.set_trace()
    resources = db.query(Resource).offset(offset).limit(limit).all()
    # print(resources)
    return resources


@router.get("/{resource_id}", response_model=schemas.DynamicResourceSchema)
def get_resource(resource_id: int):
    resource = db.query(Resource).get(resource_id)
    if resource is None:
        raise HTTPException(status_code=404, detail="User not found")
    return resource
