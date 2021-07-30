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


@router.get("/", response_model=List[schemas.DynamicResourceSchema])
def get_resources(offset: int = 0, limit: int = 100):
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
