from fastapi import APIRouter  # , Depends

from oar.lib import Resource, db

yop = None


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


@router.get("/")
def get_resources(offset: int = 0, limit: int = 100):
    # detailed = "full"
    # resources = db.queries.get_resources(None, detailed)
    # import pdb; pdb.set_trace()
    resources = db.query(Resource).offset(offset).limit(limit).all()
    # print(resources)
    return resources


@router.get("/{resource_id}")
def get_resource(resource_id: int):
    # resource = db.query(Resource).offset(offset).limit(limit).all()
    resource = Resource.query.get_or_404(resource_id)
    # print(resources)
    return resource
