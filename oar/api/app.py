from fastapi import FastAPI

from oar.lib import db

from .query import APIQuery, APIQueryCollection
from .routers import frontend, job, resource

# from oar import VERSION
# from oar.lib import config

# from oar.api import API_VERSION


def create_app():
    """Return the OAR API application instance."""
    app = FastAPI()
    db.query_class = APIQuery
    db.query_collection_class = APIQueryCollection
    app.include_router(frontend.router)
    app.include_router(resource.router)
    app.include_router(job.router)

    return app


app = create_app()
