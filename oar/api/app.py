import os

from fastapi import FastAPI, Request

# from oar import VERSION
from oar.lib import config, db

from .query import APIQuery, APIQueryCollection
from .routers import frontend, job, resource

# from oar.api import API_VERSION

default_config = {
    "API_TRUST_IDENT": 1,
    "API_DEFAULT_DATA_STRUCTURE": "simple",
    "API_DEFAULT_MAX_ITEMS_NUMBER": 500,
    "API_ABSOLUTE_URIS": 1,
}


class WSGIProxyFix(object):
    def __init__(self, app):
        self.app = app

    def __call__(self, scope, receive, send):
        user = os.environ.get("AUTHENTICATE_UID", None)
        # Activate only for http requests
        if "http" == scope["type"]:
            if user is not None:
                scope["USER"] = user
            else:
                if config.get("API_TRUST_IDENT", 0) == 1:
                    # Create a request object to facilitate access to headers
                    print("type", scope["type"])
                    request = Request(scope, receive=receive)

                    user = request.headers.get("x_remote_ident", None)
                    if user not in ("", "unknown", "(null)"):
                        scope["USER"] = user

        return self.app(scope, receive, send)


def create_app():
    """Return the OAR API application instance."""
    app = FastAPI()
    db.query_class = APIQuery
    db.query_collection_class = APIQueryCollection
    config.setdefault_config(default_config)

    app.include_router(frontend.router)
    app.include_router(resource.router)
    app.include_router(job.router)

    @app.middleware("http")
    async def reflect_database(request: Request, call_next):
        db.reflect()
        # Calls next middleware
        response = await call_next(request)
        return response

    @app.middleware("http")
    async def authenticate(request: Request, call_next):
        current_user = request.scope.get("USER", None)
        if current_user is not None:
            os.environ["OARDO_USER"] = current_user
        else:
            if "OARDO_USER" in os.environ:
                del os.environ["OARDO_USER"]

        # Calls next middleware
        response = await call_next(request)
        return response

    @app.on_event("shutdown")
    def shutdown_db_session():
        # Calls next middleware
        db.session.remove()

    app.add_middleware(WSGIProxyFix)
    return app


app = create_app()
