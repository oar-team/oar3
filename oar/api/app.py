import os

from fastapi import Depends, FastAPI, HTTPException, Request, Response
from sqlalchemy.orm import scoped_session, sessionmaker

from oar.lib.globals import get_logger, init_config, init_oar

from .query import APIQuery, APIQueryCollection
from .routers import frontend, job, media, proxy, resource, stress_factor

# from oar import VERSION
# from oar.lib import config, db


# from oar.api import API_VERSION

default_config = {
    "API_TRUST_IDENT": 1,
    "API_DEFAULT_DATA_STRUCTURE": "simple",
    "API_DEFAULT_MAX_ITEMS_NUMBER": 500,
    "API_ABSOLUTE_URIS": 1,
}


class WSGIProxyFix(object):
    def __init__(self, app, config):
        self.app = app
        self.config = config

    def __call__(self, scope, receive, send):
        user = os.environ.get("AUTHENTICATE_UID", None)
        # Activate only for http requests
        if "http" == scope["type"]:
            if user is not None:
                scope["USER"] = user
            else:
                if self.config.get("API_TRUST_IDENT", 0) == 1:
                    # Create a request object to facilitate access to headers
                    request = Request(scope, receive=receive)

                    user = request.headers.get("x-remote-ident", None)
                    if user not in ("", "unknown", "(null)"):
                        scope["USER"] = user

        return self.app(scope, receive, send)


def create_app(config=None, engine=None):
    """Return the OAR API application instance."""
    app = FastAPI()

    if not config:
        config = init_config()

    if engine is None:
        config, engine, _ = init_oar(config=config)

    session_factory = sessionmaker(bind=engine)
    # session_factory.configure(**{"query_cls": APIQuery})
    scoped = scoped_session(session_factory)
    # session = scoped()

    # db.query_class = APIQuery
    # db.query_collection_class = APIQueryCollection

    config.setdefault_config(default_config)

    app.include_router(frontend.router)
    app.include_router(resource.router)
    app.include_router(job.router)
    app.include_router(proxy.router)
    app.include_router(media.router)
    app.include_router(stress_factor.router)

    @app.middleware("http")
    async def reflect_database(request: Request, call_next):
        # db.reflect()
        # Calls next middleware
        response = await call_next(request)
        return response

    @app.middleware("http")
    async def db_session_middleware(request: Request, call_next):
        response = Response("Internal server error", status_code=500)
        try:
            request.state.db = scoped()
            request.state.config = config
            response = await call_next(request)
        finally:
            # FIXME: closing the session here causes the ephemeral session to remove all the data
            # leading to breaking tests
            # request.state.db.close()
            pass
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
        # db.session.remove()
        pass

    app.add_middleware(WSGIProxyFix, config=config)
    return app


# app = create_app()
