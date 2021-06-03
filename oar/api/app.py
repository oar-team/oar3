from fastapi import FastAPI

from .routers import frontend, resource

# from oar import VERSION
# from oar.lib import config

# from oar.api import API_VERSION


app = FastAPI()

# @app.middleware("http")
# async def add_user_if_any(request: Request, call_next):
#     response = await call_next(request)
#     return response

app.include_router(frontend.router)
app.include_router(resource.router)

# @app.get("/")
# async def root():
#    return {"api_version": API_VERSION, "oar_version": VERSION}
