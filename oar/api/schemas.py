# Using locate to cast the lexical representation of the python type into the actual type: https://stackoverflow.com/a/29831586
from pydoc import locate

from pydantic import BaseModel, create_model


class ResourceSchema(BaseModel):
    id: int

    class Config:
        orm_mode = True


attributes = {"type": (str, ...), "network_address": (locate("str"), ...)}

# The resources can be defined differently depending on the target platform, thus we need to create
# the schema(/model) dynamically.
DynamicResourceSchema = create_model(
    "DynamicResourceSchema",
    scheduler_priority=(str, ...),
    **attributes,
    __base__=ResourceSchema
)


class JobSchema(BaseModel):
    id: int

    class Config:
        orm_mode = True
