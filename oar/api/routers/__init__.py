import json
import time
from typing import Callable

from fastapi import Request, Response
from fastapi.routing import APIRoute

from oar.lib import logger


class TimestampRoute(APIRoute):
    """Route implementation that waits for the handler to
    finish and add the timestamp and the timezone to the request.
    This is  mainly to reproduce the behavior of the old rest api (made with flask).
    """

    def get_route_handler(self) -> Callable:
        original_route_handler = super().get_route_handler()

        async def custom_route_handler(request: Request) -> Response:
            timestamp = time.time()

            response = await original_route_handler(request)

            try:
                data = json.loads(response.body)
                data["api_timestamp"] = timestamp
                data["api_timezone"] = "UTC"
                response.body = json.dumps(data)
            except Exception as e:
                # The body is not valid json, we left it unchanged
                logger.warning("Could not interpret response body as JSON", e)

            return response

        return custom_route_handler
