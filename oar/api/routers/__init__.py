import json
import time
from typing import Callable

from fastapi import Request, Response
from fastapi.routing import APIRoute


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

            if (
                "content-type" in response.headers
                and response.headers["content-type"] == "application/json"
            ):
                data = json.loads(response.body)
                # print(data)
                data["api_timestamp"] = timestamp
                data["api_timezone"] = "UTC"
                response.body = json.dumps(data).encode("utf-8")

                # Update the content-length
                response.headers["content-length"] = str(len(response.body))

            return response

        return custom_route_handler
