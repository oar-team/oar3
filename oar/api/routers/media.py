# -*- coding: utf-8 -*-
"""
oar.rest_api.views.media
~~~~~~~~~~~~~~~~~~~~~~~~

Define media (aka file access) api interaction

"""
import os
import re
from typing import Optional

from fastapi import (
    APIRouter,
    Depends,
    File,
    HTTPException,
    Request,
    Response,
    UploadFile,
)

import oar.lib.tools as tools
from oar.lib import config
from oar.lib.tools import PIPE

from ..dependencies import need_authentication
from ..url_utils import list_paginate
from . import TimestampRoute

router = APIRouter(
    route_class=TimestampRoute,
    prefix="/media",
    tags=["media"],
    responses={404: {"description": "Not found"}},
)

if "OARDIR" not in os.environ:
    os.environ["OARDIR"] = "/usr/local/lib/oar"

OARDODO_CMD = os.environ["OARDIR"] + "/oardodo/oardodo"
if "OARDODO" in config:
    OARDODO_CMD = config["OARDODO"]


def user_and_filename_setup(user, path_filename):
    # user setup
    oar_user_env = os.environ.copy()
    oar_user_env["OARDO_BECOME_USER"] = user

    # Security escaping
    path_filename = re.sub(r"([$,`, ])", r"\\\1", path_filename)

    # $path =~ s/(\\*)(`|\$)/$1$1\\$2/g;

    # Get the path and replace "~" by the home directory
    pw_dir = tools.getpwnam(user).pw_dir
    path_filename = "/" + path_filename
    path_filename.replace("~", pw_dir)

    return path_filename, oar_user_env


# @app.route("/ls/<path:path>", methods=["GET"])
# @app.args({"offset": Arg(int, default=0), "limit": Arg(int)})
@router.get("/ls/{path}")
def ls(
    limit: int = 25,
    offset: int = 0,
    path: str = "~",
    user: str = Depends(need_authentication),
):
    # import pdb; pdb.set_trace()
    path, env = user_and_filename_setup(user, path)

    # Check directory's existence
    retcode = tools.call([OARDODO_CMD, "test", "-d", path], env=env)
    if retcode:
        raise HTTPException(status_code=404, detail="Path not found: {}".format(path))

    # Check directory's readability
    retcode = tools.call([OARDODO_CMD, "test", "-r", path], env=env)
    if retcode:
        raise HTTPException(
            status_code=403, detail="File could not be read: {}".format(path)
        )

    # Check if it's a directory
    file_listing = tools.check_output([OARDODO_CMD, "ls"], env=env).decode().split("\n")

    files_with_path = [path + "/" + filename for filename in file_listing[:-1]]

    # Get the listing stat -c "%f_%s_%Y_%F_%n"
    ls_results = (
        tools.check_output(
            [OARDODO_CMD, "stat", "-c", "%f_%s_%Y_%F"] + files_with_path, env=env
        )
        .decode()
        .split("\n")
    )

    file_stats = []
    for i, ls_res in enumerate(ls_results[:-1]):
        f_hex_mode, f_size, f_mtime, f_type = ls_res.split("_")
        file_stats.append(
            {
                "name": file_listing[i],
                "mode": int(f_hex_mode, 16),
                "size": int(f_size),
                "mtime": int(f_mtime),
                "type": f_type,
            }
        )

    list_paginated = list_paginate(file_stats, offset, limit)

    data = {}
    data["total"] = len(list_paginated)
    data["links"] = [{"rel": "rel", "href": "/media/ls/{path}".format(path=path)}]
    data["offset"] = offset
    data["items"] = list_paginated

    return data


@router.get("/")
def get_file(
    path_filename: str,
    tail: Optional[int] = None,
    user: str = Depends(need_authentication),
):

    path_filename, env = user_and_filename_setup(user, path_filename)

    # Check file's existence
    retcode = tools.call([OARDODO_CMD, "test", "-f", path_filename], env=env)
    if retcode:
        raise HTTPException(
            status_code=404, detail="File not found: {}".format(path_filename)
        )

    # Check file's readability
    retcode = tools.call([OARDODO_CMD, "test", "-r", path_filename], env=env)
    if retcode:
        raise HTTPException(
            status_code=403, detail="File could not be read: {}".format(path_filename)
        )

    file_content = None
    if tail:
        file_content = tools.check_output(
            [OARDODO_CMD, "tail", "-n", str(tail), path_filename], env=env
        )
    else:
        file_content = tools.check_output([OARDODO_CMD, "cat", path_filename], env=env)

    return Response(file_content, media_type="application/octet-stream")


@router.post("/chmod")
def chmod(path_filename: str, mode: str, user: str = Depends(need_authentication)):
    path_filename, env = user_and_filename_setup(user, path_filename)
    # Check file's existence
    retcode = tools.call([OARDODO_CMD, "test", "-e", path_filename], env=env)
    if retcode:
        raise HTTPException(
            status_code=404,
            detail="File not found: {}".format(path_filename),
        )

    # Security checking
    if not mode.isalnum():
        raise HTTPException(
            status_code=400,
            detail="Bad mode value: {}".format(mode),
        )

    # Do the chmod
    retcode = tools.call([OARDODO_CMD, "chmod", mode, path_filename], env=env)
    if retcode:
        raise HTTPException(
            status_code=500,
            detail="Could not set mode {} on file {}".format(mode, path_filename),
        )

    return Response(None, status_code=202)


@router.api_route("/", methods=["POST", "PUT"])
def post_file(
    request: Request,
    file: UploadFile = File(...),
    force: Optional[bool] = False,
    user: str = Depends(need_authentication),
):

    path_filename, env = user_and_filename_setup(user, file.filename)
    # Check file's existence
    if not force:
        retcode = tools.call([OARDODO_CMD, "test", "-f", path_filename], env=env)
        if not retcode:
            raise HTTPException(
                status_code=403,
                detail="The file already exists: {}".format(path_filename),
            )

    cmd = [OARDODO_CMD, "bash", "--noprofile", "--norc", "-c", "cat > " + path_filename]

    if request.headers["Content-Type"] == "application/octet-stream":
        p = tools.Popen(cmd, env=env, stdin=PIPE)
        try:
            p.communicate(request.data)
        except Exception as ex:
            p.kill()
            raise HTTPException(
                status_code=501,
                detail=str(ex),
            )
    else:
        if file.filename == "":
            raise HTTPException(
                status_code=400,
                detail="No selected file",
            )
        try:
            p = tools.Popen(cmd, env=env, stdin=file)
        except Exception as ex:
            p.kill()
            raise HTTPException(
                status_code=501,
                detail=str(ex),
            )

    data = {}
    data["links"] = [{"rel": "rel", "href": "/media/" + path_filename}]
    data["status"] = "created"
    data["success"] = "true"

    return data


@router.delete("/")
def delete(path_filename: str, user: str = Depends(need_authentication)):
    path_filename, env = user_and_filename_setup(user, path_filename)

    # Check file's existence
    retcode = tools.call([OARDODO_CMD, "test", "-e", path_filename], env=env)
    if retcode:
        raise HTTPException(
            status_code=404,
            detail="File not found: {}".format(path_filename),
        )

    # Check file write permission
    retcode = tools.call([OARDODO_CMD, "test", "-w", path_filename], env=env)
    if retcode:
        raise HTTPException(
            status_code=403,
            detail="File or directory is not writeable: {}".format(path_filename),
        )

    # Delete the file
    retcode = tools.call([OARDODO_CMD, "rm", "-rf", path_filename], env=env)
    if retcode:
        raise HTTPException(
            status_code=501,
            detail="File unkown error, rm -rf failed for : {}".format(path_filename),
        )

    return Response(None, status_code=204)
