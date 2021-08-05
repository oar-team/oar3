from typing import List, Optional

from fastapi import APIRouter, Request

from oar.lib import Job, db

from ..url_utils import replace_query_params

router = APIRouter(
    prefix="/jobs",
    tags=["jobs"],
    responses={404: {"description": "Not found"}},
)


def attach_links(job):
    rel_map = (
        ("show", "self", "show"),
        # ("nodes", "collection", "nodes"),
        # ("resources", "collection", "index"),
    )
    job["links"] = []
    for title, rel, endpoint in rel_map:
        url = replace_query_params(
            router.url_path_for(endpoint, job_id=job["id"]), params={}
        )
        # url = url_for("%s.%s" % (app.name, endpoint), job_id=job["id"])
        job["links"].append({"rel": rel, "title": title, "href": url})


@router.get("/")
def index(
    request: Request,
    user: str = None,
    start_time: int = 0,
    stop_time: int = 0,
    states: List[str] = [],
    array_id: int = None,
    job_ids: List[int] = [],
    details: str = None,
    offset: int = 0,
    limit: int = 500,
):
    # import pdb; pdb.set_trace()
    query = db.queries.get_jobs_for_user(
        user, start_time, stop_time, states, job_ids, array_id, None, details
    )
    data = {}
    page = query.paginate(request, offset, limit)
    print(page)
    data["total"] = page.total
    data["links"] = page.links
    data["offset"] = offset
    data["items"] = []
    if details:
        # TODO
        # jobs_resources = db.queries.get_assigned_jobs_resources(page.items)
        pass
    for item in page:
        attach_links(item)
        # if details:
        #     # attach_resources(item, jobs_resources)
        #     # attach_nodes(item, jobs_resources)
        data["items"].append(item)
    return data


@router.get("/{job_id}")
def show(job_id: int, detailed: Optional[str] = None):
    job = db.query(Job).get_or_404(job_id)
    data = job.asdict()

    if detailed:
        job = Job()
        job.id = job_id
        # job_resources = db.queries.get_assigned_jobs_resources([job])
        # attach_resources(data, job_resources)
        # attach_nodes(data, job_resources)

    attach_links(data)
    return data


@router.get("/{job_id}/resources")
def get_resources(
    request: Request,
    job_id: int,
    offset: int = 0,
    limit: int = 500,
):
    print("get_reso")
    job = Job()
    job.id = job_id
    query = db.queries.get_assigned_one_job_resources([job])
    page = query.paginate(request, offset, limit)
    data = {}
    data["total"] = page.total
    data["links"] = page.links
    data["offset"] = offset
    data["items"] = []
    for item in page:
        attach_links(item)
        data["items"].append(item)
