# -*- coding: utf-8 -*-
"""
oar.rest_api.views.resource
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Define resources api interaction

"""

import json

from flask import g, url_for

import oar.lib.tools as tools
from oar.lib import Resource, db
from oar.lib.resource_handling import (
    get_count_busy_resources,
    remove_resource,
    set_resource_state,
)

from ..utils import Arg
from . import Blueprint

app = Blueprint("resources", __name__, url_prefix="/resources")


@app.route("/", methods=["GET"])
@app.route("/<any(details, full):detailed>", methods=["GET"])
@app.route("/nodes/<string:network_address>", methods=["GET"])
@app.args({"offset": Arg(int, default=0), "limit": Arg(int)})
def index(offset, limit, network_address=None, detailed=False):
    """Replie a comment to the post.

    :param offset: post's unique id
    :type offset: int

    :form email: author email address
    :form body: comment body
    :reqheader Accept: the response content type depends on
                      :mailheader:`Accept` header
    :status 302: and then redirects to :http:get:`/resources/(int:resource_id)`
    :status 400: when form parameters are missing
    """
    query = db.queries.get_resources(network_address, detailed)
    page = query.paginate(offset, limit)
    g.data["total"] = page.total
    g.data["links"] = page.links
    g.data["offset"] = offset
    g.data["items"] = []
    for item in page:
        attach_links(item)
        g.data["items"].append(item)


@app.route("/<int:resource_id>", methods=["GET"])
def show(resource_id):
    resource = Resource.query.get_or_404(resource_id)
    g.data.update(resource.asdict())
    attach_links(g.data)


@app.route("/<int:resource_id>/jobs", methods=["GET"])
@app.args({"offset": Arg(int, default=0), "limit": Arg(int)})
def jobs(offset, limit, resource_id=None):
    query = db.queries.get_jobs_resource(resource_id)
    page = query.paginate(offset, limit)
    g.data["total"] = page.total
    g.data["links"] = page.links
    g.data["offset"] = offset
    g.data["items"] = []
    for item in page:
        attach_job(item)
        g.data["items"].append(item)


def attach_links(resource):
    rel_map = (
        ("node", "member", "index"),
        ("show", "self", "show"),
        ("jobs", "collection", "jobs"),
    )
    links = []
    for title, rel, endpoint in rel_map:
        if title == "node" and "network_address" in resource:
            url = url_for(
                "%s.%s" % (app.name, endpoint),
                network_address=resource["network_address"],
            )
            links.append({"rel": rel, "href": url, "title": title})
        elif title != "node" and "id" in resource:
            url = url_for("%s.%s" % (app.name, endpoint), resource_id=resource["id"])
            links.append({"rel": rel, "href": url, "title": title})
    resource["links"] = links


def attach_job(job):
    rel_map = (
        ("show", "self", "show"),
        ("nodes", "collection", "nodes"),
        ("resources", "collection", "resources"),
    )
    job["links"] = []
    for title, rel, endpoint in rel_map:
        url = url_for("%s.%s" % ("jobs", endpoint), job_id=job["id"])
        job["links"].append({"rel": rel, "href": url, "title": title})


@app.route("/", methods=["POST"])
@app.args({"hostname": Arg(str), "properties": Arg(None)})
@app.need_authentication()
def create(hostname, properties):
    """POST /resources"""
    props = json.loads(properties)
    user = g.current_user
    if (user == "oar") or (user == "root"):
        resource_fields = {"network_address": hostname}
        resource_fields.update(props)
        ins = Resource.__table__.insert().values(**resource_fields)
        result = db.session.execute(ins)
        resource_id = result.inserted_primary_key[0]
        g.data["id"] = resource_id
        g.data["uri"] = url_for("%s.%s" % (app.name, "show"), resource_id=resource_id)
        g.data["status"] = "ok"
    else:
        g.data["status"] = "Bad user"


@app.route("/<int:resource_id>/state", methods=["POST"])
@app.args({"state": Arg(str)})
@app.need_authentication()
def state(resource_id, state):
    """POST /resources/<id>/state
    Change the state
    """
    user = g.current_user
    if (user == "oar") or (user == "root"):
        set_resource_state(resource_id, state, "NO")

        tools.notify_almighty("ChState")
        tools.notify_almighty("Term")

        g.data["id"] = resource_id
        g.data["uri"] = url_for("%s.%s" % (app.name, "show"), resource_id=resource_id)
        g.data["status"] = "Change state request registered"
    else:
        g.data["status"] = "Bad user"


@app.route("/<int:resource_id>", methods=["DELETE"])
@app.need_authentication()
def delete(resource_id):
    """DELETE /resources/<id>
    Delete the resource identified by *d)
    """
    # TODO: DELETE /resources/<node>/<cpuset_id>
    #
    # if ($id == 0) {
    #  $query="WHERE network_address = \"$node\" AND cpuset = $cpuset";
    # }

    user = g.current_user

    if resource_id:
        error, error_msg = remove_resource(resource_id, user)
        g.data["id"] = resource_id
        if error == 0:
            g.data["status"] = "Deleted"
        else:
            g.data["status"] = error_msg
        g.data["exit_value"] = error

    else:
        g.data["status"] = "Can not determine resource id"
        g.data["exit_value"] = 1


@app.route("/busy", methods=["GET"])
def busy():
    g.data["busy"] = get_count_busy_resources()
