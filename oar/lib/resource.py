# coding: utf-8
from array import array
from collections import OrderedDict

from procset import ProcSet
from sqlalchemy import text

from oar.lib.hierarchy import Hierarchy
from oar.lib.models import Resource

MAX_NB_RESOURCES = 100000


class ResourceSet(object):
    default_itvs = ProcSet()

    def __init__(self, session, config):
        self.nb_resources_all = 0
        self.nb_resources_not_dead = 0
        self.nb_resources_default_not_dead = 0

        self.nb_resources = 0
        self.nb_resources_default = 0

        # prepare resource order/indirection stuff
        order_by_clause = config["SCHEDULER_RESOURCE_ORDER"]
        self.rid_i2o = array("i", [0] * MAX_NB_RESOURCES)
        self.rid_o2i = array("i", [0] * MAX_NB_RESOURCES)

        # suspend
        suspendable_roids = []
        if "SCHEDULER_AVAILABLE_SUSPENDED_RESOURCE_TYPE" not in config:
            config["SCHEDULER_AVAILABLE_SUSPENDED_RESOURCE_TYPE"] = "default"

        res_suspend_types = (
            config["SCHEDULER_AVAILABLE_SUSPENDED_RESOURCE_TYPE"]
        ).split()

        # prepare hierarchy stuff
        # "HIERARCHY_LABELS" = "resource_id,network_address"
        conf_hy_labels = (
            config["HIERARCHY_LABELS"]
            if "HIERARCHY_LABELS" in config
            else "resource_id,network_address"
        )

        hy_labels = conf_hy_labels.split(",")
        hy_labels_w_id = ["id" if v == "resource_id" else v for v in hy_labels]

        hy_roid = {}
        for hy_label in hy_labels_w_id:
            hy_roid[hy_label] = OrderedDict()

        # available_upto for pseudo job in slot
        available_upto = {}
        self.available_upto = {}

        roids = []
        default_rids = []

        self.roid_2_network_address = {}

        # retrieve resource in order from DB
        self.resources_db = (
            session.query(Resource).order_by(text(order_by_clause)).all()
        )

        # fill the different structures
        for roid, r in enumerate(self.resources_db):
            self.nb_resources_all += 1
            if r.state != "Dead":
                self.nb_resources_not_dead += 1
                if r.type == "default":
                    self.nb_resources_default_not_dead += 1

            if (r.state == "Alive") or (r.state == "Absent"):
                self.nb_resources += 0
                rid = int(r.id)
                roids.append(roid)
                if r.type == "default":
                    default_rids.append(rid)
                    self.nb_resources_default = +1

                self.rid_i2o[rid] = roid
                self.rid_o2i[roid] = rid

                # fill hy_rid structure
                for hy_label in hy_labels_w_id:
                    v = getattr(r, hy_label)
                    if v in hy_roid[hy_label]:
                        hy_roid[hy_label][v].append(roid)
                    else:
                        hy_roid[hy_label][v] = [roid]

                # fill available_upto structure
                if r.available_upto in available_upto:
                    available_upto[r.available_upto].append(roid)
                else:
                    available_upto[r.available_upto] = [roid]

                # fill resource available for suspended job
                if r.type in res_suspend_types:
                    suspendable_roids.append(roid)

            self.roid_2_network_address[roid] = r.network_address

        # global ordered resources intervals
        # print roids
        self.roid_itvs = ProcSet(*roids)  # TODO

        if "id" in hy_roid:
            hy_roid["resource_id"] = hy_roid["id"]
            del hy_roid["id"]

        # create hierarchy
        self.hierarchy = Hierarchy(hy_rid=hy_roid).hy

        # transform available_upto
        for k, v in available_upto.items():
            self.available_upto[k] = ProcSet(*v)

        #
        self.suspendable_roid_itvs = ProcSet(*suspendable_roids)

        default_roids = [self.rid_i2o[i] for i in default_rids]
        self.default_itvs = ProcSet(*default_roids)
        ResourceSet.default_itvs = self.default_itvs  # for Quotas
