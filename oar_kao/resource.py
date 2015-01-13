from oar.lib import config, Resource
from oar.kao.hierarchy import Hierarchy
from oar.kao.interval import ordered_ids2itvs
from array import *

MAX_NB_RESOURCES = 100000

class ResourceSet:

    def __init__(self):

        #prepare resource order/indirection stuff
        order_by_clause = config["SCHEDULER_RESOURCE_ORDER"]
        self.rid_i2o = array("i", [0] * MAX_NB_RESOURCES)
        self.rid_o2i = array("i", [0] * MAX_NB_RESOURCES)

        #suspend
        suspendable_roids = []
        if "SCHEDULER_AVAILABLE_SUSPENDED_RESOURCE_TYPE" not in config:
            config["SCHEDULER_AVAILABLE_SUSPENDED_RESOURCE_TYPE"] = "default"
        
        res_suspend_types = (config["SCHEDULER_AVAILABLE_SUSPENDED_RESOURCE_TYPE"]).split()

        #prepare hierarchy stuff
        #"HIERARCHY_LABELS" = "resource_id,network_address"
        conf_hy_labels = config["HIERARCHY_LABELS"] if "HIERARCHY_LABELS" in config else "resource_id,network_address"

        hy_labels = conf_hy_labels.split(",")
        hy_labels_w_id = [ "id" if v == "resource_id" else v for v in hy_labels ]

        hy_roid = {}
        for hy_label in hy_labels_w_id:
            hy_roid[hy_label] = {}

        # available_upto for pseudo job in slot
        available_upto = {}
        self.available_upto = {}

        roids = []

        #retreive resource in order from DB
        self.resources_db = Resource.query.order_by(order_by_clause).all()

        #fill the different structures
        for roid, r in enumerate(self.resources_db):
            if (r.state == "Alive") or (r.state == "Absent"):
                rid = int(r.id)
                roids.append(roid)
                self.rid_i2o[rid] = roid
                self.rid_o2i[roid] = rid

                #fill hy_rid structure
                for hy_label in hy_labels_w_id:
                    v = getattr(r,hy_label)
                    if v:
                        if hy_label not in hy_roid:
                            hy_roid[hy_label] = {}
                        if v in hy_roid[hy_label]:
                            hy_roid[hy_label][v].append(roid)
                        else:
                            hy_roid[hy_label][v] = [roid]

                #fill available_upto structure
                if r.available_upto in available_upto:
                    available_upto[r.available_upto].append(roid)
                else:
                    available_upto[r.available_upto] = [roid]

                #fill resource available for suspended job 
                if r.type in res_suspend_types:
                    suspendable_roids.append(roid)

        #global ordered resources intervals
        #print roids
        self.roid_itvs = ordered_ids2itvs(roids)

        if "id" in hy_roid:
            hy_roid["resource_id"] =  hy_roid["id"]
            del hy_roid["id"]

        #create hierarchy
        self.hierarchy = Hierarchy(hy_rid=hy_roid).hy

        #transform available_upto
        for k, v in available_upto.iteritems():
            self.available_upto[k] = ordered_ids2itvs(v)

        #
        self.suspendable_roid_itvs = ordered_ids2itvs(suspendable_roids)
