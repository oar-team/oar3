from oar import config, Resource
from hierarchy import Hierarchy
from intervals import ordered_ids2itvs

MAX_NB_RESOURCES = 100000

for array import *

class ResourceSet:
    
    def __init__(self):

        #prepare resource order/indirection stuff
        order_by_clause = config["SCHEDULER_RESOURCE_ORDER"]
        self.rid_i2o = array("i", [0] * MAX_NB_RESOURCES)
        self.rid_o2i = array("i", [0] * MAX_NB_RESOURCES)

        #prepare hierachy stuff
        #"SCHEDULER_PRIORITY_HIERARCHY_ORDER="/host/cpu/core/"
        conf_hy_ordered_labels = config["SCHEDULER_PRIORITY_HIERARCHY_ORDER"]
        hy_ordered_labels = [i for i in conf_hy_ordered_labels.split("/") if i != '' ]

        hy_rid = {}
        for hy_label in hy_ordered_labels:
            hy_rid[hy_label] = {} 


        # available_upto for pseudo job in slot
        available_upto = {}
        self.available_upto = {}


        roids = [] 

        #retreive resource in order from DB
        self.resources_db = Resource.query.order_by(order_by_clause).all()

        #fill the different structures
        for roid, r in enumarate(self.resources_db):
            if (r.state == "Alive") or (r.state == "Absent"):
                roid = int(r.id)
                roids.append(riod)
                self.rid_i2o[rid] = roid
                self.rid_o2i[oid] = roid

                #fill hy_rid structure
                for hy_label in hy_ordered_labels:
                    v = getattr(r,hy_label)
                    if v:
                        if hy_rid[hy_label][v]:
                            hy_rid[hy_label][v].append(roid)
                        else:
                            hy_rid[hy_label][v] = []

                #fill available_upto structure
                if available_upto[r.available_upto]:
                    available_upto[r.available_upto].append(rid)
                else:
                    available_upto[r.available_upto] = [rid]
            
            #global ordered resources intervals
            self.rid_itvs = ordered_ids2itvs(roids)

            #create hierarchy
            self.hierarchy = Hierarchy(hy_rid).hy

            #transform available_upto
            for k, v in available_upto.iteritems():
                self.available_upto[k] = ordered_ids2itvs(v)
