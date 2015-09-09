# coding: utf-8
from __future__ import unicode_literals, print_function
#from oar.kao.scheduling 
import oar.kao.scheduling

# assign_resources_mld_job_split_slots, find_resource_hierarchies_job


def find_legacy(itvs_avail, hy_res_rqts, hy):
    """Simple wrap function to default function for test purpose"""
    return oar.kao.scheduling.find_resource_hierarchies_job(itvs_avail, hy_res_rqts, hy)


def assign_legacy(slots_set, job, hy, min_start_time):
    """Simple wrap function to default function for test purpose"""
    return oar.kao.scheduling.assign_resources_mld_job_split_slots(slots_set, job, hy, min_start_time)
