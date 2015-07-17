# coding: utf-8
from __future__ import unicode_literals, print_function

from oar.kao.simsim import (ResourceSetSimu, SimSched, SWFWorkload)

swf_wkld = SWFWorkload("gofree_sample.swf")

print(swf_wkld.gene_jobsim_sub_time(100, 500, 1000))
