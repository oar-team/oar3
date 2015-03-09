from oar.kao.simsim import ResourceSetSimu, JobSimu, SimSched, gene_jobsim_sub_time

swf_wkld = SWFWorkload("gofree_sample.swf")

print swf_wkld.gene_jobsim_sub_time(100, 500, 1000)
