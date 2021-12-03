# Specify the default walltime if it is not specified
default_walltime = sql_to_duration("2:00:00")

for i, mold in enumerate(resource_request):
    if not mold[1]:
        print("[ADMISSION RULE] Set default walltime to {}.".format(default_walltime))
        resource_request[i] = (mold[0], default_walltime)
