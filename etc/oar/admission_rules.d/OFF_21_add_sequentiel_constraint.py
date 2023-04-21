# Title: add sequentiel constraint
# Description: add sequentiel constraint on jobs requesting less or equal to one node and having a large walltime

human_seq_walltime = "24:00:00"
seq_walltime = sql_to_duration(human_seq_walltime)

if project != "admin" and queue != "besteffort":
    e = estimate_job_nb_resources(session, config, resource_request, properties)
    if e[1]:
        for nb_res_walltime in e[2]:
            nb_res, walltime = nb_res_walltime
            if (nb_res < 12) and (walltime > seq_walltime):
                print(
                    "[ADMISSION RULE] Job is less than 2 nodes and has a walltime > {}".format(
                        human_seq_walltime
                    )
                )
                print("[ADMISSION RULE] so adding sequentiel='YES' constraint")
                if properties:
                    properties = "({}) AND  AND sequentiel = 'YES'".format(properties)
                else:
                    properties = "sequentiel = 'YES'"
                break
