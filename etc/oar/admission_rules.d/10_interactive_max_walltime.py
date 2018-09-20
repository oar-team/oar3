# Control walltime for interactive job
max_walltime = sql_to_duration('12:00:00')

if (job_type == 'INTERACTIVE') and (reservation_field == 'None'):
    for i, mold in enumerate(resource_request):
        print(i,mold)
        if max_walltime < mold[1]:
            print('[ADMISSION RULE] Walltime to big for an INTERACTIVE job so it is set to {}.'\
                  .format(max_walltime))
            # resource_request[i] is a tuple (which is immutable), it must be replaced
            resource_request[i] = (mold[0], max_walltime)
            
