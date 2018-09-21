# Prevent deploy type jobs on non-entire nodes
bad_resources = ('core', 'cpu', 'thread', 'resource_id')

if 'deploy' in types:
    for mold in resource_request:
        for resource_desc_lst in mold[0]:
            for res_type_val in resource_desc_lst['resources']:
                if res_type_val['resource'] in bad_resources:
                    raise Exception('[ADMISSION RULE] {} resource is not allowed with a deploy job.'\
                                    .format(res_type_val['resource']))
