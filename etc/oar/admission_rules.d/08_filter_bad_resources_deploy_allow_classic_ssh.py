# Check presence of bad resource type for deplot or allow_classic_ssh job's types
bad_resources = ('core', 'cpu', 'resource_id')


if ('deploy' in types) or ('allow_classic_ssh' in types):
    for mold in resource_request:
        for resource_desc_lst in mold[0]:
            for res_type_val in resource_desc_lst['resources']:
                if res_type_val['resource'] in bad_resources:
                    raise Exception('[ADMISSION RULE] {} as resource type is not allowed with a deploy or allow_classic_ssh type job.'\
                                    .format(res_type_val['resource']))
