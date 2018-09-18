# Check presence of bad resource type 
bad_resources = ('type', 'state', 'next_state', 'finaud_decision',
                 'next_finaud_decision', 'state_num', 'suspended_jobs',
                 'besteffort', 'deploy', 'expiry_date', 'desktop_computing',
                 'last_job_date', 'available_upto', 'scheduler_priority')

for mold in resource_request:
    for resource_desc_lst in mold[0]:
        for res_type_val in resource_desc_lst['resources']:
            if res_type_val['resource'] in bad_resources:
                raise Exception('# ADMISSION RULE> {} as resource type is not allowed.'\
                                .format(res_type_val['resource']))
