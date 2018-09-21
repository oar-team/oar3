# Force deploy jobs to go on resources with the deploy property
if 'deploy' in types:
    if properties:
        properties = "({}) AND deploy = 'YES'".format(properties)   
    else:
        properties = "deploy = 'YES'"
    print('[ADMISSION RULE] Automatically add the deploy constraint on the resources')
