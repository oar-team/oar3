# Filter which users can submit in admin queue  
admin_group = 'admin'
if queue == 'admin':
    import grp
    if user not in grp.getgrnam(admin_group).gr_mem:
        raise Exception('# ADMISSION RULE> Only member of the group {} can submit jobs in the admin queue'.format(admin_group))

    
