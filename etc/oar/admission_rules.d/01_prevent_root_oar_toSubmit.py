# Prevent root and oar to submit jobs.
if (user == 'root') or (user == 'oar'):
    raise Exception('# ADMISSION RULE> root and oar users are not allowed to submit jobs.')
