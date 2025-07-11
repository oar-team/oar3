# Create the "estimated_resources" job parameter that can be used
# in further admission rules, and that is used by the scheduler
# (avoid using several times the heavy estimate_job_nb_resources() function)

# estimated_resources = estimate_job_nb_resources(
#    session, config, resource_request, properties
# )
