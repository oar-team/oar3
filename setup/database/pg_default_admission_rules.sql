-- Default admission rules for OAR 2
-- $Id$

-- Specify the default value for queue parameter
INSERT INTO admission_rules (priority, enabled, rule) VALUES (1, 'YES', E'# Set default queue is no queue is set
if queue is None: queue = "default"
');

-- Prevent root and oar to submit jobs.
INSERT INTO admission_rules (priority, enabled, rule) VALUES (2, 'YES', E'# Prevent users oar and root to submit jobs
# Note: do not change this unless you want to break oar !
if (user == "root") or (user == "oar"):
    raise Exception("# ADMISSION RULE> Error: root and oar users are not allowed to submit jobs.")
');

-- Avoid the jobs to go on resources in drain mode
-- Avoid users except admin to go in the admin queue

-- Prevent the use of system properties

-- Force besteffort jobs to run in the besteffort queue
-- Force job of the besteffort queue to be of the besteffort type
-- Force besteffort jobs to run on nodes with the besteffort property
-- Verify if besteffort jobs are not reservations

-- Force deploy jobs to go on resources with the deploy property

-- Prevent deploy type jobs on non-entire nodes

-- Force desktop_computing jobs to go on nodes with the desktop_computing property

-- Limit the number of reservations that a user can do.
-- (overrided on user basis using the file: ~oar/unlimited_reservation.users)

-- Example of how to perform actions given usernames stored in a file

-- Limit walltime for interactive jobs


-- specify the default walltime if it is not specified

-- Check if types given by the user are right

-- If resource types are not specified, then we force them to default


