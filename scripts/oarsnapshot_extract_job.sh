#!/bin/bash
set -eu
JOB_ID=$1
HOST=$2
PORT=$3
USER=$4

# This script extracts the necessary data from an oar database to be able to replay the scheduling for a particular job.
# To make psql avoid prompting for the password, you can set up a .pgpassfile.

DATABASENAME="oar"

FOLDER="${JOB_ID}_OAR"
mkdir -p ${FOLDER}

# Get requested job
psql --host $HOST --port $PORT --user $USER $DATABASENAME << SQL
\COPY ( \
	SELECT * FROM JOBS WHERE job_id = ${JOB_ID} \
) to ${FOLDER}/queued_jobs.csv CSV HEADER
SQL
sed -i -e "s/Error/Waiting/g" -e "s/Terminated/Waiting/g" "${FOLDER}/queued_jobs.csv"

# Get moldable descriptions
psql --host $HOST --port $PORT --user $USER --command "\COPY (SELECT m.* FROM MOLDABLE_JOB_DESCRIPTIONS as m JOIN JOBS as j ON j.job_id = m.moldable_job_id WHERE j.job_id = ${JOB_ID}) to ${FOLDER}/moldable_descriptions.csv CSV HEADER" $DATABASENAME
sed -i -e "s/LOG/CURRENT/g" "${FOLDER}/moldable_descriptions.csv"

# Get resources group
psql --host $HOST --port $PORT --user $USER $DATABASENAME << SQL
\COPY ( \
	SELECT g.* FROM JOB_RESOURCE_GROUPS as g \
	JOIN MOLDABLE_JOB_DESCRIPTIONS as m ON m.moldable_id = g.res_group_moldable_id \
	JOIN JOBS as J ON j.job_id = m.moldable_job_id \
	WHERE j.job_id = ${JOB_ID}) \
to ${FOLDER}/job_resource_groups.csv CSV HEADER
SQL
sed -i -e "s/LOG/CURRENT/g" "${FOLDER}/job_resource_groups.csv"

# Get job resources description
psql --host $HOST --port $PORT --user $USER $DATABASENAME << SQL
\COPY ( \
	SELECT d.* FROM JOB_RESOURCE_DESCRIPTIONS as d \
	JOIN JOB_RESOURCE_GROUPS as g ON g.res_group_id = d.res_job_group_id \
	JOIN MOLDABLE_JOB_DESCRIPTIONS as m ON m.moldable_id = g.res_group_moldable_id \
	JOIN JOBS as J ON j.job_id = m.moldable_job_id \
	WHERE j.job_id = ${JOB_ID}) \
to ${FOLDER}/job_resource_descriptions.csv CSV HEADER
SQL
sed -i -e "s/LOG/CURRENT/g" "${FOLDER}/job_resource_descriptions.csv"

# Get assigned resource (for running jobs)
psql --host $HOST --port $PORT --user $USER --command "\COPY (SELECT a.* FROM ASSIGNED_RESOURCES as a JOIN MOLDABLE_JOB_DESCRIPTIONS as m ON m.moldable_job_id = a.moldable_job_id JOIN JOBS as j ON j.job_id = m.moldable_job_id WHERE j.job_id = ${JOB_ID}) to ${FOLDER}/assigned_resource.csv CSV HEADER" $DATABASENAME
sed -i -e "s/LOG/CURRENT/g" "${FOLDER}/assigned_resource.csv"

tar cvfz ${FOLDER}.roar ${FOLDER}
