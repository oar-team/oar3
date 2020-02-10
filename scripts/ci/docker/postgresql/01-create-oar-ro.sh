#!/bin/bash

psql -v --username "$POSTGRES_USER" --dbname postgres --set oar_ro_user="$POSTGRES_USER_RO" --set oar_ro_pass="$POSTGRES_PASSWORD" <<-'EOSQL'
    CREATE USER :oar_ro_user WITH PASSWORD :'oar_ro_pass';
EOSQL


psql -v --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" --set oar_ro_user="$POSTGRES_USER_RO" <<-'EOSQL'
    ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO :oar_ro_user;
EOSQL
