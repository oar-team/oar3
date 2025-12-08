ALTER TABLE jobs ADD last_karma real;
CREATE INDEX assigned_moldable_job ON jobs (assigned_moldable_job);

CREATE TYPE job_state as ENUM('Waiting','Hold','toLaunch','toError','toAckReservation','Launching','Running','Suspended','Resuming','Finishing','Terminated','Error');
ALTER TABLE jobs ALTER column state DROP default;
DROP INDEX state;
ALTER TABLE jobs ALTER column state type job_state USING state::job_state;
ALTER TABLE jobs ALTER column state SET default 'Waiting';
CREATE INDEX state ON jobs (state);

CREATE TYPE resource_index AS enum('CURRENT','LOG');
ALTER TABLE assigned_resources ALTER column assigned_resource_index DROP default;
DROP INDEX log;
ALTER TABLE assigned_resources ALTER column assigned_resource_index type resource_index USING assigned_resource_index::resource_index;
ALTER TABLE assigned_resources alter column assigned_resource_index set default 'CURRENT';
CREATE INDEX log on assigned_resources(assigned_resource_index);

-- Update the database schema version
DELETE FROM schema;
INSERT INTO schema(version, name) VALUES ('3.0.0', '');

