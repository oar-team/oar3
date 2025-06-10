ALTER TABLE jobs ADD last_karma real;
CREATE INDEX assigned_moldable_job ON jobs (assigned_moldable_job);

alter table jobs alter column state drop default;
drop index state;
create TYPE job_state as ENUM('Waiting','Hold','toLaunch','toError','toAckReservation','Launching','Running','Suspended','Resuming','Finishing','Terminated','Error');
alter table jobs alter column state type job_state USING state::job_state;
alter table jobs alter column state set default 'Waiting';
CREATE INDEX state ON jobs (state);

-- Update the database schema version
DELETE FROM schema;
INSERT INTO schema(version, name) VALUES ('3.0.0', '');

