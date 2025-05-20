ALTER TABLE jobs ADD last_karma real;
CREATE INDEX assigned_moldable_job ON jobs (assigned_moldable_job);

-- Update the database schema version
DELETE FROM schema;
INSERT INTO schema(version, name) VALUES ('3.0.0', '');

