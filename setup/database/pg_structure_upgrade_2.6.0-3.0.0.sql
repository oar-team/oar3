ALTER TABLE jobs ADD last_karma real;

-- Update the database schema version
DELETE FROM schema;
INSERT INTO schema(version, name) VALUES ('3.0.0', '');

