DROP INDEX IF EXISTS idx_events_event_id;
ALTER TABLE events ADD CONSTRAINT events_event_id_key UNIQUE (event_id);