-- PostgreSQL offline message storage, version 1. Run as the schema owner.
BEGIN;
-- Serialize concurrent installs from several nodes. The key is 'ompp' in ASCII.
SELECT pg_advisory_xact_lock(1869443184);

-- Schema version, checked by the plugin at startup.
CREATE TABLE IF NOT EXISTS mqtt_pg_schema (
    version integer PRIMARY KEY CHECK (version = 1)
);
INSERT INTO mqtt_pg_schema VALUES (1) ON CONFLICT DO NOTHING;

-- One row per exact topic with the next offset to allocate.
-- The row is kept after cleanup so that offsets are never reused.
CREATE TABLE IF NOT EXISTS mqtt_pg_topic (
    topic_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    namespace text NOT NULL CHECK (octet_length(namespace) <= 256),
    topic text NOT NULL CHECK (octet_length(topic) BETWEEN 1 AND 2048),
    next_offset bigint NOT NULL DEFAULT 1,
    UNIQUE (namespace, topic)
);

-- Stored messages, ordered by msg_offset within a topic.
-- Cleanup reads and deletes primary key ranges only.
CREATE TABLE IF NOT EXISTS mqtt_pg_message (
    topic_id bigint NOT NULL REFERENCES mqtt_pg_topic,
    msg_offset bigint NOT NULL,
    message_id bytea NOT NULL,
    sender text NOT NULL,
    -- Publish QoS, kept for SQL producers and diagnostics. Delivery uses QoS 1.
    qos smallint NOT NULL CHECK (qos BETWEEN 0 AND 2),
    retain boolean NOT NULL,
    payload bytea NOT NULL,
    properties jsonb NOT NULL,
    -- Unix milliseconds
    published_at bigint NOT NULL,
    expires_at bigint,
    PRIMARY KEY (topic_id, msg_offset)
);

-- One row per client and topic with the last acknowledged offset.
-- Checkpoints change only unindexed columns, so the free page space lets them run as HOT updates.
CREATE TABLE IF NOT EXISTS mqtt_pg_subscription (
    namespace text NOT NULL,
    client_id text NOT NULL,
    topic_id bigint NOT NULL REFERENCES mqtt_pg_topic,
    subopts jsonb NOT NULL,
    ack_offset bigint NOT NULL,
    -- Renewed by active subscribers; old rows are deleted after subscription_expiry.
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (namespace, client_id, topic_id)
) WITH (fillfactor = 80);
-- Used by cleanup to find the lowest acknowledged offset of a topic.
CREATE INDEX IF NOT EXISTS mqtt_pg_subscription_topic ON mqtt_pg_subscription (topic_id);

-- Append a JSON array and return the topic identity and first assigned offset.
-- The only supported write path for the broker and for SQL producers.
CREATE OR REPLACE FUNCTION omp_append(ns text, t text, messages jsonb)
    RETURNS TABLE(topic_id bigint, first_offset bigint) LANGUAGE plpgsql AS $$
DECLARE
    id bigint;
    first bigint;
    n integer := jsonb_array_length(messages);
BEGIN
    IF n IS NULL OR n < 1 THEN
        RAISE EXCEPTION 'invalid append batch';
    END IF;
    -- Reserve n offsets. The row lock makes other writers to this topic wait until
    -- commit, and a rollback releases the reservation.
    INSERT INTO mqtt_pg_topic(namespace, topic) VALUES (ns, t) ON CONFLICT DO NOTHING;
    UPDATE mqtt_pg_topic AS topic_row SET next_offset = next_offset + n
      WHERE namespace = ns AND topic = t
      RETURNING topic_row.topic_id, next_offset - n INTO id, first;
    -- Insert the messages at consecutive offsets in array order.
    INSERT INTO mqtt_pg_message
      (topic_id, msg_offset, message_id, sender, qos, retain, payload, properties, published_at, expires_at)
      SELECT id, first + ord - 1, decode(m->>'message_id', 'base64'),
             m->>'sender', (m->>'qos')::smallint, (m->>'retain')::boolean,
             decode(m->>'payload', 'base64'), m->'properties',
             (m->>'published_at')::bigint, (m->>'expires_at')::bigint
      FROM jsonb_array_elements(messages) WITH ORDINALITY AS a(m, ord);
    RETURN QUERY SELECT id, first;
END
$$;
COMMIT;
