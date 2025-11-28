BEGIN;

-- Ticket status enum: pending -> done/failed/cancelled
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_type WHERE typname = 'ticket_status'
    ) THEN
        CREATE TYPE ticket_status AS ENUM ( 'pending', 'done', 'failed', 'cancelled' );
    END IF;
END $$;

-- Tickets table
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM   pg_catalog.pg_tables
        WHERE  schemaname = 'public'
        AND    tablename  = 'tickets'
    ) THEN
        CREATE TABLE tickets (
            id           UUID PRIMARY KEY,
            status       ticket_status NOT NULL DEFAULT 'pending',
            runat        TIMESTAMPTZ   NOT NULL DEFAULT NOW(),  -- run at time
            nice         SMALLINT      NOT NULL DEFAULT 512, -- lower nice ⇒ higher priority
            type         TEXT          NOT NULL,
            ctime        TIMESTAMPTZ   NOT NULL DEFAULT NOW(),  -- creation time
            mtime        TIMESTAMPTZ   NULL,                    -- last modification time
            attempts     INTEGER       NOT NULL DEFAULT 0,
            payload      JSONB         NULL,
            error_reason JSONB         NULL
        );

        CREATE INDEX idx_tickets_pending_runat_nice
            ON tickets (runat, nice)
            WHERE status = 'pending';
    END IF;
END $$;

-- mtime should change only when status or runat change
CREATE OR REPLACE FUNCTION set_mtime_on_meaningful_change()
RETURNS trigger AS $$
BEGIN
    IF ROW(NEW.status, NEW.runat)
    IS DISTINCT FROM
    ROW(OLD.status, OLD.runat)
    THEN
        NEW.mtime := NOW();
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM   pg_trigger
        WHERE  tgname = 'trg_tickets_set_mtime'
    ) THEN
        CREATE TRIGGER trg_tickets_set_mtime
        BEFORE UPDATE ON tickets
        FOR EACH ROW
        EXECUTE FUNCTION set_mtime_on_meaningful_change();
    END IF;
END $$;

COMMIT;
