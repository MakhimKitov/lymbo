-- Poll pending tickets for processing.
-- $1=now, $2=limit, $3=ttr, $4=backoffBase, $5=backoffCap
WITH
rescheduled_tickets AS (
    UPDATE tickets
    SET
        attempts = attempts + 1,
        runat    = $1
			+ GREATEST($3, 0) * INTERVAL '1 second'
			+ LEAST(POWER($4, attempts), $5) * INTERVAL '1 second'
    WHERE id IN (
        SELECT id
        FROM tickets
        WHERE status = 'pending' AND runat <= $1
        ORDER BY runat ASC, nice ASC
        LIMIT $2
        FOR UPDATE SKIP LOCKED
    )
    RETURNING id, status, runat, nice, type, ctime, mtime, attempts, payload, error_reason
),
future_ticket AS (
    SELECT
        id, status, runat, nice, type, ctime, mtime, attempts, payload, error_reason
    FROM tickets
    WHERE status = 'pending'
    ORDER BY runat ASC, nice ASC
    LIMIT 1
	-- we ignore tickets that are taken by concurrent workers under "FOR UPDATE SKIP LOCKED"
	FOR SHARE SKIP LOCKED
)
SELECT
    'ticket' AS row_type,
    rescheduled_tickets.*
FROM rescheduled_tickets
UNION ALL
SELECT
    'future_ticket' AS row_type,
    future_ticket.*
FROM future_ticket
WHERE NOT EXISTS (SELECT 1 FROM rescheduled_tickets);
