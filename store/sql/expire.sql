-- $1=now
DELETE FROM tickets
WHERE id IN (
	SELECT id
	FROM tickets
	WHERE status != 'pending' AND runat <= $2
	LIMIT $1
)
