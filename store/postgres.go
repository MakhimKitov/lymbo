package store

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/ochaton/lymbo"
	"github.com/ochaton/lymbo/status"
)

//go:embed sql/*.sql
var queriesFS embed.FS

func mustReadSql(path string) string {
	data, err := queriesFS.ReadFile(path)
	if err != nil {
		panic(err)
	}
	return string(data)
}

type PostgresStore struct {
	db        *sql.DB
	pollSql   string
	expireSql string
}

// NewPostgresStore creates a new PostgresStore with the given database connection.
func NewPostgresStore(db *sql.DB) *PostgresStore {
	pollSql := mustReadSql("sql/poll.sql")
	expireSql := mustReadSql("sql/expire.sql")
	schemaSql := mustReadSql("sql/schema.sql")

	_, err := db.Exec(schemaSql)
	if err != nil {
		panic(fmt.Sprintf("failed to initialize database schema: %v", err))
	}
	return &PostgresStore{db: db, pollSql: pollSql, expireSql: expireSql}
}

var _ lymbo.Store = (*PostgresStore)(nil)

// Implement Store interface methods for PostgresStore here.

var pgGetTicketQuery = `SELECT 'ticket' as ticket, id, status, runat, nice, type, ctime, mtime, attempts, payload, error_reason
FROM tickets
WHERE id = $1`

type querier interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func (pg *PostgresStore) getTicket(ctx context.Context, q querier, id lymbo.TicketId) (lymbo.Ticket, error) {
	if id == "" {
		return lymbo.Ticket{}, lymbo.ErrTicketIDEmpty
	}
	// check for valid uuid
	if _, err := uuid.Parse(string(id)); err != nil {
		return lymbo.Ticket{}, lymbo.ErrTicketIDInvalid
	}

	row := q.QueryRowContext(ctx, pgGetTicketQuery, string(id))
	if row == nil {
		return lymbo.Ticket{}, lymbo.ErrTicketNotFound
	}

	_, t, err := pg.unmarshalKeyTicket(row)
	if err != nil {
		return lymbo.Ticket{}, err
	}
	return t, nil
}

type execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

var pgAddTicketQuery = `INSERT INTO tickets (id, status, runat, nice, type, ctime, mtime, attempts, payload, error_reason)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
ON CONFLICT (id) DO UPDATE SET
status = EXCLUDED.status,
runat = EXCLUDED.runat,
nice = EXCLUDED.nice,
type = EXCLUDED.type,
ctime = EXCLUDED.ctime,
mtime = EXCLUDED.mtime,
attempts = EXCLUDED.attempts,
payload = EXCLUDED.payload,
error_reason = EXCLUDED.error_reason`

func (pg *PostgresStore) storeTicket(ctx context.Context, q execer, t lymbo.Ticket) error {
	if t.ErrorReason == nil || t.ErrorReason == "" {
		t.ErrorReason = "null"
	}

	// fmt.Println("Storing ticket:", t.ID, t.Status, t.Runat, t.Nice, t.Type, t.Ctime, t.Mtime, t.Attempts, t.Payload, t.ErrorReason)
	_, err := q.ExecContext(ctx, pgAddTicketQuery,
		t.ID, t.Status, t.Runat, t.Nice, t.Type, t.Ctime, t.Mtime, t.Attempts, t.Payload, t.ErrorReason)
	return err
}

func (pg *PostgresStore) Get(ctx context.Context, id lymbo.TicketId) (lymbo.Ticket, error) {
	return pg.getTicket(ctx, pg.db, id)
}

func (pg *PostgresStore) Add(ctx context.Context, t lymbo.Ticket) error {
	if t.ID == "" {
		return lymbo.ErrTicketIDEmpty
	}
	if _, err := uuid.Parse(string(t.ID)); err != nil {
		return lymbo.ErrTicketIDInvalid
	}

	t.Status = status.Pending
	return pg.storeTicket(ctx, pg.db, t)
}

var pgDeleteTicketQuery = `DELETE FROM tickets WHERE id = $1`

func (pg *PostgresStore) Delete(ctx context.Context, id lymbo.TicketId) error {
	_, err := pg.db.ExecContext(ctx, pgDeleteTicketQuery, string(id))
	return err
}

func (pg *PostgresStore) Update(ctx context.Context, tid lymbo.TicketId, fn lymbo.UpdateFunc) error {
	tx, err := pg.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	t, err := pg.getTicket(ctx, tx, tid)
	if err != nil {
		return err
	}

	if err := fn(ctx, &t); err != nil {
		return err
	}

	err = pg.storeTicket(ctx, tx, t)
	if err != nil {
		return err
	}

	return tx.Commit()
}

type scanner interface {
	Scan(dest ...any) error
}

func (pg *PostgresStore) unmarshalKeyTicket(row scanner) (string, lymbo.Ticket, error) {
	var key string
	var id string
	var s string
	var runat time.Time
	var nice int
	var typ string
	var ctime time.Time
	var mtime *time.Time
	var attempts int
	var payload sql.NullString
	var errorReason sql.NullString

	err := row.Scan(&key, &id, &s, &runat, &nice, &typ, &ctime, &mtime, &attempts, &payload, &errorReason)
	if err != nil {
		return "", lymbo.Ticket{}, err
	}

	st, err := status.FromString(s)
	if err != nil {
		return "", lymbo.Ticket{}, err
	}

	return key, lymbo.Ticket{
		ID:          lymbo.TicketId(id),
		Status:      st,
		Runat:       runat,
		Nice:        nice,
		Type:        typ,
		Ctime:       ctime,
		Mtime:       mtime,
		Attempts:    attempts,
		Payload:     payload.String,
		ErrorReason: errorReason.String,
	}, nil
}

func toSeconds(d time.Duration) float64 {
	return float64(d.Milliseconds()) / 1000.0
}

func (pg *PostgresStore) PollPending(ctx context.Context, req lymbo.PollRequest) (lymbo.PollResult, error) {
	// Implementation of polling pending tickets goes here.
	rows, err := pg.db.QueryContext(ctx, pg.pollSql, req.Now, req.Limit,
		toSeconds(req.TTR),
		req.BackoffBase,
		toSeconds(req.MaxBackoffDelay),
	)
	if err != nil {
		return lymbo.PollResult{}, err
	}
	defer rows.Close()

	result := lymbo.PollResult{
		Tickets:    make([]lymbo.Ticket, 0),
		SleepUntil: nil,
	}

	for rows.Next() {
		var t lymbo.Ticket
		var key string
		key, t, err = pg.unmarshalKeyTicket(rows)
		if err != nil {
			// Skip malformed rows
			slog.WarnContext(ctx, "pgstore.PollPending: malformed row", "error", err)
			continue
		}
		switch key {
		case "ticket":
			result.Tickets = append(result.Tickets, t)
		case "future_ticket":
			result.SleepUntil = &t.Runat
			return result, nil
		default:
			return lymbo.PollResult{}, fmt.Errorf("unknown poll result key: %s", key)
		}
	}

	return result, nil
}

func (pg *PostgresStore) ExpireTickets(ctx context.Context, limit int, before time.Time) error {
	_, err := pg.db.ExecContext(ctx, pg.expireSql, limit, before)
	if err != nil {
		return err
	}
	return nil
}
