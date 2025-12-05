package lymbo

import (
	"context"
	"log/slog"
	"runtime/debug"
	"time"

	"github.com/ochaton/lymbo/status"
)

// Default configuration values.
const (
	// MinPollIntervalDefault is the default minimum time between store polls.
	MinPollIntervalDefault = 10 * time.Millisecond

	// MaxPollIntervalDefault is the default maximum time to wait between store polls.
	MaxPollIntervalDefault = 15 * time.Second

	// ExpirationBatchSize is the number of tickets to expire per batch.
	ExpirationBatchSize = 1000

	// ExpirationInterval is how often to run the expiration worker.
	ExpirationInterval = 100 * time.Millisecond
)

const (
	// MaxBackoffDelay is the maximum delay between retry attempts.
	MaxBackoffDelay = 15 * time.Second

	// DefaultBackoffBase is the default base for exponential backoff calculation.
	DefaultBackoffBase = 1.5

	secondsPerMinute       = 60
	secondsPerHour         = 60 * secondsPerMinute
	secondsPerDay          = 24 * secondsPerHour
	unixToInternal   int64 = (1969*365 + 1969/4 - 1969/100 + 1969/400) * secondsPerDay
	maxInt64         int64 = int64(^uint64(0) >> 1)
	maxUnix          int64 = maxInt64 - unixToInternal
)

// Kharon is the main orchestrator for the job processing system.
// It coordinates polling, dispatching, and processing of tickets.
type Kharon struct {
	store    Store
	settings Settings
	logger   *slog.Logger
	bus      chan *Ticket

	stats *stats
}

type counter struct {
	value int64
}

type stats struct {
	added     *counter
	polled    *counter
	scheduled *counter
	acked     *counter
	failed    *counter
	done      *counter
	retried   *counter
	canceled  *counter
	deleted   *counter
	expired   *counter
	processed *counter
}

type Stats struct {
	Added     int64 `json:"added"`
	Polled    int64 `json:"polled"`
	Scheduled int64 `json:"scheduled"`
	Acked     int64 `json:"acked"`
	Failed    int64 `json:"failed"`
	Done      int64 `json:"done"`
	Retried   int64 `json:"retried"`
	Canceled  int64 `json:"canceled"`
	Deleted   int64 `json:"deleted"`
	Expired   int64 `json:"expired"`
	Processed int64 `json:"processed"`
}

func newStats() *stats {
	return &stats{
		added:     &counter{},
		polled:    &counter{},
		scheduled: &counter{},
		acked:     &counter{},
		failed:    &counter{},
		done:      &counter{},
		retried:   &counter{},
		canceled:  &counter{},
		deleted:   &counter{},
		expired:   &counter{},
		processed: &counter{},
	}
}

func (s *stats) reset() {
	s.added.value = 0
	s.polled.value = 0
	s.scheduled.value = 0
	s.acked.value = 0
	s.failed.value = 0
	s.done.value = 0
	s.retried.value = 0
	s.canceled.value = 0
	s.deleted.value = 0
	s.expired.value = 0
	s.processed.value = 0
}

func (kh *Kharon) ResetStats() {
	kh.stats.reset()
}

// NewKharon creates a new Kharon instance with the provided store, settings, and logger.
//
// The store parameter is required and must not be nil (panics otherwise).
// The logger parameter is optional; if nil, slog.Default() is used.
// Settings are normalized to apply defaults and constraints.
func NewKharon(store Store, s *Settings, logger *slog.Logger) *Kharon {
	if store == nil {
		panic("kharon: store cannot be nil")
	}
	if logger == nil {
		logger = slog.Default()
	}
	if s == nil {
		s = DefaultSettings()
	}

	s.normalize()

	return &Kharon{
		store:    store,
		settings: *s,
		logger:   logger,
		bus:      make(chan *Ticket),
		stats:    newStats(),
	}
}

func applyOpts(ctx context.Context, t *Ticket, s status.Status, o *Opts) error {
	t.Status = s
	now := time.Now()
	t.Mtime = &now
	if o.delay > 0 || t.Status != status.Pending {
		t.Runat = o.runat
	}
	if o.errorReason != nil {
		t.ErrorReason = o.errorReason
	} else {
		t.ErrorReason = nil
	}
	if o.nice != nil {
		t.Nice = *o.nice
	}
	if o.update != nil {
		if err := o.update(ctx, t); err != nil {
			return err
		}
	}
	return nil
}

func (k *Kharon) save(ctx context.Context, tid TicketId, s status.Status, o *Opts) error {
	if o.update == nil {
		// then we can do a simple update
		pr := &o.runat
		if s == status.Pending && o.delay == 0 {
			// drop runat update
			pr = nil
		}
		return k.store.UpdateSet(ctx, tid, UpdateSet{
			Status:      &s,
			Nice:        o.nice,
			Runat:       pr,
			Payload:     nil,
			ErrorReason: o.errorReason,
		})
	}
	return k.store.Update(ctx, tid, func(ctx context.Context, t *Ticket) error {
		return applyOpts(ctx, t, s, o)
	})
}

func toOpts(opts ...Option) *Opts {
	o := &Opts{}
	for _, opt := range opts {
		opt(o)
	}
	if o.delay > 0 {
		t := time.Now().Add(o.delay)
		o.runat = t
	} else {
		// If delay is not set and status is not pending, set Runat to infinity
		o.runat = time.Unix(maxUnix, 0)
	}
	return o
}

func (k *Kharon) Ack(ctx context.Context, tid TicketId, opts ...Option) error {
	o := toOpts(opts...)
	var err error
	if o.keep {
		err = k.save(ctx, tid, status.Done, o)
	} else {
		err = k.store.Delete(ctx, tid)
	}
	if err != nil {
		return err
	}
	k.stats.acked.value++
	return nil
}

// Done marks a ticket as successfully completed.
// It automatically adds the WithKeep option to retain the ticket in the store.
func (k *Kharon) Done(ctx context.Context, tid TicketId, opts ...Option) error {
	o := toOpts(opts...)
	o.keep = true
	if err := k.save(ctx, tid, status.Done, o); err != nil {
		return err
	}
	k.stats.done.value++
	return nil
}

// Cancel marks a ticket as cancelled.
func (k *Kharon) Cancel(ctx context.Context, tid TicketId, opts ...Option) error {
	o := toOpts(opts...)
	var err error
	if !o.keep {
		err = k.store.Delete(ctx, tid)
	} else {
		err = k.save(ctx, tid, status.Cancelled, o)
	}
	if err != nil {
		return err
	}
	k.stats.canceled.value++
	return nil
}

// Fail marks a ticket as failed.
func (k *Kharon) Fail(ctx context.Context, tid TicketId, opts ...Option) error {
	o := toOpts(opts...)
	o.keep = true
	if err := k.save(ctx, tid, status.Failed, o); err != nil {
		return err
	}
	k.stats.failed.value++
	return nil
}

// Retry schedules a ticket for retry with updated parameters.
func (k *Kharon) Retry(ctx context.Context, tid TicketId, opts ...Option) error {
	o := toOpts(opts...)
	o.keep = true
	if err := k.save(ctx, tid, status.Pending, o); err != nil {
		return err
	}
	k.stats.retried.value++
	return nil
}

// Put adds a new ticket to the store with configured options.
func (k *Kharon) Put(ctx context.Context, t Ticket, opts ...Option) error {
	o := toOpts(opts...)
	o.keep = true
	// Apply options to the ticket before adding.
	if err := applyOpts(ctx, &t, status.Pending, o); err != nil {
		return err
	}
	if err := k.store.Put(ctx, t); err != nil {
		return err
	}
	k.stats.added.value++
	return nil
}

// Delete removes a ticket from the store.
func (k *Kharon) Delete(ctx context.Context, tid TicketId) error {
	if err := k.store.Delete(ctx, tid); err != nil {
		return err
	}
	k.stats.deleted.value++
	return nil
}

// Get retrieves a ticket from the store.
func (k *Kharon) Get(ctx context.Context, tid TicketId) (Ticket, error) {
	return k.store.Get(ctx, tid)
}

func (k *Kharon) Stats() Stats {
	return Stats{
		Added:     k.stats.added.value,
		Polled:    k.stats.polled.value,
		Scheduled: k.stats.scheduled.value,
		Acked:     k.stats.acked.value,
		Failed:    k.stats.failed.value,
		Done:      k.stats.done.value,
		Retried:   k.stats.retried.value,
		Canceled:  k.stats.canceled.value,
		Deleted:   k.stats.deleted.value,
		Expired:   k.stats.expired.value,
		Processed: k.stats.processed.value,
	}
}

// Run starts the Kharon job processing system with the given context and router.
// It spawns worker goroutines and begins polling for tickets to process.
// Returns when ctx is cancelled or an error occurs.
func (k *Kharon) Run(ctx context.Context, r *Router) error {
	wctx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer close(k.bus)

	for range k.settings.workers {
		go k.work(wctx, r)
	}

	if k.settings.enableExpiration {
		go k.runExpirationWorker(wctx)
	}

	k.logger.InfoContext(ctx, "kharon started",
		"workers", k.settings.workers,
		"min_poll_timeout", k.settings.minReactionDelay.String(),
		"max_poll_timeout", k.settings.maxReactionDelay.String(),
		"batch_size", k.settings.batchSize,
		"process_time", k.settings.processTime.String(),
	)

	sleepDuration := k.settings.maxReactionDelay
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(sleepDuration):
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}

				result, err := k.store.PollPending(ctx, PollRequest{
					Limit:           k.settings.batchSize,
					Now:             time.Now(),
					TTR:             k.settings.processTime,
					BackoffBase:     k.settings.backoffBase,
					MaxBackoffDelay: k.settings.maxBackoffDelay,
				})
				if err != nil {
					k.logger.ErrorContext(ctx, "error polling store", "error", err)
					sleepDuration = k.settings.maxReactionDelay
					break
				}

				if result.SleepUntil != nil {
					sleepDuration = time.Until(*result.SleepUntil)
					sleepDuration = min(sleepDuration, k.settings.maxReactionDelay)
					sleepDuration = max(sleepDuration, k.settings.minReactionDelay)
					break
				}
				if len(result.Tickets) == 0 {
					sleepDuration = k.settings.maxReactionDelay
					break
				}

				k.stats.polled.value += int64(len(result.Tickets))

				for _, t := range result.Tickets {
					k.bus <- &t
					k.stats.scheduled.value++
				}
			}
		}
	}
}

// runExpirationWorker runs a background worker that periodically expires old tickets.
func (k *Kharon) runExpirationWorker(ctx context.Context) {
	k.logger.InfoContext(ctx, "ticket expiration worker started")
	ticker := time.NewTicker(k.settings.expirationInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			k.logger.DebugContext(ctx, "ticket expiration worker exiting")
			return
		case <-ticker.C:
			if n, err := k.store.ExpireTickets(ctx, ExpirationBatchSize, time.Now()); err != nil {
				k.logger.ErrorContext(ctx, "error expiring tickets", "error", err)
			} else {
				if n > 0 {
					k.stats.expired.value += n
					k.logger.DebugContext(ctx, "ticket expiration run completed", "expired_count", n)
				}
			}
		}
	}
}

// work is the main worker loop that processes tickets from the bus.
func (k *Kharon) work(ctx context.Context, r *Router) {
	defer k.logger.DebugContext(ctx, "worker exiting")

	for {
		select {
		case <-ctx.Done():
			return
		case t, ok := <-k.bus:
			if !ok {
				return
			}
			if t == nil {
				k.logger.WarnContext(ctx, "received nil ticket")
				continue
			}

			k.processTicket(ctx, r, t)
			k.stats.processed.value++
		}
	}
}

// processTicket processes a single ticket with the appropriate handler.
func (k *Kharon) processTicket(ctx context.Context, r *Router, t *Ticket) {
	handler := r.Handler(t)
	defer func() {
		if r := recover(); r != nil {
			k.logger.ErrorContext(ctx, "panic occurred while processing ticket",
				"ticket_id", t.ID,
				"type", t.Type,
				"panic", r,
			)
			debug.PrintStack()
		}
	}()

	rctx, cancel := context.WithDeadline(ctx, t.Runat)
	defer cancel()
	err := handler.ProcessTicket(rctx, t)
	if err != nil {
		k.logger.ErrorContext(ctx, "error processing ticket",
			"ticket_id", t.ID,
			"type", t.Type,
			"error", err,
		)
	}
}
