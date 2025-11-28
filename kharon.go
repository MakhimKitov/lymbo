package lymbo

import (
	"context"
	"errors"
	"log/slog"
	"runtime/debug"
	"time"

	"golang.org/x/sync/errgroup"
)

var ErrHandlerNotFound = errors.New("kharon: handler not found")

// Kharon is the main struct for the job processing system.
type Kharon struct {
	store    Store
	settings Settings
	logger   *slog.Logger
	bus      chan *Ticket
}

type Settings struct {
	ProcessTime      time.Duration
	MaxPollInterval  time.Duration
	MinPollInterval  time.Duration
	BatchSize        int
	Workers          int
	EnableExpiration bool
}

var MinPollIntervalDefault = 10 * time.Millisecond
var MaxPollIntervalDefault = 15000 * time.Millisecond
var ExpirationBatchSize = 100

func NewKharon(store Store, s Settings, logger *slog.Logger) *Kharon {
	if store == nil {
		panic("kharon: store cannot be nil")
	}
	if logger == nil {
		logger = slog.Default()
	}
	if s.Workers <= 0 {
		s.Workers = 1
	}
	if s.MaxPollInterval <= 0 {
		s.MaxPollInterval = MaxPollIntervalDefault
	}
	if s.MinPollInterval <= 0 {
		s.MinPollInterval = MinPollIntervalDefault
	}
	if s.MaxPollInterval < s.MinPollInterval {
		s.MaxPollInterval = s.MinPollInterval
	}
	if s.BatchSize <= 0 {
		s.BatchSize = 1
	}
	if s.BatchSize > s.Workers {
		s.BatchSize = s.Workers
	}
	return &Kharon{
		store:    store,
		settings: s,
		logger:   logger,
		// unbuffered channel for good
		bus: make(chan *Ticket),
	}
}

func (k *Kharon) Done(tid TicketId, opts ...Option) error {
	opts = append(opts, WithKeep())
	return k.store.Ack(tid, opts...)
}

func (k *Kharon) Cancel(tid TicketId, opts ...Option) error {
	return k.store.Cancel(tid, opts...)
}

func (k *Kharon) Fail(tid TicketId, opts ...Option) error {
	return k.store.Fail(tid, opts...)
}

func (k *Kharon) Add(t Ticket) error {
	return k.store.Add(t)
}

func (k *Kharon) Delete(tid TicketId) error {
	return k.store.Delete(tid)
}

func (k *Kharon) Get(tid TicketId) (Ticket, error) {
	return k.store.Get(tid)
}

func (k *Kharon) Run(ctx context.Context, r *Router) error {
	wctx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer close(k.bus)

	for range k.settings.Workers {
		go k.work(wctx, r)
	}

	if k.settings.EnableExpiration {
		go func() {
			k.logger.InfoContext(ctx, "kharon: ticket expiration worker started")
			ticker := time.NewTicker(3 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-wctx.Done():
					return
				case <-ticker.C:
					err := k.store.ExpireTickets(ExpirationBatchSize, time.Now())
					if err != nil {
						k.logger.ErrorContext(ctx, "kharon: error expiring tickets", "error", err)
					}
					k.logger.InfoContext(ctx, "kharon: ticket expiration run completed")
				}
			}
		}()
	}

	sleepDuration := k.settings.MinPollInterval
	k.logger.InfoContext(ctx, "kharon started",
		"workers", k.settings.Workers,
		"min_poll_timeout", k.settings.MinPollInterval.String(),
		"max_poll_timeout", k.settings.MaxPollInterval.String(),
		"batch_size", k.settings.BatchSize,
		"process_time", k.settings.ProcessTime.String(),
	)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(sleepDuration):
			result, err := k.store.PollPending(k.settings.BatchSize, time.Now(), k.settings.ProcessTime)
			if err != nil {
				// This error is transient, we just log and continue
				k.logger.ErrorContext(ctx, "kharon: error polling store", "error", err)
				continue
			}
			if result.SleepUntil != nil {
				sleepDuration = time.Until(*result.SleepUntil)
				sleepDuration = min(sleepDuration, k.settings.MaxPollInterval)
				sleepDuration = max(sleepDuration, k.settings.MinPollInterval)
				// We should sleep
				continue
			}
			// we have tickets to process
			for _, t := range result.Tickets {
				k.bus <- &t
			}
		}
	}
}

func (k *Kharon) work(ctx context.Context, r *Router) {
	defer k.logger.DebugContext(ctx, "kharon.wrk: leaving")
	for {
		select {
		case <-ctx.Done():
			return
		case t, ok := <-k.bus:
			if !ok {
				return
			}
			if t == nil {
				k.logger.WarnContext(ctx, "kharon.wrk: nil ticket given")
				continue
			}

			handler := r.Handler(t)
			g := &errgroup.Group{}
			g.Go(func() error {
				defer func() {
					if r := recover(); r != nil {
						k.logger.ErrorContext(ctx, "kharon.wrk: panic occured: %v", r)
						debug.PrintStack()
					}
				}()
				return handler.ProcessTicket(ctx, *t)
			})
			err := g.Wait()
			if err != nil {
				k.logger.ErrorContext(ctx, "kharon.wrk: error processing ticket", "ticket_id", t.ID, "type", t.Type, "error", err)
			}
		}
	}
}
