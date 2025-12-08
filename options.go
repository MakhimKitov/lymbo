package lymbo

import (
	"context"
	"time"

	"github.com/ochaton/lymbo/status"
)

// Option is a functional option for configuring ticket operations.
type Option func(o *Opts)

// Opts contains options for ticket operations.
type Opts struct {
	// delay sets a delay before the ticket becomes eligible for processing.
	delay time.Duration

	// status sets the ticket's status.
	status *status.Status

	// keep indicates whether to retain the ticket in the store after the operation.
	// By default, completed/failed/cancelled tickets are removed.
	keep bool

	// errorReason stores the reason for failure (for Fail operations).
	errorReason any

	// nice sets the ticket's nice value (priority).
	nice *int

	// payload sets the ticket's payload data.
	payload any

	// update allows custom modification of the ticket.
	update func(ctx context.Context, t *Ticket) error
}

// WithKeep indicates that the ticket should be kept in the store after processing.
// Useful for audit trails or tracking completed work.
func WithKeep() Option {
	return func(o *Opts) {
		o.keep = true
	}
}

// WithErrorReason sets an error reason for failed ticket operations.
// The reason will be stored in the ticket's ErrorReason field.
func WithErrorReason(reason any) Option {
	return func(o *Opts) {
		o.errorReason = reason
	}
}

// WithDelay sets a delay before the ticket becomes eligible for processing.
func WithDelay(delay time.Duration) Option {
	return func(o *Opts) {
		o.delay = delay
	}
}

// WithNice sets the ticket's nice value (priority).
func WithNice(nice int) Option {
	return func(o *Opts) {
		o.nice = &nice
	}
}

// WithUpdate allows custom modification of the ticket before storing.
func WithUpdate(update func(ctx context.Context, t *Ticket) error) Option {
	return func(o *Opts) {
		o.update = update
	}
}

func WithPayload(payload any) Option {
	return func(o *Opts) {
		o.payload = payload
	}
}
