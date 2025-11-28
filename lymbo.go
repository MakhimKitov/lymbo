package lymbo

import (
	"time"
)

type PollResult struct {
	SleepUntil *time.Time // Can be nil, actually, meaning the store is empty
	Tickets    []Ticket
}

type Option func(o *Opts)

func WithExpireIn(ttl time.Duration) func(o *Opts) {
	return func(o *Opts) {
		o.ExpireIn = ttl
	}
}

func WithKeep() func(o *Opts) {
	return func(o *Opts) {
		o.Keep = true
	}
}

func WithErrorReason(reason any) func(o *Opts) {
	return func(o *Opts) {
		o.ErrorReason = reason
	}
}

type Opts struct {
	ExpireIn    time.Duration
	Keep        bool
	ErrorReason any
}

type Store interface {
	Get(TicketId) (Ticket, error)
	Add(Ticket) error
	Delete(TicketId) error
	Ack(tid TicketId, opts ...Option) error
	Cancel(tid TicketId, opts ...Option) error
	Fail(tid TicketId, opts ...Option) error
	PollPending(limit int, now time.Time, ttr time.Duration) (PollResult, error)
	ExpireTickets(limit int, now time.Time) error
}
