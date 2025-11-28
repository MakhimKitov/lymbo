package store

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/ochaton/lymbo"
	"github.com/ochaton/lymbo/status"
)

const MaxBackoffDelay = 15 * time.Second
const (
	secondsPerMinute       = 60
	secondsPerHour         = 60 * secondsPerMinute
	secondsPerDay          = 24 * secondsPerHour
	unixToInternal   int64 = (1969*365 + 1969/4 - 1969/100 + 1969/400) * secondsPerDay
	maxInt64         int64 = int64(^uint64(0) >> 1)
	maxUnix          int64 = maxInt64 - unixToInternal
)

var maxDate = time.Unix(maxUnix, 0)

type MemoryStore struct {
	mu   sync.RWMutex
	data map[lymbo.TicketId]lymbo.Ticket
}

var _ lymbo.Store = (*MemoryStore)(nil)

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		data: make(map[lymbo.TicketId]lymbo.Ticket),
	}
}

func (m *MemoryStore) Get(id lymbo.TicketId) (lymbo.Ticket, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	ticket, exists := m.data[id]

	if !exists {
		return lymbo.Ticket{}, lymbo.ErrTicketNotFound
	}
	return ticket, nil
}

func (m *MemoryStore) Add(t lymbo.Ticket) error {
	if t.ID == "" {
		return lymbo.ErrTicketIDEmpty
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	t.Status = status.Pending
	m.data[t.ID] = t

	return nil
}

func (m *MemoryStore) Delete(id lymbo.TicketId) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.data, id)
	return nil
}

func (m *MemoryStore) update(t *lymbo.Ticket, status status.Status, opts ...lymbo.Option) {
	o := &lymbo.Opts{}
	for _, opt := range opts {
		opt(o)
	}

	t.Status = status
	tm := time.Now()
	t.Mtime = &tm

	if o.ErrorReason != nil {
		t.ErrorReason = o.ErrorReason
	}

	if o.ExpireIn > 0 {
		t.Runat = tm.Add(o.ExpireIn)
	} else {
		t.Runat = maxDate
	}

	if o.Keep {
		m.data[t.ID] = *t
	} else {
		delete(m.data, t.ID)
	}
}

func (m *MemoryStore) lookupPending(tid lymbo.TicketId) (*lymbo.Ticket, error) {
	t, exists := m.data[tid]
	if !exists {
		return nil, lymbo.ErrTicketNotFound
	}
	if t.Status != status.Pending {
		return nil, errors.Join(
			fmt.Errorf("ticket %s is not pending (status: %s)", tid, t.Status),
			lymbo.ErrInvalidStatusTransition,
		)
	}
	return &t, nil
}

func (m *MemoryStore) Ack(tid lymbo.TicketId, opts ...lymbo.Option) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	t, err := m.lookupPending(tid)
	if err != nil {
		return err
	}

	m.update(t, status.Done, opts...)
	return nil
}

func (m *MemoryStore) Cancel(tid lymbo.TicketId, opts ...lymbo.Option) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	t, err := m.lookupPending(tid)
	if err != nil {
		return err
	}

	m.update(t, status.Cancelled, opts...)
	return nil
}

func (m *MemoryStore) Fail(tid lymbo.TicketId, opts ...lymbo.Option) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	t, err := m.lookupPending(tid)
	if err != nil {
		return err
	}

	m.update(t, status.Failed, opts...)
	return nil
}

func (m *MemoryStore) PollPending(limit int, now time.Time, ttr time.Duration) (lymbo.PollResult, error) {
	if limit <= 0 {
		return lymbo.PollResult{
			Tickets:    nil,
			SleepUntil: nil,
		}, lymbo.ErrLimitInvalid
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	var closest *time.Time
	var ready []lymbo.Ticket
	for _, t := range m.data {
		if t.Status != status.Pending {
			continue
		}
		if t.Runat.After(now) {
			if closest == nil || t.Runat.Before(*closest) {
				runat := t.Runat
				closest = &runat
			}
			continue
		}
		// we have to fetch all ready tickets first, because they are unordered
		ready = append(ready, t)
	}

	// if no ready tickets, return closest runat (can be nil)
	if len(ready) == 0 {
		return lymbo.PollResult{
			Tickets:    nil,
			SleepUntil: closest,
		}, nil
	}

	// Sort ready tickets by {runat, nice}
	sort.Slice(ready, func(i, j int) bool {
		if ready[i].Runat.Equal(ready[j].Runat) {
			return ready[i].Nice < ready[j].Nice
		}
		return ready[i].Runat.Before(ready[j].Runat)
	})

	// Now take up to limit tickets
	ready = ready[:min(limit, len(ready))]

	// And finally, update their runat to now + blockFor to avoid re-polling them immediately
	for _, t := range ready {
		delay := min(MaxBackoffDelay, time.Duration(math.Pow(1.5, float64(t.Attempts))))
		// TODO: randomize delay a bit
		delay += ttr
		t.Runat = now.Add(delay)
		t.Attempts += 1
		m.data[t.ID] = t
	}

	return lymbo.PollResult{
		Tickets:    ready,
		SleepUntil: nil,
	}, nil
}

func (m *MemoryStore) ExpireTickets(limit int, now time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// The idea is simple, every "done", "cancelled" or "failed" ticket
	// with runat older than now gets deleted
	for tid, t := range m.data {
		if limit <= 0 {
			break
		}
		limit--
		if t.Status == status.Pending {
			continue
		}
		if t.Runat.After(now) {
			continue
		}
		delete(m.data, tid)
	}

	return nil
}
