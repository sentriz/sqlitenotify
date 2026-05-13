package sqlitenotify

import (
	"context"
	"database/sql"
	"sync"
	"time"
)

const pollInterval = 500 * time.Millisecond

type Notifier struct {
	mu   sync.Mutex
	subs map[chan struct{}]struct{}
}

func (n *Notifier) Start(ctx context.Context, src Source) error {
	defer src.Close()

	last, err := src.Version(ctx)
	if err != nil {
		return err
	}

	t := time.NewTicker(pollInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			v, err := src.Version(ctx)
			if err != nil {
				continue
			}
			if v == last {
				continue
			}
			last = v

			n.notify()
		}
	}
}

func (n *Notifier) Listen(ctx context.Context, minInterval, maxInterval time.Duration) <-chan struct{} {
	out := make(chan struct{}, 1)
	out <- struct{}{}

	raw := make(chan struct{}, 1)
	n.mu.Lock()
	if n.subs == nil {
		n.subs = map[chan struct{}]struct{}{}
	}
	n.subs[raw] = struct{}{}
	n.mu.Unlock()
	context.AfterFunc(ctx, func() {
		n.mu.Lock()
		delete(n.subs, raw)
		n.mu.Unlock()
	})

	go func() {
		defer close(out)
		for {
			var maxIntervalC <-chan time.Time
			if maxInterval > 0 {
				maxIntervalC = time.After(maxInterval)
			}
			select {
			case <-ctx.Done():
				return
			case <-raw:
			case <-maxIntervalC:
			}

			select {
			case out <- struct{}{}:
			default:
			}

			if minInterval > 0 {
				select {
				case <-ctx.Done():
					return
				case <-time.After(minInterval):
				}
			}
		}
	}()

	return out
}

func (n *Notifier) notify() {
	n.mu.Lock()
	chs := make([]chan struct{}, 0, len(n.subs))
	for ch := range n.subs {
		chs = append(chs, ch)
	}
	n.mu.Unlock()
	for _, ch := range chs {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

type Source interface {
	Version(ctx context.Context) (int64, error)
	Close() error
}

func SQLite(db *sql.DB) Source {
	return &sqlite{db: db}
}

type sqlite struct {
	db   *sql.DB
	conn *sql.Conn
}

func (s *sqlite) Version(ctx context.Context) (int64, error) {
	if s.conn == nil {
		conn, err := s.db.Conn(ctx)
		if err != nil {
			return 0, err
		}
		s.conn = conn
	}
	var v int64
	return v, s.conn.QueryRowContext(ctx, "pragma data_version").Scan(&v)
}

func (s *sqlite) Close() error {
	if s.conn == nil {
		return nil
	}
	return s.conn.Close()
}
