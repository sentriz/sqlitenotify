package sqlitenotify

import (
	"context"
	"database/sql"
	"sync"
	"time"
)

const pollInterval = 500 * time.Millisecond

func NewNotifier(ctx context.Context, src Source) (*Notifier, error) {
	last, err := src.Version(ctx)
	if err != nil {
		src.Close()
		return nil, err
	}

	w := &Notifier{subs: map[chan struct{}]struct{}{}}

	notify := func() {
		w.mu.Lock()
		chs := make([]chan struct{}, 0, len(w.subs))
		for ch := range w.subs {
			chs = append(chs, ch)
		}
		w.mu.Unlock()
		for _, ch := range chs {
			select {
			case ch <- struct{}{}:
			default:
			}
		}
	}

	go func() {
		defer src.Close()

		t := time.NewTicker(pollInterval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				v, err := src.Version(ctx)
				if err != nil {
					continue
				}
				if v == last {
					continue
				}
				last = v

				notify()
			}
		}
	}()

	return w, nil
}

type Notifier struct {
	mu   sync.Mutex
	subs map[chan struct{}]struct{}
}

func (w *Notifier) Listen(ctx context.Context, minInterval, maxInterval time.Duration) <-chan struct{} {
	out := make(chan struct{}, 1)
	out <- struct{}{}

	raw := make(chan struct{}, 1)
	w.mu.Lock()
	w.subs[raw] = struct{}{}
	w.mu.Unlock()
	context.AfterFunc(ctx, func() {
		w.mu.Lock()
		delete(w.subs, raw)
		w.mu.Unlock()
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
