package sqlitenotify

import (
	"context"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func TestWatchPreFires(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := t.Context()

		w, err := NewNotifier(ctx, &fakeSource{})
		if err != nil {
			t.Fatal(err)
		}

		ch := w.Listen(ctx, 0, 0)

		recvCh(t, ch, "pre-fire")
	})
}

func TestWatchNotifiesOnBump(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := t.Context()

		src := &fakeSource{}
		w, err := NewNotifier(ctx, src)
		if err != nil {
			t.Fatal(err)
		}

		ch := w.Listen(ctx, 0, 0)
		<-ch

		assertEmpty(t, ch, "before bump")

		src.bump()
		time.Sleep(pollInterval)
		synctest.Wait()

		recvCh(t, ch, "after bump")
	})
}

func TestWatchCoalescesBursts(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := t.Context()

		src := &fakeSource{}
		w, err := NewNotifier(ctx, src)
		if err != nil {
			t.Fatal(err)
		}

		ch := w.Listen(ctx, 0, 0)
		<-ch

		for range 5 {
			src.bump()
			time.Sleep(pollInterval)
		}
		synctest.Wait()

		recvCh(t, ch, "first delivery")
		assertEmpty(t, ch, "coalesced")
	})
}

func TestWatchThrottle(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := t.Context()

		src := &fakeSource{}
		w, err := NewNotifier(ctx, src)
		if err != nil {
			t.Fatal(err)
		}

		const throttle = 2 * time.Second
		ch := w.Listen(ctx, throttle, 0)
		<-ch

		src.bump()
		time.Sleep(pollInterval)
		synctest.Wait()
		recvCh(t, ch, "first bump")

		src.bump()
		time.Sleep(pollInterval)
		synctest.Wait()
		assertEmpty(t, ch, "within throttle window")

		time.Sleep(throttle)
		synctest.Wait()
		recvCh(t, ch, "after throttle window")
	})
}

func TestWatchMaxInterval(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := t.Context()

		w, err := NewNotifier(ctx, &fakeSource{})
		if err != nil {
			t.Fatal(err)
		}

		const maxInterval = 3 * time.Second
		ch := w.Listen(ctx, 0, maxInterval)
		<-ch

		assertEmpty(t, ch, "before max interval")

		time.Sleep(maxInterval)
		synctest.Wait()

		recvCh(t, ch, "after max interval")
	})
}

func TestWatchCancelClosesChannel(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := t.Context()

		src := &fakeSource{}
		w, err := NewNotifier(ctx, src)
		if err != nil {
			t.Fatal(err)
		}

		listenCtx, cancel := context.WithCancel(ctx)
		ch := w.Listen(listenCtx, 0, 0)
		<-ch
		cancel()
		synctest.Wait()

		if _, ok := <-ch; ok {
			t.Fatal("expected channel closed after cancel")
		}
	})
}

type fakeSource struct {
	v atomic.Int64
}

func (f *fakeSource) Version(context.Context) (int64, error) { return f.v.Load(), nil }
func (f *fakeSource) Close() error                           { return nil }
func (f *fakeSource) bump()                                  { f.v.Add(1) }

func recvCh(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	default:
		t.Fatalf("expected receive: %s", what)
	}
}

func assertEmpty(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
		t.Fatalf("expected empty: %s", what)
	default:
	}
}
