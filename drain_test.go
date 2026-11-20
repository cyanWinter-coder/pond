package pond

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alitto/pond/v2/internal/assert"
)

// TestPoolDrainExecutesQueuedTasksBeforeDeadline covers requirements 1 and 2:
// new tasks are rejected when draining starts and queued tasks run in order
// before the deadline.
func TestPoolDrainExecutesQueuedTasksBeforeDeadline(t *testing.T) {
	pool := NewPool(1)

	var mu sync.Mutex
	executed := make([]int, 0)

	tasks := make([]Task, 5)
	for i := 0; i < 5; i++ {
		i := i
		tasks[i] = pool.Submit(func() {
			mu.Lock()
			executed = append(executed, i)
			mu.Unlock()
		})
	}

	// A deadline far enough away that every queued task can run.
	pool.DrainAndWait(time.Now().Add(5 * time.Second))

	// Requirement 1: no new tasks are accepted after draining starts.
	err := pool.Go(func() {})
	assert.Equal(t, ErrPoolStopped, err)
	_, ok := pool.TrySubmit(func() {})
	assert.Equal(t, false, ok)
	assert.True(t, pool.Stopped())

	// Requirement 2: every queued task ran, in submission order.
	assert.Equal(t, 5, len(executed))
	for i, n := range executed {
		assert.Equal(t, i, n)
		assert.Equal(t, nil, tasks[i].Wait())
	}

	// 5 user tasks plus the Drain bookkeeping task submitted via package-level Submit.
	assert.Equal(t, uint64(6), pool.SubmittedTasks())
	assert.Equal(t, uint64(5), pool.CompletedTasks())
	assert.Equal(t, uint64(5), pool.SuccessfulTasks())
	assert.Equal(t, uint64(0), pool.CanceledTasks())
	assert.Equal(t, uint64(0), pool.WaitingTasks())
	assert.Equal(t, int64(0), pool.RunningWorkers())
}

// TestPoolDrainCancelsQueuedTasksAtDeadline covers requirements 3, 4 and 5:
// tasks still queued when the deadline expires get an explicit context error,
// each future resolves exactly once and running tasks are not faked as done.
func TestPoolDrainCancelsQueuedTasksAtDeadline(t *testing.T) {
	pool := NewPool(1)

	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})

	var runningTaskDone atomic.Bool
	first := pool.Submit(func() {
		close(firstStarted)
		<-releaseFirst
		runningTaskDone.Store(true)
	})

	var queuedRan atomic.Bool
	second := pool.Submit(func() {
		queuedRan.Store(true)
	})

	<-firstStarted

	// Deadline expires while the only worker is still in the first task.
	drainDone := make(chan struct{})
	go func() {
		pool.DrainAndWait(time.Now().Add(50 * time.Millisecond))
		close(drainDone)
	}()

	// Requirement 5: the running task is not resolved while still executing.
	select {
	case <-first.Done():
		t.Fatal("running task was resolved before finishing")
	case <-time.After(80 * time.Millisecond):
	}

	// Requirement 3: the queued task is still pending until the worker gets to it.
	select {
	case <-second.Done():
		t.Fatal("queued task was resolved before the worker reached it")
	default:
	}

	// Let the running task finish; the worker then reaches the queued task,
	// observes the expired deadline and short-circuits it. The drain completes
	// once every queued task has been accounted for.
	close(releaseFirst)

	select {
	case <-drainDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Drain did not complete after the running task finished")
	}

	err := second.Wait()
	assert.True(t, errors.Is(err, ErrContextCanceled))
	assert.True(t, errors.Is(err, context.DeadlineExceeded))
	assert.True(t, errors.Is(err, ErrDrainDeadline))
	assert.Equal(t, false, queuedRan.Load())

	// Requirement 5: the running task keeps its own (successful) result.
	assert.Equal(t, nil, first.Wait())
	assert.True(t, runningTaskDone.Load())

	// Requirement 6: counters follow the existing cancellation accounting.
	assert.Equal(t, uint64(2), pool.SubmittedTasks())
	assert.Equal(t, uint64(1), pool.CompletedTasks())
	assert.Equal(t, uint64(1), pool.SuccessfulTasks())
	assert.Equal(t, uint64(1), pool.CanceledTasks())
	assert.Equal(t, uint64(0), pool.FailedTasks())
	assert.Equal(t, uint64(0), pool.DroppedTasks())
	assert.Equal(t, uint64(0), pool.WaitingTasks())
	assert.Equal(t, int64(0), pool.RunningWorkers())
}

// TestPoolDrainExpiredDeadline verifies an already-passed deadline cancels all
// queued tasks as soon as workers reach them, while running tasks are left
// untouched and keep their own result.
func TestPoolDrainExpiredDeadline(t *testing.T) {
	pool := NewPool(1)

	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})

	first := pool.Submit(func() {
		close(firstStarted)
		<-releaseFirst
	})

	second := pool.Submit(func() {})
	third := pool.Submit(func() {})

	<-firstStarted

	// Give the timer enough time to fire before the worker reaches the queue.
	drainDone := make(chan struct{})
	go func() {
		pool.DrainAndWait(time.Now().Add(30 * time.Millisecond))
		close(drainDone)
	}()

	// The running task is not resolved by the drain deadline.
	select {
	case <-first.Done():
		t.Fatal("running task was resolved by the expired deadline")
	case <-time.After(60 * time.Millisecond):
	}

	close(releaseFirst)

	select {
	case <-drainDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Drain did not complete after the running task finished")
	}

	assert.True(t, errors.Is(second.Wait(), context.DeadlineExceeded))
	assert.True(t, errors.Is(third.Wait(), context.DeadlineExceeded))
	assert.Equal(t, nil, first.Wait())

	assert.Equal(t, uint64(2), pool.CanceledTasks())
	assert.Equal(t, uint64(1), pool.SuccessfulTasks())
	assert.Equal(t, uint64(3), pool.SubmittedTasks())
}

// TestPoolDrainResolvesEachFutureOnce covers requirement 4: each task future
// is delivered exactly one outcome regardless of when the deadline fires.
func TestPoolDrainResolvesEachFutureOnce(t *testing.T) {
	pool := NewPool(2)

	const taskCount = 200
	tasks := make([]Task, taskCount)

	for i := 0; i < taskCount; i++ {
		tasks[i] = pool.Submit(func() {
			time.Sleep(time.Millisecond)
		})
	}

	// Deadline in the middle of the workload: some tasks run, some are canceled.
	pool.DrainAndWait(time.Now().Add(20 * time.Millisecond))

	for _, task := range tasks {
		// Waiting repeatedly must return the same outcome (the underlying
		// future's cancel/resolve is idempotent).
		err1 := task.Wait()
		err2 := task.Wait()
		assert.Equal(t, err1 == nil, err2 == nil)
		if err1 != nil {
			assert.True(t, errors.Is(err1, ErrContextCanceled))
		}
	}

	// Requirement 6: every submitted task is accounted for exactly once.
	assert.Equal(t, uint64(taskCount),
		pool.CompletedTasks()+pool.CanceledTasks())
	assert.Equal(t, uint64(0), pool.WaitingTasks())
	assert.Equal(t, int64(0), pool.RunningWorkers())
}

// TestPoolDrainResultPool verifies result-returning tasks get the deadline
// error with their zero value instead of a fabricated result.
func TestPoolDrainResultPool(t *testing.T) {
	pool := NewResultPool[int](1)

	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})

	first := pool.Submit(func() int {
		close(firstStarted)
		<-releaseFirst
		return 42
	})
	second := pool.Submit(func() int {
		return 7
	})

	<-firstStarted

	drainDone := make(chan struct{})
	go func() {
		pool.DrainAndWait(time.Now().Add(30 * time.Millisecond))
		close(drainDone)
	}()

	// Let the deadline expire while the only worker is still in the first task.
	time.Sleep(60 * time.Millisecond)
	close(releaseFirst)

	select {
	case <-drainDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Drain did not complete after the running task finished")
	}

	value, err := second.Wait()
	assert.True(t, errors.Is(err, context.DeadlineExceeded))
	assert.Equal(t, 0, value)

	value, err = first.Wait()
	assert.Equal(t, nil, err)
	assert.Equal(t, 42, value)
}

// TestPoolDrainSubpool covers requirement 6 for subpools: draining a subpool
// stops intake, runs queued work until the deadline and accounts for canceled
// tasks through the subpool's own counters.
func TestPoolDrainSubpool(t *testing.T) {
	parent := NewPool(4)
	subpool := parent.NewSubpool(1)

	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})

	first := subpool.Submit(func() {
		close(firstStarted)
		<-releaseFirst
	})
	second := subpool.Submit(func() {})

	<-firstStarted

	drainDone := make(chan struct{})
	go func() {
		subpool.DrainAndWait(time.Now().Add(30 * time.Millisecond))
		close(drainDone)
	}()

	// Let the deadline expire while the subpool worker is still in the first task.
	time.Sleep(60 * time.Millisecond)
	close(releaseFirst)

	select {
	case <-drainDone:
	case <-time.After(2 * time.Second):
		t.Fatal("subpool Drain did not complete after the running task finished")
	}

	assert.True(t, errors.Is(second.Wait(), context.DeadlineExceeded))
	assert.True(t, subpool.Stopped())
	assert.Equal(t, nil, first.Wait())

	assert.Equal(t, uint64(1), subpool.CanceledTasks())
	assert.Equal(t, uint64(1), subpool.SuccessfulTasks())
	assert.Equal(t, uint64(0), subpool.WaitingTasks())
	assert.Equal(t, int64(0), subpool.RunningWorkers())

	// The parent pool keeps working and stops cleanly.
	assert.Equal(t, nil, parent.Stop().Wait())
}

// TestPoolDrainIsIdempotent verifies repeated drain/stop calls after a drain
// behave like stop on an already stopped pool.
func TestPoolDrainIsIdempotent(t *testing.T) {
	pool := NewPool(2)

	pool.Submit(func() {})
	pool.DrainAndWait(time.Now().Add(time.Second))

	assert.Equal(t, nil, pool.Drain(time.Now()).Wait())
	assert.Equal(t, nil, pool.Stop().Wait())
	assert.True(t, pool.Stopped())
}

// TestPoolDrainReturnsTask verifies the returned future can be waited on via
// its Done channel and resolves without an error once the drain is finished.
func TestPoolDrainReturnsTask(t *testing.T) {
	pool := NewPool(2)
	pool.Submit(func() {})

	drain := pool.Drain(time.Now().Add(time.Second))

	select {
	case <-drain.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("drain task did not complete")
	}

	assert.Equal(t, nil, drain.Wait())
}

// TestPoolDrainRunningTaskPanic verifies a task that panics before the
// deadline is still accounted as failed (not canceled or faked successful).
func TestPoolDrainRunningTaskPanic(t *testing.T) {
	pool := NewPool(1)

	panicking := pool.Submit(func() {
		panic(errors.New("boom"))
	})

	pool.DrainAndWait(time.Now().Add(time.Second))

	err := panicking.Wait()
	assert.True(t, errors.Is(err, ErrPanic))
	assert.Equal(t, uint64(1), pool.FailedTasks())
	assert.Equal(t, uint64(0), pool.CanceledTasks())
	assert.Equal(t, uint64(0), pool.SuccessfulTasks())
}

// TestPoolDrainGoTasks verifies fire-and-forget tasks submitted via Go are
// skipped at the deadline and accounted as canceled, while earlier ones run.
func TestPoolDrainGoTasks(t *testing.T) {
	pool := NewPool(1)

	started := make(chan struct{})
	release := make(chan struct{})

	assert.Equal(t, nil, pool.Go(func() {
		close(started)
		<-release
	}))

	var ran atomic.Bool
	assert.Equal(t, nil, pool.Go(func() {
		ran.Store(true)
	}))

	<-started

	drainDone := make(chan struct{})
	go func() {
		pool.DrainAndWait(time.Now().Add(30 * time.Millisecond))
		close(drainDone)
	}()

	time.Sleep(60 * time.Millisecond)
	close(release)

	select {
	case <-drainDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Drain did not complete")
	}

	assert.Equal(t, false, ran.Load())
	assert.Equal(t, uint64(1), pool.CanceledTasks())
	assert.Equal(t, uint64(1), pool.SuccessfulTasks())
}

// TestPoolDrainOrderWithMultipleWorkers verifies that with several workers,
// tasks keep executing in order until the deadline and the remaining suffix
// of the queue is the one that gets canceled.
func TestPoolDrainOrderWithMultipleWorkers(t *testing.T) {
	pool := NewPool(4)

	const total = 40
	var executed atomic.Int64
	tasks := make([]Task, total)
	for i := 0; i < total; i++ {
		tasks[i] = pool.Submit(func() {
			time.Sleep(5 * time.Millisecond)
			executed.Add(1)
		})
	}

	pool.DrainAndWait(time.Now().Add(15 * time.Millisecond))

	executedCount := executed.Load()
	assert.True(t, executedCount > 0)
	assert.True(t, executedCount < total)

	var canceledCount uint64
	for _, task := range tasks {
		if err := task.Wait(); err != nil {
			assert.True(t, errors.Is(err, context.DeadlineExceeded))
			canceledCount++
		}
	}

	assert.Equal(t, uint64(executedCount), pool.SuccessfulTasks())
	assert.Equal(t, canceledCount, pool.CanceledTasks())
	assert.Equal(t, int64(total), executedCount+int64(canceledCount))
	assert.Equal(t, uint64(0), pool.WaitingTasks())
	assert.Equal(t, int64(0), pool.RunningWorkers())
}

// TestPoolDrainAfterContextCanceled keeps the existing cancellation behavior
// consistent: an externally canceled pool drains queued tasks with the
// context error and Drain still shuts the pool down cleanly.
func TestPoolDrainAfterContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool := NewPool(1, WithContext(ctx))

	started := make(chan struct{})
	release := make(chan struct{})

	first := pool.Submit(func() {
		close(started)
		<-release
	})
	second := pool.Submit(func() {})

	<-started
	cancel()

	drainDone := make(chan struct{})
	go func() {
		pool.DrainAndWait(time.Now().Add(time.Second))
		close(drainDone)
	}()

	close(release)

	select {
	case <-drainDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Drain did not complete after context cancellation")
	}

	// External context cancellation follows the existing semantics: both the
	// queued and the running task resolve with the context cancellation error.
	assert.True(t, errors.Is(second.Wait(), context.Canceled))
	assert.True(t, errors.Is(first.Wait(), context.Canceled))
	assert.True(t, pool.Stopped())
	assert.Equal(t, uint64(0), pool.WaitingTasks())
	assert.Equal(t, int64(0), pool.RunningWorkers())
}
