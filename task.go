package pond

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
)

var ErrPanic = errors.New("task panicked")

var ErrContextCanceled = errors.New("context canceled")

// ErrDrainDeadline is returned by tasks that were still queued when a
// deadline-driven drain (Pool.Drain) expired. It wraps context.DeadlineExceeded
// so callers can detect the timeout either by errors.Is(err, ErrDrainDeadline)
// or errors.Is(err, context.DeadlineExceeded).
var ErrDrainDeadline = errors.New("pool drain deadline exceeded")

type wrappedTask[R any, C func(error) | func(R, error)] struct {
	task          any
	callback      C
	gateCtx       context.Context
	panicRecovery bool
}

func (t wrappedTask[R, C]) Run() error {
	var result R
	var err error

	// The gate context is only consulted before the user code runs, so a
	// deadline-driven drain cancels tasks that are still queued without
	// resolving futures of tasks already executing.
	if t.gateCtx != nil {
		// Use the context cause so explicit cancellation causes (e.g. the drain
		// deadline) are preserved instead of being flattened to context.Canceled.
		if ctxErr := context.Cause(t.gateCtx); ctxErr != nil {
			err = errors.Join(ErrContextCanceled, ctxErr)
		}
	}

	if err == nil {
		result, err = invokeTask[R](t.task, t.panicRecovery)
	}

	switch c := any(t.callback).(type) {
	case func(error):
		c(err)
	case func(R, error):
		c(result, err)
	default:
		panic(fmt.Sprintf("unsupported callback type: %#v", t.callback))
	}

	return err
}

func wrapTask[R any, C func(error) | func(R, error)](task any, callback C, ctx context.Context, panicRecovery bool) func() error {
	wrapped := &wrappedTask[R, C]{
		task:          task,
		callback:      callback,
		gateCtx:       ctx,
		panicRecovery: panicRecovery,
	}

	return wrapped.Run
}

// wrapTaskWithGate wraps a task whose completion is reported through callback
// but whose execution before running the user code is gated by gateCtx. The
// gate is distinct from the future's context so a drain deadline can skip
// queued tasks without canceling futures of tasks already in flight.
func wrapTaskWithGate[R any, C func(error) | func(R, error)](task any, callback C, gateCtx context.Context, panicRecovery bool) func() error {
	wrapped := &wrappedTask[R, C]{
		task:          task,
		callback:      callback,
		gateCtx:       gateCtx,
		panicRecovery: panicRecovery,
	}

	return wrapped.Run
}

func invokeTask[R any](task any, panicRecovery bool) (output R, err error) {
	if panicRecovery {
		defer func() {
			if p := recover(); p != nil {
				if e, ok := p.(error); ok {
					err = fmt.Errorf("%w: %w, %s", ErrPanic, e, debug.Stack())
				} else {
					err = fmt.Errorf("%w: %v, %s", ErrPanic, p, debug.Stack())
				}
				return
			}
		}()
	}

	switch t := any(task).(type) {
	case func():
		t()
	case func() error:
		err = t()
	case func() R:
		output = t()
	case func() (R, error):
		output, err = t()
	default:
		panic(fmt.Sprintf("unsupported task type: %#v", task))
	}
	return
}
