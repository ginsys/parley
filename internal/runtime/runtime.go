// Package runtime owns the database lifecycle. It is not a daemon or a host
// authenticator; executable, endpoint and durable recovery wiring are separate.
package runtime

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/ginsys/parley/internal/store"
)

// Zero and unknown modes are never treated as permission to admit traffic.
type RecoveryMode uint8

const (
	Normal RecoveryMode = iota + 1
	Held
)

type Resources struct {
	// WorkerContext owns admitted work, independently of startup cancellation.
	// Services must use this context for workers, not Start's initialization context.
	WorkerContext context.Context
	Writer        *store.DB
	Queries       store.Queries
	Mode          RecoveryMode
}

// Service is trusted wiring, not an agent-selected capability. Start may publish
// admission only after runtime initializes resources. StopAdmission must stop new
// work promptly, without waiting for workers. Wait joins all workers and their
// independent outcome settlement. Both must work after a partially failed Start.
// Start's context is for initialization only and is cancelled when startup ends.
// Admitted work uses Resources.WorkerContext. Start must honor cancellation and
// return before cleanup calls StopAdmission, including on partial initialization.
type Service interface {
	Start(context.Context, Resources) error
	StopAdmission() error
	Wait() error
}

type Registration struct {
	Service      Service
	RecoveryOnly bool
}

type Config struct {
	DatabasePath string
	// InspectRecovery must check the installation's trusted external and durable
	// recovery evidence. There is deliberately no default normal inspector.
	// It must not start workers, open readers, or clear holds. Held skips all
	// automatic interrupted-dispatch mutation in this foundation slice.
	InspectRecovery func(context.Context, *store.DB) (RecoveryMode, error)
	Services        []Registration
}

// Runtime holds ownership until Stop's asynchronous cleanup actually completes.
// Cancelling a Stop wait never abandons cleanup or releases a live writer.
type Runtime struct {
	ownership *Ownership
	db        *store.DB
	services  []Service
	cancel    context.CancelFunc
	done      chan struct{}
	once      sync.Once
	err       error
}

func Start(ctx context.Context, cfg Config) (*Runtime, error) {
	return start(ctx, cfg, store.OpenExisting)
}

func start(ctx context.Context, cfg Config, open func(context.Context, string) (*store.DB, error)) (_ *Runtime, err error) {
	if cfg.InspectRecovery == nil {
		return nil, fmt.Errorf("recovery inspector required")
	}
	for _, registration := range cfg.Services {
		if registration.Service == nil {
			return nil, fmt.Errorf("service required")
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	ownership, err := Acquire(cfg.DatabasePath)
	if err != nil {
		return nil, err
	}
	workers, cancel := context.WithCancel(context.WithoutCancel(ctx))
	r := &Runtime{ownership: ownership, cancel: cancel, done: make(chan struct{})}
	// Initialization may be interrupted without killing workers of services
	// that already admit work. Cleanup always stops admission before workers.
	startup, cancelStartup := context.WithCancel(ctx)
	defer func() {
		cancelStartup()
		if err != nil {
			r.beginStop()
			<-r.done
			err = errors.Join(err, r.err)
		}
	}()
	r.db, err = open(ctx, ownership.Path())
	if err != nil {
		return nil, err
	}
	mode, err := cfg.InspectRecovery(ctx, r.db)
	if err != nil {
		return nil, fmt.Errorf("inspect recovery: %w", err)
	}
	switch mode {
	case Normal:
		if _, err := r.db.RecoverUncertain(ctx); err != nil {
			return nil, fmt.Errorf("recover interrupted dispatch: %w", err)
		}
	case Held:
	default:
		return nil, fmt.Errorf("unknown recovery mode")
	}
	if err := r.db.OpenReaders(ctx); err != nil {
		return nil, fmt.Errorf("open readers: %w", err)
	}
	resources := Resources{WorkerContext: workers, Writer: r.db, Queries: r.db.Queries(), Mode: mode}
	for _, registration := range cfg.Services {
		if mode == Held && !registration.RecoveryOnly {
			continue
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		// Register before Start so partial initialization is included in cleanup.
		r.services = append(r.services, registration.Service)
		if err := registration.Service.Start(startup, resources); err != nil {
			return nil, fmt.Errorf("start service: %w", err)
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	go func() {
		select {
		case <-ctx.Done():
			r.beginStop()
		case <-r.done:
		}
	}()
	return r, nil
}

func (r *Runtime) beginStop() {
	r.once.Do(func() {
		go func() {
			for i := len(r.services) - 1; i >= 0; i-- {
				r.err = errors.Join(r.err, r.services[i].StopAdmission())
			}
			r.cancel()
			for i := len(r.services) - 1; i >= 0; i-- {
				r.err = errors.Join(r.err, r.services[i].Wait())
			}
			if r.db != nil {
				r.err = errors.Join(r.err, r.db.Close())
			}
			r.err = errors.Join(r.err, r.ownership.Close())
			close(r.done)
		}()
	})
}

func (r *Runtime) Stop(ctx context.Context) error {
	r.beginStop()
	select {
	case <-r.done:
		return r.err
	case <-ctx.Done():
		return ctx.Err()
	}
}
