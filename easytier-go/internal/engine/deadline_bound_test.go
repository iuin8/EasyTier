package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/easytier/easytier/easytier-go/internal/coreabi"
)

func TestSetResourceDeadlineHonorsContext(t *testing.T) {
	instance := &Instance{dataPlaneCommands: make(chan dataPlaneCommand)}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := instance.setDataPlaneResourceDeadlineContext(
		ctx, coreabi.ResourceID(1), coreabi.DeadlineWrite, time.Time{},
	)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("set deadline error = %v, want context deadline exceeded", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("set deadline blocked for %v", elapsed)
	}
}

func TestPerformOperationSubmitResponseHonorsContext(t *testing.T) {
	commands := make(chan dataPlaneCommand, 1)
	instance := &Instance{dataPlaneCommands: commands}
	go func() {
		<-commands // accept the request but never answer it
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := instance.performOperationNotify(
		ctx,
		coreabi.OperationTCPShutdownWrite,
		func(context.Context, dataPlaneCore) (coreabi.OperationID, error) {
			return coreabi.OperationID(1), nil
		},
		nil,
	)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("submit error = %v, want context deadline exceeded", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("submit blocked for %v", elapsed)
	}
}

func TestCloseHonorsBudgetWithStuckCommandLoop(t *testing.T) {
	instance := &Instance{dataPlaneCommands: make(chan dataPlaneCommand)}
	conn := &streamConn{
		instance:  instance,
		resource:  coreabi.ResourceID(1),
		closeDone: make(chan struct{}),
	}
	start := time.Now()
	err := conn.Close()
	elapsed := time.Since(start)
	if elapsed > 3*time.Second {
		t.Fatalf("Close blocked for %v", elapsed)
	}
	if err == nil {
		t.Fatal("Close with a stuck command loop must report the failure")
	}
}

func TestCloseAttemptsReleaseAfterShutdownTimeout(t *testing.T) {
	commands := make(chan dataPlaneCommand, 8)
	instance := &Instance{dataPlaneCommands: commands}
	releaseSeen := make(chan struct{})
	go func() {
		for command := range commands {
			if command.kind == dataPlaneCloseResource {
				close(releaseSeen)
			}
			// Never answer: simulate a wedged guest command loop.
		}
	}()
	conn := &streamConn{
		instance:  instance,
		resource:  coreabi.ResourceID(1),
		closeDone: make(chan struct{}),
	}
	start := time.Now()
	err := conn.Close()
	elapsed := time.Since(start)
	select {
	case <-releaseSeen:
	case <-time.After(time.Second):
		t.Fatal("Close did not send dataPlaneCloseResource")
	}
	if elapsed > 3*time.Second {
		t.Fatalf("Close blocked for %v", elapsed)
	}
	if err == nil {
		t.Fatal("Close with a stuck command loop must report the shutdown failure")
	}
}
