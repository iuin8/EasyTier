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
