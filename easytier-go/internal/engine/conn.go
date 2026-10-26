package engine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/easytier/easytier/easytier-go/internal/coreabi"
)

const maxDataPlaneTransfer = 1024 * 1024

type streamConn struct {
	instance *Instance
	resource coreabi.ResourceID
	local    netip.AddrPort
	peer     netip.AddrPort

	readMu  sync.Mutex
	writeMu sync.Mutex

	closed    atomic.Bool
	closeOnce sync.Once
	closeDone chan struct{}
	closeErr  error
}

func newStreamConn(
	instance *Instance,
	result coreabi.OperationResult,
) *streamConn {
	return &streamConn{
		instance:  instance,
		resource:  result.Resource,
		local:     result.Local,
		peer:      result.Peer,
		closeDone: make(chan struct{}),
	}
}

func (conn *streamConn) Read(buffer []byte) (int, error) {
	if len(buffer) == 0 {
		return 0, nil
	}
	conn.readMu.Lock()
	defer conn.readMu.Unlock()
	if conn.closed.Load() {
		return 0, net.ErrClosed
	}
	maximum := len(buffer)
	if maximum > maxDataPlaneTransfer {
		maximum = maxDataPlaneTransfer
	}
	result, err := conn.instance.performOperation(
		context.Background(),
		coreabi.OperationTCPRead,
		func(
			callCtx context.Context,
			core dataPlaneCore,
		) (coreabi.OperationID, error) {
			return core.SubmitTCPRead(
				callCtx,
				conn.resource,
				uint32(maximum),
			)
		},
	)
	if err != nil {
		return 0, normalizeDeadlineError(err)
	}
	n := copy(buffer, result.Data)
	if n == 0 && result.EOF {
		return 0, io.EOF
	}
	return n, nil
}

func (conn *streamConn) Write(buffer []byte) (int, error) {
	if len(buffer) == 0 {
		return 0, nil
	}
	conn.writeMu.Lock()
	defer conn.writeMu.Unlock()
	if conn.closed.Load() {
		return 0, net.ErrClosed
	}
	written := 0
	for written < len(buffer) {
		end := written + maxDataPlaneTransfer
		if end > len(buffer) {
			end = len(buffer)
		}
		chunk := buffer[written:end]
		result, err := conn.instance.performOperation(
			context.Background(),
			coreabi.OperationTCPWrite,
			func(
				callCtx context.Context,
				core dataPlaneCore,
			) (coreabi.OperationID, error) {
				return core.SubmitTCPWrite(
					callCtx,
					conn.resource,
					chunk,
				)
			},
		)
		if err != nil {
			return written, normalizeDeadlineError(err)
		}
		if result.Length <= 0 || result.Length > len(chunk) {
			return written, io.ErrShortWrite
		}
		written += result.Length
	}
	return written, nil
}

// tcpShutdownTimeout bounds how long Close waits for the guest to emit FIN.
//
// Two seconds is enough for a healthy loopback/wazero round trip (the observed
// shutdown completion is sub-millisecond) while keeping a wedged guest from
// holding one goroutine per closed connection. On timeout the resource is still
// released below, so the guest-side handle can never leak; only the FIN is lost,
// which is the same outcome as a peer that vanished.
const tcpShutdownTimeout = 2 * time.Second

func (conn *streamConn) Close() error {
	conn.closeOnce.Do(func() {
		conn.closed.Store(true)
		// Half-close first: dropping the resource alone never emits FIN, so the
		// peer keeps the connection ESTABLISHED (and an exit node's forwarding
		// task waits on it forever). Release the resource in every case.
		//
		// Close must not inherit an application deadline: the guest applies the
		// stream's write deadline to shutdown(), so an expired SetDeadline or
		// SetWriteDeadline would turn the FIN into a deadline error. Clear the
		// write deadline first, bounded by the same shutdown budget.
		shutdownCtx, cancel := context.WithTimeout(context.Background(), tcpShutdownTimeout)
		clearCtx, cancelClear := context.WithTimeout(shutdownCtx, tcpShutdownTimeout/2)
		_ = conn.instance.setDataPlaneResourceDeadlineContext(
			clearCtx,
			conn.resource,
			coreabi.DeadlineWrite,
			time.Time{},
		)
		cancelClear()
		// The guest shutdown closes the write half and then waits for the peer
		// to ACK our FIN. After a sequential close the peer is already gone and
		// no ACK can arrive, so this wait always ends in a deadline. Only that
		// post-submit deadline is expected: if the operation never reached the
		// guest (submit-phase timeout), Close still reports it.
		submitted := false
		_, shutdownErr := conn.instance.performOperationNotify(
			shutdownCtx,
			coreabi.OperationTCPShutdownWrite,
			func(
				callCtx context.Context,
				core dataPlaneCore,
			) (coreabi.OperationID, error) {
				return core.SubmitTCPShutdownWrite(callCtx, conn.resource)
			},
			func() { submitted = true },
		)
		cancel()
		switch {
		case shutdownErr == nil:
		case submitted && errors.Is(shutdownErr, context.DeadlineExceeded):
			// FIN was accepted by the guest; only the ACK wait expired.
			shutdownErr = nil
		default:
			shutdownErr = fmt.Errorf("shutdown write: %w", shutdownErr)
		}
		conn.closeErr = shutdownErr
		if err := conn.instance.closeDataPlaneResource(conn.resource); err != nil && conn.closeErr == nil {
			conn.closeErr = err
		}
		close(conn.closeDone)
	})

	<-conn.closeDone
	return conn.closeErr
}

func (conn *streamConn) LocalAddr() net.Addr {
	return net.TCPAddrFromAddrPort(conn.local)
}

func (conn *streamConn) RemoteAddr() net.Addr {
	return net.TCPAddrFromAddrPort(conn.peer)
}

func (conn *streamConn) SetDeadline(deadline time.Time) error {
	if conn.closed.Load() {
		return net.ErrClosed
	}
	return conn.instance.setDataPlaneResourceDeadline(
		conn.resource,
		coreabi.DeadlineRead|coreabi.DeadlineWrite,
		deadline,
	)
}

func (conn *streamConn) SetReadDeadline(deadline time.Time) error {
	if conn.closed.Load() {
		return net.ErrClosed
	}
	return conn.instance.setDataPlaneResourceDeadline(
		conn.resource,
		coreabi.DeadlineRead,
		deadline,
	)
}

func (conn *streamConn) SetWriteDeadline(deadline time.Time) error {
	if conn.closed.Load() {
		return net.ErrClosed
	}
	return conn.instance.setDataPlaneResourceDeadline(
		conn.resource,
		coreabi.DeadlineWrite,
		deadline,
	)
}

func normalizeDeadlineError(err error) error {
	if errors.Is(err, context.DeadlineExceeded) {
		return os.ErrDeadlineExceeded
	}
	return err
}

func (instance *Instance) Dial(
	ctx context.Context,
	peer netip.AddrPort,
) (net.Conn, error) {
	timeout, err := contextTimeoutMillis(ctx)
	if err != nil {
		return nil, normalizeDeadlineError(err)
	}
	result, err := instance.performOperation(
		ctx,
		coreabi.OperationTCPConnect,
		func(
			callCtx context.Context,
			core dataPlaneCore,
		) (coreabi.OperationID, error) {
			return core.SubmitTCPConnect(callCtx, peer, timeout)
		},
	)
	if err != nil {
		return nil, normalizeDeadlineError(err)
	}
	return newStreamConn(instance, result), nil
}
