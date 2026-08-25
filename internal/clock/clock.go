// Package clock abstracts wall time so that expiry logic can be tested
// deterministically instead of with sleeps.
//
// Every TTL decision in the server reads time through this interface. Tests
// that would otherwise need a real one-second sleep to observe an expiry
// instead advance a Mock by one second, which keeps the suite fast and, more
// importantly, keeps it from being flaky on a loaded CI machine.
package clock

import (
	"sync/atomic"
	"time"
)

// Clock reports the current time.
type Clock interface {
	// NowMs returns Unix time in milliseconds.
	NowMs() int64
	// Now returns the current wall time.
	Now() time.Time
}

// System is the real clock.
type System struct{}

// NowMs returns Unix milliseconds from the system clock.
func (System) NowMs() int64 { return time.Now().UnixMilli() }

// Now returns the system wall time.
func (System) Now() time.Time { return time.Now() }

// Mock is a manually advanced clock for tests.
type Mock struct{ ms atomic.Int64 }

// NewMock returns a Mock positioned at t.
func NewMock(t time.Time) *Mock {
	m := &Mock{}
	m.ms.Store(t.UnixMilli())
	return m
}

// NowMs returns the mock's current Unix milliseconds.
func (m *Mock) NowMs() int64 { return m.ms.Load() }

// Now returns the mock's current wall time.
func (m *Mock) Now() time.Time { return time.UnixMilli(m.ms.Load()) }

// Advance moves the mock forward by d.
func (m *Mock) Advance(d time.Duration) { m.ms.Add(d.Milliseconds()) }

// SetMs positions the mock at an absolute Unix millisecond value.
func (m *Mock) SetMs(ms int64) { m.ms.Store(ms) }
