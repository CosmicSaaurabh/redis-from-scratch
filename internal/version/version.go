// Package version carries build identity.
//
// Version and Commit are set at link time by the build, which is why they are
// variables rather than constants. A binary that cannot say which commit it is
// running makes an incident twice as long.
package version

import (
	"crypto/rand"
	"encoding/hex"
	"runtime/debug"
)

var (
	// Version is the semantic version, overridden with -ldflags.
	Version = "0.3.0-dev"
	// Commit is the git revision, overridden with -ldflags.
	Commit = ""
)

// String renders the full build identity.
func String() string {
	if Commit != "" {
		return Version + "+" + Commit
	}
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, s := range info.Settings {
			if s.Key == "vcs.revision" && len(s.Value) >= 7 {
				return Version + "+" + s.Value[:7]
			}
		}
	}
	return Version
}

// NewRunID returns a random identifier for this process instance.
//
// Redis clients and replication protocols use a run id to detect that a server
// restarted underneath them, so it must be fresh on every start rather than
// derived from anything stable like the hostname or the config.
func NewRunID() string {
	var b [20]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand failing is a broken system, but a run id is not worth
		// refusing to boot over; a fixed value only degrades restart detection.
		return "0000000000000000000000000000000000000000"
	}
	return hex.EncodeToString(b[:])
}
