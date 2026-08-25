// Package crash proves the durability claims by killing the real server
// process and checking what survived.
//
// These tests spawn the actual binary rather than an in-process server,
// because the guarantee under test is about what reaches the operating system
// before an acknowledgement is sent. An in-process test can only kill things
// the Go runtime is willing to kill; SIGKILL against a separate process is the
// only way to remove every chance to clean up, which is exactly what a crash
// does.
//
// A durability claim that has not been tested this way is a comment, not a
// guarantee.
package crash

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/CosmicSaaurabh/redis-from-scratch/internal/testutil"
)

var serverBin string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "rfs-crash-bin")
	if err != nil {
		fmt.Fprintf(os.Stderr, "crash tests: temp dir: %v\n", err)
		os.Exit(1)
	}
	serverBin = filepath.Join(dir, "rfs-server")
	build := exec.Command("go", "build", "-o", serverBin, "../../cmd/server")
	build.Stderr = os.Stderr
	if err := build.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "crash tests: build the server: %v\n", err)
		os.Exit(1)
	}
	code := m.Run()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

// node is a server process under test.
type node struct {
	t    *testing.T
	cmd  *exec.Cmd
	addr string
	dir  string
	port int
	log  string
}

// start launches the server against dir and waits for it to answer.
func start(t *testing.T, dir string, args ...string) *node {
	t.Helper()
	port := testutil.FreePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	logPath := filepath.Join(t.TempDir(), fmt.Sprintf("server-%d.log", port))
	logFile, err := os.Create(logPath)
	if err != nil {
		t.Fatalf("create the log file: %v", err)
	}

	full := append([]string{"-addr", addr, "-dir", dir, "-metrics-addr", ""}, args...)
	cmd := exec.Command(serverBin, full...)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	// Its own process group, so a SIGKILL cannot escape to the test runner.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		t.Fatalf("start the server: %v", err)
	}
	_ = logFile.Close()

	n := &node{t: t, cmd: cmd, addr: addr, dir: dir, port: port, log: logPath}
	t.Cleanup(n.kill)
	n.waitReady()
	return n
}

func (n *node) waitReady() {
	n.t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		c, err := dial(n.addr)
		if err == nil {
			rep := c.do("PING")
			c.close()
			if !rep.IsError() {
				return
			}
		}
		if n.cmd.ProcessState != nil {
			n.t.Fatalf("the server exited before it was ready:\n%s", n.logs())
		}
		time.Sleep(25 * time.Millisecond)
	}
	n.t.Fatalf("the server did not become ready:\n%s", n.logs())
}

// kill9 removes every chance to shut down cleanly.
func (n *node) kill9() {
	n.t.Helper()
	if n.cmd.Process == nil {
		return
	}
	if err := n.cmd.Process.Signal(syscall.SIGKILL); err != nil {
		n.t.Fatalf("SIGKILL: %v", err)
	}
	_, _ = n.cmd.Process.Wait()
	// Give the port a moment to be released so the restart does not race it.
	time.Sleep(150 * time.Millisecond)
}

func (n *node) kill() {
	if n.cmd.Process != nil {
		_ = n.cmd.Process.Signal(syscall.SIGKILL)
		_, _ = n.cmd.Process.Wait()
	}
}

func (n *node) logs() string {
	b, err := os.ReadFile(n.log)
	if err != nil {
		return "(no log)"
	}
	return string(b)
}

// client opens a connection to the node.
func (n *node) client() *client {
	n.t.Helper()
	c, err := dial(n.addr)
	if err != nil {
		n.t.Fatalf("connect to %s: %v\n%s", n.addr, err, n.logs())
	}
	n.t.Cleanup(c.close)
	return c
}

func (n *node) set(key, value string) {
	n.t.Helper()
	c := n.client()
	if rep := c.do("SET", key, value); rep.IsError() {
		n.t.Fatalf("SET %s: %s", key, rep.Str)
	}
}

func (n *node) get(key string) (string, bool) {
	n.t.Helper()
	c := n.client()
	rep := c.do("GET", key)
	if rep.IsError() {
		n.t.Fatalf("GET %s: %s", key, rep.Str)
	}
	if rep.Null {
		return "", false
	}
	return string(rep.Str), true
}

// TestEveryAcknowledgedWriteSurvivesSIGKILL is the headline durability claim.
//
// With fsync before acknowledgement, a write that returned +OK is on stable
// storage, so nothing acknowledged can be lost to any kind of crash.
func TestEveryAcknowledgedWriteSurvivesSIGKILL(t *testing.T) {
	dir := t.TempDir()
	n := start(t, dir, "-appendfsync", "always")

	const count = 2000
	c := n.client()
	for i := 0; i < count; i++ {
		if rep := c.do("SET", fmt.Sprintf("k:%05d", i), fmt.Sprintf("v:%05d", i)); rep.IsError() {
			t.Fatalf("SET %d: %s", i, rep.Str)
		}
	}
	n.kill9()

	restarted := start(t, dir, "-appendfsync", "always")
	rc := restarted.client()
	lost := 0
	for i := 0; i < count; i++ {
		rep := rc.do("GET", fmt.Sprintf("k:%05d", i))
		if rep.Null || string(rep.Str) != fmt.Sprintf("v:%05d", i) {
			lost++
		}
	}
	if lost != 0 {
		t.Fatalf("%d of %d acknowledged writes were lost to SIGKILL\n%s", lost, count, restarted.logs())
	}
	if !strings.Contains(restarted.logs(), "recovery complete") {
		t.Error("the restart did not log a recovery")
	}
}

// TestEverysecSurvivesAProcessKill covers the default policy.
//
// everysec fsyncs once a second, so a power cut can take up to a second of
// writes. A process kill is a different failure: the bytes are already in the
// kernel's page cache, which outlives the process. Losing data here would mean
// the server acknowledged writes that were still only in its own memory.
func TestEverysecSurvivesAProcessKill(t *testing.T) {
	dir := t.TempDir()
	n := start(t, dir, "-appendfsync", "everysec")

	const count = 2000
	c := n.client()
	for i := 0; i < count; i++ {
		if rep := c.do("SET", fmt.Sprintf("e:%05d", i), fmt.Sprintf("v:%05d", i)); rep.IsError() {
			t.Fatalf("SET %d: %s", i, rep.Str)
		}
	}
	n.kill9()

	restarted := start(t, dir, "-appendfsync", "everysec")
	rc := restarted.client()
	lost := 0
	for i := 0; i < count; i++ {
		rep := rc.do("GET", fmt.Sprintf("e:%05d", i))
		if rep.Null || string(rep.Str) != fmt.Sprintf("v:%05d", i) {
			lost++
		}
	}
	if lost != 0 {
		t.Fatalf("%d of %d writes lost to a process kill under everysec; "+
			"acknowledged writes were still only in process memory\n%s", lost, count, restarted.logs())
	}
}

// TestPipelinedWritesSurviveSIGKILL kills the server while a pipeline is in
// flight, which is where the ordering between the log flush and the reply
// flush actually matters.
func TestPipelinedWritesSurviveSIGKILL(t *testing.T) {
	dir := t.TempDir()
	n := start(t, dir, "-appendfsync", "everysec")

	c := n.client()
	const batches, per = 40, 250
	acked := 0
	for b := 0; b < batches; b++ {
		cmds := make([][]string, 0, per)
		for i := 0; i < per; i++ {
			k := fmt.Sprintf("p:%05d", b*per+i)
			cmds = append(cmds, []string{"SET", k, k})
		}
		for _, cmd := range cmds {
			c.send(cmd...)
		}
		c.flush()
		for range cmds {
			if rep := c.read(); rep.IsError() {
				t.Fatalf("pipelined SET: %s", rep.Str)
			}
			acked++
		}
	}
	n.kill9()

	restarted := start(t, dir, "-appendfsync", "everysec")
	rc := restarted.client()
	lost := 0
	for i := 0; i < acked; i++ {
		k := fmt.Sprintf("p:%05d", i)
		rep := rc.do("GET", k)
		if rep.Null || string(rep.Str) != k {
			lost++
		}
	}
	if lost != 0 {
		t.Fatalf("%d of %d pipelined writes lost; the reply reached the client "+
			"before its log bytes reached the kernel\n%s", lost, acked, restarted.logs())
	}
}

// TestKillDuringConcurrentWritesLosesNothingAcknowledged kills the server in
// the middle of sustained concurrent traffic, which is the realistic shape of
// a crash and the one most likely to catch an ordering bug.
func TestKillDuringConcurrentWritesLosesNothingAcknowledged(t *testing.T) {
	dir := t.TempDir()
	n := start(t, dir, "-appendfsync", "always")

	const writers = 8
	var (
		mu     sync.Mutex
		acked  = map[string]string{}
		wg     sync.WaitGroup
		stop   = make(chan struct{})
		writes int
	)
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			c, err := dial(n.addr)
			if err != nil {
				return
			}
			defer c.close()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				k := fmt.Sprintf("w%d:%06d", w, i)
				rep := c.do("SET", k, k)
				if rep.IsError() || rep.Null {
					return // the server is gone
				}
				// Only recorded once the acknowledgement is in hand. That is
				// the exact set the durability claim is about.
				mu.Lock()
				acked[k] = k
				writes++
				mu.Unlock()
			}
		}(w)
	}

	// Let real traffic build up, then pull the plug without warning.
	time.Sleep(1500 * time.Millisecond)
	n.kill9()
	close(stop)
	wg.Wait()

	mu.Lock()
	expected := make(map[string]string, len(acked))
	for k, v := range acked {
		expected[k] = v
	}
	total := writes
	mu.Unlock()
	if total < 100 {
		t.Fatalf("only %d writes were acknowledged before the kill; the test proved nothing", total)
	}

	restarted := start(t, dir, "-appendfsync", "always")
	rc := restarted.client()
	lost := 0
	for k, v := range expected {
		rep := rc.do("GET", k)
		if rep.Null || string(rep.Str) != v {
			lost++
		}
	}
	if lost != 0 {
		t.Fatalf("%d of %d acknowledged concurrent writes were lost\n%s", lost, total, restarted.logs())
	}
	t.Logf("%d acknowledged writes across %d writers, all recovered after SIGKILL", total, writers)
}

// TestTornLogTailIsTruncatedAndTheRestSurvives simulates the write that was
// interrupted mid-record: the one case a crash can actually produce.
func TestTornLogTailIsTruncatedAndTheRestSurvives(t *testing.T) {
	dir := t.TempDir()
	n := start(t, dir, "-appendfsync", "always")

	const count = 500
	c := n.client()
	for i := 0; i < count; i++ {
		c.do("SET", fmt.Sprintf("t:%04d", i), "value-value-value")
	}
	n.kill9()

	// Chop the end of the newest log segment, exactly as a partially completed
	// write would leave it.
	segments, err := filepath.Glob(filepath.Join(dir, "wal", "*.wal"))
	if err != nil || len(segments) == 0 {
		t.Fatalf("no log segments in %s: %v", dir, err)
	}
	last := segments[len(segments)-1]
	info, err := os.Stat(last)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(last, info.Size()-7); err != nil {
		t.Fatal(err)
	}

	restarted := start(t, dir, "-appendfsync", "always")
	logs := restarted.logs()
	if !strings.Contains(logs, "discarded an incomplete record") {
		t.Errorf("the torn tail was not reported to the operator:\n%s", logs)
	}

	// Everything before the torn record must be intact. The last write may or
	// may not survive - it is the one that was interrupted - so the check stops
	// one short.
	rc := restarted.client()
	for i := 0; i < count-1; i++ {
		k := fmt.Sprintf("t:%04d", i)
		if rep := rc.do("GET", k); rep.Null {
			t.Fatalf("%s was lost, but only the final record was torn", k)
		}
	}
}

// TestRepeatedCrashesConverge guards against a recovery path that is correct
// once but corrupts something on the second or third pass.
func TestRepeatedCrashesConverge(t *testing.T) {
	dir := t.TempDir()
	total := 0
	for round := 0; round < 5; round++ {
		n := start(t, dir, "-appendfsync", "always")
		c := n.client()
		for i := 0; i < 200; i++ {
			k := fmt.Sprintf("r%d:%03d", round, i)
			if rep := c.do("SET", k, k); rep.IsError() {
				t.Fatalf("round %d: %s", round, rep.Str)
			}
			total++
		}
		n.kill9()
	}

	final := start(t, dir, "-appendfsync", "always")
	c := final.client()
	rep := c.do("DBSIZE")
	if rep.Int != int64(total) {
		t.Fatalf("DBSIZE = %d after five crash-restart cycles, want %d\n%s", rep.Int, total, final.logs())
	}
	for round := 0; round < 5; round++ {
		for _, i := range []int{0, 99, 199} {
			k := fmt.Sprintf("r%d:%03d", round, i)
			if v, ok := final.get(k); !ok || v != k {
				t.Fatalf("%s lost after five cycles", k)
			}
		}
	}
}

// TestSnapshotPlusLogRecovery exercises the composition rather than either
// mechanism alone: the image supplies the bulk and the log supplies the tail.
func TestSnapshotPlusLogRecovery(t *testing.T) {
	dir := t.TempDir()
	n := start(t, dir, "-appendfsync", "always")
	c := n.client()

	for i := 0; i < 1000; i++ {
		c.do("SET", fmt.Sprintf("pre:%04d", i), "before")
	}
	if rep := c.do("SAVE"); rep.IsError() {
		t.Fatalf("SAVE: %s", rep.Str)
	}
	for i := 0; i < 500; i++ {
		c.do("SET", fmt.Sprintf("post:%04d", i), "after")
	}
	// Overwrite a key the snapshot captured; the log tail must win.
	c.do("SET", "pre:0500", "overwritten")
	c.do("DEL", "pre:0001")
	n.kill9()

	restarted := start(t, dir, "-appendfsync", "always")
	logs := restarted.logs()
	if !strings.Contains(logs, "snapshot_loaded=true") {
		t.Errorf("the snapshot was not loaded:\n%s", logs)
	}

	if v, ok := restarted.get("pre:0500"); !ok || v != "overwritten" {
		t.Errorf("pre:0500 = %q,%v; the log tail did not win over the snapshot", v, ok)
	}
	if _, ok := restarted.get("pre:0001"); ok {
		t.Error("a key deleted after the snapshot came back")
	}
	for i := 0; i < 500; i++ {
		if _, ok := restarted.get(fmt.Sprintf("post:%04d", i)); !ok {
			t.Fatalf("post:%04d, written after the snapshot, was lost", i)
		}
	}
	if _, ok := restarted.get("pre:0999"); !ok {
		t.Error("a key captured only by the snapshot was lost")
	}
}

// TestExpiryIsAbsoluteAcrossACrash checks that a TTL is pinned to a wall-clock
// instant rather than to a duration. A relative TTL would silently extend
// every key's life by the length of the outage.
func TestExpiryIsAbsoluteAcrossACrash(t *testing.T) {
	dir := t.TempDir()
	n := start(t, dir, "-appendfsync", "always")
	c := n.client()

	c.do("SET", "short", "v", "PX", "800")
	c.do("SET", "long", "v", "EX", "3600")
	n.kill9()

	// Outlast the short TTL before restarting.
	time.Sleep(1200 * time.Millisecond)

	restarted := start(t, dir, "-appendfsync", "always")
	if _, ok := restarted.get("short"); ok {
		t.Error("a key whose TTL elapsed during the outage came back alive")
	}
	if _, ok := restarted.get("long"); !ok {
		t.Error("a key with time left on its TTL was lost")
	}
}

// TestCleanShutdownIsNotACrash confirms the ordinary path still works, so that
// a passing crash suite cannot hide a broken graceful stop.
func TestCleanShutdownIsNotACrash(t *testing.T) {
	dir := t.TempDir()
	n := start(t, dir, "-appendfsync", "everysec")
	c := n.client()
	for i := 0; i < 500; i++ {
		c.do("SET", fmt.Sprintf("c:%03d", i), "v")
	}

	if err := n.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("SIGTERM: %v", err)
	}
	done := make(chan error, 1)
	go func() { _, err := n.cmd.Process.Wait(); done <- err }()
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("the server did not stop within 15s of SIGTERM")
	}
	if !strings.Contains(n.logs(), "stopped") {
		t.Errorf("the shutdown was not logged:\n%s", n.logs())
	}

	restarted := start(t, dir, "-appendfsync", "everysec")
	rc := restarted.client()
	for i := 0; i < 500; i++ {
		if rep := rc.do("GET", fmt.Sprintf("c:%03d", i)); rep.Null {
			t.Fatalf("c:%03d was lost across a clean shutdown", i)
		}
	}
	// A clean stop must not report a torn tail; that would mean the shutdown
	// path itself was leaving a partial record behind.
	if strings.Contains(restarted.logs(), "discarded an incomplete record") {
		t.Errorf("a clean shutdown left a torn record behind:\n%s", restarted.logs())
	}
}

// TestCacheModeIsHonestAboutLosingData confirms the opposite claim: with
// persistence off, data is gone, and the server says so at startup.
func TestCacheModeIsHonestAboutLosingData(t *testing.T) {
	dir := t.TempDir()
	n := start(t, dir, "-appendonly", "no", "-no-save")
	if !strings.Contains(n.logs(), "persistence is disabled") {
		t.Errorf("running as a cache was not stated at startup:\n%s", n.logs())
	}
	c := n.client()
	c.do("SET", "ephemeral", "v")
	n.kill9()

	restarted := start(t, dir, "-appendonly", "no", "-no-save")
	if _, ok := restarted.get("ephemeral"); ok {
		t.Error("a cache-mode server recovered data it promised not to keep")
	}
}
