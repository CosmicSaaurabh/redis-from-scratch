package compat

import (
	"net"
	"testing"
	"time"

	"github.com/CosmicSaaurabh/redis-from-scratch/internal/resp"
)

// rawClient talks RESP to any server, used here for the real redis-server side
// of the comparison.
type rawClient struct {
	t    *testing.T
	conn net.Conn
	w    *resp.Writer
	r    *resp.ReplyReader
}

func dialRaw(t *testing.T, addr string) *rawClient {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 10*time.Second)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return &rawClient{
		t:    t,
		conn: conn,
		w:    resp.NewWriter(conn, 64<<10),
		r:    resp.NewReplyReader(conn, resp.DefaultLimits()),
	}
}

func (c *rawClient) do(args ...string) resp.Reply {
	c.t.Helper()
	c.w.WriteCommandStrings(args...)
	if err := c.w.Flush(); err != nil {
		c.t.Fatalf("send %v: %v", args, err)
	}
	_ = c.conn.SetReadDeadline(time.Now().Add(15 * time.Second))
	rep, err := c.r.Read()
	if err != nil {
		c.t.Fatalf("read reply to %v: %v", args, err)
	}
	return rep
}
