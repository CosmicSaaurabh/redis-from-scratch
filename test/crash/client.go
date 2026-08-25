package crash

import (
	"fmt"
	"net"
	"time"

	"github.com/CosmicSaaurabh/redis-from-scratch/internal/resp"
)

// client is a minimal RESP client. The crash tests cannot use the shared test
// harness's client because that one fails the test on an I/O error, and here a
// connection dying is the expected outcome half the time.
type client struct {
	conn net.Conn
	w    *resp.Writer
	r    *resp.ReplyReader
}

func dial(addr string) (*client, error) {
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return nil, err
	}
	return &client{
		conn: conn,
		w:    resp.NewWriter(conn, 64<<10),
		r:    resp.NewReplyReader(conn, resp.DefaultLimits()),
	}, nil
}

func (c *client) send(args ...string) { c.w.WriteCommandStrings(args...) }

func (c *client) flush() {
	if err := c.w.Flush(); err != nil {
		panic(fmt.Sprintf("flush: %v", err))
	}
}

func (c *client) read() resp.Reply {
	_ = c.conn.SetReadDeadline(time.Now().Add(20 * time.Second))
	rep, err := c.r.Read()
	if err != nil {
		// Reported as an error reply rather than a panic: a dead connection is
		// a normal outcome in a crash test and each caller decides what it
		// means.
		return resp.Reply{Kind: resp.TypeError, Str: []byte("IOERR " + err.Error())}
	}
	return rep
}

func (c *client) do(args ...string) resp.Reply {
	c.send(args...)
	if err := c.w.Flush(); err != nil {
		return resp.Reply{Kind: resp.TypeError, Str: []byte("IOERR " + err.Error())}
	}
	return c.read()
}

func (c *client) close() { _ = c.conn.Close() }
