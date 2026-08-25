package command

import (
	"crypto/subtle"
	"strconv"
	"strings"

	"github.com/CosmicSaaurabh/redis-from-scratch/internal/resp"
)

func registerConnection(t *Table) {
	t.add(&Command{Name: "ping", Arity: -1, Flags: FlagFast | FlagLoading, Summary: "Ping the server",
		Since: "1.0.0", Handler: cmdPing})
	t.add(&Command{Name: "echo", Arity: 2, Flags: FlagFast, Summary: "Echo the given string",
		Since: "1.0.0", Handler: cmdEcho})
	t.add(&Command{Name: "hello", Arity: -1, Flags: FlagFast | FlagNoAuth | FlagLoading,
		Summary: "Handshake and optionally switch protocol version", Since: "6.0.0", Handler: cmdHello})
	t.add(&Command{Name: "auth", Arity: -2, Flags: FlagFast | FlagNoAuth | FlagLoading,
		Summary: "Authenticate", Since: "1.0.0", Handler: cmdAuth})
	t.add(&Command{Name: "select", Arity: 2, Flags: FlagFast | FlagLoading, Summary: "Select a database",
		Since: "1.0.0", Handler: cmdSelect})
	t.add(&Command{Name: "swapdb", Arity: 3, Flags: FlagWrite | FlagFast, Summary: "Swap two databases",
		Since: "4.0.0", Handler: cmdSwapDB})
	t.add(&Command{Name: "quit", Arity: -1, Flags: FlagFast | FlagNoAuth | FlagLoading,
		Summary: "Close the connection", Since: "1.0.0", Handler: cmdQuit})
	t.add(&Command{Name: "reset", Arity: 1, Flags: FlagFast | FlagNoAuth | FlagLoading,
		Summary: "Reset the connection to its initial state", Since: "6.2.0", Handler: cmdReset})
	t.add(&Command{Name: "client", Arity: -2, Flags: FlagAdmin | FlagLoading, Summary: "Inspect and control connections",
		Since: "2.4.0", Handler: cmdClient})
}

func cmdPing(c *Context) error {
	switch c.Argc() {
	case 1:
		return c.SimpleString("PONG")
	case 2:
		// PING with a payload echoes it as a bulk string rather than a status
		// reply, which is what lets a client use it as a round-trip probe with
		// a correlation token.
		return c.Bulk(c.Arg(1))
	default:
		return c.ErrWrongArgs()
	}
}

func cmdEcho(c *Context) error { return c.Bulk(c.Arg(1)) }

func cmdHello(c *Context) error {
	requested := c.Conn.Proto
	i := 1
	if c.Argc() > 1 {
		n, err := strconv.Atoi(c.ArgString(1))
		if err != nil {
			return c.Err("Protocol version is not an integer or out of range")
		}
		v := resp.Version(n)
		if !v.Valid() {
			return c.ErrCode("NOPROTO", "unsupported protocol version")
		}
		requested = v
		i = 2
	}

	// AUTH and SETNAME may ride along on HELLO so that a client can complete
	// its whole handshake in one round trip.
	for ; i < c.Argc(); i++ {
		switch c.ArgLower(i) {
		case "auth":
			if i+2 >= c.Argc() {
				return c.ErrSyntax()
			}
			if !checkPassword(c, c.Arg(i+2)) {
				return c.ErrCode("WRONGPASS", msgWrongPass)
			}
			c.Conn.Authenticated = true
			i += 2
		case "setname":
			if i+1 >= c.Argc() {
				return c.ErrSyntax()
			}
			c.Conn.SetName(c.ArgString(i + 1))
			i++
		default:
			return c.ErrSyntax()
		}
	}
	if !c.Conn.Authenticated {
		return c.ErrCode(codeNoAuth, msgNoAuth)
	}

	// The protocol switches only after the handshake is known to succeed, and
	// the reply itself is written in the new dialect, which is what the RESP3
	// specification requires.
	c.Conn.Proto = requested
	c.W.SetVersion(requested)

	c.W.WriteMapHeader(7)
	c.W.WriteBulkString("server")
	c.W.WriteBulkString("redis-from-scratch")
	c.W.WriteBulkString("version")
	c.W.WriteBulkString(c.Env.Version())
	c.W.WriteBulkString("proto")
	c.W.WriteInt(int64(requested))
	c.W.WriteBulkString("id")
	c.W.WriteInt(int64(c.Conn.ID))
	c.W.WriteBulkString("mode")
	c.W.WriteBulkString("standalone")
	c.W.WriteBulkString("role")
	c.W.WriteBulkString("master")
	c.W.WriteBulkString("modules")
	c.W.WriteArrayHeader(0)
	return nil
}

func cmdAuth(c *Context) error {
	var password []byte
	switch c.Argc() {
	case 2:
		password = c.Arg(1)
	case 3:
		// The two-argument form names a user. Only the implicit "default" user
		// exists, so any other name is rejected rather than silently ignored.
		if c.ArgLower(1) != "default" {
			return c.ErrCode("WRONGPASS", msgWrongPass)
		}
		password = c.Arg(2)
	default:
		return c.ErrWrongArgs()
	}
	if c.Env.RequirePass() == "" {
		return c.Err(msgAuthNotSet)
	}
	if !checkPassword(c, password) {
		return c.ErrCode("WRONGPASS", msgWrongPass)
	}
	c.Conn.Authenticated = true
	return c.OK()
}

// checkPassword compares in constant time. A naive comparison leaks the length
// of the matching prefix through timing, which is enough to recover a password
// one byte at a time over a fast network.
func checkPassword(c *Context, given []byte) bool {
	want := c.Env.RequirePass()
	if want == "" {
		return true
	}
	return subtle.ConstantTimeCompare(given, []byte(want)) == 1
}

func cmdSelect(c *Context) error {
	n, err := strconv.Atoi(c.ArgString(1))
	if err != nil {
		return c.Err(msgNotInteger)
	}
	if n != 0 {
		return c.Err(msgSingleDB)
	}
	return c.OK()
}

func cmdSwapDB(c *Context) error { return c.Err(msgSingleDB) }

func cmdQuit(c *Context) error {
	// The reply is written first and the hang-up is deferred until after the
	// flush, so the client actually receives its +OK instead of a reset socket.
	c.Conn.CloseAfterReply()
	return c.OK()
}

func cmdReset(c *Context) error {
	c.Conn.Proto = resp.RESP2
	c.W.SetVersion(resp.RESP2)
	c.Conn.SetName("")
	c.Conn.Authenticated = c.Env.RequirePass() == ""
	return c.SimpleString("RESET")
}

func cmdClient(c *Context) error {
	switch c.ArgLower(1) {
	case "id":
		return c.Int(int64(c.Conn.ID))

	case "getname":
		if name := c.Conn.Name(); name != "" {
			return c.BulkString(name)
		}
		return c.Null()

	case "setname":
		if c.Argc() != 3 {
			return c.ErrWrongArgs()
		}
		name := c.ArgString(2)
		if strings.ContainsAny(name, " \n\r") {
			return c.Err("Client names cannot contain spaces, newlines or special characters.")
		}
		c.Conn.SetName(name)
		return c.OK()

	case "info":
		return c.BulkString(formatClientLine(c, c.Conn))

	case "list":
		var sb strings.Builder
		for _, conn := range c.Env.Clients() {
			sb.WriteString(formatClientLine(c, conn))
			sb.WriteByte('\n')
		}
		return c.BulkString(sb.String())

	case "kill":
		return clientKill(c)

	case "no-evict", "no-touch":
		// Accepted and ignored: this server has no eviction policy and no LRU
		// clock, so there is nothing for these to turn off.
		return c.OK()

	case "setinfo":
		if c.Argc() != 4 {
			return c.ErrWrongArgs()
		}
		return c.OK()

	default:
		return c.Err("Unknown CLIENT subcommand or wrong number of arguments for '%s'", c.ArgString(1))
	}
}

func clientKill(c *Context) error {
	// The old form is CLIENT KILL addr:port; the new one is a filter list.
	if c.Argc() == 3 {
		target := c.ArgString(2)
		for _, conn := range c.Env.Clients() {
			if conn.RemoteAddr == target {
				c.Env.KillClient(conn.ID)
				return c.OK()
			}
		}
		return c.Err("No such client address in client list")
	}

	var (
		byID       uint64
		haveID     bool
		byAddr     string
		skipMe     = true
		maxAge     int64
		haveMaxAge bool
	)
	for i := 2; i < c.Argc(); i += 2 {
		if i+1 >= c.Argc() {
			return c.ErrSyntax()
		}
		value := c.ArgString(i + 1)
		switch c.ArgLower(i) {
		case "id":
			n, err := strconv.ParseUint(value, 10, 64)
			if err != nil {
				return c.Err("client-id should be greater than 0")
			}
			byID, haveID = n, true
		case "addr", "laddr":
			byAddr = value
		case "skipme":
			skipMe = strings.EqualFold(value, "yes")
		case "maxage":
			n, err := strconv.ParseInt(value, 10, 64)
			if err != nil || n < 0 {
				return c.ErrSyntax()
			}
			maxAge, haveMaxAge = n, true
		case "type", "user":
			// Accepted for compatibility; every connection here is a normal
			// client, so these filters never exclude anything.
		default:
			return c.ErrSyntax()
		}
	}

	now := c.Env.Clock().Now()
	var killed int64
	for _, conn := range c.Env.Clients() {
		if skipMe && conn.ID == c.Conn.ID {
			continue
		}
		if haveID && conn.ID != byID {
			continue
		}
		if byAddr != "" && conn.RemoteAddr != byAddr {
			continue
		}
		if haveMaxAge && int64(now.Sub(conn.CreatedAt).Seconds()) < maxAge {
			continue
		}
		if c.Env.KillClient(conn.ID) {
			killed++
		}
	}
	return c.Int(killed)
}

func formatClientLine(c *Context, conn *Conn) string {
	now := c.Env.Clock().Now()
	var sb strings.Builder
	sb.WriteString("id=")
	sb.WriteString(strconv.FormatUint(conn.ID, 10))
	sb.WriteString(" addr=")
	sb.WriteString(conn.RemoteAddr)
	sb.WriteString(" laddr=")
	sb.WriteString(conn.LocalAddr)
	sb.WriteString(" name=")
	sb.WriteString(conn.Name())
	sb.WriteString(" age=")
	sb.WriteString(strconv.FormatInt(int64(now.Sub(conn.CreatedAt).Seconds()), 10))
	sb.WriteString(" idle=")
	sb.WriteString(strconv.FormatInt(conn.IdleSeconds(now), 10))
	sb.WriteString(" db=0 resp=")
	sb.WriteString(strconv.Itoa(int(conn.Proto)))
	sb.WriteString(" cmd=")
	if last := conn.LastCommand(); last != "" {
		sb.WriteString(last)
	} else {
		sb.WriteString("NULL")
	}
	sb.WriteString(" numcmds=")
	sb.WriteString(strconv.FormatUint(conn.CommandCount(), 10))
	return sb.String()
}
