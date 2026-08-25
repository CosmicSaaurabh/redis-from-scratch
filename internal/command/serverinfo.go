package command

import (
	"strconv"
	"strings"
	"time"
)

func registerServer(t *Table) {
	t.add(&Command{Name: "info", Arity: -1, Flags: FlagReadonly | FlagLoading, Summary: "Report server statistics",
		Since: "1.0.0", Handler: cmdInfo})
	t.add(&Command{Name: "command", Arity: -1, Flags: FlagLoading | FlagNoAuth, Summary: "Describe the command table",
		Since: "2.8.13", Handler: cmdCommand})
	t.add(&Command{Name: "config", Arity: -2, Flags: FlagAdmin | FlagLoading, Summary: "Read and change configuration",
		Since: "2.0.0", Handler: cmdConfig})
	t.add(&Command{Name: "save", Arity: 1, Flags: FlagAdmin, Summary: "Write a snapshot synchronously",
		Since: "1.0.0", Handler: cmdSave})
	t.add(&Command{Name: "bgsave", Arity: -1, Flags: FlagAdmin, Summary: "Write a snapshot in the background",
		Since: "1.0.0", Handler: cmdBGSave})
	t.add(&Command{Name: "bgrewriteaof", Arity: 1, Flags: FlagAdmin, Summary: "Compact the write-ahead log",
		Since: "1.0.0", Handler: cmdBGRewriteAOF})
	t.add(&Command{Name: "lastsave", Arity: 1, Flags: FlagReadonly | FlagFast, Summary: "Report the last snapshot time",
		Since: "1.0.0", Handler: cmdLastSave})
	t.add(&Command{Name: "shutdown", Arity: -1, Flags: FlagAdmin | FlagNoAuth | FlagLoading, Summary: "Stop the server",
		Since: "1.0.0", Handler: cmdShutdown})
	t.add(&Command{Name: "time", Arity: 1, Flags: FlagReadonly | FlagFast, Summary: "Report server time",
		Since: "2.6.0", Handler: cmdTime})
	t.add(&Command{Name: "debug", Arity: -2, Flags: FlagAdmin, Summary: "Internal diagnostics",
		Since: "1.0.0", Handler: cmdDebug})
	t.add(&Command{Name: "object", Arity: -2, Flags: FlagReadonly, Summary: "Inspect a key's representation",
		Since: "2.2.3", Handler: cmdObject})
	t.add(&Command{Name: "memory", Arity: -2, Flags: FlagReadonly, Summary: "Report memory usage",
		Since: "4.0.0", Handler: cmdMemory})
	t.add(&Command{Name: "wait", Arity: 3, Flags: FlagFast, Summary: "Wait for replication acknowledgements",
		Since: "3.0.0", Handler: cmdWait})
	t.add(&Command{Name: "failover", Arity: -1, Flags: FlagAdmin, Summary: "Trigger a failover",
		Since: "6.2.0", Handler: cmdFailover})
}

func cmdInfo(c *Context) error {
	sections := make([]string, 0, c.Argc()-1)
	for i := 1; i < c.Argc(); i++ {
		sections = append(sections, c.ArgLower(i))
	}
	// INFO is a verbatim text reply so that a RESP3 client can render the
	// section headings rather than showing one long escaped line.
	c.W.WriteVerbatim("txt", c.Env.Info(sections))
	return nil
}

func cmdCommand(c *Context) error {
	table := c.Table()
	if c.Argc() == 1 {
		c.W.WriteArrayHeader(table.Len())
		for _, name := range table.Names() {
			cmd, _ := table.Lookup(name)
			writeCommandInfo(c, cmd)
		}
		return nil
	}

	switch c.ArgLower(1) {
	case "count":
		return c.Int(int64(table.Len()))

	case "docs":
		names := table.Names()
		if c.Argc() > 2 {
			names = names[:0]
			for i := 2; i < c.Argc(); i++ {
				if _, ok := table.Lookup(c.ArgString(i)); ok {
					names = append(names, c.ArgLower(i))
				}
			}
		}
		c.W.WriteMapHeader(len(names))
		for _, name := range names {
			cmd, _ := table.Lookup(name)
			c.W.WriteBulkString(cmd.Name)
			c.W.WriteMapHeader(3)
			c.W.WriteBulkString("summary")
			c.W.WriteBulkString(cmd.Summary)
			c.W.WriteBulkString("since")
			c.W.WriteBulkString(cmd.Since)
			c.W.WriteBulkString("arity")
			c.W.WriteInt(int64(cmd.Arity))
		}
		return nil

	case "info":
		if c.Argc() == 2 {
			c.W.WriteArrayHeader(table.Len())
			for _, name := range table.Names() {
				cmd, _ := table.Lookup(name)
				writeCommandInfo(c, cmd)
			}
			return nil
		}
		c.W.WriteArrayHeader(c.Argc() - 2)
		for i := 2; i < c.Argc(); i++ {
			cmd, ok := table.Lookup(c.ArgString(i))
			if !ok {
				c.W.WriteNullArray()
				continue
			}
			writeCommandInfo(c, cmd)
		}
		return nil

	case "list":
		c.W.WriteArrayHeader(table.Len())
		for _, name := range table.Names() {
			c.W.WriteBulkString(name)
		}
		return nil

	case "getkeys":
		return commandGetKeys(c, table)

	default:
		return c.Err("Unknown subcommand '%s'", c.ArgString(1))
	}
}

func writeCommandInfo(c *Context, cmd *Command) {
	c.W.WriteArrayHeader(10)
	c.W.WriteBulkString(cmd.Name)
	c.W.WriteInt(int64(cmd.Arity))

	flags := make([]string, 0, 4)
	if cmd.Flags.Has(FlagWrite) {
		flags = append(flags, "write", "denyoom")
	}
	if cmd.Flags.Has(FlagReadonly) {
		flags = append(flags, "readonly")
	}
	if cmd.Flags.Has(FlagAdmin) {
		flags = append(flags, "admin", "noscript")
	}
	if cmd.Flags.Has(FlagFast) {
		flags = append(flags, "fast")
	}
	if cmd.Flags.Has(FlagLoading) {
		flags = append(flags, "loading")
	}
	c.W.WriteArrayHeader(len(flags))
	for _, f := range flags {
		c.W.WriteSimpleString(f)
	}

	c.W.WriteInt(int64(cmd.FirstKey))
	c.W.WriteInt(int64(cmd.LastKey))
	c.W.WriteInt(int64(cmd.KeyStep))
	c.W.WriteArrayHeader(0) // ACL categories
	c.W.WriteArrayHeader(0) // tips
	c.W.WriteArrayHeader(0) // key specs
	c.W.WriteArrayHeader(0) // subcommands
}

// commandGetKeys reports which arguments of a command are keys. Cluster-aware
// clients call it to decide where to route a command they do not recognise, so
// it reads the same FirstKey/LastKey/KeyStep metadata that Phase 5 routing will.
func commandGetKeys(c *Context, table *Table) error {
	if c.Argc() < 3 {
		return c.Err("Unknown subcommand or wrong number of arguments for 'GETKEYS'")
	}
	cmd, ok := table.Lookup(c.ArgString(2))
	if !ok {
		return c.Err("Invalid command specified")
	}
	argc := c.Argc() - 2
	if !arityOK(cmd.Arity, argc) {
		return c.Err("Invalid number of arguments specified for command")
	}
	if cmd.FirstKey == 0 {
		return c.Err("The command has no key arguments")
	}

	last := cmd.LastKey
	if last < 0 {
		last = argc + last
	}
	step := max(cmd.KeyStep, 1)
	var keys [][]byte
	for i := cmd.FirstKey; i <= last && i < argc; i += step {
		keys = append(keys, c.Arg(i+2))
	}
	if len(keys) == 0 {
		return c.Err("Invalid arguments specified for command")
	}
	c.W.WriteArrayHeader(len(keys))
	for _, k := range keys {
		c.W.WriteBulk(k)
	}
	return nil
}

func cmdConfig(c *Context) error {
	switch c.ArgLower(1) {
	case "get":
		if c.Argc() < 3 {
			return c.ErrWrongArgs()
		}
		all := c.Env.ConfigAll()
		matched := make(map[string]string, c.Argc())
		for i := 2; i < c.Argc(); i++ {
			pattern := c.Arg(i)
			for k, v := range all {
				if matchPattern(pattern, []byte(k), true) {
					matched[k] = v
				}
			}
		}
		c.W.WriteMapHeader(len(matched))
		for k, v := range matched {
			c.W.WriteBulkString(k)
			c.W.WriteBulkString(v)
		}
		return nil

	case "set":
		if c.Argc() < 4 || c.Argc()%2 != 0 {
			return c.ErrWrongArgs()
		}
		// Validate every pair before applying any of them, so a typo in the
		// second pair cannot leave the first one applied.
		for i := 2; i < c.Argc(); i += 2 {
			if _, ok := c.Env.ConfigGet(c.ArgString(i)); !ok {
				return c.Err("Unknown option or number of arguments for CONFIG SET - '%s'", c.ArgString(i))
			}
		}
		for i := 2; i < c.Argc(); i += 2 {
			if err := c.Env.ConfigSet(c.ArgString(i), c.ArgString(i+1)); err != nil {
				return c.Err("CONFIG SET failed - %s", err.Error())
			}
		}
		return c.OK()

	case "resetstat":
		c.Env.Stats().ResetStat()
		return c.OK()

	case "rewrite":
		return c.Err("The server is running without a config file")

	default:
		return c.Err("Unknown CONFIG subcommand or wrong number of arguments for '%s'", c.ArgString(1))
	}
}

func cmdSave(c *Context) error {
	if _, err := c.Env.Save(c.Ctx); err != nil {
		return c.Err("%s", err.Error())
	}
	return c.OK()
}

func cmdBGSave(c *Context) error {
	if err := c.Env.BackgroundSave(c.Ctx); err != nil {
		return c.Err("%s", err.Error())
	}
	return c.SimpleString("Background saving started")
}

func cmdBGRewriteAOF(c *Context) error {
	// The log is compacted by snapshotting and trimming superseded segments,
	// so this is the same operation under the name existing tooling calls.
	if err := c.Env.BackgroundSave(c.Ctx); err != nil {
		return c.Err("%s", err.Error())
	}
	return c.SimpleString("Background append only file rewriting started")
}

func cmdLastSave(c *Context) error { return c.Int(c.Env.LastSave()) }

func cmdShutdown(c *Context) error {
	save := true
	for i := 1; i < c.Argc(); i++ {
		switch c.ArgLower(i) {
		case "nosave":
			save = false
		case "save":
			save = true
		case "now", "force":
		default:
			return c.ErrSyntax()
		}
	}
	c.Env.Shutdown(save)
	// SHUTDOWN deliberately sends no reply on success: the connection dies
	// with the server, and a client that received +OK and then saw the socket
	// close could not tell a successful shutdown from a crash.
	c.Conn.CloseAfterReply()
	return nil
}

func cmdTime(c *Context) error {
	now := c.Env.Clock().Now()
	c.W.WriteArrayHeader(2)
	c.W.WriteBulkString(strconv.FormatInt(now.Unix(), 10))
	c.W.WriteBulkString(strconv.FormatInt(int64(now.Nanosecond()/1000), 10))
	return nil
}

func cmdDebug(c *Context) error {
	switch c.ArgLower(1) {
	case "sleep":
		if c.Argc() != 3 {
			return c.ErrWrongArgs()
		}
		secs, ok := c.floatArg(2)
		if !ok || secs < 0 {
			return c.ErrSyntax()
		}
		// DEBUG SLEEP blocks only this connection, not the server. Real Redis
		// blocks everything because it is single-threaded; saying so matters,
		// because tests written against Redis use this to simulate a stall.
		select {
		case <-time.After(time.Duration(secs * float64(time.Second))):
		case <-c.Ctx.Done():
		}
		return c.OK()

	case "jmap", "set-active-expire", "quicklist-packed-threshold", "stringmatch-len", "change-repl-id":
		return c.OK()

	case "object":
		if c.Argc() != 3 {
			return c.ErrWrongArgs()
		}
		rec, ok, err := c.Store().Get(c.Ctx, c.Arg(2))
		if err != nil {
			return storeErr(c, err)
		}
		if !ok {
			return c.Err("no such key")
		}
		return c.SimpleString("Value at:0x0 refcount:1 encoding:raw serializedlength:" +
			strconv.Itoa(len(rec.Value)) + " lru:0 lru_seconds_idle:0")

	case "help":
		c.W.WriteArrayHeader(2)
		c.W.WriteSimpleString("DEBUG SLEEP <seconds> -- stall this connection")
		c.W.WriteSimpleString("DEBUG OBJECT <key> -- describe a key's representation")
		return nil

	default:
		return c.Err("DEBUG subcommand '%s' not supported by this server", c.ArgString(1))
	}
}

func cmdObject(c *Context) error {
	if c.Argc() < 3 {
		if c.ArgLower(1) == "help" {
			c.W.WriteArrayHeader(1)
			c.W.WriteSimpleString("OBJECT <ENCODING|REFCOUNT|IDLETIME|FREQ> <key>")
			return nil
		}
		return c.ErrWrongArgs()
	}
	rec, ok, err := c.Store().Get(c.Ctx, c.Arg(2))
	if err != nil {
		return storeErr(c, err)
	}
	if !ok {
		return c.Err("no such key")
	}
	switch c.ArgLower(1) {
	case "encoding":
		// Redis reports "int" for values that fit its shared-integer
		// optimisation and "embstr" for short strings. This server stores raw
		// bytes, so it reports what it actually does rather than imitating an
		// optimisation it does not have.
		if _, isInt := parseStrictInt(rec.Value); isInt {
			return c.BulkString("int")
		}
		return c.BulkString("raw")
	case "refcount":
		return c.Int(1)
	case "idletime":
		return c.Int(0)
	case "freq":
		return c.Err("An LFU maxmemory policy is not selected, access frequency not tracked")
	default:
		return c.Err("Unknown subcommand or wrong number of arguments for '%s'", c.ArgString(1))
	}
}

func cmdMemory(c *Context) error {
	switch c.ArgLower(1) {
	case "usage":
		if c.Argc() < 3 {
			return c.ErrWrongArgs()
		}
		rec, ok, err := c.Store().Get(c.Ctx, c.Arg(2))
		if err != nil {
			return storeErr(c, err)
		}
		if !ok {
			return c.Null()
		}
		// An estimate, not a measurement: key bytes, value bytes and a fixed
		// allowance for the map entry and record header.
		return c.Int(int64(len(c.Arg(2)) + len(rec.Value) + 64))
	case "doctor":
		return c.BulkString("Memory reporting here is estimated, not measured. Run with a heap profile for real numbers.")
	case "stats":
		st, err := c.Store().Stats(c.Ctx)
		if err != nil {
			return storeErr(c, err)
		}
		c.W.WriteMapHeader(3)
		c.W.WriteBulkString("keys.count")
		c.W.WriteInt(st.Keys)
		c.W.WriteBulkString("keys.volatile")
		c.W.WriteInt(st.VolatileKeys)
		c.W.WriteBulkString("dataset.bytes")
		c.W.WriteInt(st.MemoryBytes)
		return nil
	default:
		return c.Err("Unknown subcommand or wrong number of arguments for '%s'", c.ArgString(1))
	}
}

func cmdWait(c *Context) error {
	// There are no replicas yet, so the honest answer is zero acknowledgements.
	// Returning the requested count instead would let a client believe it had
	// durability guarantees this server cannot provide.
	return c.Int(0)
}

func cmdFailover(c *Context) error {
	if c.Argc() > 1 && c.ArgLower(1) == "abort" {
		return c.Err("No failover in progress.")
	}
	return c.Err("FAILOVER requires connected replicas; this server is standalone")
}

// InfoBuilder assembles an INFO reply section by section.
type InfoBuilder struct {
	sb       strings.Builder
	wanted   map[string]bool
	all      bool
	inWanted bool
}

// NewInfoBuilder returns a builder restricted to the requested sections. No
// sections, or "default"/"all"/"everything", means all of them.
func NewInfoBuilder(sections []string) *InfoBuilder {
	b := &InfoBuilder{wanted: make(map[string]bool, len(sections))}
	if len(sections) == 0 {
		b.all = true
		return b
	}
	for _, s := range sections {
		switch s {
		case "all", "default", "everything":
			b.all = true
		default:
			b.wanted[s] = true
		}
	}
	return b
}

// Section starts a section and reports whether it should be rendered.
func (b *InfoBuilder) Section(name string) bool {
	b.inWanted = b.all || b.wanted[name]
	if !b.inWanted {
		return false
	}
	if b.sb.Len() > 0 {
		b.sb.WriteString("\r\n")
	}
	b.sb.WriteString("# ")
	b.sb.WriteString(strings.ToUpper(name[:1]) + name[1:])
	b.sb.WriteString("\r\n")
	return true
}

// Field appends a key/value line to the current section.
func (b *InfoBuilder) Field(key, value string) {
	if !b.inWanted {
		return
	}
	b.sb.WriteString(key)
	b.sb.WriteByte(':')
	b.sb.WriteString(value)
	b.sb.WriteString("\r\n")
}

// Int appends an integer field.
func (b *InfoBuilder) Int(key string, v int64) { b.Field(key, strconv.FormatInt(v, 10)) }

// Uint appends an unsigned integer field.
func (b *InfoBuilder) Uint(key string, v uint64) { b.Field(key, strconv.FormatUint(v, 10)) }

// Float appends a float field with two decimal places.
func (b *InfoBuilder) Float(key string, v float64) {
	b.Field(key, strconv.FormatFloat(v, 'f', 2, 64))
}

// Bool appends a 0/1 field.
func (b *InfoBuilder) Bool(key string, v bool) {
	if v {
		b.Field(key, "1")
	} else {
		b.Field(key, "0")
	}
}

// String renders the assembled reply.
func (b *InfoBuilder) String() string { return b.sb.String() }
