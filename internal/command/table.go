package command

import (
	"sort"
	"strings"
	"time"
)

// Flag describes a command's properties. The dispatcher and INFO both read
// them, and later phases will use Write and Readonly to route commands to a
// leader or a follower.
type Flag uint16

const (
	// FlagWrite mutates the keyspace.
	FlagWrite Flag = 1 << iota
	// FlagReadonly only reads.
	FlagReadonly
	// FlagAdmin is an operator command.
	FlagAdmin
	// FlagFast is expected to run in constant time.
	FlagFast
	// FlagNoAuth may run before AUTH succeeds.
	FlagNoAuth
	// FlagLoading may run while the server is still recovering.
	FlagLoading
)

// Has reports whether f contains every flag in want.
func (f Flag) Has(want Flag) bool { return f&want == want }

// Command is one entry in the command table.
type Command struct {
	// Name is the lowercase command name.
	Name string
	// Arity is the expected argument count including the command name. A
	// negative value means "at least this many", following the Redis
	// convention that COMMAND replies expose directly.
	Arity int
	// Flags describe the command.
	Flags Flag
	// FirstKey, LastKey and KeyStep locate key arguments. A FirstKey of zero
	// means the command takes no keys. Cluster routing in a later phase reads
	// these; today they feed COMMAND.
	FirstKey, LastKey, KeyStep int
	// Summary is the one-line description COMMAND DOCS returns.
	Summary string
	// Since is the version the command appeared in.
	Since string
	// Handler runs the command.
	Handler func(*Context) error

	// namePtr is a stable pointer to Name. Conn.Touch stores the most recent
	// command name atomically, and taking the address of a string parameter
	// would force it onto the heap on every single command. Pointing at the
	// table's own copy makes that store free.
	namePtr *string
}

// Table is the command registry.
type Table struct {
	cmds map[string]*Command
	// names is the sorted command list, precomputed because COMMAND and INFO
	// ask for it and neither should sort on every call.
	names []string
}

// NewTable builds the registry with every implemented command.
func NewTable() *Table {
	t := &Table{cmds: make(map[string]*Command, 96)}
	registerConnection(t)
	registerString(t)
	registerGeneric(t)
	registerServer(t)

	t.names = make([]string, 0, len(t.cmds))
	for n := range t.cmds {
		t.names = append(t.names, n)
	}
	sort.Strings(t.names)
	return t
}

func (t *Table) add(c *Command) {
	if _, dup := t.cmds[c.Name]; dup {
		panic("command: duplicate registration for " + c.Name)
	}
	c.namePtr = &c.Name
	t.cmds[c.Name] = c
}

// Lookup finds a command by name, which is matched case-insensitively.
func (t *Table) Lookup(name string) (*Command, bool) {
	c, ok := t.cmds[strings.ToLower(name)]
	return c, ok
}

// Names returns every registered command name, sorted.
func (t *Table) Names() []string { return t.names }

// Len returns the number of registered commands.
func (t *Table) Len() int { return len(t.cmds) }

// Dispatch resolves and runs one command, writing exactly one reply.
//
// Every rejection path writes a reply rather than returning an error, because
// RESP has no way to say "no reply for this one": a client that pipelined ten
// commands is waiting for ten replies and will mis-associate every subsequent
// one if a single reply is skipped. The returned error is reserved for
// failures the connection cannot continue past.
func (t *Table) Dispatch(c *Context) error {
	if len(c.Args) == 0 {
		return nil
	}
	c.table = t

	cmd, ok := t.lookupFast(c.Args[0])
	if !ok {
		c.name = strings.ToLower(string(c.Args[0]))
		c.Env.Stats().CommandsRejected.Add(1)
		return c.Err(msgUnknownCommand+" %s", c.name, argPreview(c.Args[1:]))
	}
	// The table's own name is already canonical and already allocated, so this
	// costs nothing where strings.ToLower would have cost two allocations on
	// every command.
	c.name = cmd.Name
	if !arityOK(cmd.Arity, len(c.Args)) {
		c.Env.Stats().CommandsRejected.Add(1)
		return c.ErrWrongArgs()
	}
	if !c.Conn.Authenticated && !cmd.Flags.Has(FlagNoAuth) {
		c.Env.Stats().CommandsRejected.Add(1)
		return c.ErrCode(codeNoAuth, msgNoAuth)
	}

	start := time.Now()
	err := cmd.Handler(c)
	end := time.Now()

	st := c.Env.Stats()
	st.CommandsProcessed.Add(1)
	st.ObserveLatency(end.Sub(start))
	// Reuse the timestamp already taken for the latency measurement rather
	// than reading the clock a third time.
	c.Conn.Touch(cmd.namePtr, end.Unix())
	return err
}

// maxCommandName bounds the stack buffer used for case-folding. The longest
// registered command is well under this, so anything longer cannot be a
// command and skips straight to the unknown-command path.
const maxCommandName = 32

// lookupFast resolves a command without allocating.
//
// Clients send command names in any case, and redis-benchmark sends uppercase,
// so a naive strings.ToLower(string(arg)) allocates twice per command. Folding
// into a stack array and indexing the map with string(buf[:n]) hits the
// compiler's map-lookup optimisation, which does not copy the bytes at all.
func (t *Table) lookupFast(name []byte) (*Command, bool) {
	if len(name) == 0 || len(name) > maxCommandName {
		return nil, false
	}
	var buf [maxCommandName]byte
	for i := 0; i < len(name); i++ {
		b := name[i]
		if b >= 'A' && b <= 'Z' {
			b += 32
		}
		buf[i] = b
	}
	c, ok := t.cmds[string(buf[:len(name)])]
	return c, ok
}

// arityOK applies the Redis arity convention: positive means exactly, negative
// means at least.
func arityOK(arity, argc int) bool {
	if arity >= 0 {
		return argc == arity
	}
	return argc >= -arity
}

// argPreview renders the first few arguments for the unknown-command error,
// truncating so that a malicious client cannot make the server echo megabytes
// back into its own error path.
func argPreview(args [][]byte) string {
	const maxArgs, maxLen = 3, 32
	var sb strings.Builder
	for i, a := range args {
		if i == maxArgs {
			sb.WriteString(", ...")
			break
		}
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteByte('\'')
		if len(a) > maxLen {
			sb.Write(a[:maxLen])
			sb.WriteString("...")
		} else {
			sb.Write(a)
		}
		sb.WriteByte('\'')
	}
	return sb.String()
}
