// Package resp implements the Redis Serialization Protocol, versions 2 and 3.
//
// The package is split into a request path and a reply path because the two
// have very different shapes in practice. Clients only ever send an array of
// bulk strings (or a legacy inline command), so Reader is specialised for that
// single case instead of parsing a general RESP value tree. Servers send the
// full type surface, so Writer exposes one method per reply type and degrades
// RESP3-only types to their RESP2 equivalents when the connection has not
// upgraded via HELLO 3.
package resp

// Protocol type bytes. These are the first byte of every RESP value.
const (
	TypeSimpleString = '+'
	TypeError        = '-'
	TypeInteger      = ':'
	TypeBulkString   = '$'
	TypeArray        = '*'

	// RESP3 additions.
	TypeNull      = '_'
	TypeDouble    = ','
	TypeBoolean   = '#'
	TypeBlobError = '!'
	TypeVerbatim  = '='
	TypeBigNumber = '('
	TypeMap       = '%'
	TypeSet       = '~'
	TypeAttribute = '|'
	TypePush      = '>'
)

// Version identifies the protocol dialect used on a connection.
type Version int

const (
	// RESP2 is the protocol every Redis client understands. It is the default
	// for a new connection until the client sends HELLO 3.
	RESP2 Version = 2
	// RESP3 adds native map, set, double, boolean and push types.
	RESP3 Version = 3
)

// Valid reports whether v is a protocol version this server implements.
func (v Version) Valid() bool { return v == RESP2 || v == RESP3 }

var crlf = []byte{'\r', '\n'}
