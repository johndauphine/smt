// Package legacystructliterals intentionally uses the v1.3.0 unkeyed public
// struct literals. It is built by TestLegacyUnkeyedStructLiteralsCompile.
package legacystructliterals

import "github.com/johndauphine/smt/schema"

var LegacyCapabilities = schema.Capabilities{
	true, false, true,
	false, true, false, true,
	false, true, false, true, false,
	true, false, true, false,
}

var LegacyStatement = schema.Statement{
	schema.StatementCreateTable,
	"CREATE TABLE legacy ()",
	nil,
}
