package driver

// NormalizeIdentifier applies the target dialect's identifier convention to
// a name. This is the single source of truth for "what name will end up on
// disk on the target": both `smt create` (via the per-driver Writer) and
// `smt sync` (via the schema-diff renderer) call this so the two paths
// agree on naming. Without it the AI in sync would emit the source's
// original-case identifiers and miss target tables that create wrote
// under lowercased names (PostgreSQL).
//
// Conventions:
//   - postgres: case-folded to lowercase; non-alphanumeric replaced with
//     underscore; leading digits prefixed with col_. Matches PostgreSQL's
//     unquoted-identifier folding so we don't have to quote everything.
//   - mssql, mysql: pass-through. Both engines preserve case unless the
//     server is configured otherwise.
//
// Adding a new dialect: extend the switch below.

import (
	"strings"
	"unicode"
)

// NormalizeIdentifier returns the on-disk identifier name for the given
// target dialect. dbType is the canonical driver name (postgres, mssql,
// mysql); aliases (pg, sqlserver, mariadb) are normalized via the registry
// before this is called by callers that already hold a canonical name.
func NormalizeIdentifier(dbType, name string) string {
	switch dbType {
	case "postgres":
		return normalizePostgresIdentifier(name)
	default:
		return name
	}
}

// normalizePostgresIdentifier mirrors the historical sanitizePGIdentifier
// in internal/driver/postgres/writer.go — kept identical so the create
// and sync paths produce the same names. If you change one, change both.
func normalizePostgresIdentifier(ident string) string {
	if ident == "" {
		return "col_"
	}
	s := strings.ToLower(ident)
	var sb strings.Builder
	for _, r := range s {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' {
			sb.WriteRune(r)
		} else {
			sb.WriteRune('_')
		}
	}
	s = sb.String()
	if len(s) > 0 && unicode.IsDigit(rune(s[0])) {
		s = "col_" + s
	}
	if s == "" {
		return "col_"
	}
	return s
}
