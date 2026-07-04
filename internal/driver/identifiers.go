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
	"fmt"
	"hash/crc32"
	"strings"
	"unicode"
	"unicode/utf8"
)

// pgMaxIdentifierBytes is PostgreSQL's NAMEDATALEN-1: identifiers longer than
// this are silently truncated by the server, so a name SMT thinks it created
// would differ from what's on disk. We truncate deterministically instead.
const pgMaxIdentifierBytes = 63

// NormalizeIdentifier returns the on-disk identifier name for the given
// target dialect. dbType may be a canonical driver name (postgres, mssql,
// mysql) or an alias (pg, postgresql, sqlserver, mariadb) — it is canonicalized
// here so an alias can't bypass PostgreSQL folding (#189).
func NormalizeIdentifier(dbType, name string) string {
	if isPostgresDialect(dbType) {
		return normalizePostgresIdentifier(name)
	}
	return name
}

// isPostgresDialect reports whether dbType names PostgreSQL — by canonical
// name or alias. It consults the driver registry first (authoritative, picks
// up any alias a driver declares) and falls back to the literal alias set so
// identifier folding is correct even when the registry isn't populated yet
// (e.g. unit tests of this package, which can't blank-import the engines
// without an import cycle). Without the fallback an alias like "pg" would
// silently skip folding (#189).
func isPostgresDialect(dbType string) bool {
	if Canonicalize(dbType) == "postgres" {
		return true
	}
	switch strings.ToLower(strings.TrimSpace(dbType)) {
	case "postgres", "postgresql", "pg":
		return true
	}
	return false
}

// normalizePostgresIdentifier is the single implementation of PostgreSQL
// identifier folding; the postgres writer's sanitizePGIdentifier delegates
// here, so create and sync always produce the same names.
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
	if len(s) > pgMaxIdentifierBytes {
		s = truncatePGIdentifier(s)
	}
	return s
}

// truncatePGIdentifier shortens an over-long normalized identifier to fit
// PostgreSQL's 63-byte limit, appending a hash of the full name so two names
// sharing a 63-byte prefix don't silently fold onto the same identifier. The
// hash is over the normalized string, so it's stable across runs. (True
// distinct-source collisions are caught separately by CheckIdentifierCollisions.)
func truncatePGIdentifier(s string) string {
	suffix := fmt.Sprintf("_%08x", crc32.ChecksumIEEE([]byte(s)))
	keep := pgMaxIdentifierBytes - len(suffix)
	// Back up to a rune boundary at or below keep bytes so we never split a
	// multi-byte rune (identifiers may contain Unicode letters).
	cut := keep
	if cut > len(s) {
		cut = len(s)
	}
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut] + suffix
}

// CheckIdentifierCollisions verifies that normalizing the given tables' and
// their columns' identifiers for the target dialect maps no two distinct
// source names onto the same on-disk name. Only PostgreSQL folds identifiers,
// so this is a no-op for mssql/mysql. On collision it returns an error naming
// both source identifiers so the operator can rename one before create/sync
// emits DDL that would fail late (duplicate column) or silently land two
// source objects on one target object (#189). Tables are assumed to be within
// a single schema.
func CheckIdentifierCollisions(dbType string, tables []Table) error {
	if !isPostgresDialect(dbType) {
		return nil
	}
	seenTable := make(map[string]string, len(tables))
	for _, t := range tables {
		norm := NormalizeIdentifier(dbType, t.Name)
		if prev, ok := seenTable[norm]; ok && prev != t.Name {
			return fmt.Errorf("identifier collision on postgres target: tables %q and %q both normalize to %q — rename one", prev, t.Name, norm)
		}
		seenTable[norm] = t.Name

		seenCol := make(map[string]string, len(t.Columns))
		for _, c := range t.Columns {
			cn := NormalizeIdentifier(dbType, c.Name)
			if prev, ok := seenCol[cn]; ok && prev != c.Name {
				return fmt.Errorf("identifier collision on postgres target: columns %q and %q in table %q both normalize to %q — rename one", prev, c.Name, t.Name, cn)
			}
			seenCol[cn] = c.Name
		}
	}
	return nil
}
