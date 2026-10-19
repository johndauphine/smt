package expr

import (
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"
)

// BinaryLiteral is a typed byte literal or SQL NULL. It is deliberately parsed
// through a separate opt-in entry point so the existing DEFAULT IR contract is
// unchanged. Bytes never contains source SQL.
type BinaryLiteral struct {
	Bytes []byte
	Null  bool
}

// ParseBinaryLiteral recognizes SQL Server byte literals/NULL and PostgreSQL's
// exact decode(hex-literal, 'hex') catalog form. Only documented literal-type
// decoration is accepted; arbitrary casts/functions and compound expressions
// are refused rather than interpreted as binary values.
func ParseBinaryLiteral(raw, source string) (BinaryLiteral, error) {
	switch strings.ToLower(strings.TrimSpace(source)) {
	case MSSQL, "sqlserver", "sql-server", "sql_server":
	case Postgres, "postgresql", "pg":
		return parsePostgresBinaryLiteral(raw)
	default:
		return BinaryLiteral{}, fmt.Errorf("%w: binary literal source %q", ErrUnsupported, source)
	}
	text, err := unwrapBinaryLiteral(raw)
	if err != nil {
		return BinaryLiteral{}, err
	}
	if strings.EqualFold(text, "NULL") {
		return BinaryLiteral{Null: true}, nil
	}
	if len(text) < 2 || text[0] != '0' || (text[1] != 'x' && text[1] != 'X') {
		return BinaryLiteral{}, fmt.Errorf("%w: binary default requires a hexadecimal byte literal or NULL", ErrUnsupported)
	}
	digits := text[2:]
	// SQL Server accepts an empty byte string and supplies a leading zero for
	// an odd hexadecimal nibble count (for example 0xA stores the byte 0A).
	if len(digits)%2 != 0 {
		digits = "0" + digits
	}
	value, err := hex.DecodeString(digits)
	if err != nil {
		return BinaryLiteral{}, fmt.Errorf("%w: invalid hexadecimal binary literal", ErrUnsupported)
	}
	return BinaryLiteral{Bytes: value}, nil
}

var (
	postgresBinaryDecode = regexp.MustCompile(`(?i)^(?:pg_catalog\.)?decode\s*\(\s*'([0-9a-f]*)'\s*(?:::\s*(?:pg_catalog\.)?text\s*)?,\s*'hex'\s*(?:::\s*(?:pg_catalog\.)?text\s*)?\)$`)
	postgresBinaryNull   = regexp.MustCompile(`(?i)^NULL\s*(?:::\s*(?:pg_catalog\.)?bytea)?$`)
)

func parsePostgresBinaryLiteral(raw string) (BinaryLiteral, error) {
	text, err := unwrapBinaryLiteral(raw)
	if err != nil {
		return BinaryLiteral{}, err
	}
	if postgresBinaryNull.MatchString(text) {
		return BinaryLiteral{Null: true}, nil
	}
	match := postgresBinaryDecode.FindStringSubmatch(text)
	if len(match) != 2 {
		return BinaryLiteral{}, fmt.Errorf("%w: PostgreSQL binary default requires an exact hexadecimal decode literal or NULL", ErrUnsupported)
	}
	value, err := hex.DecodeString(match[1])
	if err != nil {
		return BinaryLiteral{}, fmt.Errorf("%w: invalid PostgreSQL hexadecimal byte pairs", ErrUnsupported)
	}
	return BinaryLiteral{Bytes: value}, nil
}

// RenderBinaryLiteral emits the supported target's typed byte expression. It
// serializes parsed bytes, never rewriting or interpolating raw source SQL.
func RenderBinaryLiteral(value BinaryLiteral, target string) (string, error) {
	if strings.ToLower(strings.TrimSpace(target)) != Postgres {
		return "", fmt.Errorf("%w: binary literal target %q", ErrUnsupported, target)
	}
	if value.Null {
		if len(value.Bytes) > 0 {
			return "", fmt.Errorf("%w: NULL binary literal also contains bytes", ErrUnsupported)
		}
		return "NULL", nil
	}
	return "decode('" + hex.EncodeToString(value.Bytes) + "', 'hex')", nil
}

func unwrapBinaryLiteral(raw string) (string, error) {
	text := strings.TrimSpace(raw)
	for strings.HasPrefix(text, "(") {
		depth, closing := 0, -1
		for i, r := range text {
			switch r {
			case '(':
				depth++
			case ')':
				depth--
				if depth == 0 {
					closing = i
				}
			}
			if closing >= 0 || depth < 0 {
				break
			}
		}
		if closing != len(text)-1 {
			return "", fmt.Errorf("%w: binary literal has unmatched or non-enclosing parentheses", ErrUnsupported)
		}
		text = strings.TrimSpace(text[1:closing])
	}
	return text, nil
}
