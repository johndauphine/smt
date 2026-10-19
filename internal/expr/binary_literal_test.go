package expr

import (
	"bytes"
	"errors"
	"testing"
)

func TestBinaryLiteralOptInRoundTrip(t *testing.T) {
	for _, tc := range []struct{ raw, want string }{
		{"0xAB", "decode('ab', 'hex')"},
		{"(( 0xaB00fF ))", "decode('ab00ff', 'hex')"},
		{"0XAB", "decode('ab', 'hex')"},
		{"0xA", "decode('0a', 'hex')"},
		{"0xABC", "decode('0abc', 'hex')"},
		{"(0x)", "decode('', 'hex')"},
		{" ( (NULL) ) ", "NULL"},
	} {
		value, err := ParseBinaryLiteral(tc.raw, MSSQL)
		if err != nil {
			t.Fatal(err)
		}
		got, err := RenderBinaryLiteral(value, Postgres)
		if err != nil || got != tc.want {
			t.Fatalf("%q => %q, %v; want %q", tc.raw, got, err, tc.want)
		}
	}
}

func TestBinaryLiteralRecognizesOnlyExactPostgresCatalogForms(t *testing.T) {
	for _, raw := range []string{"decode('ab', 'hex')", "(decode('AB'::text, 'hex'::text))", "pg_catalog.decode('aB'::pg_catalog.text, 'hex'::pg_catalog.text)"} {
		literal, err := ParseBinaryLiteral(raw, Postgres)
		if err != nil || literal.Null || !bytes.Equal(literal.Bytes, []byte{0xab}) {
			t.Fatalf("catalog %q => %#v, %v", raw, literal, err)
		}
	}
	for _, raw := range []string{"NULL", "(NULL::bytea)", "NULL::pg_catalog.bytea"} {
		literal, err := ParseBinaryLiteral(raw, Postgres)
		if err != nil || !literal.Null {
			t.Fatalf("catalog NULL %q => %#v, %v", raw, literal, err)
		}
	}
	for _, raw := range []string{"decode('a', 'hex')", "decode('ab'::varchar(1), 'hex')", "decode('ab', 'base64')", "decode(current_user, 'hex')", "decode('ab', 'hex')::integer", "decode('ab', 'hex') || decode('cd', 'hex')", "custom.decode('ab', 'hex')"} {
		if _, err := ParseBinaryLiteral(raw, Postgres); !errors.Is(err, ErrUnsupported) {
			t.Fatalf("catalog %q error = %v", raw, err)
		}
	}
}

func TestBinaryLiteralRefusesInvalidOrCompoundForms(t *testing.T) {
	for _, raw := range []string{"", "0xAG", "0xABCD; SELECT 1", "(0xAB)+(0xCD)", "((0xAB)", "0xAB)", "(0xAB))", "'0xAB'", "CONVERT(varbinary(8), 0xAB)", "0xAB + 0xCD", "(NULL) + 1"} {
		if _, err := ParseBinaryLiteral(raw, MSSQL); !errors.Is(err, ErrUnsupported) {
			t.Fatalf("%q error = %v", raw, err)
		}
	}
	if _, err := ParseBinaryLiteral("0xAB", MySQL); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("source error = %v", err)
	}
	if _, err := RenderBinaryLiteral(BinaryLiteral{Bytes: []byte{0xab}}, MySQL); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("target error = %v", err)
	}
}
