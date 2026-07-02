package expr

import (
	"errors"
	"strings"
	"testing"
)

func defaultOpts(target string) Opts {
	return Opts{Target: target, Kind: "default"}
}

// TestParity_DefaultCoverageTable is the cross-target acceptance table from
// #175: every source form renders correctly on all three targets from one
// parse. Rows follow the issue's coverage table.
func TestParity_DefaultCoverageTable(t *testing.T) {
	six := 6
	cases := []struct {
		name   string
		raw    string
		source string
		col    ColInfo
		pg     string
		mssql  string
		mysql  string
	}{
		{"mssql getdate", "(getdate())", MSSQL, ColInfo{}, "CURRENT_TIMESTAMP", "GETDATE()", "CURRENT_TIMESTAMP(6)"},
		{"mssql sysdatetime", "(sysdatetime())", MSSQL, ColInfo{}, "CURRENT_TIMESTAMP", "SYSDATETIME()", "CURRENT_TIMESTAMP(6)"},
		{"pg now", "now()", Postgres, ColInfo{}, "CURRENT_TIMESTAMP", "SYSDATETIME()", "CURRENT_TIMESTAMP(6)"},
		{"mysql current_timestamp(6)", "CURRENT_TIMESTAMP(6)", MySQL, ColInfo{DatetimePrecision: &six}, "CURRENT_TIMESTAMP", "SYSDATETIME()", "CURRENT_TIMESTAMP(6)"},
		{"mssql getutcdate", "(getutcdate())", MSSQL, ColInfo{}, "(CURRENT_TIMESTAMP AT TIME ZONE 'UTC')", "GETUTCDATE()", "UTC_TIMESTAMP(6)"},
		{"mssql sysutcdatetime tz-aware col", "(sysutcdatetime())", MSSQL, ColInfo{TZAware: true}, "CURRENT_TIMESTAMP", "SYSUTCDATETIME()", "UTC_TIMESTAMP(6)"},
		{"mysql utc_timestamp", "utc_timestamp()", MySQL, ColInfo{}, "(CURRENT_TIMESTAMP AT TIME ZONE 'UTC')", "SYSUTCDATETIME()", "UTC_TIMESTAMP(6)"},
		{"mssql newid", "(newid())", MSSQL, ColInfo{}, "gen_random_uuid()", "NEWID()", "UUID()"},
		{"mysql uuid", "(uuid())", MySQL, ColInfo{}, "gen_random_uuid()", "NEWID()", "UUID()"},
		{"pg gen_random_uuid", "gen_random_uuid()", Postgres, ColInfo{}, "gen_random_uuid()", "NEWID()", "UUID()"},
		{"mssql convert date of now", "(CONVERT(date, GETDATE()))", MSSQL, ColInfo{}, "CURRENT_DATE", "CONVERT(date, GETDATE())", "CURDATE()"},
		{"mssql convert date of utcnow", "(CONVERT(date, GETUTCDATE()))", MSSQL, ColInfo{}, "(CURRENT_TIMESTAMP AT TIME ZONE 'UTC')::date", "CONVERT(date, GETUTCDATE())", "UTC_DATE()"},
		{"pg current_date", "CURRENT_DATE", Postgres, ColInfo{}, "CURRENT_DATE", "CONVERT(date, GETDATE())", "CURDATE()"},
		{"mssql isnull", "(ISNULL(N'x',0))", MSSQL, ColInfo{}, "COALESCE('x', 0)", "ISNULL('x', 0)", "COALESCE('x', 0)"},
		{"mysql ifnull", "ifnull('x',0)", MySQL, ColInfo{}, "COALESCE('x', 0)", "COALESCE('x', 0)", "IFNULL('x', 0)"},
		{"boolean 1 default", "((1))", MSSQL, ColInfo{Boolean: true}, "true", "1", "1"},
		{"boolean 0 default", "((0))", MSSQL, ColInfo{Boolean: true}, "false", "0", "0"},
		{"pg true default", "true", Postgres, ColInfo{Boolean: true}, "true", "1", "1"},
		{"string literal with function text", "('logged via GETDATE()')", MSSQL, ColInfo{Textual: true}, "'logged via GETDATE()'", "'logged via GETDATE()'", "'logged via GETDATE()'"},
		{"bare word enum default", "individual", MySQL, ColInfo{Textual: true}, "'individual'", "'individual'", "'individual'"},
		{"json empty object on json col", "'{}'", Postgres, ColInfo{JSON: true}, "'{}'", "'{}'", "JSON_OBJECT()"},
		{"json empty array on json col", "'[]'", Postgres, ColInfo{JSON: true}, "'[]'", "'[]'", "JSON_ARRAY()"},
		{"empty object on array col", "'{}'", Postgres, ColInfo{Array: true}, "'{}'", "'{}'", "JSON_ARRAY()"},
		{"null default", "(NULL)", MSSQL, ColInfo{Textual: true}, "NULL", "NULL", "NULL"},
		{"numeric default", "((0.5))", MSSQL, ColInfo{}, "0.5", "0.5", "0.5"},
		{"pg cast-stripped string", "'pending'::character varying", Postgres, ColInfo{Textual: true}, "'pending'", "'pending'", "'pending'"},
		{"pg cast-stripped uuid", "gen_random_uuid()::char(36)", Postgres, ColInfo{}, "gen_random_uuid()", "NEWID()", "UUID()"},
	}

	quotes := map[string]func(string) string{
		Postgres: func(s string) string { return `"` + strings.ToLower(s) + `"` },
		MSSQL:    func(s string) string { return "[" + s + "]" },
		MySQL:    func(s string) string { return "`" + s + "`" },
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			n := ParseDefault(tc.raw, tc.source)
			if _, isRaw := n.(Raw); isRaw {
				t.Fatalf("ParseDefault(%q) fell back to Raw", tc.raw)
			}
			for target, want := range map[string]string{Postgres: tc.pg, MSSQL: tc.mssql, MySQL: tc.mysql} {
				o := Opts{Target: target, Source: tc.source, Kind: "default", Col: tc.col, QuoteIdent: quotes[target]}
				got, err := Render(n, o)
				if err != nil {
					t.Fatalf("Render(%q → %s): %v", tc.raw, target, err)
				}
				if got != want {
					t.Errorf("Render(%q → %s) = %q, want %q", tc.raw, target, got, want)
				}
			}
		})
	}
}

// TestFailClosed_UnknownFormsRejectedOnEveryTarget pins the #175 fail-closed
// invariant: an unparseable or unknown-function default hits ErrUnsupported
// on pg, mssql, AND mysql — never a silent best-guess.
func TestFailClosed_UnknownFormsRejectedOnEveryTarget(t *testing.T) {
	forms := []string{
		"(dateadd(year,(1),getdate()))",
		"(CONVERT(varchar(10), GETDATE(), 120))",
		"(my_custom_fn())",
		"(CHARINDEX('@',[Email])>0)",
	}
	for _, raw := range forms {
		n := ParseDefault(raw, MSSQL)
		for _, target := range []string{Postgres, MSSQL, MySQL} {
			_, err := Render(n, Opts{Target: target, Source: MSSQL, Kind: "default"})
			if !errors.Is(err, ErrUnsupported) {
				t.Errorf("Render(%q → %s): want ErrUnsupported, got %v", raw, target, err)
			}
		}
	}
}

func TestRejectUnknownFunctions_SharedGate(t *testing.T) {
	for _, target := range []string{Postgres, MSSQL, MySQL} {
		if err := RejectUnknownFunctions("dateadd(year, 1, x)", target); err == nil {
			t.Errorf("gate(%s) should reject dateadd", target)
		}
		// Function-like text inside a string literal never trips the gate.
		if err := RejectUnknownFunctions("'call DATEADD() later'", target); err != nil {
			t.Errorf("gate(%s) fired inside a string literal: %v", target, err)
		}
		if err := RejectUnknownFunctions("COALESCE(a, 0)", target); err != nil {
			t.Errorf("gate(%s) rejected COALESCE: %v", target, err)
		}
	}
	// Target-native vocabulary is allowed on its own target only.
	if err := RejectUnknownFunctions("GETDATE()", MSSQL); err != nil {
		t.Errorf("mssql gate rejected GETDATE: %v", err)
	}
	if err := RejectUnknownFunctions("GETDATE()", Postgres); err == nil {
		t.Error("pg gate should reject GETDATE (renderer translates it; raw passthrough is a bug)")
	}
}

// TestEqual_AcceptanceCriteria pins the #175 Equal ACs.
func TestEqual_AcceptanceCriteria(t *testing.T) {
	eq := func(a, b string) bool {
		return Equal(ParseDefault(a, ""), ParseDefault(b, ""))
	}
	if !eq("now()", "CURRENT_TIMESTAMP") {
		t.Error("now() must equal CURRENT_TIMESTAMP")
	}
	if eq("CURRENT_DATE", "CURRENT_TIMESTAMP") {
		t.Error("CURRENT_DATE must NOT equal CURRENT_TIMESTAMP")
	}
	// Cross-dialect equivalences the legacy classifier pinned.
	pairs := [][2]string{
		{"(getdate())", "now()"},
		{"(getutcdate())", "CURRENT_TIMESTAMP"},
		{"(sysutcdatetime())", "(CURRENT_TIMESTAMP AT TIME ZONE 'UTC')"},
		{"(newid())", "gen_random_uuid()"},
		{"(newid())", "(UUID())"},
		{"((0))", "false"},
		{"((1))", "true"},
		{"0::integer", "false"},
		{"individual", "'individual'"},
		{"N'foo'", "'foo'"},
		{"'pending'::text", "(('pending'))"},
		{"(CONVERT(date, GETDATE()))", "CURRENT_DATE"},
		{"(CONVERT(date, GETDATE()))", "(CURRENT_TIMESTAMP)::date"},
		{"CURRENT_TIMESTAMP(6)", "now()"},
		{"'{}'::jsonb", "'{}'"},
	}
	for _, p := range pairs {
		if !eq(p[0], p[1]) {
			t.Errorf("expected %q ≡ %q", p[0], p[1])
		}
	}
	distinct := [][2]string{
		{"CURRENT_DATE", "now()"},
		{"CURRENT_TIME", "CURRENT_TIMESTAMP"},
		{"(CONVERT([date],getdate()))", "GETDATE()"},
		{"(CURRENT_TIMESTAMP)::date", "CURRENT_TIMESTAMP"},
		{"'pending'", "'shipped'"},
		{"0.5", "1.5"},
		{"newid()", "now()"},
		{"NULL", "0"},
		{"getdate() at time zone 'US/Eastern'", "getdate()"},
	}
	for _, p := range distinct {
		if eq(p[0], p[1]) {
			t.Errorf("expected %q ≢ %q", p[0], p[1])
		}
	}
}

// TestClassLabel_LegacyVocabulary pins the class-label vocabulary carried
// over from the legacy defaultExpressionClass, which delta messages embed.
func TestClassLabel_LegacyVocabulary(t *testing.T) {
	cases := map[string]string{
		"":                                       "",
		"   ":                                    "",
		"GETDATE()":                              "current_dt",
		"getutcdate()":                           "current_dt",
		"(sysdatetimeoffset())":                  "current_dt",
		"now()":                                  "current_dt",
		"CURRENT_TIMESTAMP":                      "current_dt",
		"CURRENT_TIMESTAMP(6)":                   "current_dt",
		"CURRENT_DATE":                           "current_date",
		"CURRENT_TIME":                           "current_t",
		"LOCALTIME":                              "current_t",
		"NEWID()":                                "uuid_gen",
		"gen_random_uuid()":                      "uuid_gen",
		"uuid()":                                 "uuid_gen",
		"newsequentialid()":                      "uuid_gen",
		"NULL":                                   "null",
		"((0))":                                  "false",
		"((1))":                                  "true",
		"false":                                  "false",
		"true":                                   "true",
		"42":                                     "constant42",
		"-1":                                     "constant-1",
		"3.14":                                   "constant3.14",
		"(('pending'))":                          "constant'pending'",
		"N'foo'":                                 "constant'foo'",
		"'pending'::text":                        "constant'pending'",
		"'{}'::jsonb":                            "constant'{}'",
		"gen_random_uuid()::char(36)":            "uuid_gen",
		"0::integer":                             "false",
		"individual":                             "constant'individual'",
		"(CONVERT(date,getdate()))":              "current_date",
		"(CURRENT_TIMESTAMP AT TIME ZONE 'UTC')": "current_dt",
	}
	for raw, want := range cases {
		if got := ClassLabel(ParseDefault(raw, "")); got != want {
			t.Errorf("ClassLabel(%q) = %q, want %q", raw, got, want)
		}
	}
}

// TestParseCheck_Forms exercises CHECK-shaped fragments end to end.
func TestParseCheck_Forms(t *testing.T) {
	boolCols := map[string]ColInfo{"isactive": {Boolean: true}, "is_active": {Boolean: true}}
	pgQuote := func(s string) string { return `"` + strings.ToLower(s) + `"` }
	mssqlQuote := func(s string) string { return "[" + s + "]" }
	mysqlQuote := func(s string) string { return "`" + s + "`" }

	cases := []struct {
		name   string
		raw    string
		target string
		quote  func(string) string
		want   string
	}{
		{"bit compare pg", "([IsActive]=(1))", Postgres, pgQuote, `"isactive" = true`},
		{"bit compare reversed pg", "((0)=[IsActive])", Postgres, pgQuote, `false = "isactive"`},
		{"bit compare mssql", "([IsActive]=(1))", MSSQL, mssqlQuote, "[IsActive] = 1"},
		{"bool domain pg", "(`is_active` in (0,1))", Postgres, pgQuote, `"is_active" IN (false, true)`},
		{"bool domain mysql", "(`is_active` in (0,1))", MySQL, mysqlQuote, "`is_active` IN (0, 1)"},
		{"amount positive", "([Amount]>(0))", Postgres, pgQuote, `"amount" > (0)`},
		{"like bracket class pg", "([HexCode] LIKE '#[0-9A-Fa-f][0-9A-Fa-f]')", Postgres, pgQuote, `"hexcode" ~ '^#[0-9A-Fa-f][0-9A-Fa-f]$'`},
		{"like bracket class mssql", "([HexCode] LIKE '#[0-9A-Fa-f][0-9A-Fa-f]')", MSSQL, mssqlQuote, "[HexCode] LIKE '#[0-9A-Fa-f][0-9A-Fa-f]'"},
		{"like bracket class mysql", "([HexCode] LIKE '#[0-9A-Fa-f][0-9A-Fa-f]')", MySQL, mysqlQuote, "(`HexCode` REGEXP '^#[0-9A-Fa-f][0-9A-Fa-f]$')"},
		{"regexp_like to pg", "regexp_like(`color`,'^#[0-9a-f]{6}$')", Postgres, pgQuote, `"color" ~ '^#[0-9a-f]{6}$'`},
		{"regexp_like stays mysql", "regexp_like(`color`,'^#[0-9a-f]{6}$')", MySQL, mysqlQuote, "(`color` REGEXP '^#[0-9a-f]{6}$')"},
		{"pg regex to mssql like", `color ~ '^[0-9a-f]{6}$'`, MSSQL, mssqlQuote, "([color] LIKE '[0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f]')"},
		{"any array to in", `status = ANY (ARRAY['a','b'])`, MSSQL, mssqlQuote, "[status] IN ('a', 'b')"},
		{"any array cast to in", `(status = ANY ((ARRAY['a','b'])::text[]))`, MySQL, mysqlQuote, "`status` IN ('a', 'b')"},
		{"concat mysql source to pg", "(concat(`first`,_latin1' ',`last`))", Postgres, pgQuote, `"first" || ' ' || "last"`},
		{"pg concat to mssql", `(first || ' ' || last)`, MSSQL, mssqlQuote, "[first] + ' ' + [last]"},
		{"pg concat to mysql", `(first || ' ' || last)`, MySQL, mysqlQuote, "CONCAT(`first`, ' ', `last`)"},
		{"is not null", "([Email] IS NOT NULL)", Postgres, pgQuote, `"email" IS NOT NULL`},
		{"and or", "([a]>(0) AND [b]<(10) OR [c]=(5))", Postgres, pgQuote, `"a" > (0) AND "b" < (10) OR "c" = (5)`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			n := ParseCheck(tc.raw, "")
			if _, isRaw := n.(Raw); isRaw {
				t.Fatalf("ParseCheck(%q) fell back to Raw", tc.raw)
			}
			got, err := Render(n, Opts{Target: tc.target, Kind: "check", Columns: boolCols, QuoteIdent: tc.quote})
			if err != nil {
				t.Fatalf("Render(%q → %s): %v", tc.raw, tc.target, err)
			}
			if got != tc.want {
				t.Errorf("Render(%q → %s) = %q, want %q", tc.raw, tc.target, got, tc.want)
			}
		})
	}
}

// TestParse_RawFallback pins which inputs are outside the grammar: they must
// come back as Raw (so callers use the legacy pipeline), not misparse.
func TestParse_RawFallback(t *testing.T) {
	outside := []string{
		"CASE WHEN a > 0 THEN 1 ELSE 0 END",
		"(SELECT max(id) FROM t)",
		"a LIKE 'x%' ESCAPE '\\'",
		"CONVERT(varchar(10), GETDATE(), 120)",
		"CAST(a AS varchar(10))",
		"dbo.fn_custom(a)",
		"amount % 2 = 0",
	}
	for _, raw := range outside {
		if _, ok := ParseCheck(raw, "").(Raw); !ok {
			t.Errorf("ParseCheck(%q) should be Raw", raw)
		}
	}
	// A default whose expression references a column is not a well-formed
	// default on any engine — it must go Raw (legacy pipeline decides), not
	// render a column reference into a DEFAULT clause.
	identDefaults := []string{
		"(CONVERT(date, [SomeColumn]))",
		"(ISNULL([a],0))",
		"(upper([code]))",
	}
	for _, raw := range identDefaults {
		if _, ok := ParseDefault(raw, MSSQL).(Raw); !ok {
			t.Errorf("ParseDefault(%q) should be Raw (column reference)", raw)
		}
	}
	// Bracketed type names parse (legacy convertDateDefault accepted them).
	if _, ok := ParseDefault("(CONVERT([date], GETDATE()))", MSSQL).(Cast); !ok {
		t.Error("CONVERT([date], GETDATE()) should parse as a date cast")
	}
}

// TestParse_StringLiteralRoundTrip closes the #175 corruption hole: function
// names inside string literals survive parse+render byte-for-byte.
func TestParse_StringLiteralRoundTrip(t *testing.T) {
	for _, target := range []string{Postgres, MSSQL, MySQL} {
		got, err := Render(ParseDefault("('logged via GETDATE()')", MSSQL), Opts{Target: target, Kind: "default", Col: ColInfo{Textual: true}})
		if err != nil {
			t.Fatalf("target %s: %v", target, err)
		}
		if got != "'logged via GETDATE()'" {
			t.Errorf("target %s: string literal corrupted: %q", target, got)
		}
	}
}

func TestParseDefault_EmptyAndNil(t *testing.T) {
	if ParseDefault("", "") != nil {
		t.Error("empty default should parse to nil")
	}
	if ParseDefault("   ", "") != nil {
		t.Error("whitespace default should parse to nil")
	}
	out, err := Render(nil, defaultOpts(Postgres))
	if err != nil || out != "" {
		t.Errorf("Render(nil) = %q, %v", out, err)
	}
	if !Equal(nil, nil) {
		t.Error("Equal(nil, nil) should be true")
	}
	if Equal(nil, ParseDefault("0", "")) {
		t.Error("Equal(nil, 0) should be false")
	}
}
