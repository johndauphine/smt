// Package expr is a small expression IR for DEFAULT and CHECK fragments
// (#175). A source expression is parsed once into a dialect-neutral tree
// (ParseDefault / ParseCheck), rendered to any target dialect with equal
// completeness (Render), and compared structurally (Equal) — parity by
// construction instead of N×M string-rewrite rules.
//
// The grammar is deliberately tiny: literals, identifiers, function calls
// collapsed into semantic categories, comparison/boolean operators, IN,
// LIKE/regex, the class-relevant casts (::date / ::time / CONVERT(date, x)),
// and AT TIME ZONE. Anything outside it parses to Raw, which Render always
// refuses (ErrUnsupported) so callers fall back to their legacy pipelines,
// which end in the shared fail-closed function gate (RejectUnknownFunctions).
// No general SQL AST, no SELECT, no subqueries.
//
// expr is a leaf package like internal/canonical: it takes primitives only,
// so package driver can import it without a cycle.
package expr

import "errors"

// Canonical dialect names, matching driver.Canonicalize output.
const (
	Postgres = "postgres"
	MSSQL    = "mssql"
	MySQL    = "mysql"
)

// ErrUnsupported reports a form the IR cannot render for the requested
// target. Callers treat it as "fall back to the legacy pipeline", which is
// itself fail-closed via RejectUnknownFunctions.
var ErrUnsupported = errors.New("expression form not supported by the expression IR")

// Node is one vertex of the parsed expression tree.
type Node interface{ isNode() }

// LitKind discriminates literal values.
type LitKind int

const (
	LitString LitKind = iota
	LitNumber
	LitBool
	LitNull
)

// Lit is a literal value. Str holds the unquoted string value; Num holds the
// number's literal text (sign and decimal point included); Bare marks a
// string that was written as an unquoted bare word (MySQL enum-style
// defaults), which renders unquoted on non-textual columns.
type Lit struct {
	Kind LitKind
	Str  string
	Num  string
	Bool bool
	Bare bool
}

// FuncCat collapses dialect spellings of the same function into one semantic
// category at parse time. GETDATE() and now() are both NowDateTime; the
// renderer picks the target's spelling.
type FuncCat int

const (
	UnknownFn     FuncCat = iota // unrecognized: Raw name kept, Render refuses
	NowDateTime                  // GETDATE / SYSDATETIME / SYSDATETIMEOFFSET / CURRENT_TIMESTAMP / NOW / LOCALTIMESTAMP / SYSTIMESTAMP (fsp arg dropped)
	UtcNow                       // GETUTCDATE / SYSUTCDATETIME / UTC_TIMESTAMP
	CurrentDate                  // CURRENT_DATE / CURDATE
	CurrentTime                  // CURRENT_TIME / CURTIME / LOCALTIME
	UuidGen                      // NEWID / NEWSEQUENTIALID / UUID / gen_random_uuid / uuid_generate_v4
	Coalesce                     // ISNULL ≡ COALESCE ≡ IFNULL
	Concat                       // CONCAT(...) ≡ a || b
	PassThroughFn                // same spelling on every target: lower/upper/nullif
)

// Call is a function application. Raw preserves the source spelling (used
// for message text and for target-native spelling preservation); Keyword
// marks parenless keyword forms like CURRENT_TIMESTAMP.
type Call struct {
	Fn      FuncCat
	Raw     string
	Args    []Node
	Keyword bool
}

// Ident is a column reference; Name is unquoted original spelling.
type Ident struct{ Name string }

// Unary is NOT x, -x, x IS NULL, or x IS NOT NULL (Op: "NOT", "-",
// "IS NULL", "IS NOT NULL").
type Unary struct {
	Op string
	X  Node
}

// Binary is an infix operator: = <> != < <= > >= AND OR + - * /.
// String concatenation is not a Binary; it normalizes to Call{Fn: Concat}.
type Binary struct {
	Op   string
	L, R Node
}

// In is x [NOT] IN (list); also the normal form of pg's x = ANY (ARRAY[...]).
type In struct {
	X    Node
	List []Node
	Not  bool
}

// Like is x [NOT] LIKE pattern, or a regex match (pg ~, MySQL REGEXP /
// REGEXP_LIKE) when Regex is set.
type Like struct {
	X       Node
	Pattern Node
	Not     bool
	Regex   bool
}

// CastKind names the class-relevant casts the IR keeps. All other casts are
// stripped at parse (matching the legacy stripPostgresCasts / classifier
// behavior): they change lexical type, not the default's semantic class.
type CastKind int

const (
	CastDate CastKind = iota
	CastTime
)

// Cast is ::date / ::time / CONVERT(date, x) / CAST(x AS date).
type Cast struct {
	X  Node
	To CastKind
}

// AtTimeZone is x AT TIME ZONE 'zone'. The UTC form over a now-family call
// normalizes to Call{Fn: UtcNow} at parse.
type AtTimeZone struct {
	X    Node
	Zone string
}

// Paren preserves explicit grouping from the source.
type Paren struct{ X Node }

// Raw is the escape hatch: the original text of anything the parser did not
// recognize. Render always refuses it.
type Raw struct{ Text string }

func (Lit) isNode()        {}
func (Call) isNode()       {}
func (Ident) isNode()      {}
func (Unary) isNode()      {}
func (Binary) isNode()     {}
func (In) isNode()         {}
func (Like) isNode()       {}
func (Cast) isNode()       {}
func (AtTimeZone) isNode() {}
func (Paren) isNode()      {}
func (Raw) isNode()        {}

// ColInfo carries the column context a renderer needs: boolean columns take
// boolean literals on pg, textual columns quote bare-word defaults, TZ-aware
// columns take CURRENT_TIMESTAMP over the AT TIME ZONE UTC form, JSON/array
// columns map '{}' to constructor calls on MySQL, and DatetimePrecision
// matches MySQL now-defaults to the column's fsp.
type ColInfo struct {
	Boolean           bool
	Textual           bool
	TZAware           bool
	JSON              bool
	Array             bool
	DatetimePrecision *int
}

// Opts parameterizes Render.
type Opts struct {
	Target string // canonical target dialect (Postgres/MSSQL/MySQL)
	Source string // canonical source dialect; may be empty
	Kind   string // "default" or "check"

	// Col describes the column owning a DEFAULT (zero value for checks).
	Col ColInfo
	// Columns resolves identifiers in CHECK predicates: lowercased original
	// column name → info. Nil is fine; lookups just miss.
	Columns map[string]ColInfo

	// QuoteIdent renders an identifier in the target's convention
	// (normalize + quote). Supplied by the caller so expr stays leaf.
	QuoteIdent func(string) string
}

func (o Opts) colFor(name string) ColInfo {
	if o.Columns == nil {
		return ColInfo{}
	}
	return o.Columns[lowerASCII(name)]
}

func (o Opts) quote(name string) string {
	if o.QuoteIdent == nil {
		return name
	}
	return o.QuoteIdent(name)
}

func lowerASCII(s string) string {
	b := []byte(s)
	for i, c := range b {
		if c >= 'A' && c <= 'Z' {
			b[i] = c + ('a' - 'A')
		}
	}
	return string(b)
}
