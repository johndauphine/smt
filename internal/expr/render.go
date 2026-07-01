package expr

import (
	"fmt"
	"regexp"
	"strings"
)

// Render serializes a parsed node in the target dialect. It returns
// ErrUnsupported (possibly wrapped) for forms the target cannot express and
// for Raw/UnknownFn nodes — never a silent best-guess. Callers fall back to
// their legacy pipeline on ErrUnsupported; that pipeline must end in
// RejectUnknownFunctions for cross-dialect runs so the whole surface stays
// fail-closed.
func Render(n Node, o Opts) (string, error) {
	if n == nil {
		return "", nil
	}
	switch v := n.(type) {
	case Raw:
		return "", fmt.Errorf("%w: unparsed expression %q", ErrUnsupported, v.Text)
	case Lit:
		return renderLit(v, o), nil
	case Ident:
		return o.quote(v.Name), nil
	case Paren:
		inner, err := Render(v.X, o)
		if err != nil {
			return "", err
		}
		return "(" + inner + ")", nil
	case Unary:
		return renderUnary(v, o)
	case Binary:
		return renderBinary(v, o)
	case In:
		return renderIn(v, o)
	case Like:
		return renderLike(v, o)
	case Call:
		return renderCall(v, o)
	case Cast:
		return renderCast(v, o)
	case AtTimeZone:
		if o.Target == Postgres {
			inner, err := Render(v.X, o)
			if err != nil {
				return "", err
			}
			return inner + " AT TIME ZONE '" + strings.ReplaceAll(v.Zone, "'", "''") + "'", nil
		}
		// Zone name vocabularies differ per engine; only pg is safe.
		return "", fmt.Errorf("%w: AT TIME ZONE for target %s", ErrUnsupported, o.Target)
	}
	return "", fmt.Errorf("%w: unhandled node %T", ErrUnsupported, n)
}

func renderLit(v Lit, o Opts) string {
	switch v.Kind {
	case LitNull:
		return "NULL"
	case LitBool:
		return boolLit(o.Target, v.Bool)
	case LitNumber:
		// A 0/1 default on a boolean-class column becomes a boolean literal
		// where the target needs one (pg rejects DEFAULT 1 on boolean).
		if o.Kind == "default" && o.Col.Boolean {
			switch v.Num {
			case "0":
				return boolLit(o.Target, false)
			case "1":
				return boolLit(o.Target, true)
			}
		}
		return v.Num
	default: // LitString
		if o.Kind == "default" && o.Target == MySQL {
			// MySQL JSON/array columns take constructor expression defaults
			// for the common empty-container defaults.
			if o.Col.Array && v.Str == "{}" {
				return "JSON_ARRAY()"
			}
			if o.Col.JSON {
				switch v.Str {
				case "{}":
					return "JSON_OBJECT()"
				case "[]":
					return "JSON_ARRAY()"
				}
			}
		}
		if v.Bare && !o.Col.Textual {
			// Bare word on a non-textual column: emit as written (legacy
			// pipelines never quoted these).
			return v.Str
		}
		return "'" + strings.ReplaceAll(v.Str, "'", "''") + "'"
	}
}

func boolLit(target string, v bool) string {
	if target == Postgres {
		if v {
			return "true"
		}
		return "false"
	}
	if v {
		return "1"
	}
	return "0"
}

func renderUnary(v Unary, o Opts) (string, error) {
	x, err := Render(v.X, o)
	if err != nil {
		return "", err
	}
	switch v.Op {
	case "NOT":
		return "NOT " + x, nil
	case "-":
		return "-" + x, nil
	case "IS NULL", "IS NOT NULL":
		return x + " " + v.Op, nil
	}
	return "", fmt.Errorf("%w: unary operator %q", ErrUnsupported, v.Op)
}

func renderBinary(v Binary, o Opts) (string, error) {
	// Boolean-column comparisons against 0/1 take boolean literals:
	// [IsActive]=(1) → "isactive" = true on pg, [IsActive] = 1 on mssql/mysql
	// (LitBool renders per-target, and the literal's grouping parens drop).
	l, r := v.L, v.R
	if isComparisonOp(v.Op) {
		l, r = rewriteBoolComparison(l, r, o)
		r, l = rewriteBoolComparison(r, l, o)
	}
	ls, err := Render(l, o)
	if err != nil {
		return "", err
	}
	rs, err := Render(r, o)
	if err != nil {
		return "", err
	}
	if v.Op == "+" && o.Target == MySQL && (isStringOperand(l, o) || isStringOperand(r, o)) {
		// MSSQL string concatenation: MySQL + is numeric-only.
		return "CONCAT(" + ls + ", " + rs + ")", nil
	}
	if v.Op == "+" && o.Target == Postgres && (isStringOperand(l, o) || isStringOperand(r, o)) {
		return ls + " || " + rs, nil
	}
	return ls + " " + v.Op + " " + rs, nil
}

func isComparisonOp(op string) bool {
	switch op {
	case "=", "<>", "!=":
		return true
	}
	return false
}

// rewriteBoolComparison maps the 0/1 literal side of a comparison to a
// boolean literal when the other side is a boolean-class column. Returns the
// (possibly rewritten) pair in the same order.
func rewriteBoolComparison(colSide, litSide Node, o Opts) (Node, Node) {
	ident, ok := stripParens(colSide).(Ident)
	if !ok || !o.colFor(ident.Name).Boolean {
		return colSide, litSide
	}
	if lit, ok := stripParens(litSide).(Lit); ok && lit.Kind == LitNumber {
		switch lit.Num {
		case "0":
			return colSide, Lit{Kind: LitBool, Bool: false}
		case "1":
			return colSide, Lit{Kind: LitBool, Bool: true}
		}
	}
	return colSide, litSide
}

func stripParens(n Node) Node {
	for {
		p, ok := n.(Paren)
		if !ok {
			return n
		}
		n = p.X
	}
}

// isStringOperand reports whether a node is textually typed as far as the
// renderer can tell: a string literal, a concat, or an identifier naming a
// textual column.
func isStringOperand(n Node, o Opts) bool {
	switch v := stripParens(n).(type) {
	case Lit:
		return v.Kind == LitString
	case Call:
		return v.Fn == Concat
	case Ident:
		return o.colFor(v.Name).Textual
	}
	return false
}

func renderIn(v In, o Opts) (string, error) {
	x, err := Render(v.X, o)
	if err != nil {
		return "", err
	}
	list := v.List
	// A pure 0/1 domain check on a boolean-class column becomes boolean
	// literals on pg: is_active IN (0, 1) → IN (false, true).
	if o.Target == Postgres {
		if ident, ok := stripParens(v.X).(Ident); ok && o.colFor(ident.Name).Boolean && isZeroOneDomain(list) {
			rewritten := make([]Node, len(list))
			for i, e := range list {
				lit := stripParens(e).(Lit)
				rewritten[i] = Lit{Kind: LitBool, Bool: lit.Num == "1"}
			}
			list = rewritten
		}
	}
	parts := make([]string, len(list))
	for i, e := range list {
		s, err := Render(e, o)
		if err != nil {
			return "", err
		}
		parts[i] = s
	}
	op := "IN"
	if v.Not {
		op = "NOT IN"
	}
	return x + " " + op + " (" + strings.Join(parts, ", ") + ")", nil
}

func isZeroOneDomain(list []Node) bool {
	if len(list) == 0 {
		return false
	}
	for _, e := range list {
		lit, ok := stripParens(e).(Lit)
		if !ok || lit.Kind != LitNumber || lit.Num != "0" && lit.Num != "1" {
			return false
		}
	}
	return true
}

func renderLike(v Like, o Opts) (string, error) {
	x, err := Render(v.X, o)
	if err != nil {
		return "", err
	}
	pat, ok := stripParens(v.Pattern).(Lit)
	if !ok || pat.Kind != LitString {
		return "", fmt.Errorf("%w: non-literal LIKE/regex pattern", ErrUnsupported)
	}
	quoted := "'" + strings.ReplaceAll(pat.Str, "'", "''") + "'"

	if !v.Regex {
		// Plain LIKE. MSSQL [class] patterns are native there; pg gets the
		// regex translation, MySQL gets REGEXP (its LIKE has no classes).
		hasClass := strings.Contains(pat.Str, "[") && strings.Contains(pat.Str, "]")
		if !hasClass || o.Target == MSSQL {
			op := "LIKE"
			if v.Not {
				op = "NOT LIKE"
			}
			return x + " " + op + " " + quoted, nil
		}
		re, classOK := likePatternToRegex(pat.Str)
		if !classOK {
			// Bracket-looking but not a translatable class: LIKE semantics
			// keep it literal on pg (matching the legacy no-rewrite path),
			// which is also what the source engine meant only on MSSQL —
			// refuse elsewhere rather than guess.
			op := "LIKE"
			if v.Not {
				op = "NOT LIKE"
			}
			return x + " " + op + " " + quoted, nil
		}
		return renderRegexMatch(x, re, v.Not, o)
	}
	return renderRegexMatch(x, pat.Str, v.Not, o)
}

func renderRegexMatch(operand, pattern string, not bool, o Opts) (string, error) {
	quoted := "'" + strings.ReplaceAll(pattern, "'", "''") + "'"
	switch o.Target {
	case Postgres:
		op := "~"
		if not {
			op = "!~"
		}
		return operand + " " + op + " " + quoted, nil
	case MySQL:
		expr := operand + " REGEXP " + quoted
		if not {
			return "NOT (" + expr + ")", nil
		}
		return "(" + expr + ")", nil
	case MSSQL:
		like, ok := regexToMSSQLLike(pattern)
		if !ok {
			return "", fmt.Errorf("%w: regex pattern %s for SQL Server target", ErrUnsupported, quoted)
		}
		op := "LIKE"
		if not {
			op = "NOT LIKE"
		}
		return "(" + operand + " " + op + " '" + strings.ReplaceAll(like, "'", "''") + "')", nil
	}
	return "", fmt.Errorf("%w: regex match for target %s", ErrUnsupported, o.Target)
}

// likePatternToRegex converts an MSSQL LIKE pattern with [class] wildcards to
// an anchored regex. ok is true only when a real bracket class was used —
// otherwise plain LIKE is the better rendering. (Port of the legacy
// sqlServerLikePatternToRegex.)
func likePatternToRegex(pattern string) (string, bool) {
	var b strings.Builder
	b.Grow(len(pattern) + 2)
	b.WriteByte('^')
	usedBracketClass := false
	for i := 0; i < len(pattern); i++ {
		ch := pattern[i]
		switch ch {
		case '%':
			b.WriteString(".*")
		case '_':
			b.WriteByte('.')
		case '[':
			end := strings.IndexByte(pattern[i+1:], ']')
			if end < 0 {
				b.WriteString(`\[`)
				continue
			}
			class := pattern[i+1 : i+1+end]
			if class == "" {
				b.WriteString(`\[\]`)
			} else {
				usedBracketClass = true
				if class[0] == '^' {
					class = "^" + regexp.QuoteMeta(class[1:])
				} else {
					class = regexp.QuoteMeta(class)
				}
				b.WriteByte('[')
				b.WriteString(class)
				b.WriteByte(']')
			}
			i += end + 1
		default:
			b.WriteString(regexp.QuoteMeta(string(ch)))
		}
	}
	b.WriteByte('$')
	return b.String(), usedBracketClass
}

// regexToMSSQLLike converts a fully-anchored regex of literal characters and
// [class] atoms (with optional {n} repetition) back to an MSSQL LIKE pattern.
// (Port of the legacy regexLiteralToSQLServerLike, plus the email-shape
// special case handled by the caller's legacy pipeline is intentionally not
// duplicated here — anything else returns ok=false.)
func regexToMSSQLLike(pattern string) (string, bool) {
	if !strings.HasPrefix(pattern, "^") || !strings.HasSuffix(pattern, "$") {
		return "", false
	}
	pattern = strings.TrimSuffix(strings.TrimPrefix(pattern, "^"), "$")
	var b strings.Builder
	for i := 0; i < len(pattern); i++ {
		ch := pattern[i]
		if ch == '[' {
			end := strings.IndexByte(pattern[i+1:], ']')
			if end < 0 {
				return "", false
			}
			token := pattern[i : i+end+2]
			i += end + 1
			repeat := 1
			if i+1 < len(pattern) && pattern[i+1] == '{' {
				close := strings.IndexByte(pattern[i+2:], '}')
				if close < 0 {
					return "", false
				}
				n := 0
				for _, r := range pattern[i+2 : i+2+close] {
					if r < '0' || r > '9' {
						return "", false
					}
					n = n*10 + int(r-'0')
				}
				if n <= 0 {
					return "", false
				}
				repeat = n
				i += close + 2
			}
			for j := 0; j < repeat; j++ {
				b.WriteString(token)
			}
			continue
		}
		if strings.ContainsRune(`\.^$*+?()|{}`, rune(ch)) {
			return "", false
		}
		if ch == '%' || ch == '_' {
			return "", false
		}
		b.WriteByte(ch)
	}
	return b.String(), true
}

func renderCall(v Call, o Opts) (string, error) {
	switch v.Fn {
	case NowDateTime:
		switch o.Target {
		case Postgres:
			return "CURRENT_TIMESTAMP", nil
		case MSSQL:
			// Native spellings keep their identity (GETDATE vs SYSDATETIME
			// differ in precision); foreign now-forms become SYSDATETIME().
			switch strings.ToLower(v.Raw) {
			case "getdate", "sysdatetime", "sysdatetimeoffset":
				return strings.ToUpper(v.Raw) + "()", nil
			}
			return "SYSDATETIME()", nil
		case MySQL:
			return mysqlNow("CURRENT_TIMESTAMP", o), nil
		}
	case UtcNow:
		switch o.Target {
		case Postgres:
			// On a TZ-aware column CURRENT_TIMESTAMP already stores the
			// correct instant; the AT TIME ZONE 'UTC' form is for naive
			// columns that must hold UTC wall-clock time.
			if o.Col.TZAware {
				return "CURRENT_TIMESTAMP", nil
			}
			return "(CURRENT_TIMESTAMP AT TIME ZONE 'UTC')", nil
		case MSSQL:
			switch strings.ToLower(v.Raw) {
			case "getutcdate", "sysutcdatetime":
				return strings.ToUpper(v.Raw) + "()", nil
			}
			return "SYSUTCDATETIME()", nil
		case MySQL:
			return mysqlNow("UTC_TIMESTAMP", o), nil
		}
	case CurrentDate:
		switch o.Target {
		case Postgres:
			return "CURRENT_DATE", nil
		case MSSQL:
			return "CONVERT(date, GETDATE())", nil
		case MySQL:
			return "CURDATE()", nil
		}
	case CurrentTime:
		switch o.Target {
		case Postgres:
			return "CURRENT_TIME", nil
		case MSSQL:
			return "CONVERT(time, GETDATE())", nil
		case MySQL:
			return "CURTIME()", nil
		}
	case UuidGen:
		switch o.Target {
		case Postgres:
			return "gen_random_uuid()", nil
		case MSSQL:
			return "NEWID()", nil
		case MySQL:
			return "UUID()", nil
		}
	case Coalesce:
		args, err := renderArgs(v.Args, o)
		if err != nil {
			return "", err
		}
		name := "COALESCE"
		// Native spellings survive on their home target.
		switch {
		case o.Target == MSSQL && strings.EqualFold(v.Raw, "isnull"):
			name = "ISNULL"
		case o.Target == MySQL && strings.EqualFold(v.Raw, "ifnull"):
			name = "IFNULL"
		}
		return name + "(" + strings.Join(args, ", ") + ")", nil
	case Concat:
		args, err := renderArgs(v.Args, o)
		if err != nil {
			return "", err
		}
		if len(args) < 2 {
			return "", fmt.Errorf("%w: CONCAT with %d argument(s)", ErrUnsupported, len(args))
		}
		switch o.Target {
		case Postgres:
			return strings.Join(args, " || "), nil
		case MSSQL:
			return strings.Join(args, " + "), nil
		case MySQL:
			return "CONCAT(" + strings.Join(args, ", ") + ")", nil
		}
	case PassThroughFn:
		args, err := renderArgs(v.Args, o)
		if err != nil {
			return "", err
		}
		return v.Raw + "(" + strings.Join(args, ", ") + ")", nil
	}
	return "", fmt.Errorf("%w: unsupported SQL expression function %q", ErrUnsupported, v.Raw)
}

// mysqlNow renders a now-family default matched to the column's fractional-
// seconds precision — MySQL requires the default's fsp to equal the
// column's. Unknown precision defaults to 6 (the fsp MySQL temporal types
// get when the source doesn't constrain them), clamped to MySQL's max.
// CHECK predicates take the bare form.
func mysqlNow(fn string, o Opts) string {
	if o.Kind == "default" {
		p := 6
		if o.Col.DatetimePrecision != nil && *o.Col.DatetimePrecision >= 0 {
			p = *o.Col.DatetimePrecision
			if p > 6 {
				p = 6
			}
		}
		if p > 0 {
			return fmt.Sprintf("%s(%d)", fn, p)
		}
	}
	if fn == "UTC_TIMESTAMP" {
		return "UTC_TIMESTAMP()"
	}
	return fn
}

func renderArgs(args []Node, o Opts) ([]string, error) {
	out := make([]string, len(args))
	for i, a := range args {
		s, err := Render(a, o)
		if err != nil {
			return nil, err
		}
		out[i] = s
	}
	return out, nil
}

func renderCast(v Cast, o Opts) (string, error) {
	inner := stripParens(v.X)
	if c, ok := inner.(Call); ok {
		switch {
		case v.To == CastDate && c.Fn == NowDateTime:
			switch o.Target {
			case Postgres:
				return "CURRENT_DATE", nil
			case MSSQL:
				return "CONVERT(date, GETDATE())", nil
			case MySQL:
				return "CURDATE()", nil
			}
		case v.To == CastDate && c.Fn == UtcNow:
			switch o.Target {
			case Postgres:
				return "(CURRENT_TIMESTAMP AT TIME ZONE 'UTC')::date", nil
			case MSSQL:
				return "CONVERT(date, GETUTCDATE())", nil
			case MySQL:
				return "UTC_DATE()", nil
			}
		case v.To == CastTime && (c.Fn == NowDateTime || c.Fn == UtcNow):
			fn := "GETDATE()"
			if c.Fn == UtcNow {
				fn = "GETUTCDATE()"
			}
			switch o.Target {
			case Postgres:
				if c.Fn == UtcNow {
					return "(CURRENT_TIMESTAMP AT TIME ZONE 'UTC')::time", nil
				}
				return "CURRENT_TIME", nil
			case MSSQL:
				return "CONVERT(time, " + fn + ")", nil
			case MySQL:
				if c.Fn == UtcNow {
					return "UTC_TIME()", nil
				}
				return "CURTIME()", nil
			}
		}
	}
	x, err := Render(v.X, o)
	if err != nil {
		return "", err
	}
	kind := "date"
	if v.To == CastTime {
		kind = "time"
	}
	switch o.Target {
	case Postgres:
		return x + "::" + kind, nil
	case MSSQL:
		return "CONVERT(" + kind + ", " + x + ")", nil
	case MySQL:
		return "CAST(" + x + " AS " + strings.ToUpper(kind) + ")", nil
	}
	return "", fmt.Errorf("%w: cast for target %s", ErrUnsupported, o.Target)
}

// --- shared fail-closed function gate ---

var expressionFunctionRE = regexp.MustCompile(`(?i)\b([a-z_][a-z0-9_]*(?:\.[a-z_][a-z0-9_]*)*)\s*\(`)

// perTargetAllowedFunctions extends the shared base allowlist with the
// target's own rendered vocabulary — the gate runs on *rendered* output, so
// it must accept everything the target's renderer legitimately emits.
var perTargetAllowedFunctions = map[string]map[string]bool{
	Postgres: {"gen_random_uuid": true},
	MSSQL: {
		"getdate": true, "getutcdate": true, "sysdatetime": true,
		"sysutcdatetime": true, "sysdatetimeoffset": true, "newid": true,
		"isnull": true, "convert": true,
	},
	MySQL: {
		"current_timestamp": true, "utc_timestamp": true, "uuid": true,
		"curdate": true, "curtime": true, "utc_date": true, "utc_time": true,
		"ifnull": true, "concat": true, "json_object": true,
		"json_array": true, "regexp_like": true, "cast": true,
	},
}

var sharedAllowedFunctions = map[string]bool{
	"and": true, "or": true, "case": true, "when": true, "then": true,
	"else": true, "end": true, "coalesce": true, "nullif": true,
	"lower": true, "upper": true, "in": true, "not": true, "exists": true,
	"any": true,
}

// RejectUnknownFunctions is the shared fail-closed gate for legacy
// (string-rewrite) pipelines: any function name outside the target's
// allowlist in the rendered expression is an error, never a silent
// passthrough. String literal contents are not scanned.
func RejectUnknownFunctions(rendered, target string) error {
	scan := stripSingleQuoted(rendered)
	extra := perTargetAllowedFunctions[target]
	for _, match := range expressionFunctionRE.FindAllStringSubmatch(scan, -1) {
		if len(match) != 2 {
			continue
		}
		name := strings.ToLower(match[1])
		if sharedAllowedFunctions[name] || extra[name] {
			continue
		}
		return fmt.Errorf("unsupported SQL expression function %q in %q", match[1], rendered)
	}
	return nil
}

// stripSingleQuoted blanks out single-quoted string contents so the function
// gate never fires on function-like text inside literals.
func stripSingleQuoted(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	in := false
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == '\'' {
			if in && i+1 < len(s) && s[i+1] == '\'' {
				i++
				continue
			}
			in = !in
			b.WriteByte(c)
			continue
		}
		if !in {
			b.WriteByte(c)
		}
	}
	return b.String()
}
