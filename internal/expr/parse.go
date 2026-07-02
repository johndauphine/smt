package expr

import (
	"strings"
)

// ParseDefault parses a column DEFAULT expression. It never fails: anything
// outside the grammar returns Raw with the original text, which Render
// refuses so callers fall back to their legacy pipeline. A lone bare word
// (MySQL enum-style default) parses as a string literal with Bare set —
// defaults cannot reference columns, so an identifier reading is impossible.
//
// The dialect parameter is accepted for API symmetry and future
// dialect-specific grammar; the tokenizer currently accepts the union of all
// three dialects' quoting forms ([x], `x`, "x", N'…', _charset'…'), which is
// also what makes the dialect-blind comparator callers work.
func ParseDefault(raw, dialect string) Node {
	n := parseFragment(raw)
	if n == nil {
		return nil
	}
	// Outer grouping parens are noise (MSSQL wraps every default in one or
	// two pairs); strip them like the legacy unwrapDefaultParens did.
	n = unwrapOuterParens(n)
	// Defaults can't reference columns: a lone identifier is a bare-word
	// string constant...
	if id, ok := n.(Ident); ok {
		return Lit{Kind: LitString, Str: id.Name, Bare: true}
	}
	// ...and any identifier deeper in the tree means the input is not a
	// well-formed default — fall back to Raw (fail-closed) rather than
	// rendering a column reference into a DEFAULT clause.
	if containsIdent(n) {
		return Raw{Text: strings.TrimSpace(raw)}
	}
	return n
}

func containsIdent(n Node) bool {
	switch v := n.(type) {
	case Ident:
		return true
	case Paren:
		return containsIdent(v.X)
	case Unary:
		return containsIdent(v.X)
	case Binary:
		return containsIdent(v.L) || containsIdent(v.R)
	case Call:
		for _, a := range v.Args {
			if containsIdent(a) {
				return true
			}
		}
	case In:
		if containsIdent(v.X) {
			return true
		}
		for _, e := range v.List {
			if containsIdent(e) {
				return true
			}
		}
	case Like:
		return containsIdent(v.X) || containsIdent(v.Pattern)
	case Cast:
		return containsIdent(v.X)
	case AtTimeZone:
		return containsIdent(v.X)
	}
	return false
}

func unwrapOuterParens(n Node) Node {
	for {
		p, ok := n.(Paren)
		if !ok {
			return n
		}
		n = p.X
	}
}

// ParseCheck parses a CHECK constraint predicate. Identifiers stay
// identifiers (they reference columns). Same Raw fallback contract as
// ParseDefault. Outer grouping is stripped — the DDL emitters re-wrap with
// their own CHECK (...) parenthesization.
func ParseCheck(raw, dialect string) Node {
	return unwrapOuterParens(parseFragment(raw))
}

func parseFragment(raw string) Node {
	s := strings.TrimSpace(raw)
	if s == "" {
		return nil
	}
	toks, ok := tokenize(s)
	if !ok || len(toks) == 0 {
		return Raw{Text: s}
	}
	p := &parser{toks: toks}
	n := p.parseOr()
	if n == nil || !p.atEnd() {
		return Raw{Text: s}
	}
	return n
}

// --- tokenizer ---

type tokKind int

const (
	tkIdent  tokKind = iota // bare identifier or keyword
	tkQIdent                // quoted identifier ([x], `x`, "x") — value unquoted
	tkString                // string literal — value unquoted, escapes resolved
	tkNumber
	tkOp    // operator symbol
	tkPunct // ( ) , [ ]
)

type token struct {
	kind tokKind
	val  string
}

func tokenize(s string) ([]token, bool) {
	var toks []token
	i := 0
	for i < len(s) {
		c := s[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			i++
		case c == '\'':
			val, next, ok := scanString(s, i)
			if !ok {
				return nil, false
			}
			toks = append(toks, token{tkString, val})
			i = next
		case (c == 'N' || c == 'n') && i+1 < len(s) && s[i+1] == '\'':
			// MSSQL Unicode string prefix.
			val, next, ok := scanString(s, i+1)
			if !ok {
				return nil, false
			}
			toks = append(toks, token{tkString, val})
			i = next
		case c == '_' && isCharsetPrefix(s, i):
			// MySQL charset introducer: _utf8mb4'…'.
			q := strings.IndexByte(s[i:], '\'')
			val, next, ok := scanString(s, i+q)
			if !ok {
				return nil, false
			}
			toks = append(toks, token{tkString, val})
			i = next
		case c == '[':
			end := strings.IndexByte(s[i+1:], ']')
			if end < 0 {
				return nil, false
			}
			toks = append(toks, token{tkQIdent, s[i+1 : i+1+end]})
			i += end + 2
		case c == '`':
			val, next, ok := scanQuoted(s, i, '`')
			if !ok {
				return nil, false
			}
			toks = append(toks, token{tkQIdent, val})
			i = next
		case c == '"':
			val, next, ok := scanQuoted(s, i, '"')
			if !ok {
				return nil, false
			}
			toks = append(toks, token{tkQIdent, val})
			i = next
		case c >= '0' && c <= '9', c == '.' && i+1 < len(s) && s[i+1] >= '0' && s[i+1] <= '9':
			j := i
			seenDot := false
			for j < len(s) && (s[j] >= '0' && s[j] <= '9' || s[j] == '.' && !seenDot) {
				if s[j] == '.' {
					seenDot = true
				}
				j++
			}
			toks = append(toks, token{tkNumber, s[i:j]})
			i = j
		case isIdentStart(c):
			j := i
			for j < len(s) && isIdentChar(s[j]) {
				j++
			}
			toks = append(toks, token{tkIdent, s[i:j]})
			i = j
		case c == '(', c == ')', c == ',':
			toks = append(toks, token{tkPunct, string(c)})
			i++
		case c == ':' && i+1 < len(s) && s[i+1] == ':':
			toks = append(toks, token{tkOp, "::"})
			i += 2
		case c == '|' && i+1 < len(s) && s[i+1] == '|':
			toks = append(toks, token{tkOp, "||"})
			i += 2
		case c == '<':
			if i+1 < len(s) && (s[i+1] == '>' || s[i+1] == '=') {
				toks = append(toks, token{tkOp, s[i : i+2]})
				i += 2
			} else {
				toks = append(toks, token{tkOp, "<"})
				i++
			}
		case c == '>':
			if i+1 < len(s) && s[i+1] == '=' {
				toks = append(toks, token{tkOp, ">="})
				i += 2
			} else {
				toks = append(toks, token{tkOp, ">"})
				i++
			}
		case c == '!':
			if i+1 < len(s) && s[i+1] == '=' {
				toks = append(toks, token{tkOp, "!="})
				i += 2
			} else if i+1 < len(s) && s[i+1] == '~' {
				toks = append(toks, token{tkOp, "!~"})
				i += 2
			} else {
				return nil, false
			}
		case c == '=', c == '+', c == '-', c == '*', c == '/', c == '~':
			toks = append(toks, token{tkOp, string(c)})
			i++
		default:
			return nil, false
		}
	}
	return toks, true
}

func scanString(s string, start int) (string, int, bool) {
	// s[start] must be the opening quote.
	var b strings.Builder
	i := start + 1
	for i < len(s) {
		if s[i] == '\'' {
			if i+1 < len(s) && s[i+1] == '\'' {
				b.WriteByte('\'')
				i += 2
				continue
			}
			return b.String(), i + 1, true
		}
		b.WriteByte(s[i])
		i++
	}
	return "", 0, false
}

func scanQuoted(s string, start int, q byte) (string, int, bool) {
	var b strings.Builder
	i := start + 1
	for i < len(s) {
		if s[i] == q {
			if i+1 < len(s) && s[i+1] == q {
				b.WriteByte(q)
				i += 2
				continue
			}
			return b.String(), i + 1, true
		}
		b.WriteByte(s[i])
		i++
	}
	return "", 0, false
}

func isCharsetPrefix(s string, i int) bool {
	j := i + 1
	for j < len(s) && isIdentChar(s[j]) {
		j++
	}
	return j > i+1 && j < len(s) && s[j] == '\''
}

func isIdentStart(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c == '_'
}

func isIdentChar(c byte) bool {
	return isIdentStart(c) || c >= '0' && c <= '9' || c == '$'
}

// --- parser ---

type parser struct {
	toks []token
	pos  int
	fail bool
}

func (p *parser) atEnd() bool { return p.pos >= len(p.toks) }

func (p *parser) peek() (token, bool) {
	if p.atEnd() {
		return token{}, false
	}
	return p.toks[p.pos], true
}

func (p *parser) next() (token, bool) {
	t, ok := p.peek()
	if ok {
		p.pos++
	}
	return t, ok
}

func (p *parser) keyword(words ...string) bool {
	if p.pos+len(words) > len(p.toks) {
		return false
	}
	for i, w := range words {
		t := p.toks[p.pos+i]
		if t.kind != tkIdent || !strings.EqualFold(t.val, w) {
			return false
		}
	}
	p.pos += len(words)
	return true
}

func (p *parser) punct(v string) bool {
	if t, ok := p.peek(); ok && t.kind == tkPunct && t.val == v {
		p.pos++
		return true
	}
	return false
}

func (p *parser) op(v string) bool {
	if t, ok := p.peek(); ok && t.kind == tkOp && t.val == v {
		p.pos++
		return true
	}
	return false
}

func (p *parser) parseOr() Node {
	l := p.parseAnd()
	if l == nil {
		return nil
	}
	for p.keyword("OR") {
		r := p.parseAnd()
		if r == nil {
			return nil
		}
		l = Binary{Op: "OR", L: l, R: r}
	}
	return l
}

func (p *parser) parseAnd() Node {
	l := p.parseNot()
	if l == nil {
		return nil
	}
	for p.keyword("AND") {
		r := p.parseNot()
		if r == nil {
			return nil
		}
		l = Binary{Op: "AND", L: l, R: r}
	}
	return l
}

func (p *parser) parseNot() Node {
	if p.keyword("NOT") {
		x := p.parseNot()
		if x == nil {
			return nil
		}
		return Unary{Op: "NOT", X: x}
	}
	return p.parsePredicate()
}

func (p *parser) parsePredicate() Node {
	l := p.parseAdditive()
	if l == nil {
		return nil
	}
	for {
		if t, ok := p.peek(); ok && t.kind == tkOp {
			switch t.val {
			case "=", "<>", "!=", "<", "<=", ">", ">=":
				p.pos++
				// pg idiom: x = ANY (ARRAY[...]) normalizes to IN.
				if t.val == "=" {
					if list, ok := p.parseAnyArray(); ok {
						l = In{X: l, List: list}
						continue
					}
				}
				r := p.parseAdditive()
				if r == nil {
					return nil
				}
				l = Binary{Op: t.val, L: l, R: r}
				continue
			case "~", "!~":
				p.pos++
				pat := p.parseAdditive()
				if pat == nil {
					return nil
				}
				l = Like{X: l, Pattern: pat, Regex: true, Not: t.val == "!~"}
				continue
			}
		}
		not := false
		save := p.pos
		if p.keyword("NOT") {
			not = true
		}
		switch {
		case p.keyword("IN"):
			if !p.punct("(") {
				return nil
			}
			var list []Node
			for {
				e := p.parseAdditive()
				if e == nil {
					return nil
				}
				list = append(list, e)
				if p.punct(",") {
					continue
				}
				break
			}
			if !p.punct(")") {
				return nil
			}
			l = In{X: l, List: list, Not: not}
			continue
		case p.keyword("LIKE"):
			pat := p.parseAdditive()
			if pat == nil {
				return nil
			}
			// ESCAPE clauses are outside the grammar.
			if t, ok := p.peek(); ok && t.kind == tkIdent && strings.EqualFold(t.val, "ESCAPE") {
				return nil
			}
			l = Like{X: l, Pattern: pat, Not: not}
			continue
		case p.keyword("REGEXP"), p.keyword("RLIKE"):
			pat := p.parseAdditive()
			if pat == nil {
				return nil
			}
			l = Like{X: l, Pattern: pat, Regex: true, Not: not}
			continue
		case p.keyword("IS"):
			if not {
				// "NOT x IS NULL" — unusual; bail.
				return nil
			}
			isNot := p.keyword("NOT")
			if !p.keyword("NULL") {
				return nil
			}
			if isNot {
				l = Unary{Op: "IS NOT NULL", X: l}
			} else {
				l = Unary{Op: "IS NULL", X: l}
			}
			continue
		}
		if not {
			p.pos = save
		}
		break
	}
	return l
}

// parseAnyArray consumes "ANY ( [(] ARRAY[elems] [)[::cast]] )" after an
// already-consumed "=", returning the element list. The [elems] body scans
// as one bracketed token (same shape the tokenizer uses for MSSQL [idents]),
// so it is sub-parsed as a comma-separated expression list. On no-match the
// position is restored and ok=false.
func (p *parser) parseAnyArray() ([]Node, bool) {
	save := p.pos
	if !p.keyword("ANY") || !p.punct("(") {
		p.pos = save
		return nil, false
	}
	extraParen := p.punct("(")
	if !p.keyword("ARRAY") {
		p.pos = save
		return nil, false
	}
	body, ok := p.peek()
	if !ok || body.kind != tkQIdent {
		p.pos = save
		return nil, false
	}
	p.pos++
	list, listOK := parseExprList(body.val)
	if !listOK {
		p.pos = save
		return nil, false
	}
	if extraParen {
		if !p.punct(")") {
			p.pos = save
			return nil, false
		}
		if p.op("::") {
			if _, ok := p.parseCastTypeName(); !ok {
				p.pos = save
				return nil, false
			}
		}
	}
	if !p.punct(")") {
		p.pos = save
		return nil, false
	}
	return list, true
}

// parseExprList parses a comma-separated expression list from raw text (the
// body of an ARRAY[...] literal).
func parseExprList(body string) ([]Node, bool) {
	toks, ok := tokenize(strings.TrimSpace(body))
	if !ok || len(toks) == 0 {
		return nil, false
	}
	sub := &parser{toks: toks}
	var list []Node
	for {
		e := sub.parseOr()
		if e == nil {
			return nil, false
		}
		list = append(list, e)
		if sub.punct(",") {
			continue
		}
		break
	}
	if !sub.atEnd() {
		return nil, false
	}
	return list, true
}

func (p *parser) parseAdditive() Node {
	l := p.parseMultiplicative()
	if l == nil {
		return nil
	}
	for {
		t, ok := p.peek()
		if !ok || t.kind != tkOp {
			break
		}
		switch t.val {
		case "||":
			p.pos++
			r := p.parseMultiplicative()
			if r == nil {
				return nil
			}
			l = foldConcat(l, r)
		case "+", "-":
			p.pos++
			r := p.parseMultiplicative()
			if r == nil {
				return nil
			}
			l = Binary{Op: t.val, L: l, R: r}
		default:
			return l
		}
	}
	return l
}

// foldConcat flattens a || b || c into one Concat call so Equal sees one
// n-ary node regardless of source associativity or CONCAT() spelling.
func foldConcat(l, r Node) Node {
	if c, ok := l.(Call); ok && c.Fn == Concat && c.Raw == "||" {
		c.Args = append(c.Args, r)
		return c
	}
	return Call{Fn: Concat, Raw: "||", Args: []Node{l, r}}
}

func (p *parser) parseMultiplicative() Node {
	l := p.parseUnary()
	if l == nil {
		return nil
	}
	for {
		t, ok := p.peek()
		if !ok || t.kind != tkOp || t.val != "*" && t.val != "/" {
			break
		}
		p.pos++
		r := p.parseUnary()
		if r == nil {
			return nil
		}
		l = Binary{Op: t.val, L: l, R: r}
	}
	return l
}

func (p *parser) parseUnary() Node {
	if p.op("-") {
		x := p.parseUnary()
		if x == nil {
			return nil
		}
		if lit, ok := x.(Lit); ok && lit.Kind == LitNumber {
			lit.Num = "-" + lit.Num
			return lit
		}
		return Unary{Op: "-", X: x}
	}
	return p.parsePostfix()
}

func (p *parser) parsePostfix() Node {
	x := p.parsePrimary()
	if x == nil {
		return nil
	}
	for {
		switch {
		case p.op("::"):
			// ::typename — possibly multi-word (character varying, timestamp
			// with time zone) with an optional (n) suffix and [] array marker.
			name, ok := p.parseCastTypeName()
			if !ok {
				return nil
			}
			switch {
			case name == "date":
				x = Cast{X: x, To: CastDate}
			case strings.HasPrefix(name, "time") && !strings.HasPrefix(name, "timestamp"):
				x = Cast{X: x, To: CastTime}
			default:
				// Lexical cast only — strip it, keeping the value (matches
				// stripPostgresCasts and the default classifier).
			}
		case p.keyword("AT", "TIME", "ZONE"):
			t, ok := p.next()
			if !ok || t.kind != tkString {
				return nil
			}
			if strings.EqualFold(t.val, "utc") {
				if isNowFamily(x) {
					x = Call{Fn: UtcNow, Raw: rawSpelling(x)}
					continue
				}
			}
			x = AtTimeZone{X: x, Zone: t.val}
		default:
			return x
		}
	}
}

// parseCastTypeName consumes a pg cast target: one or more identifier words,
// an optional (n[,m]) qualifier, and an optional [] array suffix. Returns the
// lowercased space-joined name.
func (p *parser) parseCastTypeName() (string, bool) {
	// A bracketed/quoted type name ([date], "date") is one word.
	if t, ok := p.peek(); ok && t.kind == tkQIdent && t.val != "" {
		p.pos++
		return strings.ToLower(t.val), true
	}
	var words []string
	for {
		t, ok := p.peek()
		if !ok || t.kind != tkIdent {
			break
		}
		w := strings.ToLower(t.val)
		if len(words) > 0 && !isCastContinuation(words, w) {
			break
		}
		words = append(words, w)
		p.pos++
	}
	if len(words) == 0 {
		return "", false
	}
	if p.punct("(") {
		for {
			t, ok := p.next()
			if !ok {
				return "", false
			}
			if t.kind == tkPunct && t.val == ")" {
				break
			}
		}
	}
	// text[] — the '[' token scans as a bracketed identifier with empty
	// body, which tokenize rejects... but "[]" scans as tkQIdent "".
	if t, ok := p.peek(); ok && t.kind == tkQIdent && t.val == "" {
		p.pos++
	}
	return strings.Join(words, " "), true
}

// isCastContinuation keeps multi-word pg type names together (character
// varying, double precision, timestamp/time with[out] time zone) without
// swallowing a following keyword like AND.
func isCastContinuation(words []string, next string) bool {
	switch next {
	case "varying", "precision", "with", "without", "time", "zone":
		return true
	}
	return false
}

func (p *parser) parsePrimary() Node {
	t, ok := p.peek()
	if !ok {
		return nil
	}
	switch t.kind {
	case tkNumber:
		p.pos++
		return Lit{Kind: LitNumber, Num: t.val}
	case tkString:
		p.pos++
		return Lit{Kind: LitString, Str: t.val}
	case tkQIdent:
		p.pos++
		return Ident{Name: t.val}
	case tkPunct:
		if t.val == "(" {
			p.pos++
			inner := p.parseOr()
			if inner == nil || !p.punct(")") {
				return nil
			}
			return Paren{X: inner}
		}
		return nil
	case tkIdent:
		return p.parseIdentOrCall()
	}
	return nil
}

func (p *parser) parseIdentOrCall() Node {
	t, _ := p.next()
	word := t.val
	lower := strings.ToLower(word)

	// Parenless keyword forms.
	if next, ok := p.peek(); !ok || !(next.kind == tkPunct && next.val == "(") {
		switch lower {
		case "null":
			return Lit{Kind: LitNull}
		case "true":
			return Lit{Kind: LitBool, Bool: true}
		case "false":
			return Lit{Kind: LitBool, Bool: false}
		case "current_timestamp", "localtimestamp", "systimestamp":
			return Call{Fn: NowDateTime, Raw: word, Keyword: true}
		case "current_date":
			return Call{Fn: CurrentDate, Raw: word, Keyword: true}
		case "current_time", "localtime":
			return Call{Fn: CurrentTime, Raw: word, Keyword: true}
		}
		return Ident{Name: word}
	}

	// CAST(x AS type) and CONVERT(type, x) keep their class-relevant forms.
	if lower == "cast" {
		return p.parseCast()
	}
	if lower == "convert" {
		return p.parseConvert(word)
	}

	p.pos++ // consume '('
	var args []Node
	if !p.punct(")") {
		for {
			a := p.parseOr()
			if a == nil {
				return nil
			}
			args = append(args, a)
			if p.punct(",") {
				continue
			}
			break
		}
		if !p.punct(")") {
			return nil
		}
	}

	cat := funcCategory(lower)
	switch cat {
	case NowDateTime, UtcNow, CurrentDate, CurrentTime:
		// Drop a single numeric fsp argument (CURRENT_TIMESTAMP(6), NOW(3)).
		if len(args) == 1 {
			if lit, ok := args[0].(Lit); ok && lit.Kind == LitNumber {
				args = nil
			}
		}
		if len(args) != 0 {
			return Call{Fn: UnknownFn, Raw: word, Args: args}
		}
		return Call{Fn: cat, Raw: word}
	case UuidGen:
		if len(args) != 0 {
			return Call{Fn: UnknownFn, Raw: word, Args: args}
		}
		return Call{Fn: UuidGen, Raw: word}
	case Concat:
		return Call{Fn: Concat, Raw: word, Args: args}
	case Coalesce:
		if len(args) < 2 {
			return Call{Fn: UnknownFn, Raw: word, Args: args}
		}
		return Call{Fn: Coalesce, Raw: word, Args: args}
	case PassThroughFn:
		return Call{Fn: PassThroughFn, Raw: word, Args: args}
	}
	if lower == "regexp_like" && len(args) == 2 {
		return Like{X: args[0], Pattern: args[1], Regex: true}
	}
	return Call{Fn: UnknownFn, Raw: word, Args: args}
}

func (p *parser) parseCast() Node {
	if !p.punct("(") {
		return nil
	}
	x := p.parseOr()
	if x == nil || !p.keyword("AS") {
		return nil
	}
	name, ok := p.parseCastTypeName()
	if !ok || !p.punct(")") {
		return nil
	}
	switch {
	case name == "date":
		return Cast{X: x, To: CastDate}
	case strings.HasPrefix(name, "time") && !strings.HasPrefix(name, "timestamp"):
		return Cast{X: x, To: CastTime}
	}
	// CAST to any other type is outside the class-relevant grammar; keeping
	// the legacy fail-closed behavior means not silently stripping it.
	return nil
}

// parseConvert handles the MSSQL CONVERT(type, x[, style]) form. Only
// CONVERT(date, x) / CONVERT(time, x) are class-relevant; anything else is
// outside the grammar (legacy pipelines reject it cross-dialect).
func (p *parser) parseConvert(word string) Node {
	if !p.punct("(") {
		return nil
	}
	name, ok := p.parseCastTypeName()
	if !ok {
		return nil
	}
	if !p.punct(",") {
		return nil
	}
	x := p.parseOr()
	if x == nil {
		return nil
	}
	if p.punct(",") {
		// CONVERT style argument — outside the grammar.
		return nil
	}
	if !p.punct(")") {
		return nil
	}
	switch {
	case name == "date":
		return Cast{X: x, To: CastDate}
	case name == "time":
		return Cast{X: x, To: CastTime}
	}
	return nil
}

func funcCategory(lower string) FuncCat {
	switch lower {
	case "getdate", "sysdatetime", "sysdatetimeoffset", "now", "current_timestamp", "localtimestamp", "systimestamp":
		return NowDateTime
	case "getutcdate", "sysutcdatetime", "utc_timestamp":
		return UtcNow
	case "current_date", "curdate":
		return CurrentDate
	case "current_time", "curtime", "localtime":
		return CurrentTime
	case "newid", "newsequentialid", "uuid", "gen_random_uuid", "uuid_generate_v4":
		return UuidGen
	case "isnull", "coalesce", "ifnull":
		return Coalesce
	case "concat":
		return Concat
	case "lower", "upper", "nullif":
		return PassThroughFn
	}
	return UnknownFn
}

// isNowFamily reports whether the node is a now-datetime call (used for the
// AT TIME ZONE 'utc' → UtcNow normalization).
func isNowFamily(n Node) bool {
	for {
		if pn, ok := n.(Paren); ok {
			n = pn.X
			continue
		}
		break
	}
	c, ok := n.(Call)
	return ok && (c.Fn == NowDateTime || c.Fn == UtcNow)
}

func rawSpelling(n Node) string {
	for {
		if pn, ok := n.(Paren); ok {
			n = pn.X
			continue
		}
		break
	}
	if c, ok := n.(Call); ok {
		return c.Raw
	}
	return ""
}
