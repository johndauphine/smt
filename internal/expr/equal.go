package expr

import (
	"strings"
)

// Equal reports whether two parsed expressions are semantically equivalent
// under the cross-dialect rules the deterministic comparator has always
// used: explicit grouping is ignored, every now-datetime spelling matches
// every other (including the UTC forms — GETUTCDATE() ≡ CURRENT_TIMESTAMP,
// exactly like the legacy class map), CURRENT_DATE ≢ CURRENT_TIMESTAMP,
// 0/1 ≡ false/true, bare words ≡ quoted strings (case-insensitively), and
// lexical casts were already stripped at parse. Unrecognized forms compare
// by normalized structure and spelling.
func Equal(a, b Node) bool {
	return nodeEqual(normalize(a), normalize(b))
}

// normalize rewrites a node to its comparison normal form.
func normalize(n Node) Node {
	switch v := n.(type) {
	case nil:
		return nil
	case Paren:
		return normalize(v.X)
	case Cast:
		x := normalize(v.X)
		if c, ok := x.(Call); ok && (c.Fn == NowDateTime || c.Fn == UtcNow) {
			// CONVERT(date, GETDATE()) / now()::date ≡ CURRENT_DATE — and
			// distinct from the underlying now (pinned by the legacy
			// classifier's negative cases).
			if v.To == CastDate {
				return Call{Fn: CurrentDate}
			}
			return Call{Fn: CurrentTime}
		}
		return Cast{X: x, To: v.To}
	case AtTimeZone:
		x := normalize(v.X)
		if strings.EqualFold(v.Zone, "utc") {
			if c, ok := x.(Call); ok && (c.Fn == NowDateTime || c.Fn == UtcNow) {
				return Call{Fn: NowDateTime}
			}
		}
		return AtTimeZone{X: x, Zone: strings.ToLower(v.Zone)}
	case Call:
		switch v.Fn {
		case NowDateTime, UtcNow:
			// One now-datetime class, matching the legacy classifier: local
			// and UTC spellings all map to current_dt.
			return Call{Fn: NowDateTime}
		case CurrentDate, CurrentTime, UuidGen:
			return Call{Fn: v.Fn}
		case UnknownFn:
			return Call{Fn: UnknownFn, Raw: strings.ToLower(v.Raw), Args: normalizeList(v.Args)}
		default:
			return Call{Fn: v.Fn, Raw: "", Args: normalizeList(v.Args)}
		}
	case Lit:
		switch v.Kind {
		case LitNumber:
			// Bare 0/1 defaults are boolean-intent across dialects (MSSQL
			// bit defaults arrive as ((0))/((1))).
			if v.Num == "0" {
				return Lit{Kind: LitBool, Bool: false}
			}
			if v.Num == "1" {
				return Lit{Kind: LitBool, Bool: true}
			}
			return Lit{Kind: LitNumber, Num: v.Num}
		case LitString:
			// A bare word that spells a now/uuid-family function name is that
			// function for comparison purposes (catalogs sometimes report
			// defaults with the parens stripped) — rendering keeps treating
			// it as a string, matching the legacy renderer/classifier split.
			if v.Bare {
				switch funcCategory(strings.ToLower(v.Str)) {
				case NowDateTime, UtcNow:
					return Call{Fn: NowDateTime}
				case CurrentDate:
					return Call{Fn: CurrentDate}
				case CurrentTime:
					return Call{Fn: CurrentTime}
				case UuidGen:
					return Call{Fn: UuidGen}
				}
			}
			// Bare-word and quoting spellings collapse; the legacy class
			// comparison lowercased the whole expression, so string
			// comparison stays case-insensitive.
			return Lit{Kind: LitString, Str: strings.ToLower(v.Str)}
		}
		return v
	case Ident:
		return Ident{Name: strings.ToLower(v.Name)}
	case Unary:
		return Unary{Op: v.Op, X: normalize(v.X)}
	case Binary:
		return Binary{Op: normalizeOp(v.Op), L: normalize(v.L), R: normalize(v.R)}
	case In:
		return In{X: normalize(v.X), List: normalizeList(v.List), Not: v.Not}
	case Like:
		return Like{X: normalize(v.X), Pattern: normalize(v.Pattern), Not: v.Not, Regex: v.Regex}
	case Raw:
		return Raw{Text: stripOuterParenText(strings.ToLower(strings.Join(strings.Fields(v.Text), " ")))}
	}
	return n
}

// stripOuterParenText removes matched outer paren pairs from an unparsed
// expression's text so two Raw spellings compare (and label) the way the
// legacy classifier normalized them.
func stripOuterParenText(s string) string {
	for len(s) >= 2 && s[0] == '(' && s[len(s)-1] == ')' {
		depth := 0
		matched := true
		for i := 0; i < len(s); i++ {
			switch s[i] {
			case '(':
				depth++
			case ')':
				depth--
				if depth == 0 && i < len(s)-1 {
					matched = false
				}
			}
		}
		if !matched || depth != 0 {
			return s
		}
		s = strings.TrimSpace(s[1 : len(s)-1])
	}
	return s
}

func normalizeList(list []Node) []Node {
	out := make([]Node, len(list))
	for i, e := range list {
		out[i] = normalize(e)
	}
	return out
}

func normalizeOp(op string) string {
	if op == "!=" {
		return "<>"
	}
	return op
}

func nodeEqual(a, b Node) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	switch av := a.(type) {
	case Lit:
		bv, ok := b.(Lit)
		return ok && av == bv
	case Ident:
		bv, ok := b.(Ident)
		return ok && av == bv
	case Call:
		bv, ok := b.(Call)
		if !ok || av.Fn != bv.Fn || av.Raw != bv.Raw || len(av.Args) != len(bv.Args) {
			return false
		}
		for i := range av.Args {
			if !nodeEqual(av.Args[i], bv.Args[i]) {
				return false
			}
		}
		return true
	case Unary:
		bv, ok := b.(Unary)
		return ok && av.Op == bv.Op && nodeEqual(av.X, bv.X)
	case Binary:
		bv, ok := b.(Binary)
		return ok && av.Op == bv.Op && nodeEqual(av.L, bv.L) && nodeEqual(av.R, bv.R)
	case In:
		bv, ok := b.(In)
		if !ok || av.Not != bv.Not || len(av.List) != len(bv.List) || !nodeEqual(av.X, bv.X) {
			return false
		}
		for i := range av.List {
			if !nodeEqual(av.List[i], bv.List[i]) {
				return false
			}
		}
		return true
	case Like:
		bv, ok := b.(Like)
		return ok && av.Not == bv.Not && av.Regex == bv.Regex &&
			nodeEqual(av.X, bv.X) && nodeEqual(av.Pattern, bv.Pattern)
	case Cast:
		bv, ok := b.(Cast)
		return ok && av.To == bv.To && nodeEqual(av.X, bv.X)
	case AtTimeZone:
		bv, ok := b.(AtTimeZone)
		return ok && av.Zone == bv.Zone && nodeEqual(av.X, bv.X)
	case Raw:
		bv, ok := b.(Raw)
		return ok && av == bv
	}
	return false
}

// ClassLabel names an expression's equivalence class for delta messages,
// preserving the legacy defaultExpressionClass vocabulary: "", current_dt,
// current_date, current_t, uuid_gen, null, true, false, constant<N>,
// constant'<s>', other:<normalized>.
func ClassLabel(n Node) string {
	n = normalize(n)
	switch v := n.(type) {
	case nil:
		return ""
	case Call:
		switch v.Fn {
		case NowDateTime:
			return "current_dt"
		case CurrentDate:
			return "current_date"
		case CurrentTime:
			return "current_t"
		case UuidGen:
			return "uuid_gen"
		}
	case Lit:
		switch v.Kind {
		case LitNull:
			return "null"
		case LitBool:
			if v.Bool {
				return "true"
			}
			return "false"
		case LitNumber:
			return "constant" + v.Num
		case LitString:
			return "constant'" + v.Str + "'"
		}
	}
	return "other:" + serialize(n)
}

// serialize renders a normalized node compactly for other:-class labels and
// debugging. Lossy (identifier quoting and literal escapes are simplified);
// never used as SQL.
func serialize(n Node) string {
	switch v := n.(type) {
	case nil:
		return ""
	case Lit:
		switch v.Kind {
		case LitNull:
			return "null"
		case LitBool:
			if v.Bool {
				return "true"
			}
			return "false"
		case LitNumber:
			return v.Num
		default:
			return "'" + v.Str + "'"
		}
	case Ident:
		return v.Name
	case Call:
		name := v.Raw
		if name == "" {
			name = classCallName(v.Fn)
		}
		args := make([]string, len(v.Args))
		for i, a := range v.Args {
			args[i] = serialize(a)
		}
		return name + "(" + strings.Join(args, ",") + ")"
	case Unary:
		if v.Op == "IS NULL" || v.Op == "IS NOT NULL" {
			return serialize(v.X) + " " + strings.ToLower(v.Op)
		}
		return strings.ToLower(v.Op) + " " + serialize(v.X)
	case Binary:
		return serialize(v.L) + " " + strings.ToLower(v.Op) + " " + serialize(v.R)
	case In:
		op := " in "
		if v.Not {
			op = " not in "
		}
		parts := make([]string, len(v.List))
		for i, e := range v.List {
			parts[i] = serialize(e)
		}
		return serialize(v.X) + op + "(" + strings.Join(parts, ",") + ")"
	case Like:
		op := " like "
		if v.Regex {
			op = " ~ "
		}
		if v.Not {
			op = " not" + op
		}
		return serialize(v.X) + op + serialize(v.Pattern)
	case Cast:
		kind := "date"
		if v.To == CastTime {
			kind = "time"
		}
		return serialize(v.X) + "::" + kind
	case AtTimeZone:
		return serialize(v.X) + " at time zone '" + v.Zone + "'"
	case Raw:
		return v.Text
	}
	return ""
}

func classCallName(fn FuncCat) string {
	switch fn {
	case NowDateTime:
		return "current_timestamp"
	case CurrentDate:
		return "current_date"
	case CurrentTime:
		return "current_time"
	case UuidGen:
		return "uuid"
	case Coalesce:
		return "coalesce"
	case Concat:
		return "concat"
	}
	return "fn"
}
