package canonical

import (
	"fmt"
	"strconv"
	"strings"
)

// ClickHouse has a strict but distinct type system. Nullable(T) is a type
// wrapper rather than a column constraint, and LowCardinality(T) is a storage
// optimization. The public canonical API deliberately models neither: SMT's
// column IR owns nullability and has no storage-layout knobs. Therefore source
// wrappers are stripped before normalization, matching DMT's catalog mapper.
//
// The target policy keeps values in native ClickHouse types when canonical
// semantics suffice. It intentionally does not emit Nullable or
// LowCardinality because RenderOpts contains no column-nullability or storage
// policy; that composition remains a future DDL-renderer milestone.
func clickhouseToCanonical(typeName string, _ TypeMeta) CanonicalType {
	typ := strings.TrimSpace(typeName)
	for {
		next, ok := unwrapClickHouseWrapper(typ, "Nullable")
		if !ok {
			next, ok = unwrapClickHouseWrapper(typ, "LowCardinality")
		}
		if !ok {
			break
		}
		typ = next
	}

	name, args, ok := clickhouseTypeCall(typ)
	if !ok || name == "" {
		return CanonicalType{Kind: Raw, Raw: typ}
	}
	upper := strings.ToUpper(name)

	switch upper {
	case "FIXEDSTRING":
		if len(args) != 1 {
			return CanonicalType{Kind: Raw, Raw: typ}
		}
		length, ok := clickhousePositiveInt(args[0])
		if !ok {
			return CanonicalType{Kind: Raw, Raw: typ}
		}
		return CanonicalType{Kind: Char, Length: length}
	case "DECIMAL":
		precision, scale, ok := clickhouseDecimalParameters(args, 0)
		if !ok {
			return CanonicalType{Kind: Raw, Raw: typ}
		}
		return CanonicalType{Kind: Decimal, Precision: precision, Scale: scale}
	case "DECIMAL32":
		precision, scale, ok := clickhouseDecimalParameters(args, 9)
		if !ok {
			return CanonicalType{Kind: Raw, Raw: typ}
		}
		return CanonicalType{Kind: Decimal, Precision: precision, Scale: scale}
	case "DECIMAL64":
		precision, scale, ok := clickhouseDecimalParameters(args, 18)
		if !ok {
			return CanonicalType{Kind: Raw, Raw: typ}
		}
		return CanonicalType{Kind: Decimal, Precision: precision, Scale: scale}
	case "DECIMAL128":
		precision, scale, ok := clickhouseDecimalParameters(args, 38)
		if !ok {
			return CanonicalType{Kind: Raw, Raw: typ}
		}
		return CanonicalType{Kind: Decimal, Precision: precision, Scale: scale}
	case "DECIMAL256":
		precision, scale, ok := clickhouseDecimalParameters(args, 76)
		if !ok {
			return CanonicalType{Kind: Raw, Raw: typ}
		}
		return CanonicalType{Kind: Decimal, Precision: precision, Scale: scale}
	case "DATETIME", "DATETIME64":
		fsp, withTZ, ok := clickhouseTimestampParameters(upper, args)
		if !ok {
			return CanonicalType{Kind: Raw, Raw: typ}
		}
		return CanonicalType{Kind: Timestamp, Fsp: fsp, WithTZ: withTZ}
	case "ENUM8", "ENUM16":
		values, ok := clickhouseEnumValues(args)
		if !ok {
			return CanonicalType{Kind: Raw, Raw: typ}
		}
		return CanonicalType{Kind: Enum, EnumValues: values}
	case "ARRAY":
		if len(args) != 1 || strings.TrimSpace(args[0]) == "" {
			return CanonicalType{Kind: Raw, Raw: typ}
		}
		element := clickhouseToCanonical(args[0], TypeMeta{})
		return CanonicalType{Kind: Array, Element: &element}
	}

	if len(args) != 0 {
		if upper == "OBJECT" && len(args) == 1 && strings.EqualFold(clickhouseUnquote(args[0]), "json") {
			return CanonicalType{Kind: Json}
		}
		return CanonicalType{Kind: Raw, Raw: typ}
	}

	switch upper {
	case "BOOL", "BOOLEAN":
		return CanonicalType{Kind: Boolean}
	case "INT8":
		return CanonicalType{Kind: TinyInt}
	case "UINT8":
		return CanonicalType{Kind: TinyInt, Unsigned: true}
	case "INT16":
		return CanonicalType{Kind: SmallInt}
	case "UINT16":
		return CanonicalType{Kind: SmallInt, Unsigned: true}
	case "INT32":
		return CanonicalType{Kind: Integer}
	case "UINT32":
		return CanonicalType{Kind: Integer, Unsigned: true}
	case "INT64":
		return CanonicalType{Kind: BigInt}
	case "UINT64":
		return CanonicalType{Kind: BigInt, Unsigned: true}
	case "FLOAT32":
		return CanonicalType{Kind: Real}
	case "FLOAT64":
		return CanonicalType{Kind: Double}
	case "STRING", "IPV4", "IPV6":
		return CanonicalType{Kind: Text}
	case "DATE", "DATE32":
		return CanonicalType{Kind: Date}
	case "UUID":
		return CanonicalType{Kind: Uuid}
	case "JSON":
		return CanonicalType{Kind: Json}
	default:
		// UInt128/UInt256, AggregateFunction, Map, Tuple, Variant, and
		// experimental/vendor types have no lossless home in the public IR.
		return CanonicalType{Kind: Raw, Raw: typ}
	}
}

func isClickHouse(dialect string) bool {
	return strings.EqualFold(strings.TrimSpace(dialect), "clickhouse")
}

func unwrapClickHouseWrapper(typ, wrapper string) (string, bool) {
	name, args, ok := clickhouseTypeCall(typ)
	if !ok || !strings.EqualFold(name, wrapper) || len(args) != 1 {
		return typ, false
	}
	return strings.TrimSpace(args[0]), true
}

// clickhouseTypeCall splits a complete ClickHouse Type(args...) expression.
// It is quote- and nested-parentheses-aware so Array(Nullable(String)) and
// Enum8('a,b' = 1) do not lose their inner structure.
func clickhouseTypeCall(typ string) (string, []string, bool) {
	typ = strings.TrimSpace(typ)
	if typ == "" {
		return "", nil, false
	}
	open := strings.IndexByte(typ, '(')
	if open < 0 {
		return typ, nil, true
	}
	name := strings.TrimSpace(typ[:open])
	if name == "" {
		return "", nil, false
	}
	close, ok := clickhouseMatchingParen(typ, open)
	if !ok || strings.TrimSpace(typ[close+1:]) != "" {
		return "", nil, false
	}
	args, ok := clickhouseSplitArgs(typ[open+1 : close])
	if !ok {
		return "", nil, false
	}
	return name, args, true
}

func clickhouseMatchingParen(s string, open int) (int, bool) {
	depth := 0
	inQuote := false
	for i := open; i < len(s); i++ {
		c := s[i]
		if inQuote {
			if c == '\\' && i+1 < len(s) {
				i++
				continue
			}
			if c == '\'' {
				if i+1 < len(s) && s[i+1] == '\'' {
					i++
					continue
				}
				inQuote = false
			}
			continue
		}
		switch c {
		case '\'':
			inQuote = true
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return i, true
			}
			if depth < 0 {
				return 0, false
			}
		}
	}
	return 0, false
}

func clickhouseSplitArgs(s string) ([]string, bool) {
	if strings.TrimSpace(s) == "" {
		return nil, true
	}
	var args []string
	start, depth := 0, 0
	inQuote := false
	for i := 0; i < len(s); i++ {
		c := s[i]
		if inQuote {
			if c == '\\' && i+1 < len(s) {
				i++
				continue
			}
			if c == '\'' {
				if i+1 < len(s) && s[i+1] == '\'' {
					i++
					continue
				}
				inQuote = false
			}
			continue
		}
		switch c {
		case '\'':
			inQuote = true
		case '(':
			depth++
		case ')':
			depth--
			if depth < 0 {
				return nil, false
			}
		case ',':
			if depth == 0 {
				arg := strings.TrimSpace(s[start:i])
				if arg == "" {
					return nil, false
				}
				args = append(args, arg)
				start = i + 1
			}
		}
	}
	if inQuote || depth != 0 {
		return nil, false
	}
	arg := strings.TrimSpace(s[start:])
	if arg == "" {
		return nil, false
	}
	return append(args, arg), true
}

func clickhousePositiveInt(s string) (int, bool) {
	n, err := strconv.Atoi(strings.TrimSpace(s))
	return n, err == nil && n > 0
}

func clickhouseNonNegativeInt(s string) (int, bool) {
	n, err := strconv.Atoi(strings.TrimSpace(s))
	return n, err == nil && n >= 0
}

func clickhouseDecimalParameters(args []string, impliedPrecision int) (int, int, bool) {
	if impliedPrecision > 0 {
		if len(args) != 1 {
			return 0, 0, false
		}
		scale, ok := clickhouseNonNegativeInt(args[0])
		if !ok || scale > impliedPrecision {
			return 0, 0, false
		}
		return impliedPrecision, scale, true
	}
	if len(args) != 2 {
		return 0, 0, false
	}
	precision, ok := clickhousePositiveInt(args[0])
	if !ok || precision > 76 {
		return 0, 0, false
	}
	scale, ok := clickhouseNonNegativeInt(args[1])
	if !ok || scale > precision {
		return 0, 0, false
	}
	return precision, scale, true
}

func clickhouseTimestampParameters(name string, args []string) (*int, bool, bool) {
	if name == "DATETIME" {
		if len(args) > 1 {
			return nil, false, false
		}
		fsp := 0
		return &fsp, len(args) == 1 && strings.TrimSpace(args[0]) != "", true
	}
	if len(args) < 1 || len(args) > 2 {
		return nil, false, false
	}
	fsp, ok := clickhouseNonNegativeInt(args[0])
	if !ok || fsp > 9 {
		return nil, false, false
	}
	return &fsp, len(args) == 2 && strings.TrimSpace(args[1]) != "", true
}

func clickhouseEnumValues(args []string) ([]string, bool) {
	if len(args) == 0 {
		return nil, false
	}
	values := make([]string, 0, len(args))
	for _, arg := range args {
		eq := clickhouseEnumEquals(arg)
		if eq < 0 {
			return nil, false
		}
		label := strings.TrimSpace(arg[:eq])
		code := strings.TrimSpace(arg[eq+1:])
		if len(label) < 2 || label[0] != '\'' || label[len(label)-1] != '\'' {
			return nil, false
		}
		if _, err := strconv.Atoi(code); err != nil {
			return nil, false
		}
		values = append(values, clickhouseUnquote(label))
	}
	return values, true
}

func clickhouseEnumEquals(s string) int {
	inQuote := false
	for i := 0; i < len(s); i++ {
		if inQuote {
			if s[i] == '\\' && i+1 < len(s) {
				i++
				continue
			}
			if s[i] == '\'' {
				if i+1 < len(s) && s[i+1] == '\'' {
					i++
					continue
				}
				inQuote = false
			}
			continue
		}
		if s[i] == '\'' {
			inQuote = true
		} else if s[i] == '=' {
			return i
		}
	}
	return -1
}

func clickhouseUnquote(s string) string {
	s = strings.TrimSpace(s)
	if len(s) < 2 || s[0] != '\'' || s[len(s)-1] != '\'' {
		return s
	}
	s = s[1 : len(s)-1]
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+1 < len(s) {
			i++
			b.WriteByte(s[i])
			continue
		}
		if s[i] == '\'' && i+1 < len(s) && s[i+1] == '\'' {
			i++
		}
		b.WriteByte(s[i])
	}
	return b.String()
}

func fromCanonicalClickHouse(ct CanonicalType, opts RenderOpts) (string, error) {
	integer := func(signed, unsigned string) string {
		if ct.Unsigned {
			return unsigned
		}
		return signed
	}

	switch ct.Kind {
	case Boolean:
		return "Bool", nil
	case TinyInt:
		return integer("Int8", "UInt8"), nil
	case SmallInt:
		return integer("Int16", "UInt16"), nil
	case MediumInt:
		return integer("Int32", "UInt32"), nil
	case Integer:
		return integer("Int32", "UInt32"), nil
	case BigInt:
		return integer("Int64", "UInt64"), nil
	case Decimal:
		return clickhouseDecimalDDL(ct)
	case Real:
		return "Float32", nil
	case Double:
		return "Float64", nil
	case Varchar, Text:
		return "String", nil
	case Char:
		if ct.Length > 0 {
			return fmt.Sprintf("FixedString(%d)", ct.Length), nil
		}
		return "String", nil
	case Binary:
		if ct.Length > 0 {
			return fmt.Sprintf("FixedString(%d)", ct.Length), nil
		}
		return "String", nil
	case VarBinary, Blob:
		return "String", nil
	case RowVersion:
		return "FixedString(8)", nil
	case Date:
		return "Date32", nil
	case Time:
		// Time/Time64 is experimental on the ClickHouse versions DMT supports.
		return "String", nil
	case Timestamp:
		return clickhouseTimestampDDL(ct), nil
	case Uuid:
		return "UUID", nil
	case Json, Xml, Set, Spatial:
		return "String", nil
	case Enum:
		return clickhouseEnumDDL(ct), nil
	case Array:
		element := CanonicalType{Kind: Text}
		if ct.Element != nil {
			element = *ct.Element
		}
		typ, err := fromCanonicalClickHouse(element, opts)
		if err != nil {
			return "", err
		}
		return "Array(" + typ + ")", nil
	case Raw:
		return "", fmt.Errorf("%w: %s", ErrUnknownType, ct.Raw)
	default:
		return "", fmt.Errorf("%w", ErrUnknownType)
	}
}

func clickhouseDecimalDDL(ct CanonicalType) (string, error) {
	if ct.Precision <= 0 {
		return "Decimal(38, 9)", nil
	}
	if ct.Precision > 76 || ct.Scale < 0 || ct.Scale > ct.Precision {
		return "", fmt.Errorf("%w: ClickHouse Decimal(%d,%d) is outside its supported precision/scale range", ErrUnknownType, ct.Precision, ct.Scale)
	}
	return fmt.Sprintf("Decimal(%d, %d)", ct.Precision, ct.Scale), nil
}

func clickhouseTimestampDDL(ct CanonicalType) string {
	fsp := 3 // conservative, DMT-compatible default for unspecified precision
	if value, ok := ct.Fspv(); ok && value >= 0 {
		fsp = value
	}
	if fsp > 9 {
		fsp = 9
	}
	if fsp == 0 {
		if ct.WithTZ {
			return "DateTime('UTC')"
		}
		return "DateTime"
	}
	if ct.WithTZ {
		return fmt.Sprintf("DateTime64(%d, 'UTC')", fsp)
	}
	return fmt.Sprintf("DateTime64(%d)", fsp)
}

func clickhouseEnumDDL(ct CanonicalType) string {
	if len(ct.EnumValues) == 0 || len(ct.EnumValues) > 32767 {
		return "String"
	}
	name := "Enum8"
	if len(ct.EnumValues) > 127 {
		name = "Enum16"
	}
	values := make([]string, len(ct.EnumValues))
	for i, value := range ct.EnumValues {
		values[i] = fmt.Sprintf("%s = %d", clickhouseQuote(value), i+1)
	}
	return name + "(" + strings.Join(values, ", ") + ")"
}

func clickhouseQuote(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `'`, `\'`)
	return "'" + s + "'"
}

func clickhouseMappingWarnings(ct CanonicalType, rendered string) []MappingWarning {
	var out []MappingWarning
	add := func(kind Kind, reason string) {
		out = append(out, MappingWarning{
			Kind:          kindName(kind),
			TargetDialect: "clickhouse",
			Reason:        reason,
		})
	}

	switch ct.Kind {
	case MediumInt:
		add(ct.Kind, "ClickHouse has no 24-bit integer; widened to "+rendered)
	case Varchar:
		if ct.Length > 0 {
			add(ct.Kind, "ClickHouse String does not enforce the declared maximum length; rendered as "+rendered)
		}
		if ct.National {
			add(ct.Kind, "ClickHouse has no separate national character type; rendered as "+rendered)
		}
	case Char:
		if ct.Length <= 0 {
			add(ct.Kind, "fixed character length was unspecified, so ClickHouse FixedString cannot be used; rendered as "+rendered)
		} else {
			add(ct.Kind, "ClickHouse FixedString is byte-sized; character width and padding semantics may differ; rendered as "+rendered)
		}
		if ct.National {
			add(ct.Kind, "ClickHouse has no separate national character type; rendered as "+rendered)
		}
	case Binary:
		if ct.Length <= 0 {
			add(ct.Kind, "fixed binary length was unspecified, so ClickHouse FixedString cannot be used; rendered as "+rendered)
		}
	case VarBinary:
		if ct.Length > 0 {
			add(ct.Kind, "ClickHouse String does not enforce the declared maximum byte length; rendered as "+rendered)
		}
	case Decimal:
		if ct.Precision <= 0 {
			add(ct.Kind, "source decimal had no declared precision; rendered with conservative Decimal(38, 9) default")
		}
	case Date:
		add(ct.Kind, "ClickHouse Date32 supports 1900-2299; values outside that range cannot be preserved")
	case Time:
		add(ct.Kind, "ClickHouse Time/Time64 is experimental for the supported target baseline; stored as String")
	case Timestamp:
		if _, ok := ct.Fspv(); !ok {
			add(ct.Kind, "source timestamp had no declared fractional-seconds precision; rendered with DateTime64(3) default")
		}
		if ct.WithTZ {
			add(ct.Kind, "canonical time-zone awareness has no named-zone identity; rendered with UTC time zone")
		}
	case RowVersion:
		add(ct.Kind, "ClickHouse has no rowversion token semantics; rendered as opaque FixedString(8)")
	case Json:
		add(ct.Kind, "ClickHouse native JSON availability varies by server version; stored as String")
	case Xml:
		add(ct.Kind, "ClickHouse has no native XML type; stored as String")
	case Enum:
		switch {
		case len(ct.EnumValues) == 0:
			add(ct.Kind, "enum values are unavailable, so the value set cannot be preserved; rendered as String")
		case len(ct.EnumValues) > 32767:
			add(ct.Kind, "enum has more than 32767 values, exceeding ClickHouse Enum16; rendered as String")
		default:
			add(ct.Kind, "ClickHouse enum integer codes are assigned sequentially; original codes are not represented in the canonical type")
		}
	case Set:
		add(ct.Kind, "ClickHouse has no MySQL-style multi-value SET type; stored as String")
	case Array:
		if ct.Element == nil {
			add(ct.Kind, "array element type is unavailable; rendered as Array(String)")
			break
		}
		if elementRendered, err := fromCanonicalClickHouse(*ct.Element, RenderOpts{}); err == nil {
			out = append(out, clickhouseMappingWarnings(*ct.Element, elementRendered)...)
		}
	case Spatial:
		add(ct.Kind, "ClickHouse has no portable geometry/geography column type; stored as String")
	}
	return out
}
