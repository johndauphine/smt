package orchestrator

import (
	"strings"

	"smt/internal/canonical"
	"smt/internal/driver"
)

type mappingWarning struct {
	Table         string `json:"table"`
	Column        string `json:"column"`
	SourceDialect string `json:"source_dialect"`
	TargetDialect string `json:"target_dialect"`
	SourceType    string `json:"source_type"`
	TargetType    string `json:"target_type"`
	Kind          string `json:"kind"`
	Reason        string `json:"reason"`
}

func collectMappingWarnings(tables []driver.Table, sourceDialect, targetDialect string) []mappingWarning {
	var out []mappingWarning
	for _, t := range tables {
		for _, col := range t.Columns {
			ct := canonical.ToCanonical(col.DataType, driver.MetaOf(col), sourceDialect)
			targetType, warnings, err := canonical.FromCanonicalWithWarnings(
				ct, targetDialect, canonical.RenderOpts{IsIdentity: col.IsIdentity})
			if err != nil {
				continue
			}
			for _, w := range warnings {
				out = append(out, mappingWarning{
					Table:         tableLabel(t),
					Column:        col.Name,
					SourceDialect: strings.TrimSpace(sourceDialect),
					TargetDialect: strings.TrimSpace(targetDialect),
					SourceType:    col.DataType,
					TargetType:    targetType,
					Kind:          w.Kind,
					Reason:        w.Reason,
				})
			}
		}
	}
	return out
}

func tableLabel(t driver.Table) string {
	if strings.TrimSpace(t.Schema) == "" {
		return t.Name
	}
	return t.FullName()
}
