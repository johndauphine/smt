package legacystructliterals

import "testing"

func TestLegacyCapabilitiesFieldOrder(t *testing.T) {
	if !LegacyCapabilities.CreateSchema || LegacyCapabilities.CreateTable ||
		!LegacyCapabilities.CreateColumn || LegacyCapabilities.PrimaryKeys ||
		!LegacyCapabilities.IdentityColumns || LegacyCapabilities.Defaults ||
		!LegacyCapabilities.ComputedColumns || LegacyCapabilities.SecondaryIndexes ||
		!LegacyCapabilities.StandalonePrimaryKeys || LegacyCapabilities.StandaloneForeignKeys ||
		!LegacyCapabilities.NamedUniqueConstraints || LegacyCapabilities.CheckConstraints ||
		!LegacyCapabilities.IndexExpressionKeys || LegacyCapabilities.IndexPrefixLengths ||
		!LegacyCapabilities.IndexIncludeColumns || LegacyCapabilities.FilteredIndexes {
		t.Fatalf("legacy Capabilities literal field order changed: %+v", LegacyCapabilities)
	}
}
