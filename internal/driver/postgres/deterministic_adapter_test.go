package postgres

import (
	"reflect"
	"testing"

	purepostgres "smt/internal/ddl/postgres"
	"smt/internal/driver"
)

func TestDeterministicDDLAdapterMatchesPureRenderer(t *testing.T) {
	tests := []struct {
		name  string
		table *driver.Table
	}{
		{
			name: "successful render",
			table: &driver.Table{
				Name:       "Customer Accounts",
				PrimaryKey: []string{"Account ID"},
				Columns: []driver.Column{
					{Name: "Account ID", DataType: "int", IsIdentity: true},
					{Name: "Display Name", DataType: "nvarchar", MaxLength: 80, IsNullable: true},
					{Name: "Is Active", DataType: "bit", DefaultExpression: "0"},
				},
			},
		},
		{
			name: "render error",
			table: &driver.Table{
				Name: "Subscriptions",
				Columns: []driver.Column{{
					Name: "ExpiresAt", DataType: "datetime2",
					DefaultExpression: "(dateadd(year,(1),getdate()))",
				}},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotSQL, gotTypes, gotErr := RenderCreateTableDDLWithSource(tc.table, "public", false, "fail", "mssql")
			wantSQL, wantTypes, wantErr := purepostgres.RenderCreateTableDDLWithSource(tc.table, "public", false, "fail", "mssql")

			if (gotErr == nil) != (wantErr == nil) || (gotErr != nil && gotErr.Error() != wantErr.Error()) {
				t.Fatalf("adapter error = %v, pure renderer error = %v", gotErr, wantErr)
			}
			if gotSQL != wantSQL {
				t.Fatalf("adapter SQL differs from pure renderer:\nadapter: %s\npure:    %s", gotSQL, wantSQL)
			}
			if !reflect.DeepEqual(gotTypes, wantTypes) {
				t.Fatalf("adapter column types = %#v, pure renderer = %#v", gotTypes, wantTypes)
			}
		})
	}
}
