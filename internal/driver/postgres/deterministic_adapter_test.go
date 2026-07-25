package postgres

import (
	"reflect"
	"testing"

	purepostgres "smt/internal/ddl/postgres"
	"smt/internal/driver"
)

func TestDeterministicDDLAdapterMatchesPureRenderer(t *testing.T) {
	table := &driver.Table{
		Name:       "Customer Accounts",
		PrimaryKey: []string{"Account ID"},
		Columns: []driver.Column{
			{Name: "Account ID", DataType: "int", IsIdentity: true},
			{Name: "Display Name", DataType: "nvarchar", MaxLength: 80, IsNullable: true},
			{Name: "Is Active", DataType: "bit", DefaultExpression: "0"},
		},
	}

	gotSQL, gotTypes, gotErr := RenderCreateTableDDLWithSource(table, "public", false, "fail", "mssql")
	wantSQL, wantTypes, wantErr := purepostgres.RenderCreateTableDDLWithSource(table, "public", false, "fail", "mssql")

	if gotErr != wantErr {
		t.Fatalf("adapter error = %v, pure renderer error = %v", gotErr, wantErr)
	}
	if gotSQL != wantSQL {
		t.Fatalf("adapter SQL differs from pure renderer:\nadapter: %s\npure:    %s", gotSQL, wantSQL)
	}
	if !reflect.DeepEqual(gotTypes, wantTypes) {
		t.Fatalf("adapter column types = %#v, pure renderer = %#v", gotTypes, wantTypes)
	}
}
