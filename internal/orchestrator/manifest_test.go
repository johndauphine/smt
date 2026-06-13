package orchestrator

import (
	"strings"
	"testing"

	"smt/internal/driver"
)

// Fingerprints must be stable for identical input and change when the input
// changes — the property the #64 manifest relies on for inspection and cache
// invalidation.
func TestFingerprintStableAndSensitive(t *testing.T) {
	a := []driver.Table{{Name: "Users", Columns: []driver.Column{{Name: "Id", DataType: "int"}}}}
	b := []driver.Table{{Name: "Users", Columns: []driver.Column{{Name: "Id", DataType: "int"}}}}
	c := []driver.Table{{Name: "Users", Columns: []driver.Column{{Name: "Id", DataType: "bigint"}}}}

	fa, err := fingerprintJSON(a)
	if err != nil {
		t.Fatalf("fingerprintJSON: %v", err)
	}
	fb, _ := fingerprintJSON(b)
	fc, _ := fingerprintJSON(c)

	if fa != fb {
		t.Errorf("identical input produced different fingerprints:\n %s\n %s", fa, fb)
	}
	if fa == fc {
		t.Error("differing input (int vs bigint) produced the same fingerprint")
	}
	if !strings.HasPrefix(fa, "sha256:") {
		t.Errorf("fingerprint missing sha256: prefix: %q", fa)
	}

	// The canonical snapshot loads indexes/FKs/checks, so the fingerprint
	// must move when any of those change — the gap that prompted hashing
	// the full snapshot rather than columns/PKs alone.
	withIdx := []driver.Table{{
		Name:    "Users",
		Columns: []driver.Column{{Name: "Id", DataType: "int"}},
		Indexes: []driver.Index{{Name: "ix_users_id", Columns: []string{"Id"}}},
	}}
	withFK := []driver.Table{{
		Name:        "Users",
		Columns:     []driver.Column{{Name: "Id", DataType: "int"}},
		ForeignKeys: []driver.ForeignKey{{Name: "fk", Columns: []string{"Id"}, RefTable: "Orgs", RefColumns: []string{"Id"}}},
	}}
	fIdx, _ := fingerprintJSON(withIdx)
	fFK, _ := fingerprintJSON(withFK)
	if fIdx == fa {
		t.Error("adding an index did not change the fingerprint")
	}
	if fFK == fa {
		t.Error("adding a foreign key did not change the fingerprint")
	}
}

// Non-DDL stats (row counts, sample values) must not perturb the fingerprint,
// so it tracks schema shape rather than table contents.
func TestCanonicalizeForFingerprintIgnoresStats(t *testing.T) {
	base := driver.Table{Name: "T", Columns: []driver.Column{{Name: "c", DataType: "int"}}}
	noisy := driver.Table{
		Name:             "T",
		RowCount:         9999,
		EstimatedRowSize: 128,
		Columns:          []driver.Column{{Name: "c", DataType: "int", SampleValues: []string{"1", "2"}}},
	}
	canonicalizeForFingerprint(&noisy)
	fb, _ := fingerprintJSON([]driver.Table{base})
	fn, _ := fingerprintJSON([]driver.Table{noisy})
	if fb != fn {
		t.Errorf("row-count/sample-value stats leaked into the fingerprint:\n %s\n %s", fb, fn)
	}
}

func TestFingerprintBytes(t *testing.T) {
	if fingerprintBytes([]byte("x")) == fingerprintBytes([]byte("y")) {
		t.Error("distinct bytes hashed to the same fingerprint")
	}
	if fingerprintBytes([]byte("same")) != fingerprintBytes([]byte("same")) {
		t.Error("identical bytes hashed to different fingerprints")
	}
}
