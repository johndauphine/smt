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
}

func TestFingerprintBytes(t *testing.T) {
	if fingerprintBytes([]byte("x")) == fingerprintBytes([]byte("y")) {
		t.Error("distinct bytes hashed to the same fingerprint")
	}
	if fingerprintBytes([]byte("same")) != fingerprintBytes([]byte("same")) {
		t.Error("identical bytes hashed to different fingerprints")
	}
}
