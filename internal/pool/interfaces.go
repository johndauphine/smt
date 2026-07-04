// Package pool provides type aliases for database pool interfaces.
// All interfaces are now defined in the driver package.
package pool

import (
	"smt/internal/driver"
)

// SourcePool is an alias for driver.Reader for backward compatibility.
// It represents a source database connection pool for reading schema.
type SourcePool = driver.Reader

// TargetPool is an alias for driver.Writer for backward compatibility.
// It represents a target database connection pool for executing DDL.
type TargetPool = driver.Writer
