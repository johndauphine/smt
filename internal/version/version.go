package version

// Version is the current version of smt.
// Can be overridden at build time with -ldflags "-X ...version.Version=..."
var Version = "0.9.0"

// Name is the application name.
const Name = "smt"

// Description is a short description of the application.
const Description = "Schema migration tool — extracts source schemas, generates target DDL, and applies incremental schema changes"
