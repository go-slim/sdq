// Package libsql provides an sdq storage backed by an embedded Turso/libSQL database.
//
// The storage persists job metadata and bodies in separate tables. SaveJob writes both
// records atomically, and all metadata updates are durable before UpdateJobMeta returns.
// The database runs in MVCC mode and can be used without CGO.
package libsql
