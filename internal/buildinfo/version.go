// Package buildinfo defines the version of this TDL 管理 project.
package buildinfo

const Version = "v1.11.5"

// UpstreamVersion names the upstream TDL release this project was forked from.
//
// It is reported to the Web UI and the Bot as a provenance note, nothing more:
// no code reads it to decide anything. It lived in internal/adapter/upstream,
// a package that existed to declare a dependency boundary it did not actually
// hold - every upstream import happens elsewhere - and that pulled the whole
// upstream CLI login path in for two compile-time references and a string.
//
// Remove this constant (and the fields it fills) once the download engine is
// ours: at that point there is no upstream release to name.
const UpstreamVersion = "v0.20.4"
