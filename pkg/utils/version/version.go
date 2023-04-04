// Package version provide the information of current project module
package version

import (
	"encoding/json"
	"fmt"
	"runtime"
)

var (
	// semantic version, derived by build scripts (see
	// https://github.com/kubernetes/community/blob/master/contributors/design-proposals/release/versioning.md
	// for a detailed discussion of this field)
	//
	// TODO: This field is still called "gitVersion" for legacy
	// reasons. For prerelease versions, the build metadata on the
	// semantic version is a git hash, but the version itself is no
	// longer the direct output of "git describe", but a slight
	// translation to be semver compliant.

	// NOTE: The $Format strings are replaced during 'git archive' thanks to the
	// companion .gitattributes file containing 'export-subst' in this same
	// directory.  See also https://git-scm.com/docs/gitattributes
	module       = "unknown"
	version      = "v0.0.0-master+$Format:%h$"
	branch       = ""
	gitCommit    = "$Format:%H$"          // sha1 from git, output of $(git rev-parse HEAD)
	gitTreeState = ""                     // state of git tree, either "clean" or "dirty"
	buildDate    = "1970-01-01T00:00:00Z" // build date in ISO8601 format, output of $(date -u +'%Y-%m-%dT%H:%M:%SZ')
)

// Info contains versioning information.
type Info struct {
	Module       string `json:"module"`
	Version      string `json:"version"`
	Branch       string `json:"branch"`
	GitCommit    string `json:"gitCommit"`
	GitTreeState string `json:"gitTreeState"`
	BuildDate    string `json:"buildDate"`
	GoVersion    string `json:"goVersion"`
	Compiler     string `json:"compiler"`
	Platform     string `json:"platform"`
}

// Pretty returns a pretty output representation of Info
func (info Info) Pretty() string {
	str, _ := json.MarshalIndent(info, "", "    ")
	return string(str)
}

// String returns the marshalled json string of Info
func (info Info) String() string {
	str, _ := json.Marshal(info)
	return string(str)
}

// Get returns the overall codebase version. It's for detecting
// what code a binary was built from.
func Get() Info {
	// These variables typically come from -ldflags settings and in
	// their absence fallback to the settings in version/base.go
	return Info{
		Module:       module,
		Version:      version,
		Branch:       branch,
		GitCommit:    gitCommit,
		GitTreeState: gitTreeState,
		BuildDate:    buildDate,
		GoVersion:    runtime.Version(),
		Compiler:     runtime.Compiler,
		Platform:     fmt.Sprintf("%s/%s", runtime.GOOS, runtime.GOARCH),
	}
}
