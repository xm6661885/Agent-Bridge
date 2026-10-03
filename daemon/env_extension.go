package daemon

import (
	"regexp"
	"sync"
)

// EnvDiscoverer is an install-time hook that returns env-var
// name/value pairs to bake into the daemon service file's EnvExtra.
// Discoverers run once per install, only when NoCaptureSecrets=false.
//
// Implementations must read values from os.LookupEnv themselves —
// daemon does not look them up on the discoverer's behalf. The
// returned map keys are env names; values that are empty or whose
// names fail POSIX-identifier validation are dropped by daemon as a
// belt-and-suspenders.
//
// A non-nil error is logged at WARN level and does NOT fail install.
type EnvDiscoverer func() (map[string]string, error)

var (
	envDiscoverersMu sync.RWMutex
	envDiscoverers   []EnvDiscoverer
)

func snapshotEnvDiscoverers() []EnvDiscoverer {
	envDiscoverersMu.RLock()
	defer envDiscoverersMu.RUnlock()
	out := make([]EnvDiscoverer, len(envDiscoverers))
	copy(out, envDiscoverers)
	return out
}

var envNameRegexp = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// isValidEnvName reports whether s is a syntactically valid env-var
// name. Used by every renderer (launchd / systemd / windows) so that
// malformed keys from discoverers or callers cannot leak into a
// service file where they would either fail to parse or, worse,
// inject syntax.
func isValidEnvName(s string) bool {
	return envNameRegexp.MatchString(s)
}
