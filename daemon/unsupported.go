//go:build !linux && !darwin

package daemon

import (
	"fmt"
	"runtime"
)

func newPlatformManager() (Manager, error) {
	return nil, fmt.Errorf("daemon management is not supported on %s; use a process manager (e.g. pm2) instead", runtime.GOOS)
}

// CheckLinger is a no-op on unsupported platforms (always returns false).
func CheckLinger() (enabled bool, user string) {
	return false, ""
}
