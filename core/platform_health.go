package core

import (
	"strings"
	"time"
)

// normalizeCommandName folds case and treats hyphens/underscores as equivalent.
func normalizeCommandName(s string) string {
	return strings.ToLower(strings.ReplaceAll(s, "-", "_"))
}

// PlatformHealthInfo is a per-platform health snapshot reported by
// implementations of the optional PlatformHealth interface.
type PlatformHealthInfo struct {
	Name           string
	Connected      bool
	Degraded       bool
	DegradedReason string
	DegradedSince  time.Time
}

// PlatformHealth is an optional interface that platforms can implement
// to report runtime degradation. Platforms that do not implement it
// are assumed to be healthy whenever they have started.
type PlatformHealth interface {
	PlatformHealth() PlatformHealthInfo
}
