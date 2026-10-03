//go:build integration

package integration

import (
	"strings"
)

func joinMsgContent(msgs []mockMessage) string {
	var parts []string
	for _, m := range msgs {
		parts = append(parts, m.Content)
	}
	return strings.Join(parts, "\n")
}
