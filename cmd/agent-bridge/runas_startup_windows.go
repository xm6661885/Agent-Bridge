//go:build windows

package main

import (
	"context"

	"agent-bridge/config"
)

func runRunAsUserStartupChecks(_ context.Context, _ *config.Config) error {
	return nil
}
