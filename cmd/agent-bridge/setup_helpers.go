package main

import (
	"fmt"
	"os"
	"sort"
	"strings"

	"agent-bridge/config"

	qrterminal "github.com/mdp/qrterminal/v3"
	"rsc.io/qr"
)

// initConfigPath resolves the config path and sets config.ConfigPath.
func initConfigPath(flagValue string) {
	config.ConfigPath = resolveConfigPath(flagValue)
}

func resolveTargetProject(project string) (string, error) {
	project = strings.TrimSpace(project)
	if project != "" {
		return project, nil
	}
	projects, err := config.ListProjects()
	if err != nil {
		return "", err
	}
	switch len(projects) {
	case 0:
		return "", fmt.Errorf("no project found in config")
	case 1:
		return projects[0], nil
	default:
		sort.Strings(projects)
		return "", fmt.Errorf("multiple projects found, please specify --project (%s)", strings.Join(projects, ", "))
	}
}

func tryPrintTerminalQRCode(content string) {
	if content == "" {
		return
	}
	qrterminal.GenerateWithConfig(content, qrterminal.Config{
		Level:      qrterminal.M,
		Writer:     os.Stdout,
		HalfBlocks: false,
		BlackChar:  "██",
		WhiteChar:  "  ",
		QuietZone:  4,
	})
	if _, err := fmt.Fprintln(os.Stdout); err != nil {
		return
	}
}

func saveQRCodeImage(content, path string) error {
	code, err := qr.Encode(content, qr.M)
	if err != nil {
		return fmt.Errorf("encode QR: %w", err)
	}
	code.Scale = 8
	return os.WriteFile(path, code.PNG(), 0644)
}
