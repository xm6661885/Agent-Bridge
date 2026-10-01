package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/BurntSushi/toml"
)

func TestProgressDisplay_InheritanceAndPersistence(t *testing.T) {
	tru, fal := true, false
	for _, tt := range []struct {
		name              string
		global            DisplayConfig
		project           *DisplayConfig
		cleanup, collapse bool
	}{
		{"defaults", DisplayConfig{}, nil, false, false},
		{"global", DisplayConfig{CleanupProgressOnComplete: &tru, CollapseToolMessages: &tru}, nil, true, true},
		{"project false", DisplayConfig{CleanupProgressOnComplete: &tru, CollapseToolMessages: &tru}, &DisplayConfig{CleanupProgressOnComplete: &fal, CollapseToolMessages: &fal}, false, false},
		{"independent", DisplayConfig{CleanupProgressOnComplete: &tru}, &DisplayConfig{CollapseToolMessages: &tru}, true, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cleanup, collapse := EffectiveProgressDisplay(&Config{Display: tt.global}, &ProjectConfig{Display: tt.project})
			if cleanup != tt.cleanup || collapse != tt.collapse {
				t.Fatalf("got %v/%v, want %v/%v", cleanup, collapse, tt.cleanup, tt.collapse)
			}
		})
	}
	oldPath := ConfigPath
	ConfigPath = filepath.Join(t.TempDir(), "config.toml")
	t.Cleanup(func() { ConfigPath = oldPath })
	if err := os.WriteFile(ConfigPath, []byte("[[projects]]\nname = 'test'\n[projects.agent]\ntype = 'claudecode'\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := SaveGlobalSettings(GlobalSettingsUpdate{CleanupProgressOnComplete: &tru, CollapseToolMessages: &tru}); err != nil {
		t.Fatal(err)
	}
	if err := SaveProjectSettings("test", ProjectSettingsUpdate{CleanupProgressOnComplete: &fal}); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(ConfigPath)
	if err != nil {
		t.Fatal(err)
	}
	var cfg Config
	if err := toml.Unmarshal(data, &cfg); err != nil {
		t.Fatal(err)
	}
	cleanup, collapse := EffectiveProgressDisplay(&cfg, &cfg.Projects[0])
	if cleanup || !collapse {
		t.Fatalf("saved project override/inheritance = %v/%v", cleanup, collapse)
	}
}
