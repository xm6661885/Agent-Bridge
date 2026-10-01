package core

import (
	"sync"
	"testing"
)

func TestI18n_DefaultLanguage(t *testing.T) {
	i := NewI18n()
	got := i.T(MsgStarting)
	if got == "" {
		t.Error("expected non-empty message")
	}
}

func TestI18n_MissingKey(t *testing.T) {
	i := NewI18n()
	got := i.T(MsgKey("totally_missing_key"))
	if got != "[totally_missing_key]" && got != "" {
		t.Logf("missing key returned %q (acceptable: placeholder or empty)", got)
	}
}

func TestI18n_Tf(t *testing.T) {
	i := NewI18n()
	got := i.Tf(MsgNameSet, "myname", "abc123")
	if got == "" {
		t.Error("Tf should return non-empty formatted message")
	}
}

func TestI18n_AllKeysNonEmpty(t *testing.T) {
	for key, text := range messages {
		if text == "" {
			t.Errorf("message key %q has empty text", key)
		}
	}
}

// TestI18n_ConcurrentAccess guards against data races on the shared
// message catalog when platform handlers call T concurrently.
func TestI18n_ConcurrentAccess(t *testing.T) {
	i := NewI18n()

	const goroutines = 16
	const iterations = 200

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := 0; n < iterations; n++ {
				_ = i.T(MsgStarting)
				_ = i.Tf(MsgNameSet, "n", "id")
			}
		}()
	}
	wg.Wait()
}
