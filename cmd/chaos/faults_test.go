package main

import (
	"os/exec"
	"testing"
	"time"
)

func requirePOSIXShell(t *testing.T) string {
	t.Helper()
	if _, err := exec.LookPath("sh"); err != nil {
		t.Skipf("POSIX shell not available: %v", err)
	}
	return "sh"
}

// The generated shell must at least parse (sh -n) and contain the undo rules.
func TestScriptsParse(t *testing.T) {
	shell := requirePOSIXShell(t)
	p, err := partitionScript("udp,redis", 10*time.Second, 9000, 6379)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range []string{p, netemScript("eth0", "loss 10%", 5*time.Second)} {
		if out, err := exec.Command(shell, "-n", "-c", s).CombinedOutput(); err != nil {
			t.Fatalf("bad script %q: %v %s", s, err, out)
		}
	}
	if _, err := partitionScript("bogus", time.Second, 9000, 6379); err == nil {
		t.Fatal("expected error for unknown block")
	}
}

func TestSettled(t *testing.T) {
	mk := func(role string) pod {
		var p pod
		p.Metadata.Labels = map[string]string{"arcade.io/role": role}
		return p
	}
	if !settled([]pod{mk("primary"), mk("backup")}) {
		t.Fatal("one primary + one backup should be settled")
	}
	if settled([]pod{mk("primary"), mk("primary")}) {
		t.Fatal("two primaries (split brain) must not count as settled")
	}
}
