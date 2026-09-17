package main

import (
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// These cover the command line itself rather than the proxy: every failure
// below used to exit 1 with nothing on stderr, because runProxy redirects
// log output into a per-pid file before the first thing that can fail.

func buildCLI(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "acp-multiplex")
	out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput()
	if err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	return bin
}

func runCLI(t *testing.T, bin string, args ...string) (string, string, int) {
	t.Helper()
	cmd := exec.Command(bin, args...)
	var so, se strings.Builder
	cmd.Stdout = &so
	cmd.Stderr = &se
	err := cmd.Run()
	code := 0
	if ee, ok := err.(*exec.ExitError); ok {
		code = ee.ExitCode()
	} else if err != nil {
		t.Fatalf("run %v: %v", args, err)
	}
	return so.String(), se.String(), code
}

func TestHelpFlagsPrintUsageAndSucceed(t *testing.T) {
	bin := buildCLI(t)
	for _, flag := range []string{"-h", "--help"} {
		stdout, _, code := runCLI(t, bin, flag)
		if code != 0 {
			t.Errorf("%s: exit %d, want 0", flag, code)
		}
		if !strings.Contains(stdout, "usage:") {
			t.Errorf("%s: no usage on stdout, got %q", flag, stdout)
		}
	}
}

func TestNoArgsPrintsUsageToStderr(t *testing.T) {
	bin := buildCLI(t)
	_, stderr, code := runCLI(t, bin)
	if code == 0 {
		t.Error("bare invocation exited 0, want non-zero")
	}
	if !strings.Contains(stderr, "usage:") {
		t.Errorf("no usage on stderr, got %q", stderr)
	}
}

func TestUnknownOptionIsNotTreatedAsAnAgent(t *testing.T) {
	bin := buildCLI(t)
	_, stderr, code := runCLI(t, bin, "--nope")
	if code == 0 {
		t.Error("unknown option exited 0, want non-zero")
	}
	if !strings.Contains(stderr, "unknown option") {
		t.Errorf("stderr does not name the bad option, got %q", stderr)
	}
}

func TestTcpFlagRequiresAValue(t *testing.T) {
	bin := buildCLI(t)
	_, stderr, code := runCLI(t, bin, "--tcp")
	if code == 0 {
		t.Error("--tcp with no value exited 0, want non-zero")
	}
	if !strings.Contains(stderr, "--tcp") {
		t.Errorf("stderr does not mention --tcp, got %q", stderr)
	}
}

// The one that actually bites in agent-shell: a misspelled adapter used to
// leave no trace anywhere the user looks.
func TestUnstartableAgentIsReportedOnStderr(t *testing.T) {
	bin := buildCLI(t)
	_, stderr, code := runCLI(t, bin, "acp-multiplex-no-such-agent")
	if code == 0 {
		t.Error("missing agent exited 0, want non-zero")
	}
	if !strings.Contains(stderr, "acp-multiplex-no-such-agent") {
		t.Errorf("stderr does not name the agent it could not start, got %q", stderr)
	}
}
