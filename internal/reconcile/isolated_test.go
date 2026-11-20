package reconcile

import (
	"context"
	"os"
	"os/exec"
	"regexp"
	"testing"
	"time"
)

// Each child keeps the original fixture's process-wide environment changes local.
func isolatedParallelTest(t *testing.T) bool {
	t.Helper()
	if os.Getenv("FACTORY_ISOLATED_TEST") == t.Name() {
		return false
	}
	t.Parallel()
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	args := []string{"-test.run=^" + regexp.QuoteMeta(t.Name()) + "$", "-test.v"}
	if deadline, ok := t.Deadline(); ok {
		deadline = deadline.Add(-time.Second)
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, deadline)
		defer cancel()
		args = append(args, "-test.timeout="+time.Until(deadline).String())
	}
	command := exec.CommandContext(ctx, binary, args...)
	command.Env = append(os.Environ(), "FACTORY_ISOLATED_TEST="+t.Name(), "GORACE="+os.Getenv("GORACE")+" atexit_sleep_ms=0")
	command.WaitDelay = time.Second
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("isolated test %s failed: %v\n%s", t.Name(), err, output)
	}
	return true
}
