package codex

import "context"

// StopOwnedProcessGroup preserves the attempt host's exact group ownership checks.
func StopOwnedProcessGroup(ctx context.Context, identity ProcessIdentity) error {
	return stopAttemptGroup(ctx, identity)
}

func VerifyOwnedProcessGroupGone(identity ProcessIdentity) error {
	return verifyAttemptGroupGone(identity)
}
