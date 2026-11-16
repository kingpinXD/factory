package main

import (
	"context"
	"fmt"
	"io"

	"github.com/kingpinXD/factory/internal/notify"
)

type localNotifications struct{ out io.Writer }

func (n localNotifications) DM(_ context.Context, text string) error {
	_, err := fmt.Fprintf(n.out, "local notification (Slack disabled): %s\n", text)
	return err
}
func (n localNotifications) Post(_ context.Context, channel, text string) (string, error) {
	if _, err := fmt.Fprintf(n.out, "local notification (Slack disabled, channel %s): %s\n", channel, text); err != nil {
		return "", fmt.Errorf("%w: %v", notify.ErrNotPosted, err)
	}
	return "", fmt.Errorf("%w: Slack disabled; recorded locally", notify.ErrNotPosted)
}
