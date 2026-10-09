// Package gh reads and writes GitHub through the gh CLI: issues with their
// sub-issues and links, pull requests, checks, review threads and merges.
// Every call names its repository as owner/name.
package gh

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/kingpinXD/factory/internal/proc"
)

// Client runs the gh CLI.
type Client struct {
	Runner proc.Runner
}

func (c Client) gh(ctx context.Context, args ...string) ([]byte, error) {
	return c.Runner.Run(ctx, proc.Cmd{Name: "gh", Args: args})
}

// getJSON runs gh and decodes what it prints into out.
func (c Client) getJSON(ctx context.Context, out any, args ...string) error {
	b, err := c.gh(ctx, args...)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(b, out); err != nil {
		return fmt.Errorf("gh %s: %w", args[0], err)
	}
	return nil
}

// graphql runs a GraphQL query or mutation and decodes its data into out.
// vars are gh api fields: -f for a string, -F for a number.
func (c Client) graphql(ctx context.Context, out any, query string, vars ...string) error {
	var resp struct {
		Data json.RawMessage `json:"data"`
	}
	if err := c.getJSON(ctx, &resp, append([]string{"api", "graphql", "-f", "query=" + query}, vars...)...); err != nil {
		return err
	}
	return json.Unmarshal(resp.Data, out)
}

// repoVars returns the owner and name GraphQL variables for repo.
func repoVars(repo string) []string {
	owner, name, _ := strings.Cut(repo, "/")
	return []string{"-f", "owner=" + owner, "-f", "name=" + name}
}

// User is a GitHub account.
type User struct {
	Login string `json:"login"`
}

// Me returns the login gh acts as.
func (c Client) Me(ctx context.Context) (string, error) {
	var u User
	err := c.getJSON(ctx, &u, "api", "user")
	return u.Login, err
}

// Comment is one comment on an issue, a pull request or a review thread.
type Comment struct {
	ID        int64
	Author    string
	Body      string
	URL       string
	CreatedAt time.Time
}
