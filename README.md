# factory

An always-running pipeline that takes an issue, epic or plan to merged PRs, using Claude Code sessions.

The plan and its decisions live in the author's agent harness, not in this repo.

```sh
make build      # go build ./...
make test       # go test -race ./...
make install    # builds ~/.local/bin/factory
```
