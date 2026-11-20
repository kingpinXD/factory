package toolcontract

import (
	"encoding/json"
	"time"
)

type Envelope struct {
	Op      string          `json:"op"`
	Payload json.RawMessage `json:"payload"`
}

type Input interface{ contractInput() }
type Empty struct{}

func (*Empty) contractInput()           {}
func (*Request) contractInput()         {}
func (*ContinueRequest) contractInput() {}

type TextInput struct {
	Text string `json:"text"`
	Key  string `json:"key"`
}

func (*TextInput) contractInput() {}

type BrainReadInput struct {
	Path     string  `json:"path"`
	MaxBytes *uint64 `json:"max_bytes"`
}

func (*BrainReadInput) contractInput() {}

type NestedSubmitInput struct {
	RequestKey string `json:"request_key"`
	Component  string `json:"component"`
	Prompt     string `json:"prompt"`
	OutputPath string `json:"output_path"`
}

func (*NestedSubmitInput) contractInput() {}

type ResultInput struct {
	JobID string `json:"job_id"`
	Round int    `json:"round"`
}

func (*ResultInput) contractInput() {}

type ContinueInput struct {
	JobID   string          `json:"job_id"`
	Request ContinueRequest `json:"request"`
}

func (*ContinueInput) contractInput() {}

type CancelRequest struct {
	Reason string `json:"reason"`
}
type CancelInput struct {
	JobID   string        `json:"job_id"`
	Request CancelRequest `json:"request"`
}

func (*CancelInput) contractInput() {}

type YieldInput struct {
	Children []string `json:"children"`
}

func (*YieldInput) contractInput() {}

type FileInput struct {
	Path string  `json:"path"`
	Text *string `json:"text"`
}

func (*FileInput) contractInput() {}

type TerminalInput struct {
	Command string `json:"command"`
	Cwd     string `json:"cwd"`
}

func (*TerminalInput) contractInput() {}

type ControlInput struct {
	Target string `json:"target"`
	Path   string `json:"path"`
	Text   string `json:"text"`
	Kind   string `json:"kind"`
	Key    string `json:"key"`
	Ack    int    `json:"ack"`
	To     string `json:"to,omitempty"`
	Append bool   `json:"append"`
}

func (*ControlInput) contractInput() {}

type PathInput struct {
	Path string `json:"path"`
}

func (*PathInput) contractInput() {}

type IssueInput struct {
	Repo     string `json:"repo"`
	Base     string `json:"base,omitempty"`
	Number   int    `json:"number"`
	Key      string `json:"key"`
	Title    string `json:"title"`
	Body     string `json:"body"`
	Parent   string `json:"parent"`
	Evidence string `json:"evidence"`
}

func (*IssueInput) contractInput() {}

type GitHubReadInput struct {
	Repo   string    `json:"repo"`
	Base   string    `json:"base,omitempty"`
	Number int       `json:"number,omitempty"`
	SHA    string    `json:"sha,omitempty"`
	Path   string    `json:"path,omitempty"`
	Since  time.Time `json:"since,omitempty"`
}

func (*GitHubReadInput) contractInput() {}

type PRActionInput struct {
	Repo      string `json:"repo"`
	Number    int    `json:"number"`
	Kind      string `json:"kind,omitempty"`
	ThreadID  string `json:"thread_id,omitempty"`
	CommentID int64  `json:"comment_id,omitempty"`
	Body      string `json:"body,omitempty"`
}

func (*PRActionInput) contractInput() {}
