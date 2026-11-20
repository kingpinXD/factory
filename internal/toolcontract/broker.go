package toolcontract

const ModeExistingPRRevision = "existing_pr_revision"

type Request struct {
	RevisionPR     int    `json:"revision_pr,omitempty"`
	RevisionRepo   string `json:"revision_repo,omitempty"`
	RevisionBranch string `json:"revision_branch,omitempty"`
	RevisionBase   string `json:"revision_base,omitempty"`

	SchemaVersion int    `json:"schema_version"`
	Mode          string `json:"mode,omitempty"`
	RequestKey    string `json:"request_key"`
	OwnerID       string `json:"owner_id"`
	WorkID        string `json:"work_id"`
	SetID         string `json:"set_id"`
	Lease         int    `json:"lease"`
	ParentJobID   string `json:"parent_job_id"`
	Component     string `json:"component"`
	Prompt        string `json:"prompt"`
	OutputPath    string `json:"output_path"`
}

type ContinueRequest struct {
	SchemaVersion        int      `json:"schema_version"`
	RequestKey           string   `json:"request_key"`
	ExpectedRound        int      `json:"expected_round"`
	Prompt               string   `json:"prompt"`
	OutputPath           string   `json:"output_path"`
	AcknowledgedChildren []string `json:"acknowledged_children"`
}

func (r *Request) UnmarshalJSON(data []byte) error {
	type plain Request
	var v plain
	if err := DecodeBroker(data, &v); err != nil {
		return err
	}
	*r = Request(v)
	return nil
}

func (r *ContinueRequest) UnmarshalJSON(data []byte) error {
	type plain ContinueRequest
	var v plain
	if err := DecodeBroker(data, &v); err != nil {
		return err
	}
	*r = ContinueRequest(v)
	return nil
}
