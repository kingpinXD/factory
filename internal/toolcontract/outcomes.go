package toolcontract

type ToolInputError struct{ Message string }

func (e *ToolInputError) Error() string { return e.Message }

type ToolFailure struct {
	ToolError string `json:"tool_error"`
	Code      string `json:"code,omitempty"`
}
type NoActionError struct{ Message string }

func (e *NoActionError) Error() string { return e.Message }

type ApprovedContextAbsentError struct{ Message string }

func (e *ApprovedContextAbsentError) Error() string { return e.Message }

type ReadUnavailableError struct{ Message string }

func (e *ReadUnavailableError) Error() string { return e.Message }

// EncodingError marks ambiguous or invalid encoding, never a corrective reply.
type EncodingError struct{ Cause error }

func (e *EncodingError) Error() string { return e.Cause.Error() }
func (e *EncodingError) Unwrap() error { return e.Cause }
