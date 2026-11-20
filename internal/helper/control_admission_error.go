package helper

import (
	"errors"

	"github.com/kingpinXD/factory/internal/controlrecord"
)

type controlAdmissionError struct {
	check string
	cause error
}

func (e *controlAdmissionError) Error() string              { return e.cause.Error() }
func (e *controlAdmissionError) Unwrap() error              { return e.cause }
func (e *controlAdmissionError) AdmissionSubReason() string { return e.check }

// ControlAdmissionSubReason exposes only fixed check names, never error text.
func ControlAdmissionSubReason(err error) string {
	var failure *controlAdmissionError
	if errors.As(err, &failure) && controlrecord.ValidFailureSubReason(failure.check) {
		return failure.check
	}
	return ""
}
