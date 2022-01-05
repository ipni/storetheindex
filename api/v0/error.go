package v0

import (
	"fmt"
	"net/http"
	"strings"
)

// Error is the type of error used when it is necessary to convey a specific
// status code so that it can be handled correctly higher in the call stack.
//
// For example, differentiating between a bad request and an internal server
// error allows a server to determine whether the error should be returned to a
// client or not.
type Error struct {
	err    error
	status int
}

func NewError(err error, status int) *Error {
	return &Error{
		err:    err,
		status: status,
	}
}

func (e *Error) Error() string {
	if e.err != nil {
		return e.err.Error()
	}
	if e.status == 0 {
		return ""
	}
	// If there is only status, then return status text
	if text := http.StatusText(e.status); text != "" {
		return fmt.Sprintf("%d %s", e.status, text)
	}
	return fmt.Sprintf("%d", e.status)
}

func (e *Error) Status() int {
	return e.status
}

func (e *Error) Text() string {
	parts := make([]string, 0, 5)
	if e.status != 0 {
		parts = append(parts, fmt.Sprintf("%d", e.status))
		text := http.StatusText(e.status)
		if text != "" {
			parts = append(parts, " ")
			parts = append(parts, text)
		}
	}
	if e.err != nil {
		if len(parts) != 0 {
			parts = append(parts, ": ")
		}
		parts = append(parts, e.err.Error())
	}

	return strings.Join(parts, "")
}

func (e *Error) Unwrap() error {
	return e.err
}
