package providers

import "errors"

var (
	ErrInProgress  = errors.New("discovery already in progress")
	ErrNotAllowed  = errors.New("provider not allowed by policy")
	ErrNotVerified = errors.New("provider cannot be verified")
	ErrTooSoon     = errors.New("not enough time since previous discovery")
)
