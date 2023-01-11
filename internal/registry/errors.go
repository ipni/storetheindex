package registry

import "errors"

var (
	ErrAlreadyAssigned     = errors.New("publisher already assigned to this indexer")
	ErrInProgress          = errors.New("discovery already in progress")
	ErrCannotPublish       = errors.New("publisher not allowed to publish to other provider")
	ErrFrozen              = errors.New("indexer frozen")
	ErrNotAllowed          = errors.New("peer not allowed by policy")
	ErrNoDiscovery         = errors.New("discovery not available")
	ErrNoFreeze            = errors.New("freeze not configured")
	ErrNotVerified         = errors.New("provider cannot be verified")
	ErrPublisherNotAllowed = errors.New("publisher not allowed by policy")
	ErrTooSoon             = errors.New("not enough time since previous discovery")
	ErrNoAssigner          = errors.New("not configured to work with assigner service")
)
