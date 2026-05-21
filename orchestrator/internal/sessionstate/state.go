package sessionstate

import "fmt"

type Status string

const (
	Created       Status = "created"
	RunningActive Status = "running_active"
	RunningIdle   Status = "running_idle"
	Checkpointing Status = "checkpointing"
	Checkpointed  Status = "checkpointed"
	Failed        Status = "failed"
	Destroyed     Status = "destroyed"
)

var validStatuses = map[Status]struct{}{
	Created:       {},
	RunningActive: {},
	RunningIdle:   {},
	Checkpointing: {},
	Checkpointed:  {},
	Failed:        {},
	Destroyed:     {},
}

var allStatuses = []Status{
	Created,
	RunningActive,
	RunningIdle,
	Checkpointing,
	Checkpointed,
	Failed,
	Destroyed,
}

var activeStatuses = []Status{
	Created,
	RunningActive,
	RunningIdle,
	Checkpointing,
	Checkpointed,
}

func Validate(value string) error {
	if _, ok := validStatuses[Status(value)]; ok {
		return nil
	}
	return fmt.Errorf("invalid session status %q", value)
}

func CanAcceptInput(value string) bool {
	switch Status(value) {
	case Created, RunningIdle, Checkpointed:
		return true
	default:
		return false
	}
}

func IsBusy(value string) bool {
	switch Status(value) {
	case RunningActive, Checkpointing:
		return true
	default:
		return false
	}
}

func IsTerminal(value string) bool {
	switch Status(value) {
	case Failed, Destroyed:
		return true
	default:
		return false
	}
}

func IsActive(value string) bool {
	switch Status(value) {
	case Created, RunningActive, RunningIdle, Checkpointing, Checkpointed:
		return true
	default:
		return false
	}
}

func AllStatuses() []string {
	statuses := make([]string, len(allStatuses))
	for i, status := range allStatuses {
		statuses[i] = string(status)
	}
	return statuses
}

func ActiveStatuses() []string {
	statuses := make([]string, len(activeStatuses))
	for i, status := range activeStatuses {
		statuses[i] = string(status)
	}
	return statuses
}
