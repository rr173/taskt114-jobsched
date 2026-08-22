package model

func StateRank(state State) int {
	switch state {
	case StateRunning:
		return 5
	case StatePending:
		return 4
	case StateScheduled:
		return 3
	case StateSucceeded:
		return 2
	case StateDead:
		return 1
	default:
		return 0
	}
}

func (s Stats) Active() int {
	return s.ByState[StatePending] + s.ByState[StateScheduled] + s.ByState[StateRunning]
}
func (s Stats) Terminal() int {
	return s.ByState[StateSucceeded] + s.ByState[StateDead] + s.ByState[StateCancelled]
}

func IsActive(state State) bool        { return StateRank(state) >= StateRank(StateScheduled) }
func IsRetryTerminal(state State) bool { return state == StateDead || state == StateCancelled }
func CanDispatch(state State) bool     { return state == StatePending || state == StateScheduled }

func StateLabel(state State) string {
	if ValidStates[state] {
		return string(state)
	}
	return "unknown"
}

func IsSuccessful(state State) bool  { return state == StateSucceeded }
func IsFailed(state State) bool      { return state == StateDead }
func NeedsOperator(state State) bool { return state == StateDead || state == StateCancelled }

func StateCategory(state State) string {
	switch {
	case state == StatePending || state == StateScheduled:
		return "queued"
	case state == StateRunning:
		return "in_flight"
	case state == StateSucceeded:
		return "success"
	default:
		return "terminal"
	}
}
