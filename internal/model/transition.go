package model

func CanCancel(state State) bool {
	return state == StatePending || state == StateScheduled || state == StateRunning
}
func CanRetry(state State) bool {
	return state == StateDead || state == StateRunning || state == StatePending
}

func (j Job) Retryable() bool {
	return !j.IsTerminal() && j.Attempts < j.MaxAttempts
}
