package backoff

import "time"

func Clamp(d, min, max time.Duration) time.Duration {
	if d < min {
		return min
	}
	if max > 0 && d > max {
		return max
	}
	return d
}
func Window(s Strategy, attempt int, base, max time.Duration) time.Duration {
	return Clamp(s.Next(attempt, base), base, max)
}
