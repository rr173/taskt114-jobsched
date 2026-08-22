package backoff

import (
	"testing"
	"time"
)

func TestConstant(t *testing.T) {
	c := Constant{Interval: 5 * time.Second}
	if c.Next(0, time.Second) != 5*time.Second {
		t.Fatal("constant should ignore attempt")
	}
	if c.Next(10, time.Second) != 5*time.Second {
		t.Fatal("constant should ignore attempt")
	}
}

func TestLinear(t *testing.T) {
	l := Linear{Step: 2 * time.Second}
	if got := l.Next(0, time.Second); got != time.Second {
		t.Fatalf("attempt 0: got %v want 1s", got)
	}
	if got := l.Next(3, time.Second); got != 7*time.Second {
		t.Fatalf("attempt 3: got %v want 7s", got)
	}
}

func TestExponential(t *testing.T) {
	e := Exponential{Factor: 2, Max: 100 * time.Second}
	if got := e.Next(0, time.Second); got != time.Second {
		t.Fatalf("attempt 0: got %v want 1s", got)
	}
	if got := e.Next(3, time.Second); got != 8*time.Second {
		t.Fatalf("attempt 3: got %v want 8s", got)
	}
	if got := e.Next(10, time.Second); got != 100*time.Second {
		t.Fatalf("attempt 10 capped: got %v want 100s", got)
	}
}

func TestNew(t *testing.T) {
	if _, ok := New("constant", time.Second).(Constant); !ok {
		t.Fatal("expected constant strategy")
	}
	if _, ok := New("linear", time.Second).(Linear); !ok {
		t.Fatal("expected linear strategy")
	}
	if _, ok := New("exponential", time.Second).(Exponential); !ok {
		t.Fatal("expected exponential strategy")
	}
	if _, ok := New("weird", time.Second).(Exponential); !ok {
		t.Fatal("unknown should fall back to exponential")
	}
}
