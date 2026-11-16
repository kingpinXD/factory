package store

import (
	"sort"
	"time"
)

type AvailabilityScope struct {
	Executor   string `json:"executor"`
	OwnerID    string `json:"owner_id"`
	WorkID     string `json:"work_id,omitempty"`
	Generation string `json:"generation"`
}

type AvailabilityHold struct {
	Scope       AvailabilityScope `json:"scope"`
	Source      string            `json:"source"`
	SourceID    string            `json:"source_id"`
	From        time.Time         `json:"from"`
	To          time.Time         `json:"to,omitzero"`
	Reason      string            `json:"reason"`
	NextCheckAt time.Time         `json:"next_check_at,omitzero"`
}

type AvailabilityCoverage struct {
	Scope AvailabilityScope `json:"scope"`
	From  time.Time         `json:"from"`
}

type ClockReading struct {
	Eligible time.Duration `json:"eligible"`
	Complete bool          `json:"complete"`
}

type clockInterval struct{ from, to time.Time }

// EligibleElapsed requires intervals already filtered for scope and account exceptions.
// Missing coverage proves no duration; partial coverage proves only its suffix.
func EligibleElapsed(since, now time.Time, holds []Hold, scoped []AvailabilityHold, coverageFrom time.Time) ClockReading {
	if since.IsZero() || now.IsZero() || now.Before(since) || coverageFrom.IsZero() || coverageFrom.After(now) {
		return ClockReading{}
	}
	start := since
	if coverageFrom.After(start) {
		start = coverageFrom
	}
	span := now.Sub(start)
	if !start.Add(span).Equal(now) {
		return ClockReading{}
	}
	intervals := make([]clockInterval, 0, len(holds)+len(scoped))
	add := func(from, to time.Time) bool {
		if from.IsZero() || !to.IsZero() && to.Before(from) {
			return false
		}
		if to.IsZero() || to.After(now) {
			to = now
		}
		if from.Before(start) {
			from = start
		}
		if to.After(from) {
			intervals = append(intervals, clockInterval{from, to})
		}
		return true
	}
	for _, h := range holds {
		if !add(h.From, h.To) {
			return ClockReading{}
		}
	}
	for _, h := range scoped {
		if !add(h.From, h.To) {
			return ClockReading{}
		}
	}
	sort.Slice(intervals, func(i, j int) bool { return intervals[i].from.Before(intervals[j].from) })
	cursor := start
	eligible := time.Duration(0)
	for _, h := range intervals {
		if h.from.After(cursor) {
			eligible += h.from.Sub(cursor)
		}
		if h.to.After(cursor) {
			cursor = h.to
		}
	}
	return ClockReading{Eligible: eligible + now.Sub(cursor), Complete: !coverageFrom.After(since)}
}
