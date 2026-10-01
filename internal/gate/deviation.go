package gate

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/journal"
	"github.com/wyw14/cry-96/internal/model"
)

// DefaultDeviationThreshold is how long a gate command may remain unmatched by
// an observed terminal position before a deviation alarm is raised.
const DefaultDeviationThreshold = 30 * time.Second

const resolvedHistoryLimit = 100

type DeviationStatus string

const (
	DeviationActive       DeviationStatus = "active"
	DeviationAcknowledged DeviationStatus = "acknowledged"
	DeviationResolved     DeviationStatus = "resolved"
)

// Deviation records one gate command whose target position was not confirmed
// by a real field observation. It is reviewable: command, actual position,
// duration and handling status travel together.
type Deviation struct {
	ID              uuid.UUID          `json:"id"`
	ChamberID       string             `json:"chamber_id"`
	Device          model.DeviceKind   `json:"device"`
	Direction       model.Direction    `json:"direction"`
	CommandID       uuid.UUID          `json:"command_id"`
	CommandAction   string             `json:"command_action"`
	TargetPosition  model.GatePosition `json:"target_position"`
	ActualPosition  model.GatePosition `json:"actual_position"`
	Status          DeviationStatus    `json:"status"`
	StartedAt       time.Time          `json:"started_at"`
	DetectedAt      time.Time          `json:"detected_at"`
	LastObservedAt  time.Time          `json:"last_observed_at"`
	ResolvedAt      *time.Time         `json:"resolved_at,omitempty"`
	ResolutionNote  string             `json:"resolution_note,omitempty"`
	Note            string             `json:"note,omitempty"`
	DurationSeconds float64            `json:"duration_seconds"`
}

func deviceDirection(device model.DeviceKind) (model.Direction, bool) {
	switch device {
	case model.DeviceUpstreamGate:
		return model.DirectionUpstream, true
	case model.DeviceDownstreamGate:
		return model.DirectionDownstream, true
	default:
		return "", false
	}
}

// ExpectedTargetPosition maps a gate command action to the terminal position
// that must be confirmed by a real field observation.
func ExpectedTargetPosition(action string) (model.GatePosition, bool) {
	switch action {
	case "open":
		return model.GateOpen, true
	case "close":
		return model.GateClosed, true
	default:
		return "", false
	}
}

// DeviationMonitor reconciles outstanding gate commands with observed gate
// positions. One open deviation exists per chamber/device at most; repeated
// observations update it instead of creating duplicates, and a matching
// observation resolves it automatically.
type DeviationMonitor struct {
	mu        sync.Mutex
	commands  *Commands
	state     *State
	recorder  *journal.Recorder
	threshold time.Duration
	now       func() time.Time
	active    map[string]*Deviation
	resolved  []Deviation
}

func NewDeviationMonitor(commands *Commands, state *State, recorder *journal.Recorder, threshold time.Duration, now func() time.Time) *DeviationMonitor {
	if threshold <= 0 {
		threshold = DefaultDeviationThreshold
	}
	if now == nil {
		now = time.Now
	}
	return &DeviationMonitor{
		commands: commands, state: state, recorder: recorder,
		threshold: threshold, now: now,
		active:   make(map[string]*Deviation),
		resolved: make([]Deviation, 0, resolvedHistoryLimit),
	}
}

func deviationKey(chamberID string, device model.DeviceKind) string {
	return chamberID + ":" + string(device)
}

func latestNonZero(candidate, fallback time.Time) time.Time {
	if candidate.IsZero() {
		return fallback
	}
	return candidate
}

// Reconcile compares every outstanding gate command with the latest observed
// position, raising, updating or resolving deviations as needed. It is safe to
// call repeatedly: it never duplicates an open deviation.
func (m *DeviationMonitor) Reconcile() {
	now := m.now().UTC()
	m.mu.Lock()
	raised, resolved := m.reconcileLocked(now)
	m.mu.Unlock()

	// Persist lifecycle events outside the lock: journal appends can block.
	for _, deviation := range raised {
		m.record(model.EventGateDeviationRaised, deviation)
	}
	for _, deviation := range resolved {
		m.record(model.EventGateDeviationResolved, deviation)
	}
}

// reconcileLocked mutates in-memory state and returns the deviations that were
// raised or resolved so the caller can journal them after releasing the lock.
func (m *DeviationMonitor) reconcileLocked(now time.Time) (raised []Deviation, resolved []Deviation) {
	latest := make(map[string]model.DeviceCommand)
	for _, command := range m.commands.AllPending() {
		if _, ok := deviceDirection(command.Device); !ok {
			continue
		}
		key := deviationKey(command.ChamberID, command.Device)
		if current, exists := latest[key]; !exists || command.Epoch > current.Epoch {
			latest[key] = command
		}
	}

	seen := make(map[string]struct{}, len(latest))
	for key, command := range latest {
		seen[key] = struct{}{}
		direction, _ := deviceDirection(command.Device)
		target, _ := ExpectedTargetPosition(command.Action)
		observed := m.state.Position(command.ChamberID, direction)

		if observed.Position == target {
			if deviation, ok := m.resolveLocked(key, now, "position matched command target"); ok {
				resolved = append(resolved, deviation)
			}
			continue
		}

		if existing, ok := m.active[key]; ok {
			if existing.CommandID != command.ID {
				// A newer command supersedes the stale one; close the old
				// deviation and let the new command pass the tolerance window
				// before it can alarm on its own.
				if deviation, ok := m.resolveLocked(key, now, "superseded by newer command"); ok {
					resolved = append(resolved, deviation)
				}
				if now.Sub(command.CreatedAt.UTC()) >= m.threshold {
					raised = append(raised, m.raiseLocked(key, command, direction, target, observed.Position, now))
				}
			} else {
				existing.ActualPosition = observed.Position
				existing.LastObservedAt = latestNonZero(observed.UpdatedAt, now)
			}
			continue
		}

		if now.Sub(command.CreatedAt.UTC()) >= m.threshold {
			raised = append(raised, m.raiseLocked(key, command, direction, target, observed.Position, now))
		}
	}

	// Commands that left the pending set (settled or cancelled) must not leave
	// a dangling deviation behind.
	for key := range m.active {
		if _, ok := seen[key]; !ok {
			if deviation, ok := m.resolveLocked(key, now, "command no longer pending"); ok {
				resolved = append(resolved, deviation)
			}
		}
	}
	return raised, resolved
}

func (m *DeviationMonitor) raiseLocked(key string, command model.DeviceCommand, direction model.Direction, target, actual model.GatePosition, now time.Time) Deviation {
	deviation := &Deviation{
		ID:             uuid.New(),
		ChamberID:      command.ChamberID,
		Device:         command.Device,
		Direction:      direction,
		CommandID:      command.ID,
		CommandAction:  command.Action,
		TargetPosition: target,
		ActualPosition: actual,
		Status:         DeviationActive,
		StartedAt:      command.CreatedAt.UTC(),
		DetectedAt:     now,
		LastObservedAt: now,
	}
	m.active[key] = deviation
	return *deviation
}

func (m *DeviationMonitor) resolveLocked(key string, now time.Time, note string) (Deviation, bool) {
	deviation, ok := m.active[key]
	if !ok {
		return Deviation{}, false
	}
	deviation.Status = DeviationResolved
	deviation.ResolvedAt = &now
	deviation.ResolutionNote = note
	if deviation.LastObservedAt.After(now) {
		deviation.LastObservedAt = now
	}
	m.resolved = append(m.resolved, *deviation)
	if overflow := len(m.resolved) - resolvedHistoryLimit; overflow > 0 {
		m.resolved = m.resolved[overflow:]
	}
	delete(m.active, key)
	return *deviation, true
}

func (m *DeviationMonitor) record(kind model.EventType, deviation Deviation) {
	if m.recorder == nil {
		return
	}
	_, _ = m.recorder.Record(context.Background(), deviation.ChamberID, kind, deviation.CommandID, 0, deviation)
}

// Acknowledge marks an open deviation as operator-handled. It does not resolve
// it: only a matching field observation can do that.
func (m *DeviationMonitor) Acknowledge(id uuid.UUID, note string) (Deviation, error) {
	m.Reconcile()
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, deviation := range m.active {
		if deviation.ID != id {
			continue
		}
		if deviation.Status == DeviationAcknowledged {
			return Deviation{}, fmt.Errorf("deviation %s is already acknowledged", id)
		}
		deviation.Status = DeviationAcknowledged
		deviation.Note = note
		return m.withDuration(*deviation, m.now().UTC()), nil
	}
	return Deviation{}, fmt.Errorf("open deviation %s not found", id)
}

// List returns open deviations first, followed by recently resolved ones,
// optionally filtered to one chamber. Durations are computed at read time.
func (m *DeviationMonitor) List(chamberID string) []Deviation {
	m.Reconcile()
	now := m.now().UTC()
	m.mu.Lock()
	defer m.mu.Unlock()

	result := make([]Deviation, 0, len(m.active)+len(m.resolved))
	for _, deviation := range m.active {
		if chamberID == "" || deviation.ChamberID == chamberID {
			result = append(result, m.withDuration(*deviation, now))
		}
	}
	for index := len(m.resolved) - 1; index >= 0; index-- {
		deviation := m.resolved[index]
		if chamberID == "" || deviation.ChamberID == chamberID {
			result = append(result, m.withDuration(deviation, now))
		}
	}
	sort.SliceStable(result, func(i, j int) bool {
		if (result[i].Status == DeviationResolved) != (result[j].Status == DeviationResolved) {
			return result[i].Status != DeviationResolved
		}
		return result[i].DetectedAt.After(result[j].DetectedAt)
	})
	return result
}

// Open returns every non-resolved deviation, optionally for one chamber. It is
// used by the chamber snapshot.
func (m *DeviationMonitor) Open(chamberID string) []Deviation {
	m.Reconcile()
	now := m.now().UTC()
	m.mu.Lock()
	defer m.mu.Unlock()

	result := make([]Deviation, 0, len(m.active))
	for _, deviation := range m.active {
		if chamberID == "" || deviation.ChamberID == chamberID {
			result = append(result, m.withDuration(*deviation, now))
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].DetectedAt.Before(result[j].DetectedAt) })
	return result
}

func (m *DeviationMonitor) withDuration(deviation Deviation, now time.Time) Deviation {
	end := now
	if deviation.ResolvedAt != nil {
		end = *deviation.ResolvedAt
	}
	deviation.DurationSeconds = end.Sub(deviation.StartedAt).Seconds()
	if deviation.DurationSeconds < 0 {
		deviation.DurationSeconds = 0
	}
	return deviation
}

// AssertObservation verifies that the real field position of a gate already
// matches the command's terminal target. It does not settle the command or
// advance any stage; callers must refuse to do so when it returns an error.
func (m *DeviationMonitor) AssertObservation(command model.DeviceCommand) error {
	direction, ok := deviceDirection(command.Device)
	if !ok {
		return fmt.Errorf("command %s targets non-gate device %s", command.ID, command.Device)
	}
	target, ok := ExpectedTargetPosition(command.Action)
	if !ok {
		return fmt.Errorf("command %s has unsupported action %q", command.ID, command.Action)
	}
	observed := m.state.Position(command.ChamberID, direction)
	if observed.Position != target {
		m.Reconcile()
		return fmt.Errorf("gate %s observed %q, commanded target %q", direction, observed.Position, target)
	}
	return nil
}
