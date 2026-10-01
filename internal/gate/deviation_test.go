package gate

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/journal"
	"github.com/wyw14/cry-96/internal/model"
)

func mustUUID() uuid.UUID {
	id := uuid.New()
	return id
}

type fakeClock struct{ t time.Time }

func newFakeClock() *fakeClock      { return &fakeClock{t: time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)} }
func (c *fakeClock) now() time.Time { return c.t }
func (c *fakeClock) advance(d time.Duration) time.Time {
	c.t = c.t.Add(d)
	return c.t
}

type gateHarness struct {
	clock    *fakeClock
	store    *journal.MemoryStore
	recorder *journal.Recorder
	state    *State
	commands *Commands
	ctrl     *Controller
	cycle    model.TransitCycle
}

func newGateHarness(t *testing.T, threshold time.Duration) *gateHarness {
	t.Helper()
	clock := newFakeClock()
	store := journal.NewMemoryStore()
	recorder := journal.NewRecorder(store, clock.now)
	state := NewState("chamber-1")
	commands := NewCommands()
	ctrl := NewControllerWithThreshold(state, commands, recorder, threshold, clock.now)
	planID := mustUUID()
	cycle := model.TransitCycle{
		ID: mustUUID(), ChamberID: "chamber-1", PlanID: planID,
		Direction: model.DirectionUpstream, Stage: model.StagePlanned,
		Generation: 1, CreatedAt: clock.now(), UpdatedAt: clock.now(),
	}
	return &gateHarness{
		clock: clock, store: store, recorder: recorder, state: state,
		commands: commands, ctrl: ctrl, cycle: cycle,
	}
}

func (h *gateHarness) issueOpen(t *testing.T) model.DeviceCommand {
	t.Helper()
	command, err := h.ctrl.Request(context.Background(), h.cycle, model.DirectionUpstream, true)
	if err != nil {
		t.Fatalf("issue open command: %v", err)
	}
	return command
}

func (h *gateHarness) observe(t *testing.T, position model.GatePosition) {
	t.Helper()
	if err := h.ctrl.Observe("chamber-1", model.DirectionUpstream, position); err != nil {
		t.Fatalf("observe %s: %v", position, err)
	}
}

func ackFor(command model.DeviceCommand) model.DeviceAck {
	return model.DeviceAck{
		CommandID: command.ID, CycleID: command.CycleID, ChamberID: command.ChamberID,
		Generation: command.Generation, Epoch: command.Epoch, Device: command.Device,
		Settled: true, ObservedAt: command.UpdatedAt,
	}
}

func TestDeviationToleranceWindow(t *testing.T) {
	h := newGateHarness(t, time.Minute)
	h.issueOpen(t)
	h.observe(t, model.GateOpening)

	if deviations := h.ctrl.Deviations().Open("chamber-1"); len(deviations) != 0 {
		t.Fatalf("expected no deviation within tolerance, got %d", len(deviations))
	}
}

func TestDeviationRaisedDeduplicatedAndResolved(t *testing.T) {
	threshold := time.Minute
	h := newGateHarness(t, threshold)
	command := h.issueOpen(t)

	h.clock.advance(threshold + time.Second)
	h.observe(t, model.GateOpening)
	open := h.ctrl.Deviations().Open("chamber-1")
	if len(open) != 1 {
		t.Fatalf("expected one open deviation, got %d", len(open))
	}
	first := open[0]
	if first.CommandID != command.ID || first.TargetPosition != model.GateOpen ||
		first.ActualPosition != model.GateOpening || first.Status != DeviationActive {
		t.Fatalf("unexpected deviation payload: %+v", first)
	}

	// Repeated/jittering observations must update the same deviation, never add one.
	h.clock.advance(30 * time.Second)
	h.observe(t, model.GateClosing)
	h.clock.advance(30 * time.Second)
	h.observe(t, model.GateOpening)
	open = h.ctrl.Deviations().Open("chamber-1")
	if len(open) != 1 || open[0].ID != first.ID {
		t.Fatalf("expected the same single deviation, got %+v", open)
	}
	if open[0].ActualPosition != model.GateOpening {
		t.Fatalf("expected actual position refreshed to opening, got %s", open[0].ActualPosition)
	}
	if open[0].DurationSeconds <= first.DurationSeconds {
		t.Fatalf("expected duration to grow: first=%.1f now=%.1f", first.DurationSeconds, open[0].DurationSeconds)
	}

	// A real terminal observation resolves it automatically.
	h.clock.advance(time.Second)
	h.observe(t, model.GateOpen)
	if deviations := h.ctrl.Deviations().Open("chamber-1"); len(deviations) != 0 {
		t.Fatalf("expected auto-resolution, still open: %+v", deviations)
	}
	listed := h.ctrl.Deviations().List("chamber-1")
	if len(listed) != 1 || listed[0].Status != DeviationResolved || listed[0].ResolvedAt == nil {
		t.Fatalf("expected one resolved deviation with timestamp, got %+v", listed)
	}

	// Only now can the acknowledgement settle the command.
	if _, err := h.ctrl.Confirm(context.Background(), ackFor(command)); err != nil {
		t.Fatalf("confirm after matching observation: %v", err)
	}
	if got, _ := h.commands.Get(command.ID); got.State != model.CommandSettled {
		t.Fatalf("expected settled command, got %s", got.State)
	}
}

func TestDeviationResolvedWhenCommandCancelled(t *testing.T) {
	threshold := time.Minute
	h := newGateHarness(t, threshold)
	h.issueOpen(t)

	h.clock.advance(threshold + time.Second)
	h.observe(t, model.GateOpening)
	if len(h.ctrl.Deviations().Open("chamber-1")) != 1 {
		t.Fatalf("expected an open deviation before cancellation")
	}

	h.commands.CancelChamber("chamber-1", h.clock.now())
	listed := h.ctrl.Deviations().List("chamber-1")
	if len(listed) != 1 || listed[0].Status != DeviationResolved {
		t.Fatalf("expected deviation auto-resolved after cancel, got %+v", listed)
	}
}

func TestDeviationAcknowledgeKeepsItOpen(t *testing.T) {
	threshold := time.Minute
	h := newGateHarness(t, threshold)
	h.issueOpen(t)
	h.clock.advance(threshold + time.Second)
	h.observe(t, model.GateOpening)

	first := h.ctrl.Deviations().Open("chamber-1")[0]
	acknowledged, err := h.ctrl.Deviations().Acknowledge(first.ID, "crew dispatched")
	if err != nil {
		t.Fatalf("acknowledge: %v", err)
	}
	if acknowledged.Status != DeviationAcknowledged || acknowledged.Note != "crew dispatched" {
		t.Fatalf("unexpected acknowledged payload: %+v", acknowledged)
	}

	// Acknowledgement is a handling state, not a resolution: still open.
	open := h.ctrl.Deviations().Open("chamber-1")
	if len(open) != 1 || open[0].Status != DeviationAcknowledged {
		t.Fatalf("expected deviation to remain open after acknowledge, got %+v", open)
	}
}

func TestAssertObservation(t *testing.T) {
	h := newGateHarness(t, time.Minute)
	command := h.issueOpen(t)

	if err := h.ctrl.Deviations().AssertObservation(command); err == nil {
		t.Fatal("expected mismatch while gate is still opening")
	}

	h.observe(t, model.GateOpen)
	if err := h.ctrl.Deviations().AssertObservation(command); err != nil {
		t.Fatalf("expected match after terminal observation: %v", err)
	}

	valve, err := h.commands.Issue(h.cycle, model.DeviceFillValve, "set_opening", 1, h.clock.now())
	if err != nil {
		t.Fatalf("issue valve: %v", err)
	}
	if err := h.ctrl.Deviations().AssertObservation(valve); err == nil {
		t.Fatal("expected non-gate command to be rejected")
	}
}

func TestConfirmRejectsAckWithoutMatchingObservation(t *testing.T) {
	h := newGateHarness(t, time.Minute)
	command := h.issueOpen(t)
	h.observe(t, model.GateOpening)

	if _, err := h.ctrl.Confirm(context.Background(), ackFor(command)); err == nil {
		t.Fatal("expected confirm to be refused without a real terminal position")
	}
	if got, _ := h.commands.Get(command.ID); got.State == model.CommandSettled {
		t.Fatal("command must not settle on an unmatched ack")
	}
}

func TestDeviationEventsAreRecorded(t *testing.T) {
	threshold := time.Minute
	h := newGateHarness(t, threshold)
	h.issueOpen(t)
	h.clock.advance(threshold + time.Second)
	h.observe(t, model.GateOpening)
	h.observe(t, model.GateOpen)

	events, err := h.store.Events(context.Background(), "chamber-1", 0)
	if err != nil {
		t.Fatalf("read events: %v", err)
	}
	var sawRaised, sawResolved bool
	for _, event := range events {
		if event.Type == model.EventGateDeviationRaised {
			sawRaised = true
		}
		if event.Type == model.EventGateDeviationResolved {
			sawResolved = true
		}
	}
	if !sawRaised || !sawResolved {
		t.Fatalf("expected raise+resolve events, raised=%v resolved=%v", sawRaised, sawResolved)
	}
}
