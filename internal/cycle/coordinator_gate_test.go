package cycle

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/gate"
	"github.com/wyw14/cry-96/internal/hydraulics"
	"github.com/wyw14/cry-96/internal/journal"
	"github.com/wyw14/cry-96/internal/model"
)

type cycleClock struct{ t time.Time }

func (c *cycleClock) now() time.Time { return c.t }

type gateCycleHarness struct {
	clock *cycleClock
	store *journal.MemoryStore
	rec   *journal.Recorder
	gates *gate.Controller
	hydr  *hydraulics.Controller
	coord *Coordinator
	plan  model.ConvoyPlan
}

func newGateCycleHarness(t *testing.T) *gateCycleHarness {
	t.Helper()
	clock := &cycleClock{t: time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)}
	store := journal.NewMemoryStore()
	rec := journal.NewRecorder(store, clock.now)
	state := gate.NewState("chamber-1")
	commands := gate.NewCommands()
	gates := gate.NewControllerWithThreshold(state, commands, rec, time.Minute, clock.now)
	ramp := hydraulics.NewRamp(time.Millisecond, clock.now)
	hydr := hydraulics.NewController(commands, rec, ramp, clock.now)
	coord := NewCoordinator(gates, hydr, rec, clock.now)
	plan := model.ConvoyPlan{
		ID: uuid.New(), ChamberID: "chamber-1", Direction: model.DirectionUpstream,
	}
	return &gateCycleHarness{clock: clock, store: store, rec: rec, gates: gates, hydr: hydr, coord: coord, plan: plan}
}

func ackFrom(command model.DeviceCommand) model.DeviceAck {
	return model.DeviceAck{
		CommandID: command.ID, CycleID: command.CycleID, ChamberID: command.ChamberID,
		Generation: command.Generation, Epoch: command.Epoch, Device: command.Device,
		Settled: true, ObservedAt: time.Now().UTC(),
	}
}

func TestUnmatchedAckDoesNotAdvanceStage(t *testing.T) {
	h := newGateCycleHarness(t)
	ctx := context.Background()

	cycleValue, err := h.coord.Start(ctx, h.plan)
	if err != nil {
		t.Fatalf("start cycle: %v", err)
	}
	command, err := h.gates.Request(ctx, cycleValue, model.DirectionUpstream, true)
	if err != nil {
		t.Fatalf("request gate: %v", err)
	}

	// Ack without a real terminal observation must be rejected: no settle, no advance.
	if err := h.coord.ApplyGateAck(ctx, ackFrom(command)); err == nil {
		t.Fatal("expected ApplyGateAck to fail without matching observation")
	}
	current, _ := h.coord.Current("chamber-1")
	if current.Stage != model.StagePlanned {
		t.Fatalf("stage must stay planned, got %s", current.Stage)
	}
	if got, _ := h.gates.Commands().Get(command.ID); got.State == model.CommandSettled {
		t.Fatal("command must not settle without a matching observation")
	}

	// After the field really reports the open position, the ack settles and advances.
	if err := h.gates.Observe("chamber-1", model.DirectionUpstream, model.GateOpen); err != nil {
		t.Fatalf("observe: %v", err)
	}
	if err := h.coord.ApplyGateAck(ctx, ackFrom(command)); err != nil {
		t.Fatalf("ApplyGateAck after observation: %v", err)
	}
	current, _ = h.coord.Current("chamber-1")
	if current.Stage != model.StageAdmitting {
		t.Fatalf("expected stage admitting, got %s", current.Stage)
	}
}

func TestValveAckCannotAdvanceGateStage(t *testing.T) {
	h := newGateCycleHarness(t)
	ctx := context.Background()

	cycleValue, err := h.coord.Start(ctx, h.plan)
	if err != nil {
		t.Fatalf("start cycle: %v", err)
	}
	valve, err := h.gates.Commands().Issue(cycleValue, model.DeviceFillValve, "set_opening", 1, h.clock.now())
	if err != nil {
		t.Fatalf("issue valve: %v", err)
	}

	if err := h.coord.ApplyGateAck(ctx, ackFrom(valve)); err == nil {
		t.Fatal("expected valve command ack to be rejected by the gate path")
	}
	current, _ := h.coord.Current("chamber-1")
	if current.Stage != model.StagePlanned {
		t.Fatalf("valve ack must not advance stage, got %s", current.Stage)
	}
}
