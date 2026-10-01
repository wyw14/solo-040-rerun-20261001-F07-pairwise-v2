package gate

import (
	"context"
	"errors"
	"time"

	"github.com/wyw14/cry-96/internal/journal"
	"github.com/wyw14/cry-96/internal/model"
)

// Controller links physical observations, command lifecycle and the journal.
type Controller struct {
	state      *State
	commands   *Commands
	deviations *DeviationMonitor
	recorder   *journal.Recorder
	now        func() time.Time
}

func NewController(state *State, commands *Commands, recorder *journal.Recorder, now func() time.Time) *Controller {
	return NewControllerWithThreshold(state, commands, recorder, DefaultDeviationThreshold, now)
}

// NewControllerWithThreshold allows callers (and tests) to tune how long a
// command may stay unmatched before it becomes a deviation alarm.
func NewControllerWithThreshold(state *State, commands *Commands, recorder *journal.Recorder, threshold time.Duration, now func() time.Time) *Controller {
	if now == nil {
		now = time.Now
	}
	return &Controller{
		state: state, commands: commands, recorder: recorder, now: now,
		deviations: NewDeviationMonitor(commands, state, recorder, threshold, now),
	}
}

func (c *Controller) Request(ctx context.Context, cycle model.TransitCycle, direction model.Direction, open bool) (model.DeviceCommand, error) {
	if err := ctx.Err(); err != nil {
		return model.DeviceCommand{}, err
	}
	device := model.DeviceUpstreamGate
	if direction == model.DirectionDownstream {
		device = model.DeviceDownstreamGate
	}
	action := "close"
	position := model.GateClosing
	if open {
		action = "open"
		position = model.GateOpening
	}
	command, err := c.commands.Issue(cycle, device, action, 0, c.now())
	if err != nil {
		return model.DeviceCommand{}, err
	}
	if err := c.state.Observe(cycle.ChamberID, direction, position, c.now()); err != nil {
		return model.DeviceCommand{}, err
	}
	c.deviations.Reconcile()
	if _, err := c.recorder.Record(ctx, cycle.ChamberID, model.EventCommandQueued, cycle.ID, cycle.Generation, command); err != nil {
		return model.DeviceCommand{}, err
	}
	return command, nil
}

// Confirm settles a gate command only after a real field observation has
// reached the commanded terminal position. An acknowledgement alone never
// writes a position and can never settle or advance a transit stage.
func (c *Controller) Confirm(ctx context.Context, ack model.DeviceAck) (model.DeviceCommand, error) {
	command, ok := c.commands.Get(ack.CommandID)
	if !ok {
		return model.DeviceCommand{}, errors.New("unknown command " + ack.CommandID.String())
	}
	if err := c.deviations.AssertObservation(command); err != nil {
		return model.DeviceCommand{}, err
	}
	settled, err := c.commands.Settle(ack, c.now())
	if err != nil {
		return model.DeviceCommand{}, err
	}
	c.deviations.Reconcile()
	if _, err := c.recorder.Record(ctx, settled.ChamberID, model.EventCommandSettled, settled.CycleID, settled.Generation, settled); err != nil {
		return model.DeviceCommand{}, err
	}
	return settled, nil
}

func (c *Controller) Observe(chamberID string, direction model.Direction, position model.GatePosition) error {
	if chamberID == "" {
		return errors.New("chamber is required")
	}
	if err := c.state.Observe(chamberID, direction, position, c.now()); err != nil {
		return err
	}
	// A field observation is the only signal that can match a target, so this
	// is where a lingering deviation is cleared automatically.
	c.deviations.Reconcile()
	return nil
}

func (c *Controller) State() *State                 { return c.state }
func (c *Controller) Commands() *Commands           { return c.commands }
func (c *Controller) Deviations() *DeviationMonitor { return c.deviations }
