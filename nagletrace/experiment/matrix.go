package experiment

import (
	"time"

	"nagletrace/app"
	"nagletrace/diagnose"
	"nagletrace/transport"
)

// MatrixCell is one sweep combination and its measured outcome.
type MatrixCell struct {
	ClientPattern app.WritePattern
	ClientMode    app.Interaction
	ClientNoDelay bool
	ServerQuick   bool
	ServerNoDelay bool
	LossRate      float64

	Completed    bool
	SampleCount  int
	P50          time.Duration
	P90          time.Duration
	P99          time.Duration
	Max          time.Duration
	TailFraction float64
	PrimaryType  diagnose.WaitType
	Severity     diagnose.Severity
	NagleStalls  int   `json:"nagleStalls"`
	DelackOnly   int   `json:"delackOnly"`
	RTOs         int   `json:"rtos"`
	FastRetrans  int   `json:"fastRetrans"`
	Dropped      int64 `json:"dropped"`
}

// MatrixConfig bounds the sweep.
type MatrixConfig struct {
	Requests         int
	BodySize         int
	LossRates        []float64
	OneWayDelay      time.Duration
	IncludePipelined bool
}

// DefaultMatrixConfig is the standard sweep used by the CLI and UI.
func DefaultMatrixConfig() MatrixConfig {
	return MatrixConfig{
		Requests:         20,
		BodySize:         64,
		LossRates:        []float64{0, 0.05, 0.15},
		OneWayDelay:      500 * time.Microsecond,
		IncludePipelined: true,
	}
}

// RunMatrix sweeps write pattern × interaction × option combos × loss.
func RunMatrix(cfg MatrixConfig) []MatrixCell {
	patterns := []app.WritePattern{app.WriteSplit, app.WriteMerged, app.WriteCorked}
	modes := []app.Interaction{app.StopAndWait}
	if cfg.IncludePipelined {
		modes = append(modes, app.Pipeline)
	}
	type optCombo struct {
		name          string
		clientNoDelay bool
		serverQuick   bool
		serverNoDelay bool
	}
	combos := []optCombo{
		{name: "baseline"},
		{name: "client-nodelay", clientNoDelay: true},
		{name: "both-nodelay", clientNoDelay: true, serverNoDelay: true},
		{name: "server-quickack", serverQuick: true},
	}

	var cells []MatrixCell
	for _, loss := range cfg.LossRates {
		for _, pat := range patterns {
			for _, mode := range modes {
				for _, combo := range combos {
					cell := MatrixCell{
						ClientPattern: pat,
						ClientMode:    mode,
						ClientNoDelay: combo.clientNoDelay,
						ServerQuick:   combo.serverQuick,
						ServerNoDelay: combo.serverNoDelay,
						LossRate:      loss,
					}
					runMatrixCell(cfg, &cell, pat, mode, combo, loss)
					cells = append(cells, cell)
				}
			}
		}
	}
	return cells
}

func runMatrixCell(cfg MatrixConfig, cell *MatrixCell, pat app.WritePattern,
	mode app.Interaction, combo struct {
		name          string
		clientNoDelay bool
		serverQuick   bool
		serverNoDelay bool
	}, loss float64) {

	clientOpts := transport.DefaultOptions()
	if combo.clientNoDelay {
		clientOpts = transport.NoDelayOptions()
	}
	serverOpts := transport.DefaultOptions()
	if combo.serverQuick {
		serverOpts = transport.QuickAckOptions()
	}
	if combo.serverNoDelay {
		serverOpts.NoDelay = true
	}

	s := Scenario{
		Name:       "matrix",
		ClientOpts: clientOpts,
		ServerOpts: serverOpts,
		Client: app.ClientConfig{
			Pattern: pat, Mode: mode, Requests: cfg.Requests, BodySize: cfg.BodySize,
		},
		Server: baseServer(),
		Link: LinkSpec{
			OneWayDelay: cfg.OneWayDelay,
			LossRate:    loss,
			Seed:        1234,
		},
		MaxSimTime: 180 * time.Second,
	}
	res := Run(s)
	d := res.Diagnosis

	cell.Completed = res.Completed
	cell.SampleCount = res.Summary.Count
	cell.P50, cell.P90, cell.P99, cell.Max = res.Summary.P50, res.Summary.P90, res.Summary.P99, res.Summary.Max
	cell.TailFraction = res.Summary.TailFraction
	cell.PrimaryType = d.PrimaryType
	cell.Severity = d.Severity
	cell.Dropped = res.Dropped
	for _, w := range d.Waits {
		switch w.Type {
		case diagnose.WaitNagleDelayedACK:
			cell.NagleStalls++
		case diagnose.WaitDelayedACK:
			cell.DelackOnly++
		case diagnose.WaitRTO:
			cell.RTOs++
		case diagnose.WaitFastRetransmit:
			cell.FastRetrans++
		}
	}
}
