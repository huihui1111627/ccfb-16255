// Package experiment wires the virtual clock, lossy network, two
// transport endpoints and the application workloads into one runnable
// scenario, and provides a matrix runner for sweeping option/workload
// combinations.
package experiment

import (
	"time"

	"nagletrace/app"
	"nagletrace/clock"
	"nagletrace/diagnose"
	"nagletrace/netem"
	"nagletrace/transport"
)

const (
	clientAddr = "10.0.0.1:40001"
	serverAddr = "10.0.0.2:8080"
)

// LinkSpec is the user-facing network description.
type LinkSpec struct {
	OneWayDelay time.Duration
	Jitter      time.Duration
	LossRate    float64
	Seed        int64
}

// Scenario is everything needed for one deterministic run.
type Scenario struct {
	Name          string
	ClientOpts    transport.Options
	ServerOpts    transport.Options
	Client        app.ClientConfig
	Server        app.ServerConfig
	Link          LinkSpec
	TailThreshold time.Duration
	MaxSimTime    time.Duration
}

// Result is the full output of one run.
type Result struct {
	ScenarioName string
	Trace        *transport.Trace
	Latency      *app.Latency
	Summary      app.Stats
	Diagnosis    *diagnose.Diagnosis
	NetStats     map[string]netem.DirStats
	Dropped      int64
	SimTime      time.Duration
	Completed    bool
}

// Run executes one scenario to completion (or until MaxSimTime).
func Run(s Scenario) *Result {
	clk := clock.New()
	tr := transport.NewTrace()
	net := netem.New(clk, s.Link.Seed)
	cfg := s.Link
	net.SetLink(clientAddr, serverAddr, netem.LinkConfig{
		Delay: cfg.OneWayDelay, Jitter: cfg.Jitter, LossRate: cfg.LossRate,
	})
	net.SetLink(serverAddr, clientAddr, netem.LinkConfig{
		Delay: cfg.OneWayDelay, Jitter: cfg.Jitter, LossRate: cfg.LossRate,
	})

	lat := &app.Latency{}
	clientApp := app.NewClient(clk, s.Client, lat, nil)

	ln := &transport.Listener{
		Addr:       serverAddr,
		Clk:        clk,
		Net:        net,
		Trace:      tr,
		ServerOpts: s.ServerOpts,
		AcceptFunc: func(*transport.Conn) transport.App {
			return app.NewServer(clk, s.Server, s.Client.BodySize)
		},
	}
	ln.Listen()

	dialer := &transport.Dialer{Clk: clk, Net: net, Trace: tr}
	if _, err := dialer.Dial(clientAddr, serverAddr, s.ClientOpts, clientApp); err != nil {
		panic(err)
	}

	maxTime := s.MaxSimTime
	if maxTime == 0 {
		maxTime = 10 * time.Second
	}
	completed := clk.Run(maxTime)

	tail := s.TailThreshold
	if tail == 0 {
		tail = s.ServerOpts.DelayAckTimeout - time.Millisecond
		if tail <= 0 {
			tail = 39 * time.Millisecond
		}
	}
	_, dropped := net.Totals()
	diag := diagnose.AnalyzeWithHint(tr, diagnose.Hint{MSS: s.ClientOpts.MSS})
	return &Result{
		ScenarioName: s.Name,
		Trace:        tr,
		Latency:      lat,
		Summary:      lat.Summarize(tail),
		Diagnosis:    diag,
		NetStats:     net.Stats(),
		Dropped:      dropped,
		SimTime:      clk.Now(),
		Completed:    completed && clientApp.Done(),
	}
}
