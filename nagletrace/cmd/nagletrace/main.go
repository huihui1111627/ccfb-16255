// Command nagletrace reproduces and diagnoses TCP Nagle / delayed-ACK
// interactions on a deterministic user-space transport. Subcommands:
//
//	nagletrace run [preset]      run one scenario, print the verdict
//	nagletrace matrix            sweep the option/workload matrix
//	nagletrace serve [addr]      web UI with packet timeline diagrams
//	nagletrace list              list built-in presets
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"nagletrace/diagnose"
	"nagletrace/experiment"
)

func main() {
	args := os.Args[1:]
	cmd := "run"
	if len(args) > 0 {
		cmd = args[0]
		args = args[1:]
	}
	switch cmd {
	case "list":
		for _, name := range experiment.AllPresets() {
			fmt.Printf("%-18s %s\n", name, experiment.Preset(name).Name)
		}
	case "run":
		name := experiment.PresetClassicStall
		asJSON := false
		for _, a := range args {
			if a == "--json" {
				asJSON = true
			} else {
				name = a
			}
		}
		runOne(name, asJSON)
	case "matrix":
		runMatrix()
	case "serve":
		addr := ":8080"
		if len(args) > 0 {
			addr = args[0]
		}
		if err := serve(addr); err != nil {
			fatal(err)
		}
	case "-h", "--help", "help":
		usage()
	default:
		runOne(cmd, false)
	}
}

func runOne(name string, asJSON bool) {
	res := experiment.Run(experiment.Preset(name))
	d := diagnose.Analyze(res.Trace)
	if asJSON {
		out := map[string]any{
			"scenario":  name,
			"completed": res.Completed,
			"summary":   res.Summary,
			"diagnosis": d,
			"trace":     res.Trace.Events,
			"net":       res.NetStats,
		}
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(out); err != nil {
			fatal(err)
		}
		return
	}
	fmt.Printf("scenario : %s\n", experiment.Preset(name).Name)
	fmt.Printf("completed: %v   samples: %d   sim time: %v\n\n",
		res.Completed, res.Summary.Count, res.SimTime)
	fmt.Printf("latency  : min=%s p50=%s p90=%s p99=%s max=%s  tail>=40ms: %d (%.0f%%)\n\n",
		res.Summary.Min, res.Summary.P50, res.Summary.P90, res.Summary.P99,
		res.Summary.Max, res.Summary.TailCount, 100*res.Summary.TailFraction)
	fmt.Println(d.Markdown())
	fmt.Println("packet timeline (first 25 wire events):")
	shown := 0
	for _, ev := range res.Trace.Packets() {
		if shown >= 25 {
			fmt.Printf("... %d more events\n", len(res.Trace.Packets())-shown)
			break
		}
		fmt.Printf("  %10v %-6s %-4s seq=%-6d ack=%-6d %s len=%-4d%s\n",
			ev.Time, ev.Endpoint, ev.Direction, ev.Seq, ev.Ack,
			ev.Flags, ev.PayloadLen, note(ev.Detail))
		shown++
	}
}

func note(s string) string {
	if s == "" {
		return ""
	}
	return "  ; " + s
}

func runMatrix() {
	cells := experiment.RunMatrix(experiment.DefaultMatrixConfig())
	fmt.Printf("%-12s %-16s %-14s %-8s %7s %9s %9s %9s %7s %6s %6s %6s %6s\n",
		"loss", "pattern", "mode", "options", "ok", "p50", "p90", "p99", "max", "nagle", "delack", "rto", "fast")
	for _, c := range cells {
		opts := "baseline"
		switch {
		case c.ServerQuick:
			opts = "srv-quickack"
		case c.ClientNoDelay && c.ServerNoDelay:
			opts = "both-nodelay"
		case c.ClientNoDelay:
			opts = "cli-nodelay"
		}
		fmt.Printf("%-12.2f %-16s %-14s %-14s %7t %9s %9s %9s %7s %6d %6d %6d %6d\n",
			c.LossRate, c.ClientPattern, c.ClientMode, opts, c.Completed,
			c.P50.Round(time.Microsecond), c.P90.Round(time.Microsecond),
			c.P99.Round(time.Microsecond), c.Max.Round(time.Microsecond),
			c.NagleStalls, c.DelackOnly, c.RTOs, c.FastRetrans)
	}
}

func usage() {
	fmt.Println(`nagletrace — reproduce and diagnose Nagle / delayed-ACK stalls

usage:
  nagletrace list                 list built-in scenarios
  nagletrace run [preset]         run one scenario and print the diagnosis
  nagletrace run [preset] --json  machine-readable output incl. full trace
  nagletrace matrix               sweep write-pattern × option × loss combos
  nagletrace serve [addr]         web UI (default :8080)`)
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "error:", err)
	os.Exit(1)
}
