package main

import (
	"embed"
	"encoding/json"
	"net/http"

	"nagletrace/diagnose"
	"nagletrace/experiment"
)

//go:embed static
var staticFS embed.FS

func serve(addr string) error {
	mux := http.NewServeMux()

	mux.Handle("GET /static/", http.FileServerFS(staticFS))
	mux.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		data, err := staticFS.ReadFile("static/index.html")
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(data)
	})

	mux.HandleFunc("GET /api/presets", func(w http.ResponseWriter, r *http.Request) {
		type preset struct {
			ID   string `json:"id"`
			Name string `json:"name"`
		}
		var out []preset
		for _, id := range experiment.AllPresets() {
			out = append(out, preset{ID: id, Name: experiment.Preset(id).Name})
		}
		writeJSON(w, out)
	})

	mux.HandleFunc("GET /api/run", func(w http.ResponseWriter, r *http.Request) {
		name := r.URL.Query().Get("preset")
		if name == "" {
			name = experiment.PresetClassicStall
		}
		res := experiment.Run(experiment.Preset(name))
		d := diagnose.Analyze(res.Trace)
		writeJSON(w, map[string]any{
			"scenario":  experiment.Preset(name).Name,
			"id":        name,
			"completed": res.Completed,
			"summary":   res.Summary,
			"diagnosis": d,
			"trace":     res.Trace.Events,
			"net":       res.NetStats,
			"dropped":   res.Dropped,
		})
	})

	mux.HandleFunc("GET /api/matrix", func(w http.ResponseWriter, r *http.Request) {
		cells := experiment.RunMatrix(experiment.DefaultMatrixConfig())
		writeJSON(w, cells)
	})

	return http.ListenAndServe(addr, mux)
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	if err := enc.Encode(v); err != nil {
		http.Error(w, err.Error(), 500)
	}
}
