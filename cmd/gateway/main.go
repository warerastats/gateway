package main

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"
	"strings"

	"github.com/warerastats/gateway/internal/handler"
	"github.com/warerastats/gateway/internal/scraper"
)

var s *scraper.Scraper

func init() {
	s = scraper.NewScraper()
}

func main() {
	slog.Info("Starting gateway")

	http.HandleFunc("/", handle)

	slog.Info("Gateway listening on :8080")
	err := http.ListenAndServe(":8080", nil)

	if err != nil {
		slog.Error("Server closed!", "error", err)
	}
}

func handle(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	requestedPath := strings.TrimPrefix(r.URL.Path, "/")
	if !strings.ContainsAny(requestedPath, `.`) {
		http.Error(w, "invalid method", http.StatusBadRequest)
		return
	}

	var body map[string]any
	err := json.NewDecoder(r.Body).Decode(&body)
	r.Body.Close()
	if err != nil {
		http.Error(w, "invalid JSON body", http.StatusBadRequest)
		return
	}

	prioStr := r.Header.Get("X-Priority")
	prio := 0
	if prioStr != "" {
		v, err := strconv.Atoi(prioStr)
		if err == nil {
			prio = v
		}
	}

	handler.RequestHandler(w, r, s.Batcher, requestedPath, body, prio)
}
