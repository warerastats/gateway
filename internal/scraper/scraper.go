package scraper

import (
	"log/slog"
	"net/http"
	"os"
	"strings"
)

type Scraper struct {
	client  http.Client
	apiKeys []string
	Batcher *Batcher
	baseURL string
}

func NewScraper() *Scraper {
	raw := os.Getenv("WARERA_API_KEY")
	if raw == "" {
		slog.Error("No war era API key in environment variables!")
		os.Exit(1)
	}

	parts := strings.Split(raw, ",")
	keys := make([]string, 0, len(parts))
	for _, p := range parts {
		k := strings.TrimSpace(p)
		if k != "" {
			keys = append(keys, k)
		}
	}
	if len(keys) == 0 {
		slog.Error("WARERA_API_KEY did not contain any non-empty keys!")
		os.Exit(1)
	}

	slog.Info("loaded warera api keys", "count", len(keys))

	s := &Scraper{
		client:  http.Client{},
		apiKeys: keys,
		baseURL: "https://api2.warera.io/trpc/",
	}
	s.Batcher = NewBatcher(s)
	return s
}
