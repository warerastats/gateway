package scraper

import (
	"log/slog"
	"net/http"
	"os"
)

type Scraper struct {
	client  http.Client
	apiKey  string
	Batcher *Batcher
	baseURL string
}

func NewScraper() *Scraper {
	apiKey := os.Getenv("WARERA_API_KEY")
	if apiKey == "" {
		slog.Error("No war era API key in environment variables!")
		os.Exit(1)
	}

	s := &Scraper{
		client:  http.Client{},
		apiKey:  apiKey,
		baseURL: "https://api2.warera.io/trpc/",
	}
	s.Batcher = NewBatcher(s)
	return s
}
