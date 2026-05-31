package handler

import (
	"encoding/json"
	"net/http"

	"github.com/warerastats/gateway/internal/scraper"
)

// RequestHandler enqueues the call onto the batcher and blocks until the
// upstream batch resolves or the request context is canceled.
//
// On per-call success, the inner `result.data.json` value is written to the
// caller as JSON. On a per-call tRPC error envelope, the raw upstream element
// is passed through with HTTP 200 (the caller already speaks tRPC). On a
// whole-batch failure, HTTP 502 is returned.
func RequestHandler(w http.ResponseWriter, r *http.Request, b *scraper.Batcher, method string, data map[string]any, priority int) {
	ch := b.Add(method, data, priority)

	select {
	case <-r.Context().Done():
		return
	case res := <-ch:
		if res.Err != nil {
			http.Error(w, res.Err.Error(), http.StatusBadGateway)
			return
		}

		// Try to peel the tRPC success envelope: {"result":{"data":{"json": <inner>}}}.
		var env struct {
			Result *struct {
				Data *struct {
					JSON json.RawMessage `json:"json"`
				} `json:"data"`
			} `json:"result"`
			Error json.RawMessage `json:"error"`
		}
		err := json.Unmarshal(res.Body, &env)
		if err == nil && env.Result != nil && env.Result.Data != nil && env.Result.Data.JSON != nil {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(env.Result.Data.JSON)
			return
		}

		// Either a tRPC error envelope or an unexpected shape.
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(res.Body)
	}
}
