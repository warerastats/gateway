package scraper

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	flushInterval    = 100 * time.Millisecond
	maxBatchSize     = 50
	earlyFlushThresh = 150
	rateLimitPerMin  = 200
)

// BatchResult is delivered to the waiter for a single pending call.
//
// On a per-call success or per-call tRPC error envelope, Body holds the raw
// JSON element from the upstream response array (e.g.
// {"result":{"data":{"json":...}}} or {"error":{...}}) and Err is nil.
//
// On a whole-batch failure (transport error, non-2xx response, malformed body),
// Err is non-nil for every waiter in that batch.
type BatchResult struct {
	Body json.RawMessage
	Err  error
}

type pendingCall struct {
	method   string
	input    map[string]any
	prio     int
	seq      uint64
	resultCh chan BatchResult
}

type Batcher struct {
	s *Scraper

	mu    sync.Mutex
	queue []*pendingCall
	seq   uint64

	flushCh chan struct{}

	// One token bucket per API key. Each key gets its own rateLimitPerMin
	// budget so multiple keys multiply the upstream throughput.
	buckets []*tokenBucket
	nextKey int // round-robin cursor into buckets / s.apiKeys
}

func NewBatcher(s *Scraper) *Batcher {
	buckets := make([]*tokenBucket, len(s.apiKeys))
	for i := range buckets {
		buckets[i] = newTokenBucket(rateLimitPerMin, float64(rateLimitPerMin)/60.0)
	}
	b := &Batcher{
		s:       s,
		flushCh: make(chan struct{}, 1),
		buckets: buckets,
	}
	go b.loop()
	return b
}

// Add enqueues a pending call and returns a channel that will receive exactly
// one BatchResult once the upstream batch resolves (or fails).
func (b *Batcher) Add(method string, input map[string]any, prio int) <-chan BatchResult {
	ch := make(chan BatchResult, 1)

	b.mu.Lock()
	b.seq++
	b.queue = append(b.queue, &pendingCall{
		method:   method,
		input:    input,
		prio:     prio,
		seq:      b.seq,
		resultCh: ch,
	})
	// Filler calls (prio < 0) never count toward the early-flush threshold;
	// they only top up batches that flush for other reasons.
	nonFiller := 0
	for _, p := range b.queue {
		if p.prio >= 0 {
			nonFiller++
		}
	}
	overflow := nonFiller >= earlyFlushThresh
	b.mu.Unlock()

	if overflow {
		select {
		case b.flushCh <- struct{}{}:
		default:
		}
	}

	return ch
}

func (b *Batcher) loop() {
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			b.flush()
		case <-b.flushCh:
			b.flush()
		}
	}
}

func (b *Batcher) flush() {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Dispatch as many batches as we have keys with available tokens, so
	// multi-key configurations actually get parallel throughput.
	for {
		if len(b.queue) == 0 {
			return
		}

		// Sort: priority desc, seq asc
		sort.SliceStable(b.queue, func(i, j int) bool {
			if b.queue[i].prio != b.queue[j].prio {
				return b.queue[i].prio > b.queue[j].prio
			}
			return b.queue[i].seq < b.queue[j].seq
		})

		// Filler-only queues never trigger an upstream call; they only top up
		// batches that already have non-filler work to do.
		hasNonFiller := false
		for _, p := range b.queue {
			if p.prio >= 0 {
				hasNonFiller = true
				break
			}
		}
		if !hasNonFiller {
			return
		}

		keyIdx := b.pickKey()
		if keyIdx < 0 {
			// No key has an upstream slot right now; leave the queue for next tick.
			return
		}

		n := len(b.queue)
		if n > maxBatchSize {
			n = maxBatchSize
		}

		batch := make([]*pendingCall, n)
		copy(batch, b.queue[:n])
		b.queue = b.queue[n:]

		go b.executePending(batch, b.s.apiKeys[keyIdx])
	}
}

// pickKey returns the index of an API key whose bucket had a free token (now
// consumed), advancing the round-robin cursor. Returns -1 if every key is
// currently rate-limited.
func (b *Batcher) pickKey() int {
	n := len(b.buckets)
	for i := 0; i < n; i++ {
		idx := (b.nextKey + i) % n
		if b.buckets[idx].tryConsume(1) {
			b.nextKey = (idx + 1) % n
			return idx
		}
	}
	return -1
}

func (b *Batcher) executePending(pending []*pendingCall, apiKey string) {
	methods := make([]string, 0, len(pending))
	bodyMap := make(map[string]map[string]any, len(pending))
	for i, p := range pending {
		methods = append(methods, p.method)
		bodyMap[strconv.Itoa(i)] = p.input
	}

	reqURL := b.s.baseURL + strings.Join(methods, ",") + "?batch=1"

	bodyBytes, err := json.Marshal(bodyMap)
	if err != nil {
		deliverErr(pending, fmt.Errorf("marshal request body: %w", err))
		return
	}

	req, err := http.NewRequest(http.MethodPost, reqURL, bytes.NewReader(bodyBytes))
	if err != nil {
		deliverErr(pending, fmt.Errorf("build request: %w", err))
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Api-Key", apiKey)

	resp, err := b.s.client.Do(req)
	if err != nil {
		deliverErr(pending, fmt.Errorf("upstream request: %w", err))
		return
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		deliverErr(pending, fmt.Errorf("read upstream body: %w", err))
		return
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		slog.Error("upstream non-2xx", "status", resp.StatusCode, "body", truncate(respBody, 512))
		deliverErr(pending, fmt.Errorf("upstream status %d", resp.StatusCode))
		return
	}

	var elements []json.RawMessage
	err = json.Unmarshal(respBody, &elements)
	if err != nil {
		slog.Error("upstream malformed body", "error", err, "body", truncate(respBody, 512))
		deliverErr(pending, fmt.Errorf("decode upstream body: %w", err))
		return
	}

	slog.Info("flushed batch", "n", len(pending), "status", resp.StatusCode)

	for i, p := range pending {
		if i >= len(elements) {
			p.resultCh <- BatchResult{Err: fmt.Errorf("upstream returned %d elements, expected %d", len(elements), len(pending))}
			continue
		}
		p.resultCh <- BatchResult{Body: elements[i]}
	}
}

func deliverErr(pending []*pendingCall, err error) {
	for _, p := range pending {
		p.resultCh <- BatchResult{Err: err}
	}
}

func truncate(b []byte, n int) string {
	if len(b) <= n {
		return string(b)
	}
	return string(b[:n]) + "..."
}

// tokenBucket is a simple lazy-refill bucket.
type tokenBucket struct {
	mu        sync.Mutex
	capacity  float64
	refill    float64
	tokens    float64
	lastCheck time.Time
}

func newTokenBucket(capacity int, refillPerSec float64) *tokenBucket {
	return &tokenBucket{
		capacity:  float64(capacity),
		refill:    refillPerSec,
		tokens:    float64(capacity),
		lastCheck: time.Now(),
	}
}

func (tb *tokenBucket) tryConsume(n float64) bool {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	tb.refillLocked()

	if tb.tokens < n {
		return false
	}
	tb.tokens -= n
	return true
}

func (tb *tokenBucket) refillLocked() {
	now := time.Now()
	elapsed := now.Sub(tb.lastCheck).Seconds()
	tb.lastCheck = now
	tb.tokens += elapsed * tb.refill
	if tb.tokens > tb.capacity {
		tb.tokens = tb.capacity
	}
}
