package main

import (
	"context"
	"net/http"
	"sort"
	"sync"
	"time"
)

// Result - итог одной проверки, его воркер отправляет в канал results.
type Result struct {
	URL       string
	StatusCode int
	Latency   time.Duration
	Err       string
	CheckedAt time.Time
}

// Status - накопленное состояние одного сайта.
type Status struct {
	URL         string    `json:"url"`
	Up          bool      `json:"up"`
	StatusCode  int       `json:"status_code"`
	LatencyMS   int64     `json:"latency_ms"`
	Error       string    `json:"error,omitempty"`
	LastChecked time.Time `json:"last_checked"`
	Checks      int       `json:"checks"`
	Failures    int       `json:"failures"`
}

// Monitor периодически проверяет список адресов пулом воркеров.
//
// Поток данных:
//
//	scheduler --jobs--> [worker x N] --results--> aggregator --> state + hub
type Monitor struct {
	targets  []string
	interval time.Duration
	workers  int
	client   *http.Client
	hub      *Hub

	mu    sync.RWMutex
	state map[string]*Status
}

func NewMonitor(targets []string, interval time.Duration, workers int, timeout time.Duration) *Monitor {
	if workers < 1 {
		workers = 1
	}
	m := &Monitor{
		targets:  targets,
		interval: interval,
		workers:  workers,
		client:   &http.Client{Timeout: timeout},
		hub:      NewHub(),
		state:    make(map[string]*Status, len(targets)),
	}
	for _, t := range targets {
		m.state[t] = &Status{URL: t}
	}
	return m
}

// Run блокируется, пока не отменен ctx и все горутины не завершились.
func (m *Monitor) Run(ctx context.Context) {
	jobs := make(chan string, len(m.targets))
	results := make(chan Result, len(m.targets))

	var wg sync.WaitGroup
	for i := 0; i < m.workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for url := range jobs {
				results <- m.check(ctx, url)
			}
		}()
	}

	go m.schedule(ctx, jobs)

	// Когда все воркеры вышли, закрываем results, и агрегатор завершается.
	go func() {
		wg.Wait()
		close(results)
	}()

	for r := range results {
		m.record(r)
	}
}

// schedule раз в interval кладет все адреса в канал jobs.
// При отмене ctx закрывает jobs, и воркеры корректно завершаются.
func (m *Monitor) schedule(ctx context.Context, jobs chan<- string) {
	defer close(jobs)
	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()

	for {
		for _, t := range m.targets {
			select {
			case jobs <- t:
			case <-ctx.Done():
				return
			}
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			return
		}
	}
}

func (m *Monitor) check(ctx context.Context, url string) Result {
	res := Result{URL: url, CheckedAt: time.Now().UTC()}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		res.Err = err.Error()
		return res
	}
	start := time.Now()
	resp, err := m.client.Do(req)
	res.Latency = time.Since(start)
	if err != nil {
		res.Err = err.Error()
		return res
	}
	defer resp.Body.Close()
	res.StatusCode = resp.StatusCode
	return res
}

// record обновляет состояние и рассылает обновление подписчикам.
func (m *Monitor) record(r Result) {
	m.mu.Lock()
	s := m.state[r.URL]
	s.Checks++
	s.StatusCode = r.StatusCode
	s.LatencyMS = r.Latency.Milliseconds()
	s.LastChecked = r.CheckedAt
	s.Error = r.Err
	s.Up = r.Err == "" && r.StatusCode >= 200 && r.StatusCode < 400
	if !s.Up {
		s.Failures++
	}
	snapshot := *s
	m.mu.Unlock()

	m.hub.Publish(snapshot)
}

// Snapshot возвращает копию текущего состояния, отсортированную по URL.
func (m *Monitor) Snapshot() []Status {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]Status, 0, len(m.state))
	for _, s := range m.state {
		out = append(out, *s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].URL < out[j].URL })
	return out
}
