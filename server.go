package main

import (
	"encoding/json"
	"fmt"
	"net/http"
)

// NewHandler собирает маршруты. done закрывается при остановке приложения,
// чтобы долгоживущие SSE-соединения не мешали graceful shutdown.
func NewHandler(m *Monitor, done <-chan struct{}) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		fmt.Fprint(w, indexHTML)
	})

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	})

	mux.HandleFunc("GET /api/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(m.Snapshot())
	})

	// Server-Sent Events: каждое обновление статуса приходит как отдельное событие.
	mux.HandleFunc("GET /api/events", func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming unsupported", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")

		// Сначала подписываемся, потом берем снимок, чтобы не потерять обновления.
		updates, cancel := m.hub.Subscribe()
		defer cancel()

		for _, s := range m.Snapshot() {
			writeEvent(w, s)
		}
		flusher.Flush()

		for {
			select {
			case s, ok := <-updates:
				if !ok {
					return
				}
				writeEvent(w, s)
				flusher.Flush()
			case <-r.Context().Done():
				return
			case <-done:
				return
			}
		}
	})

	return mux
}

func writeEvent(w http.ResponseWriter, s Status) {
	b, _ := json.Marshal(s)
	fmt.Fprintf(w, "data: %s\n\n", b)
}

const indexHTML = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>go-uptime</title>
<style>
  body { font-family: system-ui, sans-serif; max-width: 800px; margin: 2rem auto; padding: 0 1rem; }
  table { width: 100%; border-collapse: collapse; }
  th, td { text-align: left; padding: .5rem; border-bottom: 1px solid #ddd; }
  .up { color: #0a7d2c; font-weight: 600; }
  .down { color: #c0271d; font-weight: 600; }
</style>
</head>
<body>
<h1>go-uptime</h1>
<table>
  <thead><tr><th>URL</th><th>State</th><th>Code</th><th>Latency</th><th>Checks</th><th>Failures</th></tr></thead>
  <tbody id="rows"></tbody>
</table>
<script>
  var rows = {};
  function render(s) {
    var tr = rows[s.url];
    if (!tr) {
      tr = document.createElement('tr');
      for (var i = 0; i < 6; i++) tr.appendChild(document.createElement('td'));
      document.getElementById('rows').appendChild(tr);
      rows[s.url] = tr;
    }
    var c = tr.children;
    c[0].textContent = s.url;
    c[1].textContent = s.checks === 0 ? '...' : (s.up ? 'UP' : 'DOWN');
    c[1].className = s.checks === 0 ? '' : (s.up ? 'up' : 'down');
    c[2].textContent = s.status_code || '-';
    c[3].textContent = s.latency_ms + ' ms';
    c[4].textContent = s.checks;
    c[5].textContent = s.failures;
  }
  fetch('/api/status').then(function (r) { return r.json(); })
    .then(function (list) { list.forEach(render); });
  new EventSource('/api/events').onmessage = function (e) { render(JSON.parse(e.data)); };
</script>
</body>
</html>`
