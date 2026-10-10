package main

import "sync"

// Hub рассылает обновления статусов всем подписчикам (fan-out через каналы).
type Hub struct {
	mu   sync.Mutex
	subs map[chan Status]struct{}
}

func NewHub() *Hub {
	return &Hub{subs: make(map[chan Status]struct{})}
}

// Subscribe возвращает канал с обновлениями и функцию отписки.
func (h *Hub) Subscribe() (<-chan Status, func()) {
	ch := make(chan Status, 16)
	h.mu.Lock()
	h.subs[ch] = struct{}{}
	h.mu.Unlock()

	cancel := func() {
		h.mu.Lock()
		defer h.mu.Unlock()
		if _, ok := h.subs[ch]; ok {
			delete(h.subs, ch)
			close(ch)
		}
	}
	return ch, cancel
}

// Publish не блокируется: медленный подписчик просто пропускает обновление.
func (h *Hub) Publish(s Status) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for ch := range h.subs {
		select {
		case ch <- s:
		default:
		}
	}
}
