package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net/http"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	addr := flag.String("addr", ":8080", "адрес HTTP-сервера")
	interval := flag.Duration("interval", 10*time.Second, "интервал проверок")
	workers := flag.Int("workers", 4, "число воркеров")
	timeout := flag.Duration("timeout", 5*time.Second, "таймаут одной проверки")
	flag.Parse()

	targets := flag.Args()
	if len(targets) == 0 {
		targets = []string{"https://go.dev", "https://github.com", "https://example.com"}
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	mon := NewMonitor(targets, *interval, *workers, *timeout)
	monDone := make(chan struct{})
	go func() {
		defer close(monDone)
		mon.Run(ctx)
	}()

	srv := &http.Server{
		Addr:              *addr,
		Handler:           NewHandler(mon, ctx.Done()),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		log.Printf("watching %d targets, dashboard on %s", len(targets), *addr)
		if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("server: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("shutting down...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("shutdown: %v", err)
	}
	<-monDone
	log.Println("bye")
}
