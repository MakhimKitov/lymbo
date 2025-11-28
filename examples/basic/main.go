package main

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/ochaton/lymbo"
	"github.com/ochaton/lymbo/store"
)

func main() {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{}))
	slog.SetDefault(logger)

	kh := lymbo.NewKharon(store.NewMemoryStore(), lymbo.Settings{EnableExpiration: true}, logger)

	r := lymbo.NewRouter()
	r.HandleFunc("example", func(ctx context.Context, t lymbo.Ticket) error {
		slog.InfoContext(ctx, "processing ticket", "ticket_id", t.ID, "payload", t.Payload, "runat", t.Runat)
		time.Sleep(100 * time.Millisecond) // Simulate work
		return kh.Done(t.ID, lymbo.WithExpireIn(10*time.Second))
	})

	r.NotFoundFunc(func(ctx context.Context, t lymbo.Ticket) error {
		slog.WarnContext(ctx, "unknown ticket type", "ticket_id", t.ID, "ticket_type", t.Type)
		return kh.Fail(t.ID, lymbo.WithErrorReason("unsupported ticket type"))
	})

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Run kharon in a goroutine
	go func() {
		if err := kh.Run(ctx, r); err != nil {
			slog.ErrorContext(ctx, "kharon failed", "error", err)
			os.Exit(1)
		}
	}()

	mux := http.NewServeMux()
	mux.Handle("GET /ticket/{id}", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Extract ticket ID from URL
		id := r.PathValue("id")

		if id == "" {
			http.Error(w, "ticket ID is required", http.StatusBadRequest)
			return
		}

		// Fetch ticket from store
		ticket, err := kh.Get(lymbo.TicketId(id))
		switch err {
		case nil:
			// Write ticket details to response
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			err := json.NewEncoder(w).Encode(ticket)
			if err != nil {
				slog.ErrorContext(ctx, "failed to encode ticket", "error", err)
				http.Error(w, "failed to encode ticket", http.StatusInternalServerError)
			}
			return
		case lymbo.ErrTicketNotFound:
			http.Error(w, "ticket not found", http.StatusNotFound)
			return
		default:
			slog.ErrorContext(ctx, "failed to get ticket", "error", err)
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}
	}))

	// Cancel?
	mux.Handle("POST /ticket/{id}/cancel", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Extract ticket ID from URL
		id := r.PathValue("id")
		if id == "" {
			http.Error(w, "ticket ID is required", http.StatusBadRequest)
			return
		}

		// Cancel ticket in store
		err := kh.Cancel(lymbo.TicketId(id), lymbo.WithErrorReason("cancelled via api"))
		switch err {
		case nil:
			w.WriteHeader(http.StatusNoContent)
			return
		case lymbo.ErrTicketNotFound:
			http.Error(w, "ticket not found", http.StatusNotFound)
			return
		default:
			slog.ErrorContext(ctx, "failed to cancel ticket", "error", err)
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}
	}))

	mux.Handle("DELETE /ticket/{id}", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Extract ticket ID from URL
		id := r.PathValue("id")
		if id == "" {
			http.Error(w, "ticket ID is required", http.StatusBadRequest)
			return
		}

		// Delete ticket from store
		err := kh.Delete(lymbo.TicketId(id))
		switch err {
		case nil, lymbo.ErrTicketNotFound:
			w.WriteHeader(http.StatusNoContent)
			return
		default:
			slog.ErrorContext(ctx, "failed to delete ticket", "error", err)
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}
	}))

	mux.Handle("POST /ticket/{type}/{id}", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// ?nice={nice}&delay={delay}
		// Extract ticket ID from URL
		id := r.PathValue("id")
		if id == "" {
			http.Error(w, "ticket ID is required", http.StatusBadRequest)
			return
		}

		typ := r.PathValue("type")
		if typ == "" {
			http.Error(w, "ticket type is required", http.StatusBadRequest)
			return
		}

		var nice *int
		if n := r.URL.Query().Get("nice"); n != "" {
			// Convert nice to int
			nx, err := strconv.Atoi(n)
			if err != nil {
				http.Error(w, "invalid nice value", http.StatusBadRequest)
				return
			}
			nice = &nx
		}

		var delay *float64
		if d := r.URL.Query().Get("delay"); d != "" {
			// Convert delay to int
			dx, err := strconv.ParseFloat(d, 64)
			if err != nil {
				http.Error(w, "invalid delay value", http.StatusBadRequest)
				return
			}
			if dx < 0 {
				http.Error(w, "delay must be non-negative", http.StatusBadRequest)
				return
			}
			delay = &dx
		}

		payload, err := io.ReadAll(r.Body)
		defer r.Body.Close()
		if err != nil {
			http.Error(w, "failed to read request body", http.StatusBadRequest)
			return
		}

		// Create new ticket
		ticket, err := lymbo.NewTicket(lymbo.TicketId(id), typ)
		if err != nil {
			slog.ErrorContext(ctx, "failed to create ticket", "error", err)
			http.Error(w, "failed to create ticket", http.StatusInternalServerError)
			return
		}
		ticket = ticket.WithPayload(payload)
		if nice != nil {
			ticket = ticket.WithNice(*nice)
		}
		if delay != nil {
			d := time.Now().Add(time.Duration(*delay * float64(time.Second)))
			ticket = ticket.WithRunat(d)
		}

		// Submit ticket to kharon
		if err := kh.Add(*ticket); err != nil {
			slog.ErrorContext(ctx, "failed to add ticket", "error", err)
			http.Error(w, "failed to add ticket", http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusAccepted)
	}))

	// Start simple http server:
	server := &http.Server{
		Addr:    "127.0.0.1:8080",
		Handler: mux,
	}

	// Register sigterm, sigint:
	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, syscall.SIGINT, syscall.SIGTERM)

	// Start server in goroutine
	serverErr := make(chan error, 1)
	go func() {
		slog.InfoContext(ctx, "starting HTTP server", "addr", server.Addr)
		serverErr <- server.ListenAndServe()
	}()

	select {
	case err := <-serverErr:
		if err != nil {
			slog.Error("server error", "error", err)
		}
	case sig := <-shutdown:
		slog.InfoContext(ctx, "Shutdown signal received", slog.String("signal", sig.String()))

		// Stop background services
		cancel()

		// Give outstanding requests time to complete (30 seconds)
		shutdownCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()

		if err := server.Shutdown(shutdownCtx); err != nil {
			slog.ErrorContext(ctx, "HTTP server shutdown error", slog.Any("error", err))
			// Force close if graceful shutdown fails
			_ = server.Close()
		}

		slog.InfoContext(ctx, "HTTP server stopped gracefully")
	}
}
