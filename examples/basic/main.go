package main

import (
	"context"
	"database/sql"
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

	_ "github.com/jackc/pgx/v5/stdlib"
)

func main() {
	ctx := context.Background()

	logger := setupLogger()
	settings := lymbo.DefaultSettings().
		WithExpiration().
		WithProcessTime(3 * time.Second).
		WithWorkers(5).
		WithBatchSize(5)

	var kh *lymbo.Kharon
	dbtype := os.Getenv("DB_TYPE")
	if dbtype == "" {
		dbtype = "memory"
	}
	switch dbtype {
	case "memory":
		slog.InfoContext(ctx, "using in-memory storage")
		kh = lymbo.NewKharon(store.NewMemoryStore(), settings, logger)
	default:
		pg, err := sql.Open(dbtype, os.Getenv("DB_DSN"))
		if err != nil {
			slog.ErrorContext(ctx, "failed to connect to database", "error", err)
			os.Exit(1)
		}
		kh = lymbo.NewKharon(store.NewPostgresStore(pg), settings, logger)
	}

	r := lymbo.NewRouter()
	r.HandleFunc("example", func(ctx context.Context, t lymbo.Ticket) error {
		slog.InfoContext(ctx, "processing ticket", "ticket_id", t.ID, "payload", t.Payload, "runat", t.Runat)
		time.Sleep(100 * time.Millisecond) // Simulate work
		return kh.Done(ctx, t.ID, lymbo.WithExpireIn(10*time.Second))
	})

	r.NotFoundFunc(func(ctx context.Context, t lymbo.Ticket) error {
		slog.WarnContext(ctx, "unknown ticket type", "ticket_id", t.ID, "ticket_type", t.Type)
		return kh.Fail(ctx, t.ID, lymbo.WithErrorReason("unsupported ticket type"))
	})

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	go func() {
		if err := kh.Run(ctx, r); err != nil {
			slog.ErrorContext(ctx, "kharon failed", "error", err)
			os.Exit(1)
		}
	}()

	mux := http.NewServeMux()

	mux.Handle("GET /ticket/{id}", handleGetTicket(ctx, kh))
	mux.Handle("POST /ticket/{id}/cancel", handleCancelTicket(ctx, kh))
	mux.Handle("DELETE /ticket/{id}", handleDeleteTicket(ctx, kh))
	mux.Handle("POST /ticket/{type}/{id}", handleCreateTicket(ctx, kh))

	server := &http.Server{
		Addr:    "127.0.0.1:8080",
		Handler: mux,
	}

	runServer(ctx, server, cancel)
}

func setupLogger() *slog.Logger {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{}))
	slog.SetDefault(logger)
	return logger
}

func handleGetTicket(ctx context.Context, kh *lymbo.Kharon) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		if id == "" {
			http.Error(w, "ticket ID is required", http.StatusBadRequest)
			return
		}

		ticket, err := kh.Get(r.Context(), lymbo.TicketId(id))
		switch err {
		case nil:
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			if err := json.NewEncoder(w).Encode(ticket); err != nil {
				slog.ErrorContext(ctx, "failed to encode ticket", "error", err)
				http.Error(w, "failed to encode ticket", http.StatusInternalServerError)
			}
		case lymbo.ErrTicketNotFound:
			http.Error(w, "ticket not found", http.StatusNotFound)
		case lymbo.ErrTicketIDInvalid:
			http.Error(w, "invalid ticket ID", http.StatusBadRequest)
		default:
			slog.ErrorContext(ctx, "failed to get ticket", "error", err)
			http.Error(w, "internal server error", http.StatusInternalServerError)
		}
	}
}

func handleCancelTicket(ctx context.Context, kh *lymbo.Kharon) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		if id == "" {
			http.Error(w, "ticket ID is required", http.StatusBadRequest)
			return
		}

		err := kh.Cancel(r.Context(), lymbo.TicketId(id), lymbo.WithErrorReason("cancelled via api"))
		switch err {
		case nil:
			w.WriteHeader(http.StatusNoContent)
		case lymbo.ErrTicketNotFound:
			http.Error(w, "ticket not found", http.StatusNotFound)
		default:
			slog.ErrorContext(ctx, "failed to cancel ticket", "error", err)
			http.Error(w, "internal server error", http.StatusInternalServerError)
		}
	}
}

func handleDeleteTicket(ctx context.Context, kh *lymbo.Kharon) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		if id == "" {
			http.Error(w, "ticket ID is required", http.StatusBadRequest)
			return
		}

		err := kh.Delete(r.Context(), lymbo.TicketId(id))
		switch err {
		case nil, lymbo.ErrTicketNotFound:
			w.WriteHeader(http.StatusNoContent)
		default:
			slog.ErrorContext(ctx, "failed to delete ticket", "error", err)
			http.Error(w, "internal server error", http.StatusInternalServerError)
		}
	}
}

func parseTicketParams(r *http.Request) (nice *int, delay *float64, err error) {
	if n := r.URL.Query().Get("nice"); n != "" {
		nx, err := strconv.Atoi(n)
		if err != nil {
			return nil, nil, err
		}
		nice = &nx
	}

	if d := r.URL.Query().Get("delay"); d != "" {
		dx, err := strconv.ParseFloat(d, 64)
		if err != nil {
			return nil, nil, err
		}
		if dx < 0 {
			return nil, nil, http.ErrAbortHandler
		}
		delay = &dx
	}

	return nice, delay, nil
}

func handleCreateTicket(ctx context.Context, kh *lymbo.Kharon) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
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

		nice, delay, err := parseTicketParams(r)
		if err != nil {
			http.Error(w, "invalid parameters", http.StatusBadRequest)
			return
		}

		payload, err := io.ReadAll(r.Body)
		defer r.Body.Close()
		if err != nil {
			http.Error(w, "failed to read request body", http.StatusBadRequest)
			return
		}

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

		err = kh.Add(r.Context(), *ticket)
		switch err {
		case nil:
			break
		case lymbo.ErrTicketIDInvalid:
			slog.ErrorContext(ctx, "invalid ticket id", "error", err, "id", id)
			http.Error(w, "invalid ticket ID", http.StatusBadRequest)
			return
		default:
			slog.ErrorContext(ctx, "failed to add ticket", "error", err)
			http.Error(w, "failed to add ticket", http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusAccepted)
	}
}

func runServer(ctx context.Context, server *http.Server, cancel context.CancelFunc) {
	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, syscall.SIGINT, syscall.SIGTERM)

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
		cancel()

		shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		if err := server.Shutdown(shutdownCtx); err != nil {
			slog.ErrorContext(ctx, "HTTP server shutdown error", slog.Any("error", err))
			_ = server.Close()
		}

		slog.InfoContext(ctx, "HTTP server stopped gracefully")
	}
}
