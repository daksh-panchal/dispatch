package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/daksh-panchal/work-flow/internal/httpapi"
	"github.com/daksh-panchal/work-flow/internal/job"
	workflowpostgres "github.com/daksh-panchal/work-flow/internal/postgres"
	"github.com/daksh-panchal/work-flow/internal/worker"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	defaultAddress       = ":5080"
	defaultDatabaseURL   = "postgres://workflow:workflow_dev@localhost:5432/workflow?sslmode=disable"
	defaultLeaseDuration = 30 * time.Second
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	if err := run(logger); err != nil {
		logger.Error("work-flow stopped", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Connection pool manages reusable connections to db
	databaseURL := environmentOrDefault("DATABASE_URL", defaultDatabaseURL)
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil { // Error connecting to db
		return err
	}

	// Run migrations to ensure database schema is up to date
	if err := workflowpostgres.Migrate(ctx, pool); err != nil {
		return err
	}

	// Create the application components and start HTTP server
	jobRepository := workflowpostgres.NewJobRepository(pool)
	workerRepository := workflowpostgres.NewWorkerRepository(pool)
	jobService := job.NewService(jobRepository, time.Now)
	workerService := worker.NewService(workerRepository, jobRepository, time.Now, defaultLeaseDuration)

	server := &http.Server{
		Addr:              environmentOrDefault("HTTP_ADDR", defaultAddress),
		Handler:           httpapi.New(jobService, workerService, pool, logger),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	serverError := make(chan error, 1)
	go func() { // Start HTTP server in a separate goroutine
		logger.Info("work-flow API listening", "address", server.Addr)
		serverError <- server.ListenAndServe()
	}()

	select {
	case <-ctx.Done(): // Shutdown signal
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return server.Shutdown(shutdownCtx) // give existing requests time to finish, stop accepting new requests
	case err := <-serverError: // Server error
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}

func environmentOrDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
