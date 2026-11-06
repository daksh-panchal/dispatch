package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/daksh-panchal/dispatch/internal/httpapi"
	"github.com/daksh-panchal/dispatch/internal/job"
	"github.com/daksh-panchal/dispatch/internal/observability"
	dispatchpostgres "github.com/daksh-panchal/dispatch/internal/postgres"
	"github.com/daksh-panchal/dispatch/internal/readmodel"
	"github.com/daksh-panchal/dispatch/internal/recovery"
	"github.com/daksh-panchal/dispatch/internal/worker"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	logger := slog.New(observability.NewTraceHandler(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))
	if err := run(logger); err != nil {
		logger.Error("Dispatch stopped", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) (runErr error) {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	configured, err := loadConfig(os.LookupEnv)
	if err != nil {
		return err
	}

	pool, err := pgxpool.New(ctx, configured.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		return err
	}

	if err := dispatchpostgres.Migrate(ctx, pool); err != nil {
		return err
	}

	telemetry, err := observability.Setup(ctx, configured.ServiceVersion, configured.Environment)
	if err != nil {
		return err
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), configured.ShutdownTimeout)
		defer cancel()
		runErr = errors.Join(runErr, telemetry.Shutdown(shutdownCtx))
	}()
	logger.Info(
		"telemetry configured",
		"traces_enabled", telemetry.TracesEnabled,
		"metrics_enabled", telemetry.MetricsEnabled,
	)

	statsRepository := dispatchpostgres.NewStatsRepository(pool)
	metrics, err := observability.NewMetrics(observability.Meter(), statsRepository, logger)
	if err != nil {
		return err
	}

	jobRepository := dispatchpostgres.NewJobRepository(pool)
	workerRepository := dispatchpostgres.NewWorkerRepository(pool)
	readRepository := dispatchpostgres.NewReadRepository(pool)
	jobService := job.NewService(jobRepository, time.Now)
	readService := readmodel.NewService(readRepository, time.Now, configured.LeaseDuration)
	workerService := worker.NewService(
		workerRepository,
		jobRepository,
		time.Now,
		configured.LeaseDuration,
		configured.RetryBase,
		configured.RetryMaximum,
	)
	recoveryService := recovery.NewService(
		jobRepository,
		time.Now,
		configured.RecoveryInterval,
		configured.RetryBase,
		configured.RetryMaximum,
		configured.RecoveryBatchSize,
		logger,
		metrics,
	)
	backgroundCtx, cancelBackground := context.WithCancel(ctx)
	defer cancelBackground()
	var background sync.WaitGroup
	background.Add(1)
	go func() {
		defer background.Done()
		recoveryService.Run(backgroundCtx)
	}()

	server := &http.Server{
		Addr: configured.HTTPAddress,
		Handler: httpapi.New(
			jobService,
			workerService,
			pool,
			logger,
			httpapi.WithObserver(metrics),
			httpapi.WithReader(readService),
		),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	serverError := make(chan error, 1)
	go func() {
		logger.Info("Dispatch API listening", "address", server.Addr)
		serverError <- server.ListenAndServe()
	}()

	var serveErr error
	select {
	case <-ctx.Done():
		logger.Info("shutdown requested")
	case err := <-serverError:
		if errors.Is(err, http.ErrServerClosed) {
			serveErr = nil
		} else {
			serveErr = err
		}
	}

	cancelBackground()
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), configured.ShutdownTimeout)
	defer cancelShutdown()
	shutdownErr := server.Shutdown(shutdownCtx)

	backgroundDone := make(chan struct{})
	go func() {
		background.Wait()
		close(backgroundDone)
	}()
	select {
	case <-backgroundDone:
	case <-shutdownCtx.Done():
		shutdownErr = errors.Join(shutdownErr, shutdownCtx.Err())
	}

	return errors.Join(serveErr, shutdownErr)
}
