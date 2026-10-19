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

	"github.com/daksh-panchal/work-flow/internal/demo"
	"github.com/daksh-panchal/work-flow/internal/demoapi"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	if err := run(logger); err != nil {
		logger.Error("work-flow demo stopped", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	configured, err := loadConfig(os.LookupEnv)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	scheduler, err := demo.NewHTTPClient(configured.WorkFlowAPIURL)
	if err != nil {
		return err
	}
	manager := demo.NewManager(ctx, scheduler, logger)
	server := &http.Server{
		Addr:              configured.HTTPAddress,
		Handler:           demoapi.New(manager, scheduler, logger),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      0,
		IdleTimeout:       60 * time.Second,
	}

	serverError := make(chan error, 1)
	go func() {
		logger.Info(
			"work-flow demo API listening",
			"address", server.Addr,
			"workflow_api", configured.WorkFlowAPIURL,
		)
		serverError <- server.ListenAndServe()
	}()

	var serveErr error
	select {
	case <-ctx.Done():
		logger.Info("demo shutdown requested")
	case err := <-serverError:
		if !errors.Is(err, http.ErrServerClosed) {
			serveErr = err
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), configured.ShutdownTimeout)
	defer cancel()
	return errors.Join(serveErr, server.Shutdown(shutdownCtx))
}
