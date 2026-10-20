package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/messi/fintech-platform/internal/api"
	"github.com/messi/fintech-platform/internal/events"
	"github.com/messi/fintech-platform/internal/fraud"
	"github.com/messi/fintech-platform/internal/outbox"
	"github.com/messi/fintech-platform/internal/store"
	"github.com/messi/fintech-platform/internal/store/memstore"
	"github.com/messi/fintech-platform/internal/store/postgres"
	"github.com/redis/go-redis/v9"
)

func getEnv(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	logger.Info("starting fintech payment and fraud platform", "version", "1.0.0")

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	httpPort := getEnv("HTTP_PORT", "8090")
	authToken := getEnv("API_AUTH_TOKEN", "slice-secret-token-2026")
	dbURL := getEnv("DATABASE_URL", "")
	redisURL := getEnv("REDIS_URL", "")
	kafkaBrokersStr := getEnv("KAFKA_BROKERS", "")

	var s store.Store
	var pool *pgxpool.Pool
	var tracker fraud.VelocityTracker
	var auditor fraud.AuditStore
	var publisher events.Publisher

	// 1. Initialize PostgreSQL or In-Memory Store
	if dbURL != "" {
		logger.Info("connecting to postgresql", "url", dbURL)
		var err error
		poolCtx, poolCancel := context.WithTimeout(ctx, 5*time.Second)
		pool, err = pgxpool.New(poolCtx, dbURL)
		poolCancel()
		if err == nil && pool.Ping(ctx) == nil {
			logger.Info("connected to postgresql successfully")
			s = postgres.New(pool)
			auditor = fraud.NewPostgresAuditStore(pool)
		} else {
			logger.Warn("could not connect to postgresql, falling back to in-memory store", "error", err)
			s = memstore.New()
			auditor = fraud.NewMemAuditStore()
		}
	} else {
		logger.Info("no DATABASE_URL provided, running in-memory mode")
		s = memstore.New()
		auditor = fraud.NewMemAuditStore()
	}

	// 2. Initialize Redis Velocity Tracker
	if redisURL != "" {
		logger.Info("connecting to redis", "addr", redisURL)
		rClient := redis.NewClient(&redis.Options{
			Addr:        redisURL,
			DialTimeout: 2 * time.Second,
		})
		if rClient.Ping(ctx).Err() == nil {
			logger.Info("connected to redis successfully")
			tracker = fraud.NewRedisVelocityTracker(rClient)
		} else {
			logger.Warn("could not connect to redis, falling back to in-memory tracker")
			tracker = fraud.NewMemVelocityTracker()
		}
	} else {
		tracker = fraud.NewMemVelocityTracker()
	}

	// 3. Initialize Kafka Publisher & Outbox Relay
	if kafkaBrokersStr != "" {
		brokers := strings.Split(kafkaBrokersStr, ",")
		logger.Info("initializing kafka publisher", "brokers", brokers)
		publisher = events.NewKafkaPublisher(brokers)
	} else {
		logger.Info("running in-memory event publisher")
		publisher = events.NewMemPublisher()
	}

	// 4. Initialize Fraud Engine
	fraudCfg := fraud.DefaultConfig()
	if fcStr := os.Getenv("FRAUD_FAIL_CLOSED"); fcStr != "" {
		fraudCfg.FailClosed, _ = strconv.ParseBool(fcStr)
	}
	fraudEngine := fraud.NewEngine(fraudCfg, tracker, auditor)

	// 5. Start Background Workers: Outbox Relay & Event Consumer
	if pool != nil {
		relay := outbox.NewRelay(pool, publisher, outbox.DefaultConfig())
		go func() {
			logger.Info("outbox relay background worker started")
			if err := relay.Start(ctx); err != nil && err != context.Canceled {
				logger.Error("outbox relay error", "error", err)
			}
		}()
	}

	if kafkaBrokersStr != "" {
		brokers := strings.Split(kafkaBrokersStr, ",")
		consumerCfg := events.ConsumerConfig{
			Brokers:      brokers,
			Topic:        getEnv("KAFKA_PAYMENT_TOPIC", "payment.events"),
			DLQTopic:     getEnv("KAFKA_DLQ_TOPIC", "payment.dlq"),
			GroupID:      getEnv("KAFKA_CONSUMER_GROUP", "payment-processor-group"),
			MaxRetries:   3,
			RetryBackoff: 100 * time.Millisecond,
		}
		consumer := events.NewConsumer(consumerCfg, pool, publisher)
		consumer.RegisterHandler(events.EventPaymentCompleted, func(ctx context.Context, ev events.Event) error {
			logger.Info("async payment.completed received", "id", ev.ID, "aggregate_id", ev.AggregateID)
			return nil
		})
		consumer.RegisterHandler(events.EventPaymentFailed, func(ctx context.Context, ev events.Event) error {
			logger.Info("async payment.failed received", "id", ev.ID, "aggregate_id", ev.AggregateID)
			return nil
		})
		go func() {
			logger.Info("kafka consumer background worker started")
			if err := consumer.Start(ctx); err != nil && err != context.Canceled {
				logger.Error("kafka consumer error", "error", err)
			}
		}()
	}

	// 6. Build HTTP Server
	server := api.NewServer(s, fraudEngine, auditor)
	rCfg := api.DefaultRouterConfig()
	rCfg.AuthToken = authToken
	rCfg.Logger = logger

	router := api.NewRouter(server, rCfg)

	httpServer := &http.Server{
		Addr:         ":" + httpPort,
		Handler:      router,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		logger.Info("http server listening", "port", httpPort)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("http server failed", "error", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	logger.Info("shutting down gracefully...")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	_ = httpServer.Shutdown(shutdownCtx)
	if pool != nil {
		pool.Close()
	}
	_ = publisher.Close()
	logger.Info("shutdown complete")
}
