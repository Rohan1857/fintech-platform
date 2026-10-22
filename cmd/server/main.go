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

	"github.com/Rohan1857/fintech-platform/internal/api"
	"github.com/Rohan1857/fintech-platform/internal/events"
	"github.com/Rohan1857/fintech-platform/internal/fraud"
	"github.com/Rohan1857/fintech-platform/internal/outbox"
	"github.com/Rohan1857/fintech-platform/internal/store"
	"github.com/Rohan1857/fintech-platform/internal/store/memstore"
	"github.com/Rohan1857/fintech-platform/internal/store/postgres"
	"github.com/jackc/pgx/v5/pgxpool"
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
	authToken, err := loadAuthToken()
	if err != nil {
		logger.Error("invalid API authentication configuration", "error", err)
		os.Exit(1)
	}
	dbURL := getEnv("DATABASE_URL", "")
	redisURL := getEnv("REDIS_URL", "")
	kafkaBrokersStr := getEnv("KAFKA_BROKERS", "")
	allowDevMocks, _ := strconv.ParseBool(getEnv("ALLOW_IN_MEMORY_DEV_MODE", "false"))

	var s store.Store
	var pool *pgxpool.Pool
	var tracker fraud.VelocityTracker
	var auditor fraud.AuditStore
	var publisher events.Publisher

	// 1. Initialize PostgreSQL Storage
	if dbURL != "" {
		logger.Info("connecting to postgresql")
		poolCtx, poolCancel := context.WithTimeout(ctx, 5*time.Second)
		var err error
		pool, err = pgxpool.New(poolCtx, dbURL)
		poolCancel()
		if err != nil || pool.Ping(ctx) != nil {
			if !allowDevMocks {
				logger.Error("FATAL: PostgreSQL connection failed and ALLOW_IN_MEMORY_DEV_MODE is false", "error", err)
				os.Exit(1)
			}
			logger.Warn("DEV MODE: could not connect to postgresql, falling back to in-memory store", "error", err)
			s = memstore.New()
			auditor = fraud.NewMemAuditStore()
		} else {
			logger.Info("connected to postgresql successfully")
			s = postgres.New(pool)
			auditor = fraud.NewPostgresAuditStore(pool)
		}
	} else {
		if !allowDevMocks {
			logger.Error("FATAL: DATABASE_URL is required in production mode. Set ALLOW_IN_MEMORY_DEV_MODE=true for local dev.")
			os.Exit(1)
		}
		logger.Warn("DEV MODE: no DATABASE_URL provided, running in-memory mode")
		s = memstore.New()
		auditor = fraud.NewMemAuditStore()
	}

	// 2. Initialize Redis Velocity Tracker
	if redisURL != "" {
		logger.Info("connecting to redis", "addr", redisURL)
		var rOpts *redis.Options
		if parsedOpts, err := redis.ParseURL(redisURL); err == nil {
			rOpts = parsedOpts
		} else {
			rOpts = &redis.Options{
				Addr: redisURL,
			}
		}
		rOpts.DialTimeout = 2 * time.Second
		rClient := redis.NewClient(rOpts)
		if err := rClient.Ping(ctx).Err(); err != nil {
			if !allowDevMocks {
				logger.Error("FATAL: Redis connection failed and ALLOW_IN_MEMORY_DEV_MODE is false", "error", err)
				os.Exit(1)
			}
			logger.Warn("DEV MODE: could not connect to redis, falling back to in-memory tracker", "error", err)
			tracker = fraud.NewMemVelocityTracker()
		} else {
			logger.Info("connected to redis successfully")
			tracker = fraud.NewRedisVelocityTracker(rClient)
		}
	} else {
		if !allowDevMocks {
			logger.Error("FATAL: REDIS_URL is required in production mode. Set ALLOW_IN_MEMORY_DEV_MODE=true for local dev.")
			os.Exit(1)
		}
		logger.Warn("DEV MODE: no REDIS_URL provided, using in-memory velocity tracker")
		tracker = fraud.NewMemVelocityTracker()
	}

	// 3. Initialize Kafka Publisher & Outbox Relay
	if kafkaBrokersStr != "" {
		brokers := strings.Split(kafkaBrokersStr, ",")
		logger.Info("initializing kafka publisher", "brokers", brokers)
		publisher = events.NewKafkaPublisher(brokers)
	} else {
		if !allowDevMocks {
			logger.Error("FATAL: KAFKA_BROKERS is required in production mode. Set ALLOW_IN_MEMORY_DEV_MODE=true for local dev.")
			os.Exit(1)
		}
		logger.Warn("DEV MODE: no KAFKA_BROKERS provided, using in-memory event publisher")
		publisher = events.NewMemPublisher()
	}

	// 4. Initialize Fraud Engine
	fraudCfg := fraud.DefaultConfig()
	if fcStr := os.Getenv("FRAUD_FAIL_CLOSED"); fcStr != "" {
		fraudCfg.FailClosed, _ = strconv.ParseBool(fcStr)
	}
	fraudEngine := fraud.NewEngine(fraudCfg, tracker, auditor)

	// 5. Start Background Workers: Outbox Relay & Event Consumer
	expiryDone := make(chan struct{})
	go func() {
		defer close(expiryDone)
		runHoldExpiry(ctx, s, 30*time.Second, logger)
	}()
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
	<-expiryDone
	if pool != nil {
		pool.Close()
	}
	_ = publisher.Close()
	logger.Info("shutdown complete")
}
