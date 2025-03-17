package main

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gorilla/mux"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Item struct {
	PK   int    `json:"pk"`
	Data string `json:"data"`
}

type service struct {
	db *pgxpool.Pool
}

var (
	httpRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "Total number of HTTP requests received",
		},
		[]string{"method", "endpoint"},
	)
	httpRequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "Histogram of response time for handler in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"method", "endpoint"},
	)
	dbQueryDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "db_query_duration_seconds",
			Help:    "Histogram of database query durations",
			Buckets: prometheus.DefBuckets,
		},
	)
	logger = slog.New(slog.NewJSONHandler(os.Stdout, nil))
)

func init() {
	prometheus.MustRegister(httpRequestsTotal, httpRequestDuration, dbQueryDuration)
}

func handleJSONResponse(w http.ResponseWriter, data interface{}, err error, logMessage string) {
	switch {
	case err != nil:
		logger.Error(logMessage, "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	case data == nil:
		logger.Error(logMessage)
		w.WriteHeader(http.StatusNoContent)
	default:
		if err := json.NewEncoder(w).Encode(data); err != nil {
			logger.Error("Error encoding JSON", "error", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}
}

func requestIDMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestID := r.Header.Get("X-Request-ID")
		if requestID == "" {
			uuidV7, err := uuid.NewV7()
			if err != nil {
				logger.Error("Failed to generate UUIDv7", "error", err)
				http.Error(w, "Internal Server Error", http.StatusInternalServerError)
				return
			}
			requestID = uuidV7.String()
		}
		r.Header.Set("X-Request-ID", requestID)
		w.Header().Set("X-Request-ID", requestID)
		next.ServeHTTP(w, r)
	})
}
func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestID := r.Header.Get("X-Request-ID")
		startTime := time.Now()
		logger.Info("Incoming request", "requestID", requestID, "method", r.Method, "url", r.URL.Path, "requestID", requestID)
		nrw := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}
		next.ServeHTTP(nrw, r)
		duration := time.Since(startTime)
		logger.Info("Response sent", "requestID", requestID, "status", nrw.statusCode, "duration", duration.String())
	})
}

type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}
func (s *service) executeDBQuery(ctx context.Context, query string, args ...interface{}) (pgx.Rows, error) {
	timer := prometheus.NewTimer(dbQueryDuration)
	defer timer.ObserveDuration()
	return s.db.Query(ctx, query, args...)
}
func instrumentHandler(handlerFunc http.HandlerFunc, endpoint string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		timer := prometheus.NewTimer(httpRequestDuration.WithLabelValues(r.Method, endpoint))
		defer timer.ObserveDuration()

		httpRequestsTotal.WithLabelValues(r.Method, endpoint).Inc()
		handlerFunc(w, r)
	}
}

func (s *service) selfCheck(w http.ResponseWriter, r *http.Request) {
	if err := s.db.Ping(context.Background()); err != nil {
		handleJSONResponse(w, nil, err, "Database self-check failed")
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *service) addItem(w http.ResponseWriter, r *http.Request) {
	var item Item
	if err := json.NewDecoder(r.Body).Decode(&item); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	rows, err := s.executeDBQuery(r.Context(), "INSERT INTO my_table (data) VALUES ($1) RETURNING pk", item.Data)
	if err != nil {
		handleJSONResponse(w, nil, err, "Failed to insert item")
		return
	}
	defer rows.Close()
	rows.Next()
	if err := rows.Scan(&item.PK); err != nil {
		handleJSONResponse(w, nil, err, "Failed to scan item after insert")
		return
	}
	handleJSONResponse(w, item, nil, "Item inserted successfully")
}

func (s *service) getItem(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["id"]
	rows, err := s.executeDBQuery(r.Context(), "SELECT pk, data FROM my_table WHERE pk = $1", id)
	if err != nil {
		handleJSONResponse(w, nil, err, "Failed to fetch item")
		return
	}
	defer rows.Close()
	if !rows.Next() {
		handleJSONResponse(w, nil, nil, "Item not found")
	}
	var item Item
	if err := rows.Scan(&item.PK, &item.Data); err != nil {
		handleJSONResponse(w, nil, err, "Failed to scan item")
		return
	}
	handleJSONResponse(w, item, nil, "Item retrieved successfully")
}

func (s *service) getAllItems(w http.ResponseWriter, r *http.Request) {
	rows, err := s.executeDBQuery(r.Context(), "SELECT pk, data FROM my_table")
	if err != nil {
		handleJSONResponse(w, nil, err, "Failed to fetch all items")
		return
	}

	items := []Item{}
	var item Item
	_, err = pgx.ForEachRow(rows, []any{&item.PK, &item.Data}, func() error {
		items = append(items, item)
		return nil
	})
	if err != nil {
		handleJSONResponse(w, nil, err, "Failed to scan item")
		return
	}
	handleJSONResponse(w, items, nil, "All items retrieved successfully")
}

func main() {
	pgPool, err := pgxpool.New(context.Background(), os.Getenv("PG"))
	if err != nil {
		logger.Error("Database connection failed", "error", err)
		os.Exit(1)
	}
	srvc := service{db: pgPool}

	r := mux.NewRouter()
	r.Use(requestIDMiddleware, loggingMiddleware)
	r.Handle("/metrics", promhttp.Handler())
	r.HandleFunc("/self-check", instrumentHandler(srvc.selfCheck, "/self-check")).Methods(http.MethodGet)
	r.HandleFunc("/items", instrumentHandler(srvc.addItem, "/items")).Methods("POST")
	r.HandleFunc("/items/{id}", instrumentHandler(srvc.getItem, "/items/{id}")).Methods("GET")
	r.HandleFunc("/items", instrumentHandler(srvc.getAllItems, "/items")).Methods("GET")

	s := &http.Server{
		Addr:           ":8080",
		Handler:        r,
		ReadTimeout:    1 * time.Second,
		WriteTimeout:   1 * time.Second,
		MaxHeaderBytes: 1 << 10,
	}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-signalChan
		logger.Info("Shutting down server...")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := s.Shutdown(ctx); err != nil {
			logger.Error("Server shutdown failed", "error", err)
		}
		logger.Info("Server gracefully stopped")
	}()

	logger.Info("Server running on :8080")
	if err := s.ListenAndServe(); errors.Is(err, http.ErrServerClosed) {
		logger.Error("Server failed", "error", err)
	}
}
