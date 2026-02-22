package logger

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"
)

// QueryStats holds query statistics
type QueryStats struct {
	TotalQueries    int64
	SlowQueries     int64
	FailedQueries   int64
	TotalDuration   time.Duration
	AverageDuration time.Duration
}

// SQLQueryLogger provides enhanced SQL logging with statistics
type SQLQueryLogger struct {
	*SQLLogger
	stats     *QueryStats
	statsMu   *StatTracker
	recordSQL bool
}

// NewSQLQueryLogger creates a new SQL query logger with statistics
func NewSQLQueryLogger(config *Config) *SQLQueryLogger {
	return &SQLQueryLogger{
		SQLLogger: NewSQLLogger(config),
		stats:     &QueryStats{},
		statsMu:   NewStatTracker(),
		recordSQL: true,
	}
}

// Begin logs the beginning of a query execution with timing
func (l *SQLQueryLogger) Begin(ctx context.Context, sql string, args ...interface{}) {
	l.statsMu.IncrementTotal()

	if l.SQLLogger != nil && l.config.Level >= Info {
		l.SQLLogger.Begin(ctx, sql, args...)
	}

	// Store start time in context for later use
	if ctx != nil {
		ctx = context.WithValue(ctx, "query_start", time.Now())
		if l.recordSQL {
			ctx = context.WithValue(ctx, "query_sql", sql)
			ctx = context.WithValue(ctx, "query_args", args)
		}
	}
}

// End logs the end of a query execution with statistics
func (l *SQLQueryLogger) End(ctx context.Context, sql string, duration time.Duration, err error) {
	l.statsMu.RecordDuration(duration)

	if err != nil {
		l.statsMu.IncrementFailed()
	}

	if duration >= l.config.SlowThreshold {
		l.statsMu.IncrementSlow()
	}

	// Update average duration
	l.statsMu.UpdateStats()

	if l.SQLLogger != nil {
		l.SQLLogger.End(ctx, sql, duration, err)
	}
}

// GetStats returns the current query statistics
func (l *SQLQueryLogger) GetStats() *QueryStats {
	return &QueryStats{
		TotalQueries:    l.statsMu.totalQueries,
		SlowQueries:     l.statsMu.slowQueries,
		FailedQueries:   l.statsMu.failedQueries,
		TotalDuration:   l.statsMu.totalDuration,
		AverageDuration: l.statsMu.GetAverageDuration(),
	}
}

// ResetStats resets the query statistics
func (l *SQLQueryLogger) ResetStats() {
	l.statsMu.mu.Lock()
	defer l.statsMu.mu.Unlock()

	l.statsMu.totalQueries = 0
	l.statsMu.slowQueries = 0
	l.statsMu.failedQueries = 0
	l.statsMu.totalDuration = 0
}

// StatTracker tracks query statistics
type StatTracker struct {
	totalQueries  int64
	slowQueries   int64
	failedQueries int64
	totalDuration time.Duration
	mu            sync.RWMutex
}

// NewStatTracker creates a new stat tracker
func NewStatTracker() *StatTracker {
	return &StatTracker{}
}

// IncrementTotal increments the total query counter
func (s *StatTracker) IncrementTotal() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.totalQueries++
}

// IncrementSlow increments the slow query counter
func (s *StatTracker) IncrementSlow() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.slowQueries++
}

// IncrementFailed increments the failed query counter
func (s *StatTracker) IncrementFailed() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failedQueries++
}

// RecordDuration records a query duration
func (s *StatTracker) RecordDuration(duration time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.totalDuration += duration
}

// UpdateStats updates derived statistics
func (s *StatTracker) UpdateStats() {
	// Called automatically when needed
}

// GetAverageDuration returns the average query duration
func (s *StatTracker) GetAverageDuration() time.Duration {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.totalQueries == 0 {
		return 0
	}

	return time.Duration(int64(s.totalDuration) / s.totalQueries)
}

// QueryInfo holds information about a query
type QueryInfo struct {
	SQL      string
	Args     []interface{}
	Duration time.Duration
	Error    error
	Start    time.Time
	End      time.Time
}

// FormatQuery formats a query with its arguments
func FormatQuery(sql string, args []interface{}) string {
	if len(args) == 0 {
		return sql
	}

	argStrs := make([]string, len(args))
	for i, arg := range args {
		argStrs[i] = formatValue(arg)
	}

	return fmt.Sprintf("%s [%s]", sql, strings.Join(argStrs, ", "))
}

// formatValue formats a value for logging
func formatValue(v interface{}) string {
	if v == nil {
		return "NULL"
	}

	switch val := v.(type) {
	case string:
		return fmt.Sprintf("'%s'", strings.ReplaceAll(val, "'", "''"))
	case []byte:
		return fmt.Sprintf("'%x'", val)
	case time.Time:
		return fmt.Sprintf("'%s'", val.Format(time.RFC3339))
	default:
		return fmt.Sprintf("%v", v)
	}
}

// QueryLogger wraps a logger with query-specific methods
type QueryLogger struct {
	logger Logger
}

// NewQueryLogger creates a new query logger
func NewQueryLogger(logger Logger) *QueryLogger {
	return &QueryLogger{
		logger: logger,
	}
}

// Query executes a query with logging
func (ql *QueryLogger) Query(ctx context.Context, sql string, args ...interface{}) func(error) {
	start := time.Now()

	if ql.logger != nil {
		ql.logger.Begin(ctx, sql, args...)
	}

	return func(err error) {
		duration := time.Since(start)
		if ql.logger != nil {
			ql.logger.End(ctx, sql, duration, err)
		}
	}
}

// LogQuery logs a query with its result
func LogQuery(ctx context.Context, sql string, args []interface{}, duration time.Duration, err error) {
	End(ctx, sql, duration, err)
}

// WithQueryContext adds query info to context
func WithQueryContext(ctx context.Context, info *QueryInfo) context.Context {
	return context.WithValue(ctx, "query_info", info)
}

// GetQueryContext retrieves query info from context
func GetQueryContext(ctx context.Context) *QueryInfo {
	if ctx == nil {
		return nil
	}

	info, ok := ctx.Value("query_info").(*QueryInfo)
	if !ok {
		return nil
	}

	return info
}
