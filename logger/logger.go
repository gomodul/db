package logger

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

// LogLevel represents the logging level
type LogLevel int

const (
	// Silent level - no logging
	Silent LogLevel = iota
	// Error level - errors only
	Error
	// Warn level - warnings and errors
	Warn
	// Info level - info, warnings, and errors
	Info
	// Debug level - all logs
	Debug
)

// String returns the string representation of the log level
func (l LogLevel) String() string {
	switch l {
	case Silent:
		return "SILENT"
	case Error:
		return "ERROR"
	case Warn:
		return "WARN"
	case Info:
		return "INFO"
	case Debug:
		return "DEBUG"
	default:
		return "UNKNOWN"
	}
}

// Logger is the interface for logging database operations
type Logger interface {
	// Log logs a message at the specified level
	Log(ctx context.Context, level LogLevel, msg string, data ...interface{})

	// Begin logs the beginning of a query execution
	Begin(ctx context.Context, sql string, args ...interface{})

	// End logs the end of a query execution
	End(ctx context.Context, sql string, duration time.Duration, err error)
}

// Config holds logger configuration
type Config struct {
	// Level is the minimum log level
	Level LogLevel

	// Writer is the output writer
	Writer io.Writer

	// SlowThreshold is the threshold for slow query logging
	SlowThreshold time.Duration

	// IgnoreErr disables error logging
	IgnoreErr bool

	// Color enables colored output
	Color bool

	// WithTrace adds trace information to logs
	WithTrace bool
}

// DefaultConfig returns the default logger configuration
func DefaultConfig() *Config {
	return &Config{
		Level:         Info,
		Writer:        os.Stdout,
		SlowThreshold: 200 * time.Millisecond,
		IgnoreErr:     false,
		Color:         true,
		WithTrace:     false,
	}
}

// SQLLogger is the default SQL logger implementation
type SQLLogger struct {
	config *Config
	mu     sync.RWMutex
}

// NewSQLLogger creates a new SQL logger
func NewSQLLogger(config *Config) *SQLLogger {
	if config == nil {
		config = DefaultConfig()
	}

	return &SQLLogger{
		config: config,
	}
}

// Log logs a message at the specified level
func (l *SQLLogger) Log(ctx context.Context, level LogLevel, msg string, data ...interface{}) {
	if level < l.config.Level {
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	l.logMessage(level, msg, data...)
}

// Begin logs the beginning of a query execution
func (l *SQLLogger) Begin(ctx context.Context, sql string, args ...interface{}) {
	if l.config.Level < Info {
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	msg := formatSQL(sql, args)
	l.logMessage(Info, "%s", "Query: "+msg)
}

// End logs the end of a query execution
func (l *SQLLogger) End(ctx context.Context, sql string, duration time.Duration, err error) {
	level := Info

	if err != nil && !l.config.IgnoreErr {
		level = Error
	} else if duration >= l.config.SlowThreshold {
		level = Warn
	}

	if level < l.config.Level {
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	msg := formatSQL(sql, nil)

	if err != nil {
		l.logMessage(Error, "Query failed (%s): %s - Error: %v", duration, msg, err)
	} else if duration >= l.config.SlowThreshold {
		l.logMessage(Warn, "Slow query (%s): %s", duration, msg)
	} else {
		l.logMessage(Debug, "Query completed (%s): %s", duration, msg)
	}
}

// logMessage writes a log message
func (l *SQLLogger) logMessage(level LogLevel, msg string, data ...interface{}) {
	formattedMsg := msg
	if len(data) > 0 {
		formattedMsg = fmt.Sprintf(msg, data...)
	}

	if l.config.Color {
		formattedMsg = l.colorize(level, formattedMsg)
	} else {
		formattedMsg = fmt.Sprintf("[%s] %s", level.String(), formattedMsg)
	}

	fmt.Fprintln(l.config.Writer, formattedMsg)
}

// colorize adds color to the log message
func (l *SQLLogger) colorize(level LogLevel, msg string) string {
	colors := map[LogLevel]string{
		Error: "\033[31m", // Red
		Warn:  "\033[33m", // Yellow
		Info:  "\033[32m", // Green
		Debug: "\033[36m", // Cyan
	}

	reset := "\033[0m"

	if color, ok := colors[level]; ok {
		return fmt.Sprintf("%s[%s]%s %s", color, level.String(), reset, msg)
	}

	return fmt.Sprintf("[%s] %s", level.String(), msg)
}

// formatSQL formats SQL query with arguments
func formatSQL(sql string, args []interface{}) string {
	if len(args) == 0 {
		return sql
	}

	// Simple formatting - in production, you might want more sophisticated formatting
	return fmt.Sprintf("%s [%v]", sql, args)
}

// NullLogger is a logger that does nothing
type NullLogger struct{}

// NewNullLogger creates a new null logger
func NewNullLogger() *NullLogger {
	return &NullLogger{}
}

// Log does nothing
func (l *NullLogger) Log(ctx context.Context, level LogLevel, msg string, data ...interface{}) {}

// Begin does nothing
func (l *NullLogger) Begin(ctx context.Context, sql string, args ...interface{}) {}

// End does nothing
func (l *NullLogger) End(ctx context.Context, sql string, duration time.Duration, err error) {}

// global logger instance
var (
	globalLogger Logger = NewNullLogger()
	loggerMu    sync.RWMutex
)

// SetGlobalLogger sets the global logger
func SetGlobalLogger(logger Logger) {
	loggerMu.Lock()
	defer loggerMu.Unlock()
	globalLogger = logger
}

// GetGlobalLogger returns the global logger
func GetGlobalLogger() Logger {
	loggerMu.RLock()
	defer loggerMu.RUnlock()
	return globalLogger
}

// Log logs a message using the global logger
func Log(ctx context.Context, level LogLevel, msg string, data ...interface{}) {
	GetGlobalLogger().Log(ctx, level, msg, data...)
}

// Begin logs the beginning of a query using the global logger
func Begin(ctx context.Context, sql string, args ...interface{}) {
	GetGlobalLogger().Begin(ctx, sql, args...)
}

// End logs the end of a query using the global logger
func End(ctx context.Context, sql string, duration time.Duration, err error) {
	GetGlobalLogger().End(ctx, sql, duration, err)
}
