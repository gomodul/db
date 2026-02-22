package logger

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// OTEL integration provides OpenTelemetry tracing for database operations

const (
	// InstrumentationName is the instrumentation name for OTEL
	InstrumentationName = "github.com/gomodul/db"
)

// TracerConfig holds configuration for OpenTelemetry tracer
type TracerConfig struct {
	// TracerProvider is the tracer provider to use
	TracerProvider trace.TracerProvider

	// Attributes are additional attributes to add to all spans
	Attributes []attribute.KeyValue
}

// OTELLogger provides OpenTelemetry tracing integration
type OTELLogger struct {
	tracer trace.Tracer
	config *TracerConfig
}

// NewOTELLogger creates a new OpenTelemetry logger
func NewOTELLogger(config *TracerConfig) *OTELLogger {
	if config == nil {
		config = &TracerConfig{}
	}

	tracer := otel.Tracer(InstrumentationName,
		trace.WithInstrumentationVersion("1.0.0"),
	)

	if config.TracerProvider != nil {
		tracer = config.TracerProvider.Tracer(InstrumentationName,
			trace.WithInstrumentationVersion("1.0.0"),
		)
	}

	return &OTELLogger{
		tracer: tracer,
		config: config,
	}
}

// StartSpan starts a new span for a database operation
func (l *OTELLogger) StartSpan(ctx context.Context, name string) (context.Context, trace.Span) {
	attrs := l.config.Attributes
	if attrs == nil {
		attrs = []attribute.KeyValue{}
	}

	return l.tracer.Start(ctx, name, trace.WithAttributes(attrs...))
}

// LogQuery logs a query with OpenTelemetry tracing
func (l *OTELLogger) LogQuery(ctx context.Context, sql string, args []interface{}) (context.Context, trace.Span) {
	ctx, span := l.StartSpan(ctx, "db.query")

	// Add SQL query attribute
	span.SetAttributes(attribute.String("db.statement", sql))

	// Add query parameters if available (be careful with sensitive data)
	if len(args) > 0 {
		// In production, you might want to sanitize or redact sensitive values
		span.SetAttributes(attribute.String("db.args", formatArgs(args)))
	}

	return ctx, span
}

// LogQueryEnd logs the end of a query execution
func (l *OTELLogger) LogQueryEnd(span trace.Span, duration time.Duration, err error) {
	// Set duration as an attribute
	span.SetAttributes(attribute.Float64("db.duration_ms", float64(duration.Milliseconds())))

	if err != nil {
		// Record error
		span.SetStatus(codes.Error, err.Error())
		span.RecordError(err)
	} else {
		span.SetStatus(codes.Ok, "")
	}

	span.End()
}

// Begin starts a query span
func (l *OTELLogger) Begin(ctx context.Context, sql string, args ...interface{}) {
	_, span := l.LogQuery(ctx, sql, args)

	// Store span in context for later retrieval
	if ctx != nil {
		ctx = context.WithValue(ctx, "otel_span", span)
	}
}

// End ends a query span
func (l *OTELLogger) End(ctx context.Context, sql string, duration time.Duration, err error) {
	// Retrieve span from context if available
	if ctx != nil {
		if span, ok := ctx.Value("otel_span").(trace.Span); ok {
			l.LogQueryEnd(span, duration, err)
			return
		}
	}

	// If no span in context, this might be a new span
	// In this case, we can't do much without the original span
}

// Log implements the Logger interface for OTEL
func (l *OTELLogger) Log(ctx context.Context, level LogLevel, msg string, data ...interface{}) {
	// For OTEL, we might want to add events to the current span
	span := trace.SpanFromContext(ctx)
	if span.IsRecording() {
		span.AddEvent(msg, trace.WithAttributes(
			attribute.String("level", level.String()),
			attribute.String("message", msg),
		))
	}
}

// TracerWrapper wraps a logger with OTEL tracing
type TracerWrapper struct {
	logger  Logger
	otel    *OTELLogger
	enabled bool
}

// NewTracerWrapper creates a new tracer wrapper
func NewTracerWrapper(logger Logger, otel *OTELLogger) *TracerWrapper {
	return &TracerWrapper{
		logger:  logger,
		otel:    otel,
		enabled: true,
	}
}

// Enable enables OTEL tracing
func (w *TracerWrapper) Enable() {
	w.enabled = true
}

// Disable disables OTEL tracing
func (w *TracerWrapper) Disable() {
	w.enabled = false
}

// Log logs to both the underlying logger and OTEL
func (w *TracerWrapper) Log(ctx context.Context, level LogLevel, msg string, data ...interface{}) {
	if w.logger != nil {
		w.logger.Log(ctx, level, msg, data...)
	}

	if w.enabled && w.otel != nil {
		w.otel.Log(ctx, level, msg, data...)
	}
}

// Begin starts a query with both logging and tracing
func (w *TracerWrapper) Begin(ctx context.Context, sql string, args ...interface{}) {
	if w.logger != nil {
		w.logger.Begin(ctx, sql, args...)
	}

	if w.enabled && w.otel != nil {
		w.otel.Begin(ctx, sql, args...)
	}
}

// End ends a query with both logging and tracing
func (w *TracerWrapper) End(ctx context.Context, sql string, duration time.Duration, err error) {
	if w.logger != nil {
		w.logger.End(ctx, sql, duration, err)
	}

	if w.enabled && w.otel != nil {
		w.otel.End(ctx, sql, duration, err)
	}
}

// SpanBuilder helps build spans with common attributes
type SpanBuilder struct {
	tracer trace.Tracer
	attrs  []attribute.KeyValue
}

// NewSpanBuilder creates a new span builder
func NewSpanBuilder(tracer trace.Tracer) *SpanBuilder {
	return &SpanBuilder{
		tracer: tracer,
		attrs:  []attribute.KeyValue{},
	}
}

// WithAttribute adds an attribute to the span
func (b *SpanBuilder) WithAttribute(key string, value interface{}) *SpanBuilder {
	switch v := value.(type) {
	case string:
		b.attrs = append(b.attrs, attribute.String(key, v))
	case int:
		b.attrs = append(b.attrs, attribute.Int(key, v))
	case int64:
		b.attrs = append(b.attrs, attribute.Int64(key, v))
	case float64:
		b.attrs = append(b.attrs, attribute.Float64(key, v))
	case bool:
		b.attrs = append(b.attrs, attribute.Bool(key, v))
	}
	return b
}

// WithAttributes adds multiple attributes to the span
func (b *SpanBuilder) WithAttributes(attrs ...attribute.KeyValue) *SpanBuilder {
	b.attrs = append(b.attrs, attrs...)
	return b
}

// Start starts a new span with the configured attributes
func (b *SpanBuilder) Start(ctx context.Context, name string) (context.Context, trace.Span) {
	return b.tracer.Start(ctx, name, trace.WithAttributes(b.attrs...))
}

// formatArgs formats query arguments for logging
func formatArgs(args []interface{}) string {
	if len(args) == 0 {
		return ""
	}

	result := "["
	for i, arg := range args {
		if i > 0 {
			result += ", "
		}

		switch v := arg.(type) {
		case string:
			result += `"` + v + `"`
		case nil:
			result += "null"
		default:
			result += fmt.Sprintf("%v", v)
		}
	}
	result += "]"

	return result
}
