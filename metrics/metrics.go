package metrics

import (
	"sync"
	"time"
)

// Collector defines the interface for collecting metrics
type Collector interface {
	RecordCounter(name string, value float64)
	RecordGauge(name string, value float64)
	RecordTiming(name string, duration time.Duration)
	RecordHistogram(name string, value float64)
}

// DefaultCollector provides a simple in-memory metrics collector
type DefaultCollector struct {
	mu     sync.RWMutex
	counters map[string]*CounterStat
	gauges   map[string]*GaugeStat
	timings  map[string]*TimingStat
}

// CounterStat tracks counter metrics
type CounterStat struct {
	Name  string
	Value float64
}

// GaugeStat tracks gauge metrics
type GaugeStat struct {
	Name  string
	Value float64
}

// TimingStat tracks timing metrics
type TimingStat struct {
	Name      string
	Count     int64
	Sum       time.Duration
	Min       time.Duration
	Max       time.Duration
	LastValue time.Duration
}

// NewDefaultCollector creates a new default metrics collector
func NewDefaultCollector() *DefaultCollector {
	return &DefaultCollector{
		counters: make(map[string]*CounterStat),
		gauges:   make(map[string]*GaugeStat),
		timings:  make(map[string]*TimingStat),
	}
}

// RecordCounter records a counter metric
func (c *DefaultCollector) RecordCounter(name string, value float64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if stat, exists := c.counters[name]; exists {
		stat.Value += value
	} else {
		c.counters[name] = &CounterStat{
			Name:  name,
			Value: value,
		}
	}
}

// RecordGauge records a gauge metric
func (c *DefaultCollector) RecordGauge(name string, value float64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if stat, exists := c.gauges[name]; exists {
		stat.Value = value
	} else {
		c.gauges[name] = &GaugeStat{
			Name:  name,
			Value: value,
		}
	}
}

// RecordTiming records a timing metric
func (c *DefaultCollector) RecordTiming(name string, duration time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if stat, exists := c.timings[name]; exists {
		stat.Count++
		stat.Sum += duration
		if duration < stat.Min {
			stat.Min = duration
		}
		if duration > stat.Max {
			stat.Max = duration
		}
		stat.LastValue = duration
	} else {
		c.timings[name] = &TimingStat{
			Name:      name,
			Count:     1,
			Sum:       duration,
			Min:       duration,
			Max:       duration,
			LastValue: duration,
		}
	}
}

// RecordHistogram records a histogram metric (stored as timing for simplicity)
func (c *DefaultCollector) RecordHistogram(name string, value float64) {
	c.RecordTiming(name, time.Duration(value))
}

// GetCounters returns all counter metrics
func (c *DefaultCollector) GetCounters() map[string]*CounterStat {
	c.mu.RLock()
	defer c.mu.RUnlock()

	result := make(map[string]*CounterStat, len(c.counters))
	for k, v := range c.counters {
		result[k] = v
	}
	return result
}

// GetGauges returns all gauge metrics
func (c *DefaultCollector) GetGauges() map[string]*GaugeStat {
	c.mu.RLock()
	defer c.mu.RUnlock()

	result := make(map[string]*GaugeStat, len(c.gauges))
	for k, v := range c.gauges {
		result[k] = v
	}
	return result
}

// GetTimings returns all timing metrics
func (c *DefaultCollector) GetTimings() map[string]*TimingStat {
	c.mu.RLock()
	defer c.mu.RUnlock()

	result := make(map[string]*TimingStat, len(c.timings))
	for k, v := range c.timings {
		result[k] = v
	}
	return result
}

// Reset clears all metrics
func (c *DefaultCollector) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.counters = make(map[string]*CounterStat)
	c.gauges = make(map[string]*GaugeStat)
	c.timings = make(map[string]*TimingStat)
}

// GetStats returns all metrics as a map
func (c *DefaultCollector) GetStats() map[string]interface{} {
	c.mu.RLock()
	defer c.mu.RUnlock()

	result := make(map[string]interface{})

	for name, stat := range c.counters {
		result[name] = stat.Value
	}

	for name, stat := range c.gauges {
		result[name] = stat.Value
	}

	for name, stat := range c.timings {
		result[name] = map[string]interface{}{
			"count": stat.Count,
			"sum":   stat.Sum.String(),
			"min":   stat.Min.String(),
			"max":   stat.Max.String(),
			"avg":   (stat.Sum / time.Duration(stat.Count)).String(),
		}
	}

	return result
}
