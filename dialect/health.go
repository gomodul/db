package dialect

import "time"

// HealthStatus represents the health status of a driver
type HealthStatus struct {
	Healthy bool          // true if the driver is healthy
	Latency time.Duration // latency of the health check
	Message string        // optional message
	Details map[string]interface{} // additional details
}

// NewHealthyStatus creates a new healthy status
func NewHealthyStatus(latency time.Duration) *HealthStatus {
	return &HealthStatus{
		Healthy: true,
		Latency: latency,
		Message: "OK",
	}
}

// NewUnhealthyStatus creates a new unhealthy status
func NewUnhealthyStatus(message string) *HealthStatus {
	return &HealthStatus{
		Healthy: false,
		Message: message,
	}
}

// WithDetail adds a detail to the health status
func (h *HealthStatus) WithDetail(key string, value interface{}) *HealthStatus {
	if h.Details == nil {
		h.Details = make(map[string]interface{})
	}
	h.Details[key] = value
	return h
}
