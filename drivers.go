package db

import (
	"fmt"
	"net/url"
	"slices"
	"strings"
	"sync"

	"github.com/gomodul/db/dialect"
)

var driverRegistry = struct {
	sync.RWMutex
	drivers map[string]dialect.Driver
}{
	drivers: make(map[string]dialect.Driver),
}

// RegisterUniversalDriver registers a universal driver with a name.
func RegisterUniversalDriver(name string, driver dialect.Driver) {
	driverRegistry.Lock()
	defer driverRegistry.Unlock()
	driverRegistry.drivers[name] = driver
}

// GetUniversalDriver returns a registered universal driver by name.
func GetUniversalDriver(name string) dialect.Driver {
	driverRegistry.RLock()
	defer driverRegistry.RUnlock()
	return driverRegistry.drivers[name]
}

// UnregisterDriver removes a registered driver by name.
func UnregisterDriver(name string) {
	driverRegistry.Lock()
	defer driverRegistry.Unlock()
	delete(driverRegistry.drivers, name)
}

// RegisteredDrivers returns all registered driver names, sorted.
func RegisteredDrivers() []string {
	driverRegistry.RLock()
	defer driverRegistry.RUnlock()
	names := make([]string, 0, len(driverRegistry.drivers))
	for name := range driverRegistry.drivers {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

// IsDriverRegistered checks if a driver is registered.
func IsDriverRegistered(name string) bool {
	driverRegistry.RLock()
	defer driverRegistry.RUnlock()
	_, ok := driverRegistry.drivers[name]
	return ok
}

// DetectDriverFromDSN attempts to detect the driver type from a DSN string.
//
// Examples:
//
//	DetectDriverFromDSN("postgres://localhost:5432/db") -> "postgres", nil
//	DetectDriverFromDSN("mysql://user:pass@localhost/db") -> "mysql", nil
//	DetectDriverFromDSN("mongodb://localhost:27017/db") -> "mongodb", nil
//	DetectDriverFromDSN("redis://localhost:6379/0") -> "redis", nil
//	DetectDriverFromDSN("file:test.db") -> "sqlite", nil
func DetectDriverFromDSN(dsn string) (string, error) {
	if dsn == "" {
		return "", fmt.Errorf("empty DSN")
	}

	if strings.HasPrefix(dsn, "file:") || strings.HasSuffix(dsn, ".db") ||
		strings.HasSuffix(dsn, ".sqlite") || strings.HasSuffix(dsn, ".sqlite3") {
		return "sqlite", nil
	}

	u, err := url.Parse(dsn)
	if err != nil {
		if idx := strings.Index(dsn, "://"); idx > 0 {
			return dsn[:idx], nil
		}
		return "", fmt.Errorf("unable to detect driver from DSN: %w", err)
	}

	scheme := u.Scheme
	if scheme == "" {
		return "", fmt.Errorf("no scheme found in DSN")
	}

	switch scheme {
	case "postgresql":
		scheme = "postgres"
	case "elastic":
		scheme = "elasticsearch"
	}

	return scheme, nil
}

// GetDriverFromConfig returns a universal driver based on the Config DSN.
func GetDriverFromConfig(cfg Config) (dialect.Driver, error) {
	driverName, err := DetectDriverFromDSN(cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("failed to detect driver: %w", err)
	}

	drv := GetUniversalDriver(driverName)
	if drv != nil {
		return drv, nil
	}

	return nil, fmt.Errorf("%w: %s", dialect.ErrNoDriver, driverName)
}
