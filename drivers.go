package db

import (
	"fmt"
	"net/url"
	"slices"
	"strings"
	"sync"

	"github.com/gomodul/db/dialect"
)

// Legacy driver registry (for backward compatibility with Dialector)
var legacyDrivers = struct {
	sync.RWMutex
	dialects map[string]Dialector
}{
	dialects: make(map[string]Dialector),
}

// New driver registry (for universal Driver interface)
var universalDrivers = struct {
	sync.RWMutex
	drivers map[string]dialect.Driver
}{
	drivers: make(map[string]dialect.Driver),
}

// ============ Legacy Driver Registration (Dialector) ============

// RegisterDriver registers a legacy database driver with a name.
// Multiple names can be registered for the same dialector.
//
// Deprecated: Use RegisterUniversalDriver instead.
func RegisterDriver(name string, dialectors ...Dialector) {
	legacyDrivers.Lock()
	defer legacyDrivers.Unlock()

	for _, d := range dialectors {
		legacyDrivers.dialects[name] = d
	}
}

// UnregisterDriver removes a registered driver by name.
func UnregisterDriver(name string) {
	legacyDrivers.Lock()
	universalDrivers.Lock()
	defer legacyDrivers.Unlock()
	defer universalDrivers.Unlock()

	delete(legacyDrivers.dialects, name)
	delete(universalDrivers.drivers, name)
}

// GetDriver returns a registered dialector by name (legacy).
// Returns nil if no driver is registered with the given name.
func GetDriver(name string) Dialector {
	legacyDrivers.RLock()
	defer legacyDrivers.RUnlock()
	return legacyDrivers.dialects[name]
}

// ============ Universal Driver Registration ============

// RegisterUniversalDriver registers a universal driver with a name.
func RegisterUniversalDriver(name string, driver dialect.Driver) {
	universalDrivers.Lock()
	defer universalDrivers.Unlock()
	universalDrivers.drivers[name] = driver
}

// GetUniversalDriver returns a registered universal driver by name.
func GetUniversalDriver(name string) dialect.Driver {
	universalDrivers.RLock()
	defer universalDrivers.RUnlock()
	return universalDrivers.drivers[name]
}

// RegisteredDrivers returns a list of all registered driver names.
func RegisteredDrivers() []string {
	legacyDrivers.RLock()
	universalDrivers.RLock()
	defer legacyDrivers.RUnlock()
	defer universalDrivers.RUnlock()

	names := make([]string, 0)
	for name := range legacyDrivers.dialects {
		names = append(names, name)
	}
	for name := range universalDrivers.drivers {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

// IsDriverRegistered checks if a driver is registered.
func IsDriverRegistered(name string) bool {
	legacyDrivers.RLock()
	universalDrivers.RLock()
	defer legacyDrivers.RUnlock()
	defer universalDrivers.RUnlock()

	_, ok := legacyDrivers.dialects[name]
	if ok {
		return true
	}
	_, ok = universalDrivers.drivers[name]
	return ok
}

// DetectDriverFromDSN attempts to detect the driver type from a DSN string.
// It parses the DSN and returns the scheme/driver name.
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

	// Check for SQLite file syntax
	if strings.HasPrefix(dsn, "file:") || strings.HasSuffix(dsn, ".db") ||
		strings.HasSuffix(dsn, ".sqlite") || strings.HasSuffix(dsn, ".sqlite3") {
		return "sqlite", nil
	}

	// Parse as URL to extract scheme
	u, err := url.Parse(dsn)
	if err != nil {
		// Not a URL, try to detect from prefix
		if idx := strings.Index(dsn, "://"); idx > 0 {
			return dsn[:idx], nil
		}
		return "", fmt.Errorf("unable to detect driver from DSN: %w", err)
	}

	scheme := u.Scheme
	if scheme == "" {
		return "", fmt.Errorf("no scheme found in DSN")
	}

	// Normalize some common aliases
	switch scheme {
	case "postgresql":
		scheme = "postgres"
	case "elasticsearch", "elastic":
		scheme = "elasticsearch"
	}

	return scheme, nil
}

// GetDialectorFromConfig returns a legacy dialector based on the Config.
//
// Deprecated: Use GetDriverFromConfig instead.
func GetDialectorFromConfig(cfg Config) (Dialector, error) {
	driverName, err := DetectDriverFromDSN(cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("failed to detect driver: %w", err)
	}

	dialector := GetDriver(driverName)
	if dialector == nil {
		return nil, fmt.Errorf("%w: %s", ErrNoDriver, driverName)
	}

	return dialector, nil
}

// GetDriverFromConfig returns a universal driver based on the Config.
func GetDriverFromConfig(cfg Config) (dialect.Driver, error) {
	driverName, err := DetectDriverFromDSN(cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("failed to detect driver: %w", err)
	}

	// Try universal driver first
	drivers := GetUniversalDriver(driverName)
	if drivers != nil {
		return drivers, nil
	}

	// If no universal driver, return error
	return nil, fmt.Errorf("%w: %s (no universal driver registered)", dialect.ErrNoDriver, driverName)
}

// MustGetDialectorFromConfig panics if unable to get a dialector from Config.
// Useful for initialization in package init() functions.
func MustGetDialectorFromConfig(cfg Config) Dialector {
	d, err := GetDialectorFromConfig(cfg)
	if err != nil {
		panic(err)
	}
	return d
}
