package security

import (
	"fmt"
	"regexp"
	"strings"
)

// Security configuration
var (
	// allowedKeywordsInRaw contains SQL keywords that are allowed in raw queries
	allowedKeywordsInRaw = map[string]bool{
		"SELECT": true, "INSERT": true, "UPDATE": true, "DELETE": true,
		"FROM": true, "WHERE": true, "AND": true, "OR": true, "NOT": true,
		"IN": true, "NOT IN": true, "LIKE": true, "NOT LIKE": true,
		"BETWEEN": true, "IS": true, "IS NULL": true, "IS NOT NULL": true,
		"ORDER": true, "BY": true, "ASC": true, "DESC": true,
		"LIMIT": true, "OFFSET": true, "GROUP": true, "HAVING": true,
		"JOIN": true, "INNER": true, "LEFT": true, "RIGHT": true, "OUTER": true,
		"ON": true, "AS": true, "DISTINCT": true, "EXISTS": true,
		"UNION": true, "UNION ALL": true, "WITH": true,
		"CASE": true, "WHEN": true, "THEN": true, "ELSE": true, "END": true,
		"COUNT": true, "SUM": true, "AVG": true, "MIN": true, "MAX": true,
		"COALESCE": true, "NULLIF": true, "CAST": true,
	}

	// dangerousPatterns contains patterns that could indicate SQL injection
	dangerousPatterns = []*regexp.Regexp{
		regexp.MustCompile(`(?i)--\s*$`), // SQL comment at end
		regexp.MustCompile(`(?i);\s*(DROP|DELETE|EXEC|EXECUTE)`), // Multiple statements with DANGEROUS commands
		regexp.MustCompile(`(?i)\b(DROP\s+(TABLE|DATABASE)|TRUNCATE|ALTER\s+(TABLE|DATABASE))\b`), // DDL in queries
		regexp.MustCompile(`(?i)(\b xp_|sp_|exec\s*\()`), // SQL Server stored procedures
		regexp.MustCompile(`(?i)\b(LOAD_FILE|INTO\s+OUTFILE)\b`), // MySQL file operations
		regexp.MustCompile(`(?i)\b(pg_sleep|WAITFOR\s+DELAY)\b`), // Sleep attacks
	}
)

// SecurityWarning represents a security warning for a query
type SecurityWarning struct {
	Severity string // "low", "medium", "high"
	Message  string
}

// RawQueryConfig holds configuration for raw query validation
type RawQueryConfig struct {
	AllowMultipleStatements bool
	AllowDDL                bool
	AllowComments           bool
	MaxQueryLength          int
	StrictMode              bool
}

// DefaultRawQueryConfig returns the default security config for raw queries
func DefaultRawQueryConfig() *RawQueryConfig {
	return &RawQueryConfig{
		AllowMultipleStatements: false,
		AllowDDL:                false,
		AllowComments:           false,
		MaxQueryLength:          10000,
		StrictMode:              false,
	}
}

// ValidateRawQuery validates a raw SQL query for potential security issues
// Returns warnings if any are found, or an error if the query is dangerous
func ValidateRawQuery(sql string, cfg *RawQueryConfig) ([]*SecurityWarning, error) {
	if cfg == nil {
		cfg = DefaultRawQueryConfig()
	}

	var warnings []*SecurityWarning

	// Check query length
	if cfg.MaxQueryLength > 0 && len(sql) > cfg.MaxQueryLength {
		return nil, fmt.Errorf("query too long: %d chars (max: %d)", len(sql), cfg.MaxQueryLength)
	}

	// Check for dangerous patterns
	for _, pattern := range dangerousPatterns {
		if pattern.MatchString(sql) {
			return nil, fmt.Errorf("potentially dangerous SQL pattern detected: %s", pattern.String())
		}
	}

	// Check for multiple statements
	if !cfg.AllowMultipleStatements && strings.Contains(sql, ";") {
		// Allow semicolon at the end
		trimmed := strings.TrimSpace(sql)
		if strings.Count(trimmed, ";") > 1 || (strings.HasSuffix(trimmed, ";") && strings.Count(trimmed, ";") > 1) {
			warnings = append(warnings, &SecurityWarning{
				Severity: "high",
				Message:  "Multiple SQL statements detected. Consider using separate queries.",
			})
		}
	}

	// Check for DDL statements
	if !cfg.AllowDDL {
		ddlPattern := regexp.MustCompile(`(?i)^\s*(DROP|CREATE|ALTER|TRUNCATE)\s+(TABLE|DATABASE|INDEX|SCHEMA)`)
		if ddlPattern.MatchString(sql) {
			return nil, fmt.Errorf("DDL statements are not allowed in raw queries")
		}
	}

	// Check for comments
	if !cfg.AllowComments {
		if strings.Contains(sql, "--") || strings.Contains(sql, "/*") {
			warnings = append(warnings, &SecurityWarning{
				Severity: "medium",
				Message:  "SQL comments detected. Ensure user input doesn't contain comments.",
			})
		}
	}

	// In strict mode, fail on warnings
	if cfg.StrictMode && len(warnings) > 0 {
		return nil, fmt.Errorf("strict mode: query has security warnings: %s", warnings[0].Message)
	}

	return warnings, nil
}

// SanitizeFieldName sanitizes a field name to prevent SQL injection
// Use this when field names come from user input
func SanitizeFieldName(name string) string {
	// Remove any characters that aren't alphanumeric, underscore, or dot
	reg := regexp.MustCompile(`[^a-zA-Z0-9_.]`)
	cleaned := reg.ReplaceAllString(name, "")

	// Ensure it doesn't start with a number
	if len(cleaned) > 0 && cleaned[0] >= '0' && cleaned[0] <= '9' {
		cleaned = "_" + cleaned
	}

	return cleaned
}

// ValidateFieldName validates that a field name is safe
func ValidateFieldName(name string) error {
	if name == "" {
		return fmt.Errorf("field name cannot be empty")
	}

	// Check for dangerous patterns
	dangerous := []string{"--", ";", "/*", "*/", "xp_", "sp_", "DROP", "DELETE", "TRUNCATE", "EXEC"}
	upperName := strings.ToUpper(name)
	for _, d := range dangerous {
		if strings.Contains(upperName, d) {
			return fmt.Errorf("field name contains dangerous pattern: %s", d)
		}
	}

	// Only allow alphanumeric, underscore, and dot
	reg := regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_.]*$`)
	if !reg.MatchString(name) {
		return fmt.Errorf("invalid field name format: %s", name)
	}

	return nil
}

// IsParameterizedQuery checks if a query uses parameterization
func IsParameterizedQuery(sql string) bool {
	// Check for placeholder patterns
	placeholderPatterns := []string{
		"?",   // Standard placeholder
		"$1",  // PostgreSQL style
		"$2",  "$3", "$4", "$5", "$6", "$7", "$8", "$9",
		":1",  // Oracle/PostgreSQL style
		":2",  ":3", ":4", ":5", ":6", ":7", ":8", ":9",
		":param", ":", // Named parameters
		"%s",  // sprintf style (discouraged but still parameterized)
	}

	for _, pattern := range placeholderPatterns {
		if strings.Contains(sql, pattern) {
			return true
		}
	}

	return false
}

// GetSecurityHelp returns help text for secure raw query usage
func GetSecurityHelp() string {
	return `
SECURE RAW QUERY USAGE:

1. ALWAYS use parameterized queries:
   ❌ BAD:  db.Raw("SELECT * FROM users WHERE name = '" + userInput + "'")
   ✅ GOOD: db.Raw("SELECT * FROM users WHERE name = ?", userInput)

2. Never concatenate user input into SQL:
   ❌ BAD:  db.Raw("SELECT * FROM " + tableName + " WHERE id = ?", id)
   ✅ GOOD: Use table name validation or allowlist

3. Validate field names from user input:
   ❌ BAD:  db.Where(userInput + " = ?", value)
   ✅ GOOD: if err := ValidateFieldName(userInput); err == nil { db.Where(userInput + " = ?", value) }

4. Use helper methods instead of raw SQL when possible:
   ❌ BAD:  db.Raw("SELECT * FROM users WHERE id IN (1,2,3)")
   ✅ GOOD: db.Where("id IN ?", []int{1,2,3})

5. Limit raw query usage to:
   - Complex joins not supported by builder
   - Database-specific features
   - Performance-critical sections

For advanced security, use ValidateRawQuery() before executing raw queries.
`
}

// AddSecurityWarningToError adds security context to database errors
func AddSecurityWarningToError(err error, sql string) error {
	if err == nil {
		return nil
	}

	// Check if it might be an SQL injection error
	errorPatterns := []string{
		"unclosed quotation mark",
		"incorrect syntax",
		"unterminated string",
		"SQL injection",
		"quoted string not properly terminated",
	}

	errorMsg := strings.ToLower(err.Error())
	for _, pattern := range errorPatterns {
		if strings.Contains(errorMsg, pattern) {
			return fmt.Errorf("%w\n\nSECURITY WARNING: This error might indicate an SQL injection attempt.\nQuery: %s\n\n%s",
				err, sql, "Always use parameterized queries with placeholders like '?', '$1', etc.")
		}
	}

	return err
}
