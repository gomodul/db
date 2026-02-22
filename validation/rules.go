package validation

import (
	"fmt"
	"net"
	"net/url"
	"reflect"
	"regexp"
	"strconv"
	"unicode"
)

// Required checks if a value is not zero or empty
func Required(value interface{}) error {
	if value == nil {
		return fmt.Errorf("is required")
	}

	rv := reflect.ValueOf(value)
	switch rv.Kind() {
	case reflect.String:
		if rv.String() == "" {
			return fmt.Errorf("is required")
		}
	case reflect.Slice, reflect.Array, reflect.Map:
		if rv.Len() == 0 {
			return fmt.Errorf("is required")
		}
	case reflect.Ptr, reflect.Interface:
		if rv.IsNil() {
			return fmt.Errorf("is required")
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if rv.Int() == 0 {
			return fmt.Errorf("is required")
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		if rv.Uint() == 0 {
			return fmt.Errorf("is required")
		}
	case reflect.Float32, reflect.Float64:
		if rv.Float() == 0 {
			return fmt.Errorf("is required")
		}
	case reflect.Bool:
		if !rv.Bool() {
			return fmt.Errorf("is required")
		}
	}

	return nil
}

// Email validates an email address
func Email(value interface{}) error {
	str, ok := value.(string)
	if !ok {
		return fmt.Errorf("must be a string")
	}

	if str == "" {
		return nil // Empty is ok for optional fields
	}

	emailRegex := regexp.MustCompile(`^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$`)
	if !emailRegex.MatchString(str) {
		return fmt.Errorf("must be a valid email address")
	}

	return nil
}

// URL validates a URL
func URL(value interface{}) error {
	str, ok := value.(string)
	if !ok {
		return fmt.Errorf("must be a string")
	}

	if str == "" {
		return nil // Empty is ok for optional fields
	}

	_, err := url.Parse(str)
	if err != nil {
		return fmt.Errorf("must be a valid URL")
	}

	return nil
}

// Alpha validates that a string contains only letters
func Alpha(value interface{}) error {
	str, ok := value.(string)
	if !ok {
		return fmt.Errorf("must be a string")
	}

	if str == "" {
		return nil
	}

	for _, r := range str {
		if !unicode.IsLetter(r) {
			return fmt.Errorf("must contain only letters")
		}
	}

	return nil
}

// Alphanumeric validates that a string contains only letters and numbers
func Alphanumeric(value interface{}) error {
	str, ok := value.(string)
	if !ok {
		return fmt.Errorf("must be a string")
	}

	if str == "" {
		return nil
	}

	for _, r := range str {
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) {
			return fmt.Errorf("must contain only letters and numbers")
		}
	}

	return nil
}

// Numeric validates that a value is numeric
func Numeric(value interface{}) error {
	switch v := value.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		return nil
	case string:
		if v == "" {
			return nil
		}
		_, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return fmt.Errorf("must be numeric")
		}
		return nil
	default:
		rv := reflect.ValueOf(value)
		switch rv.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
			reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
			reflect.Float32, reflect.Float64:
			return nil
		default:
			return fmt.Errorf("must be numeric")
		}
	}
}

// Min checks if a numeric value is greater than or equal to the minimum
func Min(value interface{}) error {
	// This is a placeholder - use MinWithParam instead
	return fmt.Errorf("min requires a parameter")
}

// MinWithParam checks if a value meets the minimum requirement
func MinWithParam(value interface{}, param string) error {
	min, err := strconv.ParseFloat(param, 64)
	if err != nil {
		return fmt.Errorf("invalid min parameter")
	}

	switch v := value.(type) {
	case int:
		if float64(v) < min {
			return fmt.Errorf("must be at least %s", param)
		}
	case int8:
		if float64(v) < min {
			return fmt.Errorf("must be at least %s", param)
		}
	case int16:
		if float64(v) < min {
			return fmt.Errorf("must be at least %s", param)
		}
	case int32:
		if float64(v) < min {
			return fmt.Errorf("must be at least %s", param)
		}
	case int64:
		if float64(v) < min {
			return fmt.Errorf("must be at least %s", param)
		}
	case uint:
		if float64(v) < min {
			return fmt.Errorf("must be at least %s", param)
		}
	case uint8:
		if float64(v) < min {
			return fmt.Errorf("must be at least %s", param)
		}
	case uint16:
		if float64(v) < min {
			return fmt.Errorf("must be at least %s", param)
		}
	case uint32:
		if float64(v) < min {
			return fmt.Errorf("must be at least %s", param)
		}
	case uint64:
		if float64(v) < min {
			return fmt.Errorf("must be at least %s", param)
		}
	case float32:
		if float64(v) < min {
			return fmt.Errorf("must be at least %s", param)
		}
	case float64:
		if v < min {
			return fmt.Errorf("must be at least %s", param)
		}
	case string:
		if v == "" {
			return nil
		}
		if float64(len(v)) < min {
			return fmt.Errorf("must be at least %s characters", param)
		}
	default:
		rv := reflect.ValueOf(value)
		switch rv.Kind() {
		case reflect.Slice, reflect.Array:
			if float64(rv.Len()) < min {
				return fmt.Errorf("must have at least %s items", param)
			}
		default:
			return fmt.Errorf("min validation not supported for this type")
		}
	}

	return nil
}

// Max checks if a numeric value is less than or equal to the maximum
func Max(value interface{}) error {
	// This is a placeholder - use MaxWithParam instead
	return fmt.Errorf("max requires a parameter")
}

// MaxWithParam checks if a value meets the maximum requirement
func MaxWithParam(value interface{}, param string) error {
	max, err := strconv.ParseFloat(param, 64)
	if err != nil {
		return fmt.Errorf("invalid max parameter")
	}

	switch v := value.(type) {
	case int:
		if float64(v) > max {
			return fmt.Errorf("must be at most %s", param)
		}
	case int8:
		if float64(v) > max {
			return fmt.Errorf("must be at most %s", param)
		}
	case int16:
		if float64(v) > max {
			return fmt.Errorf("must be at most %s", param)
		}
	case int32:
		if float64(v) > max {
			return fmt.Errorf("must be at most %s", param)
		}
	case int64:
		if float64(v) > max {
			return fmt.Errorf("must be at most %s", param)
		}
	case uint:
		if float64(v) > max {
			return fmt.Errorf("must be at most %s", param)
		}
	case uint8:
		if float64(v) > max {
			return fmt.Errorf("must be at most %s", param)
		}
	case uint16:
		if float64(v) > max {
			return fmt.Errorf("must be at most %s", param)
		}
	case uint32:
		if float64(v) > max {
			return fmt.Errorf("must be at most %s", param)
		}
	case uint64:
		if float64(v) > max {
			return fmt.Errorf("must be at most %s", param)
		}
	case float32:
		if float64(v) > max {
			return fmt.Errorf("must be at most %s", param)
		}
	case float64:
		if v > max {
			return fmt.Errorf("must be at most %s", param)
		}
	case string:
		if v == "" {
			return nil
		}
		if float64(len(v)) > max {
			return fmt.Errorf("must be at most %s characters", param)
		}
	default:
		rv := reflect.ValueOf(value)
		switch rv.Kind() {
		case reflect.Slice, reflect.Array:
			if float64(rv.Len()) > max {
				return fmt.Errorf("must have at most %s items", param)
			}
		default:
			return fmt.Errorf("max validation not supported for this type")
		}
	}

	return nil
}

// Length checks if a string/slice has a specific length
func Length(value interface{}) error {
	// This is a placeholder - use LengthWithParam instead
	return fmt.Errorf("length requires a parameter")
}

// LengthWithParam checks if a value has a specific length
func LengthWithParam(value interface{}, param string) error {
	length, err := strconv.Atoi(param)
	if err != nil {
		return fmt.Errorf("invalid length parameter")
	}

	switch v := value.(type) {
	case string:
		if len(v) != length {
			return fmt.Errorf("must be exactly %s characters", param)
		}
	default:
		rv := reflect.ValueOf(value)
		switch rv.Kind() {
		case reflect.Slice, reflect.Array:
			if rv.Len() != length {
				return fmt.Errorf("must have exactly %s items", param)
			}
		default:
			return fmt.Errorf("length validation not supported for this type")
		}
	}

	return nil
}

// IsEmail checks if a string is a valid email address
func IsEmail(str string) bool {
	if str == "" {
		return false
	}
	emailRegex := regexp.MustCompile(`^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$`)
	return emailRegex.MatchString(str)
}

// IsURL checks if a string is a valid URL
func IsURL(str string) bool {
	if str == "" {
		return false
	}
	u, err := url.Parse(str)
	return err == nil && u.Scheme != "" && u.Host != ""
}

// IsIPv4 checks if a string is a valid IPv4 address
func IsIPv4(str string) bool {
	ip := net.ParseIP(str)
	return ip != nil && ip.To4() != nil
}

// IsIPv6 checks if a string is a valid IPv6 address
func IsIPv6(str string) bool {
	ip := net.ParseIP(str)
	return ip != nil && ip.To4() == nil
}

// IsMAC checks if a string is a valid MAC address
func IsMAC(str string) bool {
	_, err := net.ParseMAC(str)
	return err == nil
}

// IsAlpha checks if a string contains only letters
func IsAlpha(str string) bool {
	if str == "" {
		return false
	}
	for _, r := range str {
		if !unicode.IsLetter(r) {
			return false
		}
	}
	return true
}

// IsAlphanumeric checks if a string contains only letters and numbers
func IsAlphanumeric(str string) bool {
	if str == "" {
		return false
	}
	for _, r := range str {
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) {
			return false
		}
	}
	return true
}

// IsNumeric checks if a string is numeric
func IsNumeric(str string) bool {
	if str == "" {
		return false
	}
	_, err := strconv.ParseFloat(str, 64)
	return err == nil
}

// IsEmpty checks if a value is empty
func IsEmpty(value interface{}) bool {
	if value == nil {
		return true
	}

	rv := reflect.ValueOf(value)
	switch rv.Kind() {
	case reflect.String:
		return rv.String() == ""
	case reflect.Slice, reflect.Array, reflect.Map:
		return rv.Len() == 0
	case reflect.Ptr, reflect.Interface:
		return rv.IsNil()
	}

	return reflect.DeepEqual(rv.Interface(), reflect.Zero(rv.Type()).Interface())
}
