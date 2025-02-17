package test

import "time"

// ConvertToTimestamp receives a datetime string in "DD-MM-YYYY HH:mm" format,
// treats it as UTC, and returns the timestamp in milliseconds
func ConvertToTimestamp(datetimeStr string) (int64, error) {
	layout := "02-01-2006 15:04" // Go's reference date format
	t, err := time.Parse(layout, datetimeStr)
	if err != nil {
		return 0, err
	}

	// Convert to milliseconds
	return t.UnixMilli(), nil
}
