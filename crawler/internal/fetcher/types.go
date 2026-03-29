package fetcher

import (
	"time"
)

// FetchJob represents a URL to be fetched
type FetchJob struct {
	URL     string	 `json:"url"`
	Attempt int    `json:"attempt"`
	JobID   string	 `json:"job_id"`
}

type retryPayload struct {
	URL           string    `json:"url"`
	Attempt       int       `json:"attempt"`
	JobID         string    `json:"job_id"`
	NextAttemptAt time.Time `json:"next_attempt_at"`
	Reason		  string    `json:"reason,omitempty"`
}

type successPayload struct {
	JobID             string    `json:"job_id"`
	URL               string    `json:"url"`
	S3Key             string    `json:"s3_key"`
	StatusCode        int       `json:"status_code"`
	ContentType       string    `json:"content_type"`
	FetchTimestampUTC time.Time `json:"fetch_timestamp_utc"`
	ContentHash       string    `json:"content_hash"`
}

type dlqPayload struct {
	JobID   string `json:"job_id"`
	URL     string `json:"url"`
	Attempt int    `json:"attempt"`
	Reason  string `json:"reason"`
}

