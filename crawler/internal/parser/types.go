package parser

import (
	"time"
)

// ParseJob represents a job received from Kafka to parse fetched content
type ParseJob struct {
	JobID             string    `json:"job_id"`
	URL               string    `json:"url"`
	S3Key             string    `json:"s3_key"`
	StatusCode        int       `json:"status_code"`
	ContentType       string    `json:"content_type"`
	FetchTimestampUTC time.Time `json:"fetch_timestamp_utc"`
	ContentHash       string    `json:"content_hash"`
}

// FetchedContent represents the raw content stored in S3 by the fetcher
type FetchedContent struct {
	URL               string              `json:"url"`
	JobID             string              `json:"job_id"`
	StatusCode        int                 `json:"status_code"`
	Headers           map[string][]string `json:"headers"`
	BodyB64           string              `json:"body_b64"`
	ContentType       string              `json:"content_type"`
	FetchTimestampUTC string              `json:"fetch_timestamp_utc"`
	ContentHashSHA256 string              `json:"content_hash_sha256"`
	Attempt           int                 `json:"attempt"`
}

// ParseResult represents the parsed output stored in S3
type ParseResult struct {
	// Source info
	JobID     string `json:"job_id"`
	URL       string `json:"url"`
	SourceKey string `json:"source_s3_key"`

	// Extracted content
	Title       string            `json:"title,omitempty"`
	Description string            `json:"description,omitempty"`
	Text        string            `json:"text"`
	TextHash    string            `json:"text_hash"` // SHA256 of extracted text
	TextLength  int               `json:"text_length"`
	Language    string            `json:"language,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"`

	// Extracted links
	Links         []ExtractedLink `json:"links"`
	LinksCount    int             `json:"links_count"`
	NewLinksCount int             `json:"new_links_count"` // Links not in bloom filter
	SkippedCount  int             `json:"skipped_count"`   // Links skipped (already seen)

	// Processing info
	ParseTimestampUTC time.Time `json:"parse_timestamp_utc"`
	ProcessingTimeMs  int64     `json:"processing_time_ms"`
}

// ExtractedLink represents a link found in the page
type ExtractedLink struct {
	URL      string `json:"url"`
	Text     string `json:"text,omitempty"` // Anchor text
	Rel      string `json:"rel,omitempty"`  // rel attribute
	IsNew    bool   `json:"is_new"`         // Not in bloom filter
	LinkType string `json:"type,omitempty"` // internal, external, resource
}

// successPayload is emitted after successful parsing
type successPayload struct {
	JobID             string    `json:"job_id"`
	URL               string    `json:"url"`
	SourceS3Key       string    `json:"source_s3_key"`
	ParsedS3Key       string    `json:"parsed_s3_key"`
	LinksExtracted    int       `json:"links_extracted"`
	NewLinksQueued    int       `json:"new_links_queued"`
	TextLength        int       `json:"text_length"`
	ParseTimestampUTC time.Time `json:"parse_timestamp_utc"`
}

// dlqPayload is emitted when parsing fails permanently
type dlqPayload struct {
	JobID  string `json:"job_id"`
	URL    string `json:"url"`
	S3Key  string `json:"s3_key"`
	Reason string `json:"reason"`
}

// FrontierJob represents a URL to be queued for fetching
type FrontierJob struct {
	URL       string `json:"url"`
	JobID     string `json:"job_id"`
	Attempt   int    `json:"attempt"`
	ParentURL string `json:"parent_url,omitempty"`
	Depth     int    `json:"depth,omitempty"`
}
