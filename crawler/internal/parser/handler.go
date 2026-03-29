package parser

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
)

// handleRecord processes a single Kafka record
func (p *Parser) handleRecord(ctx context.Context, rec *kgo.Record) (bool, error) {
	start := time.Now()

	// Parse the incoming job (success event from fetcher)
	var job ParseJob
	if err := json.Unmarshal(rec.Value, &job); err != nil {
		p.logger.Printf("failed to unmarshal job: %v", err)
		return true, nil // Skip malformed messages
	}

	if job.S3Key == "" || job.URL == "" {
		p.logger.Printf("invalid job: missing s3_key or url")
		return true, nil
	}

	// Skip non-HTML content
	if !isHTMLContentType(job.ContentType) {
		p.logger.Printf("skipping non-html content: %s type=%s", job.URL, job.ContentType)
		return true, nil
	}

	// Step 1: Fetch raw content from S3
	rawData, err := p.s3.Get(ctx, job.S3Key)
	if err != nil {
		p.logger.Printf("s3 get failed: %v", err)
		return p.dlq(ctx, job, fmt.Sprintf("s3 get: %v", err))
	}

	// Step 2: Parse the stored content
	var content FetchedContent
	if err := json.Unmarshal(rawData, &content); err != nil {
		return p.dlq(ctx, job, fmt.Sprintf("unmarshal content: %v", err))
	}

	// Step 3: Decode the base64 body
	bodyBytes, err := base64.StdEncoding.DecodeString(content.BodyB64)
	if err != nil {
		return p.dlq(ctx, job, fmt.Sprintf("decode body: %v", err))
	}

	// Step 3.5: Decompress if gzip/deflate encoded
	bodyBytes, err = decompressBody(bodyBytes, content.Headers)
	if err != nil {
		p.logger.Printf("decompress warning: %v (using raw body)", err)
		// Try to continue with raw body - might still work
		bodyBytes, _ = base64.StdEncoding.DecodeString(content.BodyB64)
	}

	// Step 4: Parse base URL
	baseURL, err := url.Parse(job.URL)
	if err != nil {
		return p.dlq(ctx, job, fmt.Sprintf("parse url: %v", err))
	}

	// Step 5: Extract text and links
	extracted, err := p.extractor.Extract(bodyBytes, baseURL)
	if err != nil {
		return p.dlq(ctx, job, fmt.Sprintf("extract: %v", err))
	}

	// Step 6: Process and deduplicate links
	links, newLinks, skipped := p.processLinks(ctx, extracted.Links, baseURL)

	// Step 7: Queue new URLs to frontier
	queued := 0
	for _, link := range newLinks {
		frontierJob := FrontierJob{
			URL:       link.URL,
			JobID:     uuid.New().String(),
			Attempt:   1,
			ParentURL: job.URL,
		}
		if err := p.emit(ctx, p.cfg.FrontierReadyTopic, frontierJob); err != nil {
			p.logger.Printf("failed to queue url=%s: %v", link.URL, err)
		} else {
			queued++
		}
	}

	// Step 8: Create parse result
	textHash := sha256.Sum256([]byte(extracted.Text))
	result := ParseResult{
		JobID:             job.JobID,
		URL:               job.URL,
		SourceKey:         job.S3Key,
		Title:             extracted.Title,
		Description:       extracted.Description,
		Text:              extracted.Text,
		TextHash:          hex.EncodeToString(textHash[:]),
		TextLength:        len(extracted.Text),
		Language:          extracted.Language,
		Metadata:          extracted.Metadata,
		Links:             links,
		LinksCount:        len(links),
		NewLinksCount:     len(newLinks),
		SkippedCount:      skipped,
		ParseTimestampUTC: time.Now().UTC(),
		ProcessingTimeMs:  time.Since(start).Milliseconds(),
	}

	// Step 9: Store parsed result to S3
	urlHash := sha256.Sum256([]byte(job.URL))
	parsedS3Key := path.Join("parsed", job.JobID, hex.EncodeToString(urlHash[:]))

	resultBytes, err := json.Marshal(result)
	if err != nil {
		return p.dlq(ctx, job, fmt.Sprintf("marshal result: %v", err))
	}

	if err := p.s3.PutToBucket(ctx, p.cfg.S3ParsedBucket, parsedS3Key, "application/json", resultBytes); err != nil {
		p.logger.Printf("s3 put failed: %v (will retry)", err)
		return false, fmt.Errorf("s3 put: %w", err)
	}

	// Step 10: Emit success event
	success := successPayload{
		JobID:             job.JobID,
		URL:               job.URL,
		SourceS3Key:       job.S3Key,
		ParsedS3Key:       parsedS3Key,
		LinksExtracted:    len(links),
		NewLinksQueued:    queued,
		TextLength:        len(extracted.Text),
		ParseTimestampUTC: time.Now().UTC(),
	}
	if err := p.emit(ctx, p.cfg.ParseSuccessTopic, success); err != nil {
		return false, fmt.Errorf("emit success: %w", err)
	}

	p.logger.Printf("parsed url=%s text=%d links=%d new=%d time=%dms",
		job.URL, len(extracted.Text), len(links), queued, time.Since(start).Milliseconds())

	return true, nil
}

// processLinks normalizes, deduplicates, and classifies links
func (p *Parser) processLinks(ctx context.Context, rawLinks []RawLink, baseURL *url.URL) ([]ExtractedLink, []ExtractedLink, int) {
	seen := make(map[string]bool)
	var allLinks []ExtractedLink
	var newLinks []ExtractedLink
	skipped := 0

	for _, raw := range rawLinks {
		// Normalize the URL
		normalized, err := NormalizeURL(raw.Href, baseURL)
		if err != nil || normalized == "" {
			continue
		}

		// Skip if already processed in this batch
		if seen[normalized] {
			continue
		}
		seen[normalized] = true

		// Skip non-crawlable URLs
		if !IsValidCrawlURL(normalized) {
			continue
		}

		// Classify the link
		linkType := ClassifyLink(normalized, baseURL)

		// Check bloom filter for deduplication
		isNew, err := p.bloom.AddIfNotExists(ctx, normalized)
		if err != nil {
			p.logger.Printf("bloom filter error: %v", err)
			// On error, assume it's new to avoid missing URLs
			isNew = true
		}

		link := ExtractedLink{
			URL:      normalized,
			Text:     truncateString(raw.Text, 200),
			Rel:      raw.Rel,
			IsNew:    isNew,
			LinkType: linkType,
		}

		allLinks = append(allLinks, link)

		if isNew {
			// Only queue internal links for crawling
			if linkType == "internal" {
				newLinks = append(newLinks, link)
			}
		} else {
			skipped++
		}
	}

	return allLinks, newLinks, skipped
}

// dlq sends a job to the dead letter queue
func (p *Parser) dlq(ctx context.Context, job ParseJob, reason string) (bool, error) {
	payload := dlqPayload{
		JobID:  job.JobID,
		URL:    job.URL,
		S3Key:  job.S3Key,
		Reason: reason,
	}

	p.logger.Printf("dlq url=%s reason=%s", job.URL, reason)

	if err := p.emit(ctx, p.cfg.ParseDLQTopic, payload); err != nil {
		return false, err
	}
	return true, nil
}

// emit publishes a message to a Kafka topic
func (p *Parser) emit(ctx context.Context, topic string, payload interface{}) error {
	b, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	rec := &kgo.Record{
		Topic: topic,
		Value: b,
	}
	return p.kafkaClient.ProduceSync(ctx, rec).FirstErr()
}

// isHTMLContentType checks if content type is HTML
func isHTMLContentType(contentType string) bool {
	ct := strings.ToLower(contentType)
	return strings.Contains(ct, "text/html") ||
		strings.Contains(ct, "application/xhtml")
}

// truncateString truncates a string to maxLen
func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen]
}

// decompressBody decompresses the body if Content-Encoding header indicates compression
func decompressBody(body []byte, headers map[string][]string) ([]byte, error) {
	// Find Content-Encoding header (case-insensitive)
	var encoding string
	for key, values := range headers {
		if strings.EqualFold(key, "Content-Encoding") && len(values) > 0 {
			encoding = strings.ToLower(strings.TrimSpace(values[0]))
			break
		}
	}

	if encoding == "" {
		return body, nil // No compression
	}

	switch encoding {
	case "gzip":
		reader, err := gzip.NewReader(bytes.NewReader(body))
		if err != nil {
			return nil, fmt.Errorf("gzip reader: %w", err)
		}
		defer reader.Close()
		return io.ReadAll(reader)

	case "deflate":
		reader := flate.NewReader(bytes.NewReader(body))
		defer reader.Close()
		return io.ReadAll(reader)

	case "identity", "none":
		return body, nil

	default:
		// Unknown encoding, return as-is
		return body, nil
	}
}
